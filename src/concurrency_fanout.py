"""
Tests the specific mechanism discussed in the design chat: Quickwit (and any
BYOC deployment of it) compensates for S3's inherently higher per-request
latency by firing many small range-GETs *concurrently* rather than
serially, so that a query's wall-clock time tracks roughly one round-trip's
latency instead of the sum of all of them (Little's Law: throughput ~=
concurrency / latency). That strategy only works if the storage backend can
actually sustain a lot of concurrent in-flight requests without silently
serializing them, throttling disproportionately, or degrading per-request
latency as concurrency rises.

This module answers exactly that, independent of the full duration-based
Layer 3 soak test: for a single split-sized object, sweep the number of
concurrent range-GETs fired at once (1, 8, 16, 32, ...) and measure whether
wall-clock time for the whole batch stays close to a single request's
latency (good -- the backend parallelizes properly) or climbs toward
concurrency x single-request-latency (bad -- the backend is effectively
serializing "concurrent" requests, which breaks the entire premise this
architecture depends on regardless of what its raw single-request latency
looks like).

CONCURRENCY MODEL: the sweep is asyncio-based (aioboto3/aiobotocore), not
thread-based. An earlier thread-per-request version (ThreadPoolExecutor)
produced false "degrades at concurrency = 32" verdicts on low-latency links
-- confirmed by running it against real AWS S3 from a low-latency EC2
instance and seeing the *same* collapse there. On a low-latency link, the
per-request Python work each thread does under the GIL (SigV4 signing,
response parsing) is no longer hidden behind network wait time, so threads
queue for the GIL instead of running concurrently, and the tool measures its
own ceiling instead of the backend's. asyncio runs everything on one thread,
so there is no GIL contention between in-flight requests; re-running the
same comparison confirmed both AWS S3 and the vendor endpoint under test
stayed above the efficiency floor through the full sweep once the harness
stopped being the bottleneck.

NOISE AND REPEATS: a single sample per concurrency level is not reliable
close to the pass/fail line. Real AWS S3, tested repeatedly from the same
EC2 instance, produced efficiency values hovering around 0.4-0.5 in the
32-64 concurrency range and flipped between "degrades at 32" and "degrades
at 64" across back-to-back runs with no code change -- ordinary network
jitter, not a real difference in AWS's behavior. Each level therefore runs
`repeats` times (default 3, see DEFAULT_LEVEL_REPEATS), and the run whose
wall-clock time is the median of the repeats is kept, so an isolated slow
or fast trial cannot flip the verdict on its own.
"""
from __future__ import annotations

import asyncio
import math
import os
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aioboto3
from botocore.exceptions import BotoCoreError, ClientError

from .qw_s3_client import QwS3Client, QwS3Config, boto_config, connection_failure
from .report import _percentile

DEFAULT_CONCURRENCY_LEVELS = [1, 8, 16, 32, 64, 128, 256, 512, 1024]
RANGE_SIZE_BYTES = 8 * 1024  # matches query_sim's term/field lookup size
DEFAULT_SPLIT_SIZE_MB = 8.0  # small mature split, per docs/03
DEFAULT_LEVEL_REPEATS = 3  # see module docstring, "NOISE AND REPEATS"


@dataclass
class FanoutLevelResult:
    concurrency: int
    wall_clock_s: float
    per_request_latencies_s: list
    error_count: int
    throttle_count: int

    @property
    def p50_per_request_s(self) -> float:
        return statistics.median(self.per_request_latencies_s) if self.per_request_latencies_s else float("nan")

    @property
    def p99_per_request_s(self) -> float:
        if not self.per_request_latencies_s:
            return float("nan")
        return _percentile(self.per_request_latencies_s, 99)

    @property
    def speedup(self) -> float:
        """How many requests' worth of latency the batch absorbed at once.

        About 1 means the backend served the batch one request at a time,
        however many the client offered. About `concurrency` means it served
        them all at once. This is the measurement that answers the question
        the sweep exists to ask.

        It replaces an earlier `efficiency` ratio of median latency to batch
        wall clock, which could not answer that question. Wall clock is
        bounded below by the *slowest* request in the batch, while the
        numerator was the *median*, so the ratio fell as concurrency rose for
        any backend, including AWS S3. Measured from one laptop, AWS S3
        scored 0.09 at concurrency 256 with zero errors, and a 0.4 floor
        therefore failed the reference implementation.
        """
        if self.wall_clock_s <= 0 or not self.per_request_latencies_s:
            return float("nan")
        return self.concurrency * self.p50_per_request_s / self.wall_clock_s

    @property
    def requests_per_s(self) -> float:
        """Batch throughput. Its peak across the sweep is where something
        saturates, which may be the runner rather than the backend."""
        if self.wall_clock_s <= 0:
            return float("nan")
        return self.concurrency / self.wall_clock_s

    @property
    def efficiency(self) -> float:
        """Median request latency over batch wall clock, kept as a diagnostic.

        It shows how far latency spreads within a batch. It is not a
        serialization test, and nothing gates on it. See `speedup`.
        """
        if self.wall_clock_s <= 0 or not self.per_request_latencies_s:
            return float("nan")
        return self.p50_per_request_s / self.wall_clock_s


def prepare_fanout_object(client: QwS3Client, bucket: str, key: str,
                           size_mb: float = DEFAULT_SPLIT_SIZE_MB) -> int:
    """Uploads a synthetic split-sized object to fan concurrent reads out against."""
    size = int(size_mb * 1024 * 1024)
    chunk = os.urandom(min(size, 4 * 1024 * 1024))
    payload = (chunk * (size // len(chunk) + 1))[:size]
    res = client.put_split(bucket, key, payload)
    if not res["ok"]:
        raise RuntimeError(f"could not upload fanout test object: {res.get('error')}")
    return size


def _async_client_kwargs(cfg: QwS3Config, top_concurrency: int) -> dict:
    """
    Sizes the connection pool comfortably past the top of the sweep, so
    aiohttp's own connector limit is never what caps observed concurrency
    -- the async equivalent of the old build_fanout_client()'s job.
    """
    boto_cfg = boto_config(cfg, max(top_concurrency * 2, 20))
    return dict(
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        aws_session_token=cfg.session_token,
        region_name=cfg.region,
        config=boto_cfg,
        verify=cfg.verify_tls,
    )


async def _get_range_async(s3_client, bucket: str, key: str, start: int, end: int) -> dict:
    t0 = time.perf_counter()
    range_hdr = f"bytes={start}-{end}"
    try:
        resp = await s3_client.get_object(Bucket=bucket, Key=key, Range=range_hdr)
        body = await resp["Body"].read()
        return {"ok": True, "op": "get_object_range", "latency_s": time.perf_counter() - t0,
                "bytes": len(body)}
    except ClientError as e:
        return {"ok": False, "op": "get_object_range", "latency_s": time.perf_counter() - t0,
                "error": e.response.get("Error", {}).get("Code", str(e))}
    except (BotoCoreError, OSError) as e:
        # A dropped connection counts as an error at this level. Running out
        # of open files stops the sweep instead; see connection_failure().
        return connection_failure("get_object_range", t0, e)


async def _run_one_level(s3_client, bucket: str, key: str, obj_size: int, k: int) -> FanoutLevelResult:
    max_offset = max(0, obj_size - RANGE_SIZE_BYTES - 1)
    offsets = [random.randint(0, max_offset) for _ in range(k)]

    t0 = time.perf_counter()
    responses = await asyncio.gather(*[
        _get_range_async(s3_client, bucket, key, off, off + RANGE_SIZE_BYTES - 1)
        for off in offsets
    ])
    wall_clock = time.perf_counter() - t0

    latencies = [r["latency_s"] for r in responses]
    errors = [r for r in responses if not r["ok"]]
    throttles = [r for r in errors if (r.get("error") or "").lower()
                 in ("slowdown", "requestlimitexceeded", "503", "throttlingexception")]

    return FanoutLevelResult(
        concurrency=k, wall_clock_s=wall_clock, per_request_latencies_s=latencies,
        error_count=len(errors), throttle_count=len(throttles),
    )


async def _run_level_with_repeats(s3_client, bucket: str, key: str, obj_size: int,
                                   k: int, repeats: int) -> FanoutLevelResult:
    """Runs one concurrency level `repeats` times and keeps the trial whose
    wall-clock time is the median, so an isolated slow or fast trial (see
    "NOISE AND REPEATS" in the module docstring) cannot decide the verdict
    on its own."""
    trials = [await _run_one_level(s3_client, bucket, key, obj_size, k) for _ in range(repeats)]
    trials.sort(key=lambda r: r.wall_clock_s)
    return trials[len(trials) // 2]


async def _warm_up_connections(s3_client, bucket: str, key: str, obj_size: int, count: int):
    """
    Opens `count` connections against the endpoint before any level is
    timed, so the timed sweep measures the backend's ability to sustain
    concurrent requests, not the one-time cost of establishing that many
    new TLS connections at once. Without this, the top concurrency levels
    fold connection setup (DNS, TCP handshake, TLS handshake) into the
    same measurement as request handling, and that setup cost scales with
    concurrency for any backend, well-behaved or not -- it isn't something
    the sweep is meant to detect.
    """
    max_offset = max(0, obj_size - RANGE_SIZE_BYTES - 1)
    offsets = [random.randint(0, max_offset) for _ in range(count)]
    await asyncio.gather(*[
        _get_range_async(s3_client, bucket, key, off, off + RANGE_SIZE_BYTES - 1)
        for off in offsets
    ])


async def _run_fanout_sweep_async(cfg: QwS3Config, bucket: str, key: str, obj_size: int,
                                   concurrency_levels: list, repeats: int) -> list:
    session = aioboto3.Session()
    top = max(concurrency_levels)
    kwargs = _async_client_kwargs(cfg, top)
    results = []
    async with session.client("s3", **kwargs) as s3_client:
        await _warm_up_connections(s3_client, bucket, key, obj_size, top)
        for k in concurrency_levels:
            results.append(await _run_level_with_repeats(s3_client, bucket, key, obj_size, k, repeats))
    return results


def run_fanout_sweep(cfg: QwS3Config, bucket: str, key: str, obj_size: int,
                      concurrency_levels: Optional[list] = None,
                      repeats: int = DEFAULT_LEVEL_REPEATS) -> list:
    """
    First warms up the connection pool by firing an untimed batch at the
    top concurrency level (see _warm_up_connections), then for each
    concurrency level K fires K concurrent range-GETs at random offsets
    within the object (mirroring the term/field lookup shape used in
    query_sim.py) `repeats` times, keeping the median trial (see "NOISE AND
    REPEATS" in the module docstring), and records wall-clock time for the
    whole batch plus each individual request's latency. See the module
    docstring for why this runs on asyncio rather than a thread pool.
    """
    concurrency_levels = concurrency_levels or DEFAULT_CONCURRENCY_LEVELS
    return asyncio.run(_run_fanout_sweep_async(cfg, bucket, key, obj_size, concurrency_levels, repeats))


def summarize_fanout(results: list, efficiency_floor: float = 0.4,
                      min_speedup: float = 2.0) -> dict:
    """
    Answers one question: does the backend serve concurrent requests at the
    same time, or one after another?

    A backend that serializes shows a speedup near 1 at every level, however
    many requests the client offers. Both AWS S3 and the vendors measured so
    far stay far above that. The summary also records where batch throughput
    peaks, which is where something saturates -- possibly the runner, not the
    backend -- and keeps the older efficiency ratio as a diagnostic.
    """
    rows = []
    serializes_at = None
    degrades_at = None
    for r in results:
        eff, speedup, rate = r.efficiency, r.speedup, r.requests_per_s
        rows.append({
            "concurrency": r.concurrency,
            "wall_clock_s": r.wall_clock_s if math.isfinite(r.wall_clock_s) else None,
            "p50_per_request_s": r.p50_per_request_s if math.isfinite(r.p50_per_request_s) else None,
            "p99_per_request_s": r.p99_per_request_s if math.isfinite(r.p99_per_request_s) else None,
            "efficiency": eff if math.isfinite(eff) else None,
            "speedup": speedup if math.isfinite(speedup) else None,
            "requests_per_s": rate if math.isfinite(rate) else None,
            "error_count": r.error_count,
            "throttle_count": r.throttle_count,
        })
        # An error or a throttle fails any level, including the first. Speedup
        # is only judged above concurrency 1, because one request cannot run
        # in parallel with itself; that level exists to establish the
        # single-request latency.
        failed = bool(r.error_count or r.throttle_count) or (
            r.concurrency > 1
            and (not math.isfinite(speedup) or speedup < min_speedup)
        )
        if serializes_at is None and failed:
            serializes_at = r.concurrency
        if degrades_at is None and (not math.isfinite(eff) or eff < efficiency_floor):
            degrades_at = r.concurrency
    rates = [row["requests_per_s"] for row in rows if row["requests_per_s"]]
    peak = max(rates) if rates else None
    return {
        "levels": rows,
        "serializes_at_concurrency": serializes_at,
        "min_speedup": min(
            (row["speedup"] for row in rows
             if row["concurrency"] > 1 and row["speedup"] is not None),
            default=None,
        ),
        "min_speedup_required": min_speedup,
        "peak_requests_per_s": peak,
        "peak_at_concurrency": next(
            (row["concurrency"] for row in rows if row["requests_per_s"] == peak), None
        ),
        # Diagnostic only. Nothing gates on these two.
        "degrades_at_concurrency": degrades_at,
        "efficiency_floor": efficiency_floor,
    }


def render_fanout_markdown(summary: dict, out_path: Path):
    lines = [
        "# Concurrency Fan-Out Sweep",
        "",
        "Fires an increasing number of *concurrent* range-GETs against a single "
        "split-sized object and measures whether wall-clock time for the whole "
        "batch stays close to one request's latency (good) or climbs toward "
        "concurrency x single-request-latency (bad -- the backend is "
        "effectively serializing concurrent requests).",
        "",
        "| Concurrency | Wall-clock (s) | p50/req (s) | p99/req (s) | Efficiency | Errors | Throttles |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["levels"]:
        lines.append(
            f"| {row['concurrency']} | {row['wall_clock_s']} | {row['p50_per_request_s']} | "
            f"{row['p99_per_request_s']} | {row['efficiency']} | {row['error_count']} | "
            f"{row['throttle_count']} |"
        )
    lines.append("")
    lines.append(f"Efficiency floor for a passing result: **{summary['efficiency_floor']}** "
                  "(1.0 = perfect parallelization, falling toward 0 = effectively serialized).")
    lines.append("")
    if summary["degrades_at_concurrency"]:
        lines.append(
            f"**Degrades at concurrency = {summary['degrades_at_concurrency']}.** Above this "
            "level, this backend can no longer hide its per-request latency behind "
            "concurrency -- BYOC-style deployments relying on high fan-out to hit "
            "sub-second query latency should not be certified past this concurrency "
            "level on this endpoint without further investigation."
        )
    else:
        lines.append(
            "**No degradation found within the tested range.** The backend sustained "
            "increasing concurrency without wall-clock time collapsing toward serial "
            "behavior or throttling appearing."
        )
    out_path.write_text("\n".join(lines))
