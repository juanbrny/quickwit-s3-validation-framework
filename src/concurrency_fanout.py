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

IMPORTANT correctness note: the boto3 client's own connection pool is fixed
at construction time to cfg.max_concurrency (see qw_s3_client.py). If this
sweep used a client sized for typical query traffic (default 50), sweeping
past that would measure *our own tool's* connection-pool ceiling, not the
vendor's. build_fanout_client() below sizes the pool to comfortably exceed
the top of the sweep so any degradation observed is the backend's, not
ours.
"""
from __future__ import annotations

import dataclasses
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .qw_s3_client import QwS3Client, QwS3Config

DEFAULT_CONCURRENCY_LEVELS = [1, 8, 16, 32, 64, 128, 256]
RANGE_SIZE_BYTES = 8 * 1024  # matches query_sim's term/field lookup size
DEFAULT_SPLIT_SIZE_MB = 8.0  # small mature split, per docs/03


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
        s = sorted(self.per_request_latencies_s)
        idx = min(len(s) - 1, int(len(s) * 0.99))
        return s[idx]

    @property
    def efficiency(self) -> float:
        """
        1.0 = ideal: wall-clock for the whole concurrent batch is no worse
        than a single request's typical latency (the backend fully
        parallelized the batch).
        Approaching 0 (specifically 1/concurrency in the fully-serialized
        case) = the backend processed the "concurrent" requests one at a
        time despite the client offering them all at once.
        """
        if self.wall_clock_s <= 0 or not self.per_request_latencies_s:
            return float("nan")
        return self.p50_per_request_s / self.wall_clock_s


def build_fanout_client(base_cfg: QwS3Config, concurrency_levels: list) -> QwS3Client:
    """
    Clones base_cfg but forces max_concurrency high enough to comfortably
    exceed the top of the sweep, so the connection pool on our side is
    never what limits observed concurrency. See module docstring.
    """
    top = max(concurrency_levels) if concurrency_levels else 1
    fanout_cfg = dataclasses.replace(base_cfg, max_concurrency=max(top * 2, 20))
    return QwS3Client(fanout_cfg)


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


def run_fanout_sweep(client: QwS3Client, bucket: str, key: str, obj_size: int,
                      concurrency_levels: Optional[list] = None) -> list:
    """
    For each concurrency level K, fires K concurrent range-GETs at random
    offsets within the object (mirroring the term/field lookup shape used
    in query_sim.py), using a thread pool sized to K, and records wall-clock
    time for the whole batch plus each individual request's latency.
    """
    concurrency_levels = concurrency_levels or DEFAULT_CONCURRENCY_LEVELS
    results = []

    for k in concurrency_levels:
        max_offset = max(0, obj_size - RANGE_SIZE_BYTES - 1)
        offsets = [random.randint(0, max_offset) for _ in range(k)]

        def _one(offset):
            return client.get_range(bucket, key, offset, offset + RANGE_SIZE_BYTES - 1)

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=k) as pool:
            futures = [pool.submit(_one, off) for off in offsets]
            responses = [f.result() for f in futures]
        wall_clock = time.perf_counter() - t0

        latencies = [r["latency_s"] for r in responses]
        errors = [r for r in responses if not r["ok"]]
        throttles = [r for r in errors if (r.get("error") or "").lower()
                     in ("slowdown", "requestlimitexceeded", "503", "throttlingexception")]

        results.append(FanoutLevelResult(
            concurrency=k, wall_clock_s=wall_clock, per_request_latencies_s=latencies,
            error_count=len(errors), throttle_count=len(throttles),
        ))

    return results


def summarize_fanout(results: list, efficiency_floor: float = 0.4) -> dict:
    """
    Finds the concurrency level (if any) at which the backend stops
    sustaining the fan-out -- i.e., where efficiency drops below
    efficiency_floor or throttling appears. That's the point past which
    this backend can no longer hide S3-style per-request latency behind
    concurrency the way the architecture needs it to, at least on this
    connection.
    """
    rows = []
    degrades_at = None
    for r in results:
        eff = r.efficiency
        rows.append({
            "concurrency": r.concurrency,
            "wall_clock_s": round(r.wall_clock_s, 4),
            "p50_per_request_s": round(r.p50_per_request_s, 4),
            "p99_per_request_s": round(r.p99_per_request_s, 4),
            "efficiency": round(eff, 3) if eff == eff else None,  # NaN check
            "error_count": r.error_count,
            "throttle_count": r.throttle_count,
        })
        if degrades_at is None and r.concurrency > 1:
            if r.throttle_count > 0 or (eff == eff and eff < efficiency_floor):
                degrades_at = r.concurrency
    return {"levels": rows, "degrades_at_concurrency": degrades_at, "efficiency_floor": efficiency_floor}


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
