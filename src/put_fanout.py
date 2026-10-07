"""
The write-side counterpart to concurrency_fanout.py. That module tests
whether a backend can sustain many concurrent range-GETs, the pattern the
searcher depends on. This module tests the equivalent question for the
indexer: can this backend sustain many concurrent PutObject calls without
serializing them or degrading per-request latency?

This matters for the same reason range-GET concurrency matters, and for a
documented reason specific to ingest: per docs/01_s3_interaction_analysis.md
section 3, Quickwit's own engineering team chose the c5n.2xlarge instance
type for the indexer fleet *for its network throughput to S3, not its
compute power*, and reached ~27 MB/s per core in their adversarial
benchmark. Sustained ingest throughput at the higher throughput tiers
(100TB/day and up) depends on many indexer nodes committing splits with
enough overlap in time that their PUTs land on the backend concurrently,
not on any single PUT being fast. A backend with good single-request PUT
latency that quietly serializes concurrent PUTs would look fine under a
light ingest load and fall behind at the tiers that matter.

This sweeps *independent* concurrent PutObject calls (one indexer-node-style
commit per call), not concurrent parts of a single multipart upload -- that
second pattern is real too (see qw_s3_client.py's _multipart_put, which
uploads parts concurrently for exactly this reason) but is bounded by
S3's own per-object part-count math, not by backend concurrency handling in
the way this sweep measures.

Same asyncio/warm-up/repeats-with-median design as concurrency_fanout.py,
for the same reasons documented there. Uploaded test objects are deleted at
the end of the sweep via a bulk DeleteObjects call.
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Optional

import aioboto3
from botocore.exceptions import ClientError

from .concurrency_fanout import FanoutLevelResult, DEFAULT_LEVEL_REPEATS
from .qw_s3_client import QwS3Config, boto_config, checksum_kwargs_for

DEFAULT_CONCURRENCY_LEVELS = [1, 8, 16, 32, 64, 128]
DEFAULT_PUT_SIZE_KB = 512  # a small immature-split-sized commit, per docs/03


def _async_client_kwargs(cfg: QwS3Config, top_concurrency: int) -> dict:
    boto_cfg = boto_config(cfg, max(top_concurrency * 2, 20))
    return dict(
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name=cfg.region,
        config=boto_cfg,
    )


async def _put_one_async(s3_client, cfg: QwS3Config, bucket: str, key: str, payload: bytes) -> dict:
    t0 = time.perf_counter()
    extra = checksum_kwargs_for(cfg, payload)
    try:
        await s3_client.put_object(Bucket=bucket, Key=key, Body=payload, **extra)
        return {"ok": True, "op": "put_object", "latency_s": time.perf_counter() - t0, "key": key}
    except ClientError as e:
        return {"ok": False, "op": "put_object", "latency_s": time.perf_counter() - t0, "key": key,
                "error": e.response.get("Error", {}).get("Code", str(e))}


async def _run_one_level(s3_client, cfg: QwS3Config, bucket: str, prefix: str,
                          payload: bytes, k: int, level_tag: str) -> tuple[FanoutLevelResult, list]:
    keys = [f"{prefix}/put-fanout-{level_tag}-{i}.split" for i in range(k)]

    t0 = time.perf_counter()
    responses = await asyncio.gather(*[
        _put_one_async(s3_client, cfg, bucket, key, payload) for key in keys
    ])
    wall_clock = time.perf_counter() - t0

    latencies = [r["latency_s"] for r in responses]
    errors = [r for r in responses if not r["ok"]]
    throttles = [r for r in errors if (r.get("error") or "").lower()
                 in ("slowdown", "requestlimitexceeded", "503", "throttlingexception")]
    uploaded_keys = [r["key"] for r in responses if r["ok"]]

    result = FanoutLevelResult(
        concurrency=k, wall_clock_s=wall_clock, per_request_latencies_s=latencies,
        error_count=len(errors), throttle_count=len(throttles),
    )
    return result, uploaded_keys


async def _run_level_with_repeats(s3_client, cfg: QwS3Config, bucket: str, prefix: str,
                                   payload: bytes, k: int, repeats: int) -> tuple[FanoutLevelResult, list]:
    """Mirrors concurrency_fanout.py's repeats-with-median approach -- see
    that module's "NOISE AND REPEATS" docstring section for why a single
    trial per level is not reliable close to the pass/fail line."""
    trials = []
    all_keys = []
    for r in range(repeats):
        result, keys = await _run_one_level(s3_client, cfg, bucket, prefix, payload, k, level_tag=f"{k}-{r}")
        trials.append(result)
        all_keys.extend(keys)
    trials.sort(key=lambda r: r.wall_clock_s)
    return trials[len(trials) // 2], all_keys


async def _warm_up_connections(s3_client, cfg: QwS3Config, bucket: str, prefix: str,
                                payload: bytes, count: int) -> list:
    """Same rationale as concurrency_fanout.py's _warm_up_connections: opens
    `count` connections before any level is timed, so the timed sweep
    measures the backend's ability to sustain concurrent PUTs, not the
    one-time cost of establishing that many new TLS connections at once."""
    keys = [f"{prefix}/put-fanout-warmup-{i}.split" for i in range(count)]
    responses = await asyncio.gather(*[
        _put_one_async(s3_client, cfg, bucket, key, payload) for key in keys
    ])
    return [r["key"] for r in responses if r["ok"]]


async def _run_put_fanout_sweep_async(cfg: QwS3Config, bucket: str, prefix: str,
                                       payload: bytes, concurrency_levels: list,
                                       repeats: int) -> list:
    session = aioboto3.Session()
    top = max(concurrency_levels)
    kwargs = _async_client_kwargs(cfg, top)
    results = []
    all_keys = []
    async with session.client("s3", **kwargs) as s3_client:
        all_keys.extend(await _warm_up_connections(s3_client, cfg, bucket, prefix, payload, top))
        for k in concurrency_levels:
            result, keys = await _run_level_with_repeats(s3_client, cfg, bucket, prefix, payload, k, repeats)
            results.append(result)
            all_keys.extend(keys)

        # Clean up every object this sweep created, in batches of 1000
        # (DeleteObjects' own limit), so a certification run doesn't leave
        # thousands of throwaway test objects behind in the vendor's bucket.
        for i in range(0, len(all_keys), 1000):
            batch = all_keys[i:i + 1000]
            try:
                await s3_client.delete_objects(
                    Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
                )
            except ClientError:
                pass  # best-effort cleanup; leftover test objects don't affect the sweep's results

    return results


def run_put_fanout_sweep(cfg: QwS3Config, bucket: str, prefix: str,
                          concurrency_levels: Optional[list] = None,
                          object_size_kb: float = DEFAULT_PUT_SIZE_KB,
                          repeats: int = DEFAULT_LEVEL_REPEATS) -> list:
    """
    For each concurrency level K, fires K concurrent, independent
    PutObject calls (mirroring K indexer nodes committing splits at
    roughly the same time), `repeats` times per level, keeping the median
    trial. Test objects are deleted at the end of the sweep. See the
    module docstring for why this tests something concurrency_fanout.py
    does not.
    """
    concurrency_levels = concurrency_levels or DEFAULT_CONCURRENCY_LEVELS
    size = max(1024, int(object_size_kb * 1024))
    chunk = os.urandom(min(size, 256 * 1024))
    payload = (chunk * (size // len(chunk) + 1))[:size]
    return asyncio.run(_run_put_fanout_sweep_async(cfg, bucket, prefix, payload, concurrency_levels, repeats))


def summarize_put_fanout(results: list, efficiency_floor: float = 0.4) -> dict:
    """Same shape and interpretation as concurrency_fanout.summarize_fanout,
    applied to PUT results instead of range-GET results."""
    from .concurrency_fanout import summarize_fanout
    return summarize_fanout(results, efficiency_floor=efficiency_floor)


def render_put_fanout_markdown(summary: dict, out_path: Path):
    lines = [
        "# Concurrent PUT Fan-Out Sweep",
        "",
        "Fires an increasing number of *concurrent, independent* PutObject calls "
        "(mirroring multiple indexer nodes committing splits at roughly the same "
        "time) and measures whether wall-clock time for the whole batch stays "
        "close to one request's latency (good) or climbs toward "
        "concurrency x single-request-latency (bad -- the backend is "
        "effectively serializing concurrent writes).",
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
            "level, this backend can no longer sustain concurrent PUTs without "
            "individual commits slowing down -- ingest throughput at higher "
            "throughput tiers, where many indexer nodes commit splits with "
            "overlapping timing, should not be certified past this concurrency "
            "level on this endpoint without further investigation."
        )
    else:
        lines.append(
            "**No degradation found within the tested range.** The backend sustained "
            "increasing concurrent PUT load without wall-clock time collapsing toward "
            "serial behavior or throttling appearing."
        )
    out_path.write_text("\n".join(lines))
