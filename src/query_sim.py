"""
Simulates query-time GET traffic using Quickwit's own documented formula
(docs/01_s3_interaction_analysis.md section 5):

    GET requests ~= num_splits_hit
                  * ((num_search_fields * num_terms * 3) + fieldnorm_fields + 1)
                  + docs_returned

Byte ranges aren't published, so each modeled GET is a small-to-medium
byte-range read (a few KB to a few hundred KB), which is the shape that
matters for validating a storage backend's range-GET path under fan-out --
see the "assumption flagged" note in docs/01 section 5.

The first GET against any given split in a run additionally fetches a
larger "footer" range (the hotcache), modeling first-touch cost; subsequent
hits against the same split in the run skip that extra GET, modeling a warm
in-process cache -- again mirroring documented Quickwit behavior.

Every GET a single query needs is dispatched CONCURRENTLY, not one after
another -- this is not an implementation detail, it's the entire point.
Quickwit (and any BYOC deployment of it) compensates for S3's inherently
higher per-request latency by firing its whole fan-out at once, so wall-
clock time for a query tracks roughly one round-trip's latency instead of
the sum of all of them (Little's Law: throughput ~= concurrency / latency).
An earlier version of this module issued every GET in a query serially,
which both misrepresented real Quickwit behavior and meant this simulator
could never actually reveal whether a backend can sustain that concurrency
-- see the "query_wall_clock" metric below, and concurrency_fanout.py for a
faster, more targeted version of the same test.
"""
from __future__ import annotations

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from .ingest_merge_sim import ResultRow, ResultSink
from .qw_s3_client import QwS3Client

FOOTER_RANGE_BYTES = 64 * 1024      # modeled hotcache/footer fetch size
TERM_LOOKUP_RANGE_BYTES = 8 * 1024  # modeled posting-list block size
DOC_FETCH_RANGE_BYTES = 2 * 1024    # modeled per-returned-doc fetch size


def _pick_profile(profiles: list[dict]) -> dict:
    weights = [p["weight"] for p in profiles]
    return random.choices(profiles, weights=weights, k=1)[0]


def _build_query_jobs(hit_keys: list[str], profile: dict, warm_splits: set) -> list[tuple]:
    """
    Builds the full list of range-GET "jobs" (op_name, key, start, end) a
    single simulated query needs, across every split it hits. Building the
    whole list up front -- rather than issuing GETs split-by-split,
    term-by-term -- is what lets query_worker_loop fire them all
    concurrently instead of serially; see the module docstring and the
    "query_wall_clock" metric below.
    """
    jobs = []
    for key in hit_keys:
        if key not in warm_splits:
            jobs.append(("get_footer", key, 0, FOOTER_RANGE_BYTES - 1))
            warm_splits.add(key)

        term_gets = profile["num_search_fields"] * profile["num_terms"] * 3
        fieldnorm_gets = profile["fieldnorm_fields"]
        base_gets = term_gets + fieldnorm_gets + 1  # +1 timestamp fast field, per docs

        for _ in range(base_gets):
            start = random.randint(0, 10 * 1024 * 1024)
            jobs.append(("get_term_or_field", key, start, start + TERM_LOOKUP_RANGE_BYTES - 1))

        for _ in range(profile.get("docs_returned", 0)):
            start = random.randint(0, 10 * 1024 * 1024)
            jobs.append(("get_doc", key, start, start + DOC_FETCH_RANGE_BYTES - 1))
    return jobs


def query_worker_loop(client: QwS3Client, bucket: str, split_keys_provider,
                       profiles: list[dict], qps: float, duration_s: float,
                       stop_event: threading.Event, sink: ResultSink,
                       intra_query_concurrency: Optional[int] = None):
    """
    Simulates one searcher's query traffic. Re-fetches the live key list
    from split_keys_provider() every iteration so it tracks splits as the
    concurrently-running ingest/merge sim publishes and retires them (see
    SplitKeyRegistry in ingest_merge_sim.py).

    Every GET a single query needs (footer, term/field lookups, doc
    fetches, across every split it hits) is dispatched CONCURRENTLY through
    a shared thread pool, not one at a time -- this is the actual mechanism
    that makes S3-backed sub-second search possible despite S3's higher
    per-request latency (Little's Law: throughput ~= concurrency /
    latency). A query's wall-clock time is logged as its own metric
    ("query_wall_clock") precisely so the scorecard can show whether that
    mechanism is actually holding up on this endpoint, rather than only
    reporting each GET's own latency in isolation. For a more targeted,
    much faster diagnostic of the same property, see
    concurrency_fanout.py -- this loop tells you it happens during
    realistic mixed traffic; that module tells you exactly where it breaks.

    intra_query_concurrency bounds the thread pool used per worker
    (defaults to the shared client's own max_concurrency / connection pool
    size, mirroring QW_S3_MAX_CONCURRENCY) -- since `client` is normally
    shared across all query workers spawned by run_query_sim, this is a
    total across workers, not per-worker, which mirrors a real searcher
    process sharing one bounded connection pool.
    """
    max_workers = intra_query_concurrency or client.cfg.max_concurrency
    warm_splits: set[str] = set()
    interval = 1.0 / max(qps, 0.01)
    end_time = time.time() + duration_s

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while time.time() < end_time and not stop_event.is_set():
            loop_start = time.time()
            split_keys = split_keys_provider()
            if not split_keys:
                stop_event.wait(min(interval, 1.0))
                continue

            profile = _pick_profile(profiles)
            num_splits_hit = random.randint(1, min(5, len(split_keys)))
            hit_keys = random.sample(split_keys, num_splits_hit)
            jobs = _build_query_jobs(hit_keys, profile, warm_splits)

            query_t0 = time.perf_counter()
            futures = [pool.submit(client.get_range, bucket, k, s, e) for (_, k, s, e) in jobs]
            all_ok = True
            for (op_name, _key, _s, _e), fut in zip(jobs, futures):
                res = fut.result()
                sink.write(ResultRow(ts=time.time(), worker="searcher", op=op_name,
                                      ok=res["ok"], latency_s=res["latency_s"],
                                      bytes=res.get("bytes", 0), error=res.get("error")))
                all_ok = all_ok and res["ok"]
            query_wall_clock = time.perf_counter() - query_t0

            # The metric that actually answers "does concurrency deliver the
            # throughput this architecture depends on here": if the backend
            # sustains concurrent range-GETs well, this stays close to a
            # single GET's latency even with dozens of `jobs`. If it climbs
            # toward len(jobs) x single-request latency, "concurrent"
            # requests are effectively being serialized somewhere in the path.
            sink.write(ResultRow(ts=time.time(), worker="searcher", op="query_wall_clock",
                                  ok=all_ok, latency_s=query_wall_clock, bytes=0,
                                  error=None if all_ok else "one_or_more_gets_failed"))

            elapsed = time.time() - loop_start
            stop_event.wait(min(max(0.0, interval - elapsed), max(0, end_time-time.time())))


def run_query_sim(client: QwS3Client, bucket: str, split_keys_provider,
                   profiles: list[dict], qps: float, duration_min: float,
                   out_path: Path, num_workers: int = 4,
                   stop_event: threading.Event = None, block: bool = True):
    """
    split_keys_provider: callable returning the current list of live split
    keys (queries should hit whatever the ingest/merge sim has actually
    published -- typically `registry.snapshot` from a shared
    SplitKeyRegistry so this runs concurrently with ingest/merge, not after).
    """
    sink = ResultSink(out_path)
    stop_event = stop_event or threading.Event()
    duration_s = duration_min * 60
    threads = []

    for _ in range(num_workers):
        t = threading.Thread(target=sink.run, args=(query_worker_loop, (
            client, bucket, split_keys_provider, profiles, qps / num_workers,
            duration_s, stop_event, sink
        ), stop_event), daemon=True)
        threads.append(t)
        t.start()

    if not block:
        return threads, sink

    try:
        for t in threads:
            t.join()
    finally:
        sink.close()
