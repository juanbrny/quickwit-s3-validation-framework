"""
Tests for the search simulation: how many reads one query sends, and when the
queries are sent.

Both were wrong in a way that made the test harder than real Quickwit
traffic. Against a NetApp StorageGRID system, every single read finished
within its limit, yet whole queries failed theirs. The cause was the
simulation, not the storage:

* Document reads were sent once per split the query touched, instead of once
  per query. A query the documented formula sizes at 155 reads sent 355.
* The four search workers started together. One query per second arrived as
  four queries at once, every four seconds. The four slowed each other down,
  and they were slow together: 392, 525, 438 and 445 ms in one group.
"""
import threading
import time

from src.query_sim import _build_query_jobs, query_worker_loop, worker_offsets

PHRASE = dict(num_search_fields=3, num_terms=2, fieldnorm_fields=2, docs_returned=50)
SINGLE_TERM = dict(num_search_fields=1, num_terms=1, fieldnorm_fields=1, docs_returned=20)


def _count(jobs, op):
    return sum(1 for job in jobs if job[0] == op)


def test_documents_are_read_once_per_query_not_once_per_split():
    """
    The documented formula, from docs/background/01_s3_interaction_analysis.md:
    GETs = splits_hit x (fields x terms x 3 + fieldnorm + 1) + docs_returned
    """
    for splits in (1, 3, 5):
        keys = [f"split-{i}" for i in range(splits)]
        jobs = _build_query_jobs(keys, PHRASE, warm_splits=set(keys))
        assert _count(jobs, "get_doc") == 50
        assert _count(jobs, "get_term_or_field") == splits * (3 * 2 * 3 + 2 + 1)


def test_the_largest_query_matches_the_documented_formula():
    keys = [f"split-{i}" for i in range(5)]
    jobs = _build_query_jobs(keys, PHRASE, warm_splits=set(keys))
    assert len(jobs) == 5 * (3 * 2 * 3 + 2 + 1) + 50 == 155


def test_documents_are_spread_across_the_splits_the_query_hit():
    keys = ["a", "b", "c", "d"]
    jobs = _build_query_jobs(keys, SINGLE_TERM, warm_splits=set(keys))
    assert {job[1] for job in jobs if job[0] == "get_doc"} == set(keys)


def test_workers_start_one_query_interval_apart():
    assert worker_offsets(qps=1.0, num_workers=4) == [0.0, 1.0, 2.0, 3.0]
    assert worker_offsets(qps=10.0, num_workers=4) == [0.0, 0.1, 0.2, 0.3]


class _FakeClient:
    """Answers every read at once, so only scheduling is being measured."""

    class cfg:
        max_concurrency = 50

    def get_range(self, bucket, key, start, end):
        return {"ok": True, "latency_s": 0.0, "bytes": end - start + 1}


class _Sink:
    def __init__(self):
        self.rows, self.lock = [], threading.Lock()

    def write(self, row):
        with self.lock:
            self.rows.append(row)


def test_staggered_workers_send_evenly_spaced_queries():
    """
    Four workers at 20 queries per second in total must send one query every
    50 ms, not four queries at the same moment every 200 ms.
    """
    qps, workers, stop, sink = 20.0, 4, threading.Event(), _Sink()
    threads = [
        threading.Thread(
            target=query_worker_loop,
            args=(_FakeClient(), "bucket", lambda: ["split-0"], [dict(SINGLE_TERM, weight=1.0)],
                  qps / workers, 1.2, stop, sink, None, offset),
        )
        for offset in worker_offsets(qps, workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    starts = sorted(r.ts - r.latency_s for r in sink.rows if r.op == "query_wall_clock")
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    # Started together, three gaps in four would be near zero.
    near_zero = sum(1 for g in gaps if g < 0.015)
    assert len(starts) >= 12
    assert near_zero <= len(gaps) // 4, gaps
