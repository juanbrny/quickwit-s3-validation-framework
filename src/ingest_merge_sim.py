"""
Simulates the ingest -> merge -> GC lifecycle described in
docs/background/01_s3_interaction_analysis.md sections 3-4, driven by the op-mix from
workload_model.compute_op_mix().

Each "indexer node" is a thread that:
  1. Every commit_timeout_s, PUTs an immature split of the modeled size.
     The nodes start spread over one commit interval, as real indexers are.
  2. Accumulates produced split keys; once merge_factor of them exist,
     starts a merge in its own thread, so the merge never delays the next
     upload. The merge downloads every input in full (GET), uploads one
     merged split (PUT, multipart if it crosses the multipart threshold),
     publishes it in place of its inputs, waits gc_grace_s, then
     bulk-deletes the inputs (DELETE) -- mirroring Quickwit's
     merge -> publish -> GC sequence.

This is the Python engine. It still runs the 100GB tier and the tool's own
tests. Larger tiers use the Rust engine in `loadgen/`, which follows the same
rules; see src/load_engine.py for why.

All request outcomes are appended to a shared, thread-safe results list and
flushed to a JSONL file for `report.py` to consume.
"""
from __future__ import annotations

import json
import os
import random
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from .qw_s3_client import QwS3Client
from .workload_model import OpMix


@dataclass
class ResultRow:
    ts: float
    worker: str
    op: str
    ok: bool
    latency_s: float
    bytes: int = 0
    error: Optional[str] = None


# The merged split is capped for test cost, above the 128 MiB multipart
# threshold so the multipart path runs. Full-size merges are tested later.
MERGED_SPLIT_CAP_MIB = 160


class ResultSink:
    def __init__(self, out_path: Path):
        self._lock = threading.Lock()
        self._f = open(out_path, "x", buffering=1)
        self._closed = False
        self.errors = []
        self.children: list[threading.Thread] = []

    def write(self, row: ResultRow):
        with self._lock:
            # A merge cut off at the end of the run may finish a request
            # after the file is closed. That request is outside the
            # measured time, so it is dropped.
            if not self._closed:
                self._f.write(json.dumps(asdict(row)) + "\n")

    def spawn(self, target, args, stop_event, worker):
        """Start a task in its own thread, with the same error handling."""
        thread = threading.Thread(target=self.run, args=(target, args, stop_event, worker),
                                  daemon=True)
        with self._lock:
            self.children = [t for t in self.children if t.is_alive()]
            self.children.append(thread)
        thread.start()

    def run(self, target, args, stop_event, worker="worker"):
        try:
            target(*args)
        except Exception as error:
            # Keep the message, not only the type. An earlier version kept the
            # type alone, in memory, and the operator was told to "inspect raw
            # results" that held no trace of the cause. The caller removes
            # credentials before anything is printed or saved.
            with self._lock:
                self.errors.append(
                    {"worker": worker, "type": type(error).__name__, "message": str(error)}
                )
            stop_event.set()

    def close(self):
        with self._lock:
            self._closed = True
            self._f.close()


class SplitKeyRegistry:
    """
    Thread-safe registry of currently-live (published, not-yet-GC'd) split
    keys, shared between the ingest/merge sim and the query sim so that
    queries actually hit objects that exist -- concurrently, the way an
    indexer and a searcher pool run concurrently against the same bucket in
    a real Quickwit deployment, not sequentially.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._keys: list[str] = []

    def add(self, key: str):
        with self._lock:
            self._keys.append(key)

    def remove_many(self, keys: list[str]):
        self.replace(keys, None)

    def replace(self, inputs: list[str], merged: Optional[str]):
        """Swap merged inputs for their output in one step, so a search
        never sees both or neither."""
        with self._lock:
            gone = set(inputs)
            self._keys = [k for k in self._keys if k not in gone]
            if merged:
                self._keys.append(merged)

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._keys)


def _synthetic_payload(size_mb: float) -> bytes:
    size = max(1024, int(size_mb * 1024 * 1024))
    # Deterministic-ish but not all-zero (some backends fast-path all-zero
    # objects in ways that don't reflect real split content).
    chunk = os.urandom(min(size, 4 * 1024 * 1024))
    return (chunk * ((size + len(chunk) - 1) // len(chunk)))[:size]


def indexer_worker(node_id: int, client: QwS3Client, bucket: str, prefix: str,
                    op_mix: OpMix, duration_s: float, stop_event: threading.Event,
                    sink: ResultSink, merge_factor: int, registry: "SplitKeyRegistry", commit_timeout_s: float = 60,
                    start_offset_s: float = 0.0, gc_grace_s: float = 60):
    produced_keys: list[str] = []
    end_time = time.time() + duration_s
    split_seq = 0
    # Started together, every node uploaded in the same second of each
    # minute. Real indexers commit at independent times.
    if start_offset_s > 0:
        stop_event.wait(min(start_offset_s, max(0, end_time - time.time())))

    while time.time() < end_time and not stop_event.is_set():
        loop_start = time.time()

        # 1. Immature split PUT
        split_seq += 1
        key = f"{prefix}/node{node_id}/immature-{split_seq}-{int(time.time()*1000)}.split"
        payload = _synthetic_payload(op_mix.immature_put_size_mb)
        res = client.put_split(bucket, key, payload)
        sink.write(ResultRow(ts=time.time(), worker=f"indexer-{node_id}", op=res["op"],
                              ok=res["ok"], latency_s=res["latency_s"],
                              bytes=len(payload), error=res.get("error")))
        if res["ok"]:
            produced_keys.append(key)
            registry.add(key)  # published: queries may now hit this split

        # 2. Trigger a merge once enough immature splits have accumulated
        # A merge runs in its own thread. Run inline, a slow merge stopped
        # every upload of its node for minutes at a time.
        if len(produced_keys) >= merge_factor:
            batch = produced_keys[:merge_factor]
            produced_keys = produced_keys[merge_factor:]
            sink.spawn(_do_merge, (node_id, client, bucket, prefix, batch, op_mix, sink,
                                   registry, stop_event, gc_grace_s),
                       stop_event, f"merger-{node_id}")

        # 3. Sleep out the remainder of the commit interval (commit_timeout_s, default 60s)
        elapsed = time.time() - loop_start
        sleep_for = max(0.0, commit_timeout_s - elapsed)
        stop_event.wait(min(sleep_for, max(0, end_time - time.time())))


def _do_merge(node_id: int, client: QwS3Client, bucket: str, prefix: str,
              input_keys: list[str], op_mix: OpMix, sink: ResultSink,
              registry: "SplitKeyRegistry", stop_event: threading.Event = None,
              gc_grace_s: float = 60):
    # Full-object GET on each input (merge reads whole splits, not ranges)
    total_bytes = 0
    for k in input_keys:
        res = client.get_full(bucket, k)
        sink.write(ResultRow(ts=time.time(), worker=f"merger-{node_id}", op=res["op"],
                              ok=res["ok"], latency_s=res["latency_s"],
                              bytes=res.get("bytes", 0), error=res.get("error")))
        total_bytes += res.get("bytes", 0)

    # PUT the merged output. put_split() switches to multipart automatically
    # once size crosses MULTIPART_THRESHOLD_BYTES (128 MiB, confirmed against
    # Pomsky's real MultiPartPolicy default). The cap here (160 MB) sits just
    # above that threshold on purpose -- an earlier 64 MB cap sat entirely
    # below it, so `load` runs never actually exercised the multipart path,
    # only the single-PutObject one, regardless of how large a real merged
    # split would be for the tier under test.
    merged_key = f"{prefix}/node{node_id}/merged-{int(time.time()*1000)}.split"
    merged_payload = _synthetic_payload(min(total_bytes / (1024 * 1024), MERGED_SPLIT_CAP_MIB))
    res = client.put_split(bucket, merged_key, merged_payload)
    sink.write(ResultRow(ts=time.time(), worker=f"merger-{node_id}", op=res["op"],
                          ok=res["ok"], latency_s=res["latency_s"],
                          bytes=len(merged_payload), error=res.get("error")))
    # Publish: new searches read the merged split instead of its inputs.
    registry.replace(input_keys, merged_key if res["ok"] else None)

    # Searches that chose an input before the publish may still read it.
    # Deleting at once made them read missing files: 440 NoSuchKey errors
    # in one AWS run, all blamed on the storage.
    stop_event = stop_event or threading.Event()
    if stop_event.wait(gc_grace_s):
        return
    del_res = client.delete_batch(bucket, input_keys)
    sink.write(ResultRow(ts=time.time(), worker=f"gc-{node_id}", op=del_res["op"],
                          ok=del_res["ok"], latency_s=del_res["latency_s"],
                          bytes=0, error=str(del_res.get("errors")) if del_res.get("errors") else None))


def run_ingest_merge_sim(client: QwS3Client, bucket: str, prefix: str, op_mix: OpMix,
                          duration_min: float, out_path: Path, registry: "SplitKeyRegistry",
                          merge_factor: int = 10, stop_event: threading.Event = None,
                          block: bool = True, commit_timeout_s: float = 60,
                          gc_grace_s: float = 60):
    """
    If `block=False`, returns the list of started threads immediately so the
    caller can run this concurrently with run_query_sim() and
    run_consistency_probes() against the shared `registry` and `stop_event`
    -- this is how `run_validation.py load` actually drives all three at
    once, matching a real deployment where indexers, mergers, and searchers
    hit the bucket simultaneously (see docs/background/02_test_methodology.md Layer 3).
    """
    sink = ResultSink(out_path)
    stop_event = stop_event or threading.Event()
    threads = []
    duration_s = duration_min * 60
    nodes = op_mix.num_indexer_nodes
    for node_id in range(nodes):
        t = threading.Thread(target=sink.run, args=(indexer_worker, (
            node_id, client, bucket, prefix, op_mix, duration_s, stop_event, sink,
            merge_factor, registry, commit_timeout_s,
            commit_timeout_s * node_id / nodes, gc_grace_s,
        ), stop_event, f"indexer-{node_id}"), daemon=True)
        threads.append(t)
        t.start()

    if not block:
        return threads, sink

    try:
        for t in threads:
            t.join()
    finally:
        sink.close()
