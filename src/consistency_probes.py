"""
Probes the consistency properties Quickwit's file-backed-metastore-on-S3
mode actually relies on (docs/background/01_s3_interaction_analysis.md section 6):

  1. Read-after-write: GET immediately after PUT must return the new content.
  2. List-after-write: a fresh ListObjectsV2 must include a just-written key.
  3. Delete-visibility: a GET immediately after DELETE must 404.

Each probe is run repeatedly, interleaved with the load test, and must
succeed within `deadline_s` (defaults to Quickwit's default
`polling_interval` of 30s) on every attempt -- not "eventually", since a
searcher polling every 30s that misses a write for longer than that is a
real correctness gap, not just a benchmark curiosity.

NOTE: Quickwit's file-backed metastore does NOT use conditional writes /
compare-and-swap for multi-writer safety (it's explicitly documented as
single-writer-only). These probes therefore do not test CAS semantics --
only plain read-after-write, which is what Quickwit's current design
actually depends on. See docs/01 section 6 for the "why".
"""
from __future__ import annotations

import json
import time
import threading
import uuid
from dataclasses import asdict
from pathlib import Path

from .ingest_merge_sim import ResultRow, ResultSink
from .qw_s3_client import QwS3Client


def probe_read_after_write(client: QwS3Client, bucket: str, prefix: str) -> dict:
    key = f"{prefix}/consistency/raw-{uuid.uuid4().hex}.txt"
    payload = uuid.uuid4().bytes * 100
    t_put0 = time.time()
    put_res = client.put_split(bucket, key, payload)
    t_put1 = time.time()
    get_res = client.get_full(bucket, key)
    t_get1 = time.time()
    content_matches = get_res.get("ok") and get_res.get("bytes") == len(payload)
    return {
        "probe": "read_after_write", "key": key,
        "put_ok": put_res["ok"], "get_ok": get_res["ok"],
        "content_matches": content_matches,
        "put_to_get_latency_s": t_get1 - t_put1,
        "success": bool(put_res["ok"] and get_res["ok"] and content_matches),
    }


def probe_list_after_write(client: QwS3Client, bucket: str, prefix: str) -> dict:
    key = f"{prefix}/consistency/law-{uuid.uuid4().hex}.txt"
    client.put_split(bucket, key, b"x" * 1024)
    list_res = client.list_prefix(bucket, f"{prefix}/consistency/")
    found = list_res.get("ok") and key in list_res.get("keys", [])
    return {
        "probe": "list_after_write", "key": key,
        "list_ok": list_res.get("ok", False), "found": bool(found),
        "success": bool(found),
    }


def probe_delete_visibility(client: QwS3Client, bucket: str, prefix: str) -> dict:
    key = f"{prefix}/consistency/del-{uuid.uuid4().hex}.txt"
    client.put_split(bucket, key, b"x" * 1024)
    del_res = client.delete_batch(bucket, [key])
    get_res = client.get_full(bucket, key)
    is_gone = (not get_res["ok"]) and get_res.get("error") in ("NoSuchKey", "404", "NotFound")
    return {
        "probe": "delete_visibility", "key": key,
        "delete_ok": del_res.get("ok", False), "is_gone": bool(is_gone),
        "success": bool(del_res.get("ok") and is_gone),
    }


def run_consistency_probes(client: QwS3Client, bucket: str, prefix: str,
                            duration_min: float, interval_s: float,
                            deadline_s: float, out_path: Path, stop_event=None):
    stop_event = stop_event or threading.Event()
    end_time = time.time() + duration_min * 60
    results = []
    # Write each result as it happens, preserving interrupted-run evidence.
    with open(out_path, "x", buffering=1) as output:
        while time.time() < end_time and not stop_event.is_set():
            for probe_fn in (probe_read_after_write, probe_list_after_write, probe_delete_visibility):
                if stop_event.is_set():
                    break
                t0 = time.time()
                r = probe_fn(client, bucket, prefix)
                r["ts"] = t0
                r["elapsed_s"] = time.time() - t0
                r["within_deadline"] = r["elapsed_s"] <= deadline_s
                results.append(r)
                output.write(json.dumps(r) + "\n")
            stop_event.wait(min(interval_s, max(0, end_time-time.time())))
    total = len(results)
    successes = sum(1 for r in results if r["success"] and r["within_deadline"])
    return {"total_probes": total, "successes": successes,
            "success_pct": (100.0 * successes / total) if total else 0.0}
