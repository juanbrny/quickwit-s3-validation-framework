"""Run the workload with the Rust load engine in `loadgen/`.

One Python process runs its code on one core at a time. Each S3 request
needs about 1 ms of that time, so one process tops out near 900 small reads
per second, and it slows every request well before that. In one AWS run at
the 1TB tier, small reads took 71 ms during the workload and 30 ms with
nothing else running. The test machine, not S3, set the response times.

The Rust engine uses every core, and the same S3 library as BYOC
(`aws-sdk-s3`). It writes the same `ingest_merge.jsonl` and `query.jsonl`
rows, so the report reads them unchanged. This module builds its settings,
starts it, and returns its summary.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENGINE_DIR = ROOT / "loadgen"
BINARY = ENGINE_DIR / "target" / "release" / "byoc-loadgen"
BUILD_COMMAND = "cargo build --release --manifest-path loadgen/Cargo.toml"

# The Python engine can still send the 100GB workload: about 50 small reads
# per second. Every larger tier needs the Rust engine.
PYTHON_ENGINE_TIERS = frozenset({"100GB"})
SEARCH_WORKERS = 4


def find_engine():
    """The engine binary, or None. `BYOC_LOADGEN` names another path."""
    named = os.environ.get("BYOC_LOADGEN")
    if named:
        path = Path(named)
        return path if path.is_file() and os.access(path, os.X_OK) else None
    return BINARY if BINARY.is_file() and os.access(BINARY, os.X_OK) else None


def missing_engine_message(tier):
    cargo = shutil.which("cargo")
    install = (
        "" if cargo else
        " Install Rust first from https://rustup.rs, then open a new shell."
    )
    return (
        f"The {tier} tier needs the Rust load engine, and it is not built."
        f"{install} Build it once with `{BUILD_COMMAND}`, then run the same"
        " command again. One Python process cannot send this workload: it"
        " would measure the test machine, not the storage."
    )


def choose_engine(requested, tier):
    """Return ("rust", path) or ("python", None), or raise ValueError."""
    binary = find_engine()
    if requested == "rust":
        if not binary:
            raise ValueError(missing_engine_message(tier))
        return "rust", binary
    if requested == "python":
        return "python", None
    if binary:
        return "rust", binary
    if tier in PYTHON_ENGINE_TIERS:
        return "python", None
    raise ValueError(missing_engine_message(tier))


def engine_digest(binary):
    """Fingerprint of the binary that ran, for the run record."""
    h = hashlib.sha256()
    with open(binary, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def engine_settings(cfg, bucket, prefix, op_mix, constants, profiles, duration_s,
                    out_dir, merged_cap_bytes, multipart_threshold, part_bytes):
    """The JSON document the engine reads on standard input."""
    verify = cfg.verify_tls
    if verify is False:
        raise ValueError(
            "The Rust load engine always verifies the endpoint certificate."
            " Pass --ca-bundle with the certificate of the endpoint or of its"
            " certificate authority, instead of --insecure-skip-tls-verify."
        )
    return {
        "connection": {
            "endpoint": cfg.endpoint_url,
            "region": cfg.region,
            "access_key": cfg.access_key,
            "secret_key": cfg.secret_key,
            "session_token": cfg.session_token,
            "bucket": bucket,
            "prefix": prefix,
            "ca_bundle": verify if isinstance(verify, str) else None,
        },
        "storage": {
            "force_path_style": cfg.force_path_style,
            "disable_multi_object_delete": cfg.disable_multi_object_delete,
            "disable_multipart_upload": cfg.disable_multipart_upload,
            "checksum_algorithm": cfg.checksum_algorithm,
            "multipart_threshold_bytes": multipart_threshold,
            "multipart_part_bytes": part_bytes,
            "multipart_concurrency": 10,
            "max_concurrency": cfg.max_concurrency,
        },
        "workload": {
            "duration_s": duration_s,
            "indexers": op_mix.num_indexer_nodes,
            "split_bytes": max(1024, int(op_mix.immature_put_size_mb * 1024 * 1024)),
            "commit_timeout_s": constants["commit_timeout_s"],
            "merge_factor": constants["merge_factor"],
            "merged_split_cap_bytes": merged_cap_bytes,
            "gc_grace_s": constants["gc_grace_period_s"],
            "query_qps": op_mix.query_qps,
            "searchers": SEARCH_WORKERS,
            "query_profiles": profiles,
        },
        "out_dir": str(out_dir),
    }


class EngineError(RuntimeError):
    pass


def run_engine(binary, settings, stop_event, on_started=None, keep=None):
    """Run the engine to the end and return its summary.

    `stop_event` ends the run early, for example when a visibility check
    fails. Ctrl-C reaches the engine directly, because it shares this
    terminal; it then stops cleanly and still prints its summary. The
    summary is also stored in `keep`, so an interrupted run keeps it.
    """
    keep = {} if keep is None else keep
    process = subprocess.Popen(
        [str(binary)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    # The settings hold the secret key, so they go through a pipe, never
    # the command line, where other users of the machine could read them.
    process.stdin.write(json.dumps(settings))
    process.stdin.close()

    def watch():
        stop_event.wait()
        if process.poll() is None:
            process.terminate()

    threading.Thread(target=watch, daemon=True).start()
    summary, error = None, None
    try:
        for line in process.stdout:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            kind = event.get("event")
            if kind == "started" and on_started:
                on_started(event)
            elif kind == "finished":
                summary = keep["summary"] = event
            elif kind == "error":
                error = event.get("message", "unknown error")
    except KeyboardInterrupt:
        # Without a terminal, for example in a container started by a
        # script, Ctrl-C reaches only this process. Pass it on, then let the
        # engine write its summary.
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            rest, _ = process.communicate(timeout=60)
            for line in rest.splitlines():
                if '"finished"' in line:
                    summary = keep["summary"] = json.loads(line)
        except (subprocess.TimeoutExpired, ValueError):
            process.kill()
        raise
    finally:
        code = process.wait()
        stop_event.set()
    if error or summary is None:
        raise EngineError(
            f"The load engine stopped: {error or f'exit code {code}, no summary'}."
        )
    return summary
