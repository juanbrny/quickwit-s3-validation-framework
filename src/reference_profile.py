"""Bundled AWS S3 reference profiles, used when no measured baseline exists.

Most vendors have no Amazon Web Services (AWS) account, so asking them to
record their own AWS Simple Storage Service (S3) run blocks the whole
certification. This module supplies the reference latency instead, from a
published profile in `config/reference_profiles/`.

The profile holds three numbers, not a table of per-operation latencies:

* `first_byte_p99_s`, the tail latency of one small request.
* `per_stream_mb_s`, the rate one request transfers bytes at.
* `delete_p99_s`, the tail latency of one delete request.

A per-operation table would need re-measuring whenever a tier's object size
changes. The first two numbers plus the recorded object size cover every read
and upload, at every tier:

    reference p99 = first_byte_p99 + bytes / effective throughput

A delete moves no bytes, but it is not a small read either: S3 updates its
index for every key. Graded as a small read, AWS S3 itself failed: its bulk
deletes of 10 keys took about 300 ms, against a limit of 255 ms.

A measured baseline always wins when the operator supplies one. See
`docs/measurement_policy.md`.
"""

from __future__ import annotations

import math
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
PROFILE_DIR = ROOT / "config" / "reference_profiles"
DEFAULT_PROFILE = "aws-s3-ec2-same-region"
NO_PROFILE = "none"

# Operations whose latency is one round trip plus the whole concurrent read
# fan-out of a single query, rather than one request.
FANOUT_OPS = ("query_wall_clock",)
# Operations that upload, and therefore follow Quickwit's multipart policy.
UPLOAD_OPS = ("put_object", "multipart_upload")
# One request that deletes many keys, and one request per key.
BULK_DELETE_OPS = ("delete_objects_bulk",)
LOOP_DELETE_OPS = ("delete_object_loop",)


def available_profiles():
    return sorted(p.stem for p in PROFILE_DIR.glob("*.yaml"))


def load_profile(name=DEFAULT_PROFILE):
    """Load a profile by id, or by path. Returns None for "none"."""
    if not name or name == NO_PROFILE:
        return None
    path = Path(name)
    if not path.is_file():
        path = PROFILE_DIR / (name + ".yaml")
    if not path.is_file():
        raise ValueError(
            f"Unknown reference profile {name!r}. Available: "
            + ", ".join(available_profiles())
            + f", or {NO_PROFILE}."
        )
    profile = yaml.safe_load(path.read_text(encoding="utf-8"))
    for section, field, required in (
        ("latency", "first_byte_p99_s", True),
        ("latency", "fanout_tail_factor", True),
        ("latency", "delete_p99_s", False),
        ("throughput", "per_stream_mb_s", True),
    ):
        value = profile.get(section, {}).get(field)
        if value is None and not required:
            continue
        if not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f"Reference profile {name}: {section}.{field} must be positive.")
    return profile


def reference_p99(profile, op, median_bytes, keys_per_delete=10):
    """The p99 this profile predicts for one operation, in seconds.

    `median_bytes` is the recorded median payload of that operation in this
    run, so the prediction follows the tier's real object sizes instead of an
    assumed one. `keys_per_delete` is how many keys one garbage collection
    step deletes: a flavor without bulk delete sends one request per key.
    """
    if not profile:
        return None
    latency = profile["latency"]["first_byte_p99_s"]
    # A profile from before delete_p99_s grades deletes as small requests.
    delete = profile["latency"].get("delete_p99_s", latency)
    if op in BULK_DELETE_OPS:
        return delete
    if op in LOOP_DELETE_OPS:
        return delete * max(1, keys_per_delete)
    if op in FANOUT_OPS:
        # A query completes when its slowest concurrent read returns. The p99
        # of that maximum sits above the p99 of a single read.
        return latency * profile["latency"]["fanout_tail_factor"]
    if not isinstance(median_bytes, (int, float)) or median_bytes <= 0:
        return latency
    rates = profile["throughput"]
    if op in UPLOAD_OPS:
        # Uploads and downloads can differ. A measured profile records both;
        # an older one has only the download speed, which then serves both.
        stream = rates.get("upload_per_stream_mb_s", rates["per_stream_mb_s"])
        throughput = stream * 1024 * 1024 * _upload_streams(profile, median_bytes)
    else:
        throughput = rates["per_stream_mb_s"] * 1024 * 1024
    return latency + median_bytes / throughput


def _upload_streams(profile, size_bytes):
    """How many parts Quickwit uploads at the same time for this object size."""
    throughput = profile["throughput"]
    threshold = throughput["multipart_threshold_mb"] * 1024 * 1024
    if size_bytes < threshold:
        return 1
    part = throughput["multipart_part_gb"] * 1024 * 1024 * 1024
    parts = max(1, math.ceil(size_bytes / part))
    return min(parts, throughput["max_concurrent_parts"])


def describe(profile):
    """The provenance the report prints next to the verdict."""
    if not profile:
        return None
    return {
        "id": profile.get("id"),
        "version": profile.get("version"),
        "status": profile.get("status"),
        "title": profile.get("title"),
        "context": profile.get("context", {}),
        "latency": profile.get("latency", {}),
        "throughput": profile.get("throughput", {}),
    }


# ---- deriving a profile from a measured AWS run ----------------------------
#
# The bundled profile started from published AWS figures. `derive_profile()`
# replaces them with a run recorded by this tool, from an EC2 instance in the
# same region as the bucket. Each number measures exactly what the model uses
# it for:
#
#   first_byte_p99_s       p99 of the small reads (2 KiB and 8 KiB)
#   fanout_tail_factor     search p99 / small-read p99
#   per_stream_mb_s        median speed of whole-file downloads, one stream each
#   upload_per_stream_mb_s median speed of single-stream uploads
#   delete_p99_s           p99 of the delete requests: the visibility checks'
#                          single-key deletes, and the merges' bulk deletes
#
# Speeds use the time after the first byte: each transfer's latency minus the
# median small-read latency, which is the typical time to the first byte.

SMALL_READS = ("get_term_or_field", "get_doc")
DOWNLOADS = ("get_object_full",)
MIN_SAMPLES = {"small reads": 100, "searches": 100, "downloads": 10, "uploads": 10,
               "deletes": 100}
IN_REGION_MEDIAN_S = 0.050  # a small read from inside the region is far faster
# A single test process can only send so many requests before its own work
# slows every request down. Small reads during the workload may then take far
# longer than the same reads with nothing else running. Past this factor, the
# figures describe the test machine, not S3.
OVERLOAD_FACTOR = 2.0
SEARCH_RATE_MIN = 0.9  # the run must reach 90% of its search rate target
# A speed is only measured from a transfer where moving the bytes takes most
# of the time. On a small transfer the time after the first byte is nearly
# zero, and dividing by it gives absurd speeds.
LARGE_TRANSFER_MIB = 16
TRANSFER_DOMINATES = 3.0  # transfer time at least 3x the first-byte time


def _percentile(values, pct):
    from .report import _percentile as percentile

    return percentile(sorted(values), pct)


def derive_profile(run_dir):
    """Measure the profile's numbers from one AWS run.

    Returns (values, errors, warnings). An error means the run cannot give
    honest numbers, and no profile should be written from it.
    """
    import json

    from .run_store import load_bundle

    run_dir = Path(run_dir)
    manifest, issues = load_bundle(run_dir)
    load = manifest["stages"].get("load", {})
    errors, warnings = list(issues), []
    host = manifest["identity"]["endpoint"].split("//")[-1].split("/")[0].split(":")[0]
    if not host.endswith((".amazonaws.com", ".amazonaws.com.cn")):
        errors.append(f"The endpoint {host} is not AWS S3.")
    if load.get("status") != "COMPLETED":
        errors.append("The workload step did not complete.")
    if load.get("options", {}).get("flavor") not in ("none", "aws"):
        errors.append("The run did not use AWS S3's default settings (flavor aws).")
    if load.get("pauses"):
        errors.append("The test machine paused during the run.")

    rows = []
    for name in ("ingest_merge.jsonl", "query.jsonl"):
        file = run_dir / name
        if file.exists():
            rows += [json.loads(line) for line in file.read_text().splitlines() if line.strip()]
    ok = [r for r in rows if r.get("ok")]
    failed = len(rows) - len(ok)
    if rows and failed / len(rows) > 0.001:
        warnings.append(f"{failed} of {len(rows)} requests failed; AWS S3 rarely fails.")

    small = [r["latency_s"] for r in ok if r["op"] in SMALL_READS]
    searches = [r["latency_s"] for r in ok if r["op"] == "query_wall_clock"]
    part = 5 * 1024 ** 3
    small_p50 = _percentile(small, 50) if small else 0.0

    def large(r):
        return (
            r.get("bytes", 0) >= LARGE_TRANSFER_MIB * 1024 * 1024
            and r["latency_s"] - small_p50 >= TRANSFER_DOMINATES * small_p50
        )

    # A 30-minute run has only a few merge deletes, so most delete samples
    # come from the visibility checks, which delete one key every few seconds.
    deletes = [r["latency_s"] for r in ok if r["op"] in BULK_DELETE_OPS]
    probes = run_dir / "consistency.jsonl"
    if probes.exists():
        for line in probes.read_text().splitlines():
            probe = json.loads(line) if line.strip() else {}
            if probe.get("delete_ok") and isinstance(probe.get("delete_latency_s"), (int, float)):
                deletes.append(probe["delete_latency_s"])
    downloads = [r for r in ok if r["op"] in DOWNLOADS and large(r)]
    uploads = [
        r for r in ok
        if r["op"] in UPLOAD_OPS and large(r) and r["bytes"] <= part  # one stream
    ]
    counts = {"small reads": len(small), "searches": len(searches),
              "downloads": len(downloads), "uploads": len(uploads), "deletes": len(deletes)}
    for name, need in MIN_SAMPLES.items():
        if counts[name] < need:
            kind = f"large {name} (at least {LARGE_TRANSFER_MIB} MiB)" if name in ("downloads", "uploads") else name
            message = f"Only {counts[name]} {kind}; at least {need} are needed."
            if name == "deletes":
                message += (
                    " Runs recorded before the visibility checks timed their deletes"
                    " have only the merge deletes. Record a new run."
                )
            errors.append(message)
    if errors:
        return None, errors, warnings

    first_byte_p99 = _percentile(small, 99)
    first_byte_p50 = _percentile(small, 50)

    # The read concurrency test sends small reads with nothing else running.
    # Its single-request median is what S3 answers when the client is idle.
    idle = None
    fanout = run_dir / "fanout.json"
    if fanout.exists():
        levels = json.loads(fanout.read_text()).get("levels", [])
        idle = next((lv.get("p50_per_request_s") for lv in levels if lv.get("concurrency") == 1), None)
    if idle and first_byte_p50 > OVERLOAD_FACTOR * idle:
        errors.append(
            f"The test machine was overloaded. Small reads took {first_byte_p50 * 1000:.0f} ms"
            f" in the median during the workload, and {idle * 1000:.0f} ms with nothing else"
            " running. The figures would describe the test machine, not S3. Run a"
            " lighter workload, for example --tier 100GB."
        )
    target = load.get("op_mix", {}).get("query_qps")
    seconds = load.get("measurement_duration_s")
    if target and seconds and len(searches) / seconds < SEARCH_RATE_MIN * target:
        errors.append(
            f"The run reached {len(searches) / seconds:.1f} searches per second of the"
            f" {target:g} it should send, so it could not generate its own workload."
            " Run a lighter workload, for example --tier 100GB."
        )
    reference = idle or first_byte_p50
    if reference > IN_REGION_MEDIAN_S:
        warnings.append(
            f"Small reads take {reference * 1000:.0f} ms in the median with nothing else"
            " running. From inside the bucket's region they take about 20 to 30 ms, so"
            " this runner is probably outside it."
        )
    if errors:
        return None, errors, warnings

    def speed(transfers):
        rates = [
            r["bytes"] / (1024 * 1024) / (r["latency_s"] - first_byte_p50)
            for r in transfers
            if r["latency_s"] > first_byte_p50
        ]
        return _percentile(rates, 50) if rates else None

    values = {
        "first_byte_p99_s": round(first_byte_p99, 4),
        "fanout_tail_factor": round(_percentile(searches, 99) / first_byte_p99, 2),
        "per_stream_mb_s": round(speed(downloads), 1),
        "upload_per_stream_mb_s": round(speed(uploads), 1),
        "delete_p99_s": round(_percentile(deletes, 99), 4),
        "counts": counts,
        "small_read_p50_s": round(first_byte_p50, 4),
        "run_id": manifest["run_id"],
        "region": manifest["identity"]["region"],
        "runner": load.get("options", {}).get("runner_location", "unspecified"),
        "tier": load.get("options", {}).get("tier"),
        "duration_min": load.get("options", {}).get("duration_min"),
        "measured_at": load.get("finished_at"),
        "tool_commit": load.get("environment", {}).get("commit", "unknown"),
    }
    return values, errors, warnings


PROFILE_TEMPLATE = """\
# Bundled AWS S3 reference profile.
#
# A vendor does not need an Amazon Web Services (AWS) account to get a result.
# This file is the bar the tool grades against when no measured baseline is
# supplied. It was written by `run_validation.py profile` from a run recorded
# with this tool. To refresh it, record a new run and run that command again.

id: {id}
version: {version}
status: published
title: AWS S3 Standard, read from an EC2 instance in the same region

context:
  storage_class: S3 Standard
  runner: "{runner}"
  region: {region}
  measured_at: "{measured_at}"
  run_id: {run_id}
  tool_commit: "{tool_commit}"
  tier: {tier}
  duration_min: {duration_min}
  source: Measured with this tool, then derived with run_validation.py profile.
  samples:
    small_reads: {small_reads}
    searches: {searches}
    downloads: {downloads}
    uploads: {uploads}
    deletes: {deletes}
  references:
    - "AWS S3 best practices whitepaper. Make one concurrent request for each 85-90 MB/s of desired network throughput."
    - "dvassallo/s3-benchmark, 155 instance types. The 90th percentile time to first byte is around 20 ms from an in-region EC2 instance, and one thread moves about 93 MB/s."

latency:
  # The 99th percentile (p99) of the {small_reads} small reads (2 KiB and 8 KiB).
  # The median was {small_read_p50_ms} ms.
  first_byte_p99_s: {first_byte_p99_s}

  # A search ends when its slowest read returns. Measured as the p99 of
  # {searches} searches, divided by the small-read p99.
  fanout_tail_factor: {fanout_tail_factor}

  # The p99 of {deletes} delete requests: the visibility checks' single-key
  # deletes and the merges' bulk deletes. A delete moves no bytes, but S3
  # updates its index for every key, so it is slower than a small read.
  delete_p99_s: {delete_p99_s}

throughput:
  # Median speed of {downloads} whole-file downloads, one stream each, in MiB/s.
  per_stream_mb_s: {per_stream_mb_s}

  # Median speed of {uploads} single-stream uploads, in MiB/s.
  upload_per_stream_mb_s: {upload_per_stream_mb_s}

  # BYOC uploads above 128 MiB as multipart, with a 5 GiB target part size,
  # and sends the parts at the same time.
  multipart_threshold_mb: 128
  multipart_part_gb: 5
  max_concurrent_parts: 10
"""


def render_profile(values, profile_id=DEFAULT_PROFILE, version=1):
    return PROFILE_TEMPLATE.format(
        id=profile_id,
        version=version,
        small_read_p50_ms=round(values["small_read_p50_s"] * 1000),
        small_reads=values["counts"]["small reads"],
        searches=values["counts"]["searches"],
        downloads=values["counts"]["downloads"],
        uploads=values["counts"]["uploads"],
        deletes=values["counts"]["deletes"],
        **{k: v for k, v in values.items() if k != "counts"},
    )
