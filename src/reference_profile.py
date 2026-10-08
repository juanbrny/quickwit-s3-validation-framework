"""Bundled AWS S3 reference profiles, used when no measured baseline exists.

Most vendors have no Amazon Web Services (AWS) account, so asking them to
record their own AWS Simple Storage Service (S3) run blocks the whole
certification. This module supplies the reference latency instead, from a
published profile in `config/reference_profiles/`.

The profile holds two numbers, not a table of per-operation latencies:

* `first_byte_p99_s`, the tail latency of one small request.
* `per_stream_mb_s`, the rate one request transfers bytes at.

A per-operation table would need re-measuring whenever a tier's object size
changes. These two numbers plus the recorded object size cover every operation
and every tier:

    reference p99 = first_byte_p99 + bytes / effective throughput

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
    for section, field in (
        ("latency", "first_byte_p99_s"),
        ("latency", "fanout_tail_factor"),
        ("throughput", "per_stream_mb_s"),
    ):
        value = profile.get(section, {}).get(field)
        if not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f"Reference profile {name}: {section}.{field} must be positive.")
    return profile


def reference_p99(profile, op, median_bytes):
    """The p99 this profile predicts for one operation, in seconds.

    `median_bytes` is the recorded median payload of that operation in this
    run, so the prediction follows the tier's real object sizes instead of an
    assumed one.
    """
    if not profile:
        return None
    latency = profile["latency"]["first_byte_p99_s"]
    if op in FANOUT_OPS:
        # A query completes when its slowest concurrent read returns. The p99
        # of that maximum sits above the p99 of a single read.
        return latency * profile["latency"]["fanout_tail_factor"]
    if not isinstance(median_bytes, (int, float)) or median_bytes <= 0:
        return latency
    throughput = profile["throughput"]["per_stream_mb_s"] * 1024 * 1024
    if op in UPLOAD_OPS:
        throughput *= _upload_streams(profile, median_bytes)
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
