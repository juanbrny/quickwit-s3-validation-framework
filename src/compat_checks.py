"""
Layer 2 (docs/02_test_methodology.md): scripted, functional checks of the
specific S3 behaviors Quickwit's `storage.s3.*` config knobs exist to route
around (docs/01_s3_interaction_analysis.md section 2).

Each check returns (passed: bool, detail: str). `probe_flavor()` tries the
built-in Quickwit flavor presets in order and reports the first one that
gets every check passing, mirroring exactly how a user would pick
`storage.s3.flavor: <x>` in their own config.
"""
from __future__ import annotations

import uuid

from .qw_s3_client import AUTO_PROBE_ORDER, QwS3Client, QwS3Config


def check_path_style_addressing(client: QwS3Client, bucket: str) -> tuple[bool, str]:
    key = f"compat/path-style-{uuid.uuid4().hex}.txt"
    res = client.put_split(bucket, key, b"path-style-check")
    return res["ok"], res.get("error", "ok")


def check_multipart_upload(client: QwS3Client, bucket: str) -> tuple[bool, str]:
    """
    Goes through client.put_split(), which respects
    cfg.disable_multipart_upload -- exactly like Quickwit's own storage
    layer would. For a flavor that disables multipart (e.g. gcs), this
    means the check exercises (and must pass) the single-PutObject fallback
    for a large object instead of raw multipart, since that's what Quickwit
    would actually do in that configuration. For flavors that don't disable
    it, this forces the multipart code path with a small part-size override
    so we don't move 5GB in a compat check.
    """
    key = f"compat/multipart-{uuid.uuid4().hex}.split"
    payload = uuid.uuid4().bytes * (12 * 1024 * 1024 // 16)  # 12MB
    if client.cfg.disable_multipart_upload:
        res = client.put_split(bucket, key, payload)
        return res["ok"], res.get("error", "ok (multipart disabled -> single PutObject)")
    try:
        resp = client._multipart_put(bucket, key, payload, part_size=5 * 1024 * 1024)
        return True, "ok (multipart)"
    except Exception as e:
        return False, str(e)


def check_multi_object_delete(client: QwS3Client, bucket: str) -> tuple[bool, str]:
    """
    Goes through client.delete_batch(), which respects
    cfg.disable_multi_object_delete -- so a flavor that disables bulk delete
    (e.g. gcs, digital_ocean) is checked against the per-object DeleteObject
    fallback Quickwit actually uses in that mode, not against raw
    DeleteObjects, which is exactly the operation that flavor exists to
    avoid.
    """
    keys = [f"compat/mod-{uuid.uuid4().hex}.txt" for _ in range(5)]
    for k in keys:
        client.put_split(bucket, k, b"x")
    res = client.delete_batch(bucket, keys)
    detail = "ok" if res["ok"] else str(res.get("errors") or res.get("error"))
    return res["ok"], f"{detail} (mode={res['op']})"


def check_range_get(client: QwS3Client, bucket: str) -> tuple[bool, str]:
    key = f"compat/range-{uuid.uuid4().hex}.txt"
    payload = bytes(range(256)) * 100  # 25,600 bytes, content is checkable
    client.put_split(bucket, key, payload)
    checks = []
    try:
        # start-end range
        r1 = client._client.get_object(Bucket=bucket, Key=key, Range="bytes=0-99")
        checks.append(r1["Body"].read() == payload[0:100])
        # open-ended range
        r2 = client._client.get_object(Bucket=bucket, Key=key, Range=f"bytes={len(payload)-50}-")
        checks.append(r2["Body"].read() == payload[-50:])
        # suffix range
        r3 = client._client.get_object(Bucket=bucket, Key=key, Range="bytes=-50")
        checks.append(r3["Body"].read() == payload[-50:])
        return all(checks), f"start-end={checks[0]} open-ended={checks[1]} suffix={checks[2]}"
    except Exception as e:
        return False, str(e)


def check_checksum_algorithm(client: QwS3Client, bucket: str) -> tuple[bool, str]:
    key = f"compat/checksum-{uuid.uuid4().hex}.split"
    res = client.put_split(bucket, key, b"checksum-check-payload")
    return res["ok"], res.get("error", f"ok (algorithm={client.cfg.checksum_algorithm})")


CHECKS = [
    ("path_style_addressing", check_path_style_addressing),
    ("multipart_upload", check_multipart_upload),
    ("multi_object_delete", check_multi_object_delete),
    ("range_get_semantics", check_range_get),
    ("checksum_algorithm", check_checksum_algorithm),
]


def run_all_checks(client: QwS3Client, bucket: str) -> dict:
    results = {}
    for name, fn in CHECKS:
        try:
            ok, detail = fn(client, bucket)
        except Exception as e:
            ok, detail = False, f"unhandled exception: {e}"
        results[name] = {"passed": ok, "detail": detail}
    return results


def probe_flavor(endpoint_url: str, access_key: str, secret_key: str, bucket: str,
                  region: str = "us-east-1") -> dict:
    """
    Try Quickwit's built-in flavor presets in order; return the first one
    where every check passes, plus the full per-flavor results for the
    report. This directly answers "what storage.s3.yaml block should this
    vendor's users ship?"
    """
    attempts = {}
    for flavor in AUTO_PROBE_ORDER:
        cfg = QwS3Config.from_flavor(flavor, endpoint_url, access_key, secret_key, region)
        client = QwS3Client(cfg)
        try:
            client.ensure_bucket(bucket)
        except Exception as e:
            attempts[flavor] = {"error": f"bucket setup failed: {e}", "all_passed": False}
            continue
        results = run_all_checks(client, bucket)
        all_passed = all(r["passed"] for r in results.values())
        attempts[flavor] = {"results": results, "all_passed": all_passed,
                             "yaml": cfg.as_quickwit_yaml() if all_passed else None}
        if all_passed:
            return {"recommended_flavor": flavor, "attempts": attempts}
    return {"recommended_flavor": None, "attempts": attempts}
