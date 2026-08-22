"""
S3 client wrapper that mirrors Quickwit's `storage.s3.*` configuration
surface (see docs/01_s3_interaction_analysis.md section 2), so that the same
compatibility axes Quickwit exposes to end users can be flipped on/off here.

This intentionally does NOT reimplement Quickwit's Rust storage code. It
implements the same *behavioral choices* using boto3, so we can determine
which combination of knobs makes a given endpoint work -- and ship that
combination as the recommended `storage.s3.*` block for that vendor.
"""
from __future__ import annotations

import dataclasses
import hashlib
import io
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

# Known Quickwit "flavor" presets, transcribed from storage-config.md
FLAVOR_PRESETS = {
    "none": dict(force_path_style=False, disable_multi_object_delete=False,
                 disable_multipart_upload=False, checksum_algorithm="crc32c",
                 region_override=None),
    "minio": dict(force_path_style=True, disable_multi_object_delete=False,
                  disable_multipart_upload=False, checksum_algorithm="crc32c",
                  region_override="minio"),
    "garage": dict(force_path_style=True, disable_multi_object_delete=False,
                   disable_multipart_upload=False, checksum_algorithm="crc32c",
                   region_override="garage"),
    "digital_ocean": dict(force_path_style=True, disable_multi_object_delete=True,
                           disable_multipart_upload=False, checksum_algorithm="crc32c",
                           region_override=None),
    "gcs": dict(force_path_style=False, disable_multi_object_delete=True,
                disable_multipart_upload=True, checksum_algorithm="disabled",
                region_override=None),
}

# Candidates tried, in order, by compat_checks.py auto-probe mode.
AUTO_PROBE_ORDER = ["none", "minio", "garage", "digital_ocean", "gcs"]

# Pomsky's real MultiPartPolicy default (quickwit-storage/src/object_storage/policy.rs):
# multipart_threshold_num_bytes = 128 MiB, target_part_num_bytes = 5 GiB. An earlier
# version of this constant assumed a ~5GB threshold (conflating it with the target part
# size); confirmed against Pomsky source that the actual trigger is 128 MiB, 40x smaller.
MULTIPART_THRESHOLD_BYTES = 128 * 1024 * 1024


@dataclasses.dataclass
class QwS3Config:
    endpoint_url: Optional[str]
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    force_path_style: bool = False
    disable_multi_object_delete: bool = False
    disable_multipart_upload: bool = False
    checksum_algorithm: str = "crc32c"   # crc32c | md5 | disabled
    region_override: Optional[str] = None
    max_concurrency: int = 50

    @classmethod
    def from_flavor(cls, flavor: str, endpoint_url, access_key, secret_key,
                     region="us-east-1", max_concurrency=50) -> "QwS3Config":
        preset = FLAVOR_PRESETS[flavor]
        return cls(
            endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key,
            region=preset["region_override"] or region,
            force_path_style=preset["force_path_style"],
            disable_multi_object_delete=preset["disable_multi_object_delete"],
            disable_multipart_upload=preset["disable_multipart_upload"],
            checksum_algorithm=preset["checksum_algorithm"],
            region_override=preset["region_override"],
            max_concurrency=max_concurrency,
        )

    def as_quickwit_yaml(self) -> str:
        """Render the equivalent storage.s3.* block a user would ship."""
        lines = ["storage:", "  s3:"]
        if self.endpoint_url:
            lines.append(f"    endpoint: {self.endpoint_url}")
        if self.region_override:
            lines.append(f"    region: {self.region_override}")
        if self.force_path_style:
            lines.append("    force_path_style_access: true")
        if self.disable_multi_object_delete:
            lines.append("    disable_multi_object_delete: true")
        if self.disable_multipart_upload:
            lines.append("    disable_multipart_upload: true")
        if self.checksum_algorithm != "crc32c":
            lines.append(f"    checksum_algorithm: {self.checksum_algorithm}")
        return "\n".join(lines)


class QwS3Client:
    """Thin wrapper exposing exactly the operations Quickwit issues."""

    def __init__(self, cfg: QwS3Config):
        self.cfg = cfg
        boto_cfg = BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path" if cfg.force_path_style else "auto"},
            max_pool_connections=max(cfg.max_concurrency, 10),
            retries={"max_attempts": 3, "mode": "standard"},
        )
        self._client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint_url,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name=cfg.region,
            config=boto_cfg,
        )

    # ---- bucket lifecycle -------------------------------------------------
    def ensure_bucket(self, bucket: str):
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError:
            kwargs = {"Bucket": bucket}
            if self.cfg.region not in ("us-east-1",):
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.cfg.region}
            self._client.create_bucket(**kwargs)

    # ---- uploads: single PUT vs multipart, mirroring Quickwit's rule ------
    def put_split(self, bucket: str, key: str, data: bytes) -> dict:
        """
        Mirrors Pomsky's real MultiPartPolicy: single PutObject below
        MULTIPART_THRESHOLD_BYTES (128 MiB), multipart at or above it,
        unless multipart is disabled (GCS flavor), in which case fall back
        to a single PutObject regardless of size (matching
        `disable_multipart_upload: true`).
        """
        t0 = time.perf_counter()
        extra = self._checksum_kwargs(data)
        try:
            if self.cfg.disable_multipart_upload or len(data) < MULTIPART_THRESHOLD_BYTES:
                resp = self._client.put_object(Bucket=bucket, Key=key, Body=data, **extra)
                op = "put_object"
            else:
                resp = self._multipart_put(bucket, key, data)
                op = "multipart_upload"
            return {"ok": True, "op": op, "latency_s": time.perf_counter() - t0,
                    "etag": resp.get("ETag")}
        except ClientError as e:
            return {"ok": False, "op": "put_object", "latency_s": time.perf_counter() - t0,
                    "error": e.response.get("Error", {}).get("Code", str(e))}

    def _checksum_kwargs(self, data: bytes) -> dict:
        return checksum_kwargs_for(self.cfg, data)

    def _multipart_put(self, bucket: str, key: str, data: bytes, part_size=5 * 1024 * 1024 * 1024):
        mp = self._client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mp["UploadId"]
        offsets = list(enumerate(range(0, len(data), part_size), start=1))

        def _upload_one(item):
            part_number, offset = item
            chunk = data[offset: offset + part_size]
            p = self._client.upload_part(
                Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=chunk,
            )
            return {"ETag": p["ETag"], "PartNumber": part_number}

        try:
            # Real S3 clients -- including the AWS SDK for Rust that Quickwit
            # uses -- upload multipart parts concurrently, not one at a time.
            # A backend that quietly serializes concurrent UploadPart calls
            # would look fine against a sequential uploader but bottleneck
            # real ingest throughput for mature splits. Bounded to 10
            # in-flight parts, matching s3transfer's own default.
            with ThreadPoolExecutor(max_workers=min(len(offsets), 10)) as pool:
                parts = sorted(pool.map(_upload_one, offsets), key=lambda p: p["PartNumber"])
            return self._client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            self._client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise

    # ---- reads: full-object (merge) and range (query) ----------------------
    def get_full(self, bucket: str, key: str) -> dict:
        t0 = time.perf_counter()
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key)
            body = resp["Body"].read()
            return {"ok": True, "op": "get_object_full", "latency_s": time.perf_counter() - t0,
                    "bytes": len(body)}
        except ClientError as e:
            return {"ok": False, "op": "get_object_full", "latency_s": time.perf_counter() - t0,
                    "error": e.response.get("Error", {}).get("Code", str(e))}

    def get_range(self, bucket: str, key: str, start: int, end: Optional[int]) -> dict:
        t0 = time.perf_counter()
        range_hdr = f"bytes={start}-{end}" if end is not None else f"bytes={start}-"
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key, Range=range_hdr)
            body = resp["Body"].read()
            return {"ok": True, "op": "get_object_range", "latency_s": time.perf_counter() - t0,
                    "bytes": len(body)}
        except ClientError as e:
            return {"ok": False, "op": "get_object_range", "latency_s": time.perf_counter() - t0,
                    "error": e.response.get("Error", {}).get("Code", str(e))}

    # ---- deletes: bulk vs per-object fallback ------------------------------
    def delete_batch(self, bucket: str, keys: list[str]) -> dict:
        t0 = time.perf_counter()
        try:
            if self.cfg.disable_multi_object_delete:
                errors = []
                for k in keys:
                    try:
                        self._client.delete_object(Bucket=bucket, Key=k)
                    except ClientError as e:
                        errors.append(str(e))
                ok = len(errors) == 0
                return {"ok": ok, "op": "delete_object_loop", "latency_s": time.perf_counter() - t0,
                        "count": len(keys), "errors": errors}
            else:
                resp = self._client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True},
                )
                errors = resp.get("Errors", [])
                return {"ok": len(errors) == 0, "op": "delete_objects_bulk",
                        "latency_s": time.perf_counter() - t0, "count": len(keys),
                        "errors": errors}
        except ClientError as e:
            return {"ok": False, "op": "delete_objects_bulk", "latency_s": time.perf_counter() - t0,
                    "error": e.response.get("Error", {}).get("Code", str(e))}

    # ---- listing (GC reconciliation) --------------------------------------
    def list_prefix(self, bucket: str, prefix: str) -> dict:
        t0 = time.perf_counter()
        keys = []
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                keys.extend(o["Key"] for o in page.get("Contents", []))
            return {"ok": True, "op": "list_objects_v2", "latency_s": time.perf_counter() - t0,
                    "keys": keys}
        except ClientError as e:
            return {"ok": False, "op": "list_objects_v2", "latency_s": time.perf_counter() - t0,
                    "error": e.response.get("Error", {}).get("Code", str(e))}

    # ---- metastore-mode full read/overwrite --------------------------------
    def metastore_read(self, bucket: str, key: str) -> dict:
        return self.get_full(bucket, key)

    def metastore_write(self, bucket: str, key: str, data: bytes) -> dict:
        return self.put_split(bucket, key, data)


def checksum_kwargs_for(cfg: QwS3Config, data: bytes) -> dict:
    """Shared by QwS3Client (sync) and put_fanout.py's async client, so both
    exercise the same upload-integrity behavior Quickwit's checksum_algorithm
    setting controls."""
    if cfg.checksum_algorithm == "md5":
        return {"ContentMD5": _b64_md5(data)}
    if cfg.checksum_algorithm == "disabled":
        return {}
    # crc32c: let the SDK attach its native trailer checksum (default boto3 behavior
    # when ChecksumAlgorithm is specified)
    return {"ChecksumAlgorithm": "CRC32C"}


def _b64_md5(data: bytes) -> str:
    import base64
    return base64.b64encode(hashlib.md5(data).digest()).decode()
