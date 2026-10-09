"""
S3 client wrapper that mirrors Quickwit's `storage.s3.*` configuration
surface (see docs/background/01_s3_interaction_analysis.md section 2), so that the same
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
import errno

from botocore.exceptions import BotoCoreError, ClientError

# Storage flavors. Each one is a named combination of the `storage.s3.*`
# knobs Quickwit exposes.
#
# Two groups exist, and the difference matters for what a user can ship:
#
# * Upstream flavors (UPSTREAM_FLAVORS) are transcribed from Quickwit's
#   storage-config.md. A user can select them by name with
#   `storage.s3.flavor: <name>`.
# * Local flavors (everything else) are candidate configurations this
#   framework adds for vendors Quickwit has no flavor for. Quickwit does not
#   know these names. A user must ship the explicit knobs instead, which is
#   what `as_quickwit_yaml()` always renders.
FLAVOR_PRESETS = {
    "none": dict(force_path_style=False, disable_multi_object_delete=False,
                 disable_multipart_upload=False, checksum_algorithm="crc32c",
                 region_override=None),
    # Same knobs as "none". It exists so a run against AWS S3 itself can say
    # so, instead of reading as "no flavor selected".
    "aws": dict(force_path_style=False, disable_multi_object_delete=False,
                disable_multipart_upload=False, checksum_algorithm="crc32c",
                region_override=None),
    "minio": dict(force_path_style=True, disable_multi_object_delete=False,
                  disable_multipart_upload=False, checksum_algorithm="crc32c",
                  region_override="minio"),
    "garage": dict(force_path_style=True, disable_multi_object_delete=False,
                   disable_multipart_upload=False, checksum_algorithm="crc32c",
                   region_override="garage"),
    # SeaweedFS and Scality (RING S3 Connector, CloudServer) flavors, with
    # the evidence for each knob:
    #
    # * Bulk delete stays on. SeaweedFS registers DeleteMultipleObjects
    #   (weed/s3api/s3api_server.go) and Scality lists Multi-Object Delete
    #   as supported.
    # * Multipart stays on. Both implement CreateMultipartUpload, UploadPart
    #   and CompleteMultipartUpload.
    # * Path style is on. Both support virtual-hosted style, but only with
    #   extra setup: SeaweedFS routes bucket subdomains only when started
    #   with a virtual host domain, and Scality needs wildcard DNS (Domain
    #   Name System) plus a matching certificate, and cannot serve hosted
    #   style at all when the endpoint is an IP address. Path style works in
    #   every deployment.
    # * MD5 (Message Digest 5), not CRC32C. Both break on the AWS SDK's
    #   default trailing checksum: SeaweedFS writes the trailer into the
    #   stored object (seaweedfs issue 6548), and Scality CloudServer
    #   rejects it with "400 BadRequest: trailing checksum is not supported"
    #   (cloudserver issue 5553). See boto_config() for the botocore option
    #   that makes this setting authoritative.
    #
    # The two flavors hold the same knobs today. They stay separate so a
    # report names the vendor that was actually tested.
    "seaweedfs": dict(force_path_style=True, disable_multi_object_delete=False,
                      disable_multipart_upload=False, checksum_algorithm="md5",
                      region_override=None),
    "scality": dict(force_path_style=True, disable_multi_object_delete=False,
                    disable_multipart_upload=False, checksum_algorithm="md5",
                    region_override=None),
    # NetApp StorageGRID, with the evidence for each knob (docs 12.0):
    #
    # * Bulk delete stays on. DeleteObjects is supported, and "multiple
    #   objects can be deleted in the same request message".
    # * Multipart stays on. CreateMultipartUpload, UploadPart,
    #   CompleteMultipartUpload and UploadPartCopy are all supported.
    # * Path style is on. Virtual-hosted style needs S3 endpoint domain names
    #   configured in the Grid Manager, plus matching DNS records: "If you
    #   don't add S3 endpoint domain names and the list is empty, support for
    #   S3 virtual-hosted-style requests is disabled."
    # * MD5, not CRC32C. The PutObject page lists `Content-MD5` as supported,
    #   and lists both `x-amz-sdk-checksum-algorithm` and `x-amz-trailer` as
    #   unsupported. Those two are exactly what boto3 sends for CRC32C, so the
    #   default checksum path cannot work here. See boto_config(), which stops
    #   botocore adding a trailer of its own.
    # * No region override. The grid administrator sets the region, and
    #   us-east-1 is the documented example, so the run keeps --region.
    "storagegrid": dict(force_path_style=True, disable_multi_object_delete=False,
                        disable_multipart_upload=False, checksum_algorithm="md5",
                        region_override=None),
    "digital_ocean": dict(force_path_style=True, disable_multi_object_delete=True,
                           disable_multipart_upload=False, checksum_algorithm="crc32c",
                           region_override=None),
    "gcs": dict(force_path_style=False, disable_multi_object_delete=True,
                disable_multipart_upload=True, checksum_algorithm="disabled",
                region_override=None),
}

# Flavors Quickwit itself accepts as `storage.s3.flavor: <name>`.
UPSTREAM_FLAVORS = frozenset({"none", "minio", "garage", "digital_ocean", "gcs"})

# Flavors that keep every AWS S3 default. A run on one of these needs no
# deviation, so it can reach a plain CERTIFIED verdict.
DEFAULT_FLAVORS = frozenset({"none", "aws"})

# Candidates tried, in order, by compat_checks.py auto-probe mode. Least
# deviation first, so the probe recommends the mildest configuration that
# works. "aws" is absent on purpose: it repeats "none" exactly.
AUTO_PROBE_ORDER = ["none", "minio", "garage", "seaweedfs", "scality",
                    "storagegrid", "digital_ocean", "gcs"]


def equivalent_flavors(flavor: str) -> list:
    """Every flavor name that selects the same settings as this one.

    SeaweedFS, Scality and StorageGRID need the same four settings today. A
    StorageGRID operator should not have to ship a configuration labelled
    `seaweedfs`, so the report names all of them.
    """
    signature = flavor_signature(flavor)
    return [
        name
        for name in FLAVOR_PRESETS
        if name != flavor and flavor_signature(name) == signature
    ]


def flavor_signature(flavor: str) -> tuple:
    """Knob values of a flavor, for finding flavors that are identical."""
    preset = FLAVOR_PRESETS[flavor]
    return tuple(sorted(preset.items(), key=lambda item: item[0]))


def same_settings(a: Optional[str], b: Optional[str]) -> bool:
    """True when two flavor names select the same knob values.

    `none` and `aws` hold identical knobs, so a run with one of them agrees
    with a recommendation of the other.
    """
    if a is None or b is None:
        return False
    if a not in FLAVOR_PRESETS or b not in FLAVOR_PRESETS:
        return a == b
    return flavor_signature(a) == flavor_signature(b)


def flavor_label(flavor: Optional[str]) -> str:
    """How a flavor reads to a person.

    The internal name `none` means "no flavor setting, Quickwit's defaults".
    Printed bare, "flavor: none" reads like a missing value, so it is never
    shown that way.
    """
    if flavor is None:
        return "not recorded"
    if flavor == "none":
        return "Quickwit defaults (no flavor setting)"
    if flavor == "aws":
        return "aws (AWS S3 defaults)"
    return flavor


def flavor_note(flavor: Optional[str]) -> Optional[str]:
    """Warn when Quickwit does not accept this flavor name in its config."""
    if flavor is None or flavor in UPSTREAM_FLAVORS or flavor in DEFAULT_FLAVORS:
        return None
    return (
        f"Quickwit has no built-in flavor named `{flavor}`. "
        "Ship the storage.s3 settings below verbatim instead of `flavor: "
        f"{flavor}`."
    )


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
    # True verifies against the system trust store. A path verifies against a
    # private certificate authority (CA) bundle, which on-premises appliances
    # usually need. False skips verification.
    verify_tls: object = True
    # Temporary credentials (keys starting with ASIA) only work with their
    # session token. Without it, AWS rejects every signed request.
    session_token: Optional[str] = None

    @classmethod
    def from_flavor(cls, flavor: str, endpoint_url, access_key, secret_key,
                     region="us-east-1", max_concurrency=50,
                     verify_tls=True, session_token=None) -> "QwS3Config":
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
            verify_tls=verify_tls,
            session_token=session_token,
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
        boto_cfg = boto_config(cfg, max(cfg.max_concurrency, 10))
        self._client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint_url,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            aws_session_token=cfg.session_token,
            region_name=cfg.region,
            config=boto_cfg,
            verify=cfg.verify_tls,
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
        except BotoCoreError as e:
            return connection_failure("put_object", t0, e)

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
        except BotoCoreError as e:
            return connection_failure("get_object_full", t0, e)

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
        except BotoCoreError as e:
            return connection_failure("get_object_range", t0, e)

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
        except BotoCoreError as e:
            return connection_failure("delete_objects_bulk", t0, e)

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
        except BotoCoreError as e:
            return connection_failure("list_objects_v2", t0, e)

    # ---- metastore-mode full read/overwrite --------------------------------
    def metastore_read(self, bucket: str, key: str) -> dict:
        return self.get_full(bucket, key)

    def metastore_write(self, bucket: str, key: str, data: bytes) -> dict:
        return self.put_split(bucket, key, data)


class RunnerLimitError(RuntimeError):
    """The test machine, not the storage, ran out of a resource."""


def is_runner_fault(error: BaseException) -> bool:
    """True when an error comes from this machine running out of open files.

    Such an error is not the storage's fault. Counting it as a failed request
    would blame the vendor for a limit on the test machine.
    """
    seen, depth = error, 0
    while seen is not None and depth < 10:
        if isinstance(seen, OSError) and seen.errno == errno.EMFILE:
            return True
        if "Too many open files" in str(seen):
            return True
        seen, depth = seen.__cause__ or seen.__context__, depth + 1
    return False


def connection_failure(op: str, t0: float, error: BaseException) -> dict:
    """Record a connection-level error as one failed request.

    A dropped connection or a timeout is a failed request, which is exactly
    what the failed-requests check counts. An earlier version let these
    errors escape, which stopped the whole workload over one request and
    recorded nothing. Running out of open files is the exception: it stops
    the run, with a message naming the real cause.
    """
    if is_runner_fault(error):
        raise RunnerLimitError(
            "This machine ran out of open files. That is a limit of the test"
            " machine, not of the storage. Run `ulimit -n 4096` in this shell"
            " and start again."
        ) from error
    return {"ok": False, "op": op, "latency_s": time.perf_counter() - t0,
            "error": type(error).__name__}


def ensure_file_descriptors(required: int) -> int:
    """Raise this process's open-file limit to cover a concurrency sweep.

    Each in-flight request needs its own socket, and a socket needs a file
    descriptor. macOS ships a low per-shell default, so a 1024-way sweep hits
    "Too many open files" before it reaches the endpoint.

    That failure belongs to the runner, not to the backend. The sweep treats
    any error at a concurrency level as the point where the backend stops
    coping, so an exhausted descriptor table would be recorded as a vendor
    fault. Raise the limit where the system allows it, and stop before the
    sweep starts where it does not.
    """
    try:
        import resource
    except ImportError:  # not a POSIX platform
        return required
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < required:
        target = required if hard == resource.RLIM_INFINITY else min(required, hard)
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        except (ValueError, OSError):
            pass
        soft = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    if soft < required:
        raise ValueError(
            f"This machine allows {soft} open files per process. The sweep needs"
            f" about {required}, one socket per concurrent request. Raise the"
            f" limit with `ulimit -n {required}` in this shell, or lower the top"
            " concurrency with --levels."
        )
    return soft


def boto_config(cfg: "QwS3Config", max_pool_connections: int) -> BotoConfig:
    """Shared botocore settings for every client this framework builds.

    `request_checksum_calculation="when_required"` matters. botocore 1.36 and
    later add their own CRC32 (Cyclic Redundancy Check, 32-bit) trailer to
    every upload, even when the caller asks for no checksum or for
    Content-MD5 (Message Digest 5). That default would send a trailing
    checksum under the `md5` and `disabled` settings, which is the exact
    request SeaweedFS corrupts and Scality rejects with 400 BadRequest. With
    this option, boto3 sends only the checksum `checksum_algorithm` selects,
    so the setting stays authoritative. An explicit `crc32c` request is still
    sent, because asking for it makes it required.
    """
    return BotoConfig(
        signature_version="s3v4",
        s3={"addressing_style": "path" if cfg.force_path_style else "auto"},
        max_pool_connections=max_pool_connections,
        retries={"max_attempts": 3, "mode": "standard"},
        request_checksum_calculation="when_required",
    )


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
