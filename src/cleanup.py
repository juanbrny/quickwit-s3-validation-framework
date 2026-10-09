"""Remove every object one run wrote, and nothing else.

A 30-minute run leaves hundreds of megabytes in the bucket: uploaded files,
merged files, the concurrency test objects and the visibility check objects.
Nothing removed them, so a test bucket grew with every run.

Every object a run writes lives under `qwcert/<run id>/`. Cleanup lists that
prefix and deletes what it finds:

1. unfinished multipart uploads, which hold storage without showing as objects;
2. the objects themselves;
3. old versions and delete markers, if the bucket keeps versions.

It never deletes the bucket, and never touches a key outside the run's prefix.
The bucket may hold other data, so the prefix is checked twice: once by the
listing, and again before each delete.
"""

from __future__ import annotations

import re
import time

from botocore.exceptions import BotoCoreError, ClientError

from .qw_s3_client import QwS3Client

BATCH = 1000  # the most keys one DeleteObjects request accepts
RUN_ID = re.compile(r"[0-9a-f]{32}")


def run_prefix(run_id: str) -> str:
    """The folder one run writes into.

    A run id is always 32 hexadecimal characters. Anything else is refused,
    because an empty or short id would widen the prefix to every run, or to
    the whole bucket.
    """
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise ValueError(f"Refusing to clean up: {run_id!r} is not a run id.")
    return f"qwcert/{run_id}/"


def _error(error) -> str:
    if isinstance(error, ClientError):
        return error.response.get("Error", {}).get("Code", type(error).__name__)
    return type(error).__name__


def _listed(client: QwS3Client, bucket: str, prefix: str):
    paginator = client._client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            yield item


def _abort_uploads(client, bucket, prefix, summary):
    try:
        paginator = client._client.get_paginator("list_multipart_uploads")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for upload in page.get("Uploads", []):
                if not upload["Key"].startswith(prefix):
                    continue
                client._client.abort_multipart_upload(
                    Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
                )
                summary["multipart_uploads_aborted"] += 1
    except (ClientError, BotoCoreError) as error:
        summary["errors"].append(f"listing unfinished multipart uploads: {_error(error)}")


def _delete_objects(client, bucket, prefix, summary):
    keys, sizes = [], 0
    for item in _listed(client, bucket, prefix):
        keys.append(item["Key"])
        sizes += item.get("Size", 0)
    keys = [k for k in keys if k.startswith(prefix)]
    for i in range(0, len(keys), BATCH):
        batch = keys[i : i + BATCH]
        # delete_batch follows the flavor: one bulk request, or one request
        # per object where the storage does not support bulk delete.
        result = client.delete_batch(bucket, batch)
        failed = result.get("errors") or ([result["error"]] if result.get("error") else [])
        summary["objects_deleted"] += len(batch) - len(failed)
        summary["errors"].extend(str(e)[:200] for e in failed)
    summary["bytes_deleted"] = sizes


def _delete_versions(client, bucket, prefix, summary):
    try:
        status = client._client.get_bucket_versioning(Bucket=bucket).get("Status")
    except (ClientError, BotoCoreError):
        return  # versioning is not supported, so there are no old versions
    if status not in ("Enabled", "Suspended"):
        return
    try:
        paginator = client._client.get_paginator("list_object_versions")
        targets = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
                if item["Key"].startswith(prefix):
                    targets.append({"Key": item["Key"], "VersionId": item["VersionId"]})
        for i in range(0, len(targets), BATCH):
            batch = targets[i : i + BATCH]
            if client.cfg.disable_multi_object_delete:
                for target in batch:
                    client._client.delete_object(Bucket=bucket, **target)
            else:
                client._client.delete_objects(
                    Bucket=bucket, Delete={"Objects": batch, "Quiet": True}
                )
            summary["versions_deleted"] += len(batch)
    except (ClientError, BotoCoreError) as error:
        summary["errors"].append(f"deleting old versions: {_error(error)}")


def clean_run(client: QwS3Client, bucket: str, run_id: str) -> dict:
    """Delete everything under one run's prefix, then check that nothing is left."""
    prefix = run_prefix(run_id)
    started = time.perf_counter()
    summary = {
        "bucket": bucket,
        "prefix": prefix,
        "multipart_uploads_aborted": 0,
        "objects_deleted": 0,
        "bytes_deleted": 0,
        "versions_deleted": 0,
        "objects_left": None,
        "errors": [],
    }
    try:
        _abort_uploads(client, bucket, prefix, summary)
        _delete_objects(client, bucket, prefix, summary)
        _delete_versions(client, bucket, prefix, summary)
        summary["objects_left"] = sum(1 for _ in _listed(client, bucket, prefix))
    except (ClientError, BotoCoreError) as error:
        summary["errors"].append(_error(error))
    summary["seconds"] = round(time.perf_counter() - started, 1)
    summary["complete"] = summary["objects_left"] == 0 and not summary["errors"]
    return summary


def describe(summary: dict) -> str:
    """One line a person can read."""
    line = (
        f"deleted {summary['objects_deleted']} objects"
        f" ({summary['bytes_deleted'] / 1e6:.1f} MB) under {summary['prefix']}"
    )
    if summary["multipart_uploads_aborted"]:
        line += f", aborted {summary['multipart_uploads_aborted']} unfinished uploads"
    if summary["versions_deleted"]:
        line += f", removed {summary['versions_deleted']} old versions"
    if summary["complete"]:
        return "Cleanup " + line + ". Nothing is left."
    left = summary["objects_left"]
    return (
        "Cleanup incomplete: " + line + "."
        + (f" {left} objects are still there." if left else "")
        + (" Errors: " + "; ".join(summary["errors"][:3]) if summary["errors"] else "")
        + " Run `cleanup` again to finish."
    )


# Before every object lived in its run's folder, the compatibility checks
# wrote into a shared `compat/` folder. These names are exactly what they
# generated: a fixed word, a 32-character random id, and a fixed ending. A
# person or another program is very unlikely to produce such a name, so only
# keys that match it completely are removed, and only when asked.
OLD_COMPAT_KEY = re.compile(
    r"compat/(path-style|multipart|mod|range|checksum)-[0-9a-f]{32}\.(txt|split)"
)


def clean_old_compat_objects(client: QwS3Client, bucket: str) -> dict:
    """Remove compatibility objects left by versions before per-run folders."""
    keys = [
        item["Key"]
        for item in _listed(client, bucket, "compat/")
        if OLD_COMPAT_KEY.fullmatch(item["Key"])
    ]
    deleted, errors = 0, []
    for i in range(0, len(keys), BATCH):
        result = client.delete_batch(bucket, keys[i : i + BATCH])
        failed = result.get("errors") or ([result["error"]] if result.get("error") else [])
        deleted += len(keys[i : i + BATCH]) - len(failed)
        errors.extend(str(e)[:200] for e in failed)
    return {"bucket": bucket, "old_compat_objects_deleted": deleted, "errors": errors}
