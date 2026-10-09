"""
Cleanup removes every object one run wrote, and nothing else.

Before it existed, a 12-second run left 12 objects and 48 MB in the bucket,
and a 30-minute run left hundreds of megabytes. Four of those objects sat
under a shared `compat/` folder, where no cleanup could safely tell them from
someone else's data. Every object now lives under `qwcert/<run id>/`.

The safety rule matters more than the cleanup: the bucket may hold other
data, so most of these tests check what must survive.
"""
import boto3
import pytest

from run_validation import main
from src.cleanup import clean_run, run_prefix
from src.qw_s3_client import QwS3Client, QwS3Config
from src.run_store import read_json

KEYS = ["--access-key", "testing", "--secret-key", "testing"]
SHORT = ["--tier", "100GB", "--duration-min", "0.05", "--levels", "1,2", "--repeats", "1"]


def _s3(endpoint):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="testing",
                        aws_secret_access_key="testing", region_name="us-east-1")


def _keys(s3, bucket, prefix=""):
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for page in pages for o in page.get("Contents", [])]


def test_validate_leaves_the_bucket_as_it_found_it(moto_server_endpoint, tmp_path):
    s3 = _s3(moto_server_endpoint)
    s3.create_bucket(Bucket="shared")
    # Someone else's data, including a key that looks like the tool's own.
    s3.put_object(Bucket="shared", Key="production/index.json", Body=b"keep")
    s3.put_object(Bucket="shared", Key="compat/path-style-0123.txt", Body=b"keep")
    s3.put_object(Bucket="shared", Key="qwcert/someone-elses-run/x", Body=b"keep")
    run = tmp_path / "run"
    assert main(["validate", "--endpoint", moto_server_endpoint, "--bucket", "shared",
                 *KEYS, "--run-dir", str(run), *SHORT]) == 0
    run_id = read_json(run / "manifest.json")["run_id"]
    assert _keys(s3, "shared", f"qwcert/{run_id}/") == []
    assert sorted(_keys(s3, "shared")) == [
        "compat/path-style-0123.txt",
        "production/index.json",
        "qwcert/someone-elses-run/x",
    ]
    summary = read_json(run / "cleanup.json")
    assert summary["complete"] and summary["objects_deleted"] > 0


def test_every_object_a_run_writes_is_inside_its_own_folder(moto_server_endpoint, tmp_path):
    """The rule that makes a safe cleanup possible at all."""
    s3 = _s3(moto_server_endpoint)
    run = tmp_path / "run"
    assert main(["validate", "--endpoint", moto_server_endpoint, "--bucket", "own-folder",
                 *KEYS, "--run-dir", str(run), *SHORT, "--keep-objects"]) == 0
    run_id = read_json(run / "manifest.json")["run_id"]
    keys = _keys(s3, "own-folder")
    assert keys, "--keep-objects should leave the objects in place"
    assert all(k.startswith(f"qwcert/{run_id}/") for k in keys), keys
    assert any("/compat/" in k for k in keys)  # compatibility objects too


def test_cleanup_can_run_later_and_twice(moto_server_endpoint, tmp_path):
    s3 = _s3(moto_server_endpoint)
    run = tmp_path / "run"
    main(["validate", "--endpoint", moto_server_endpoint, "--bucket", "later",
          *KEYS, "--run-dir", str(run), *SHORT, "--keep-objects"])
    assert _keys(s3, "later")
    assert main(["cleanup", "--run-dir", str(run), *KEYS]) == 0
    assert _keys(s3, "later") == []
    assert main(["cleanup", "--run-dir", str(run), *KEYS]) == 0  # nothing left to do


def _client(endpoint):
    return QwS3Client(QwS3Config.from_flavor("none", endpoint, "testing", "testing"))


RUN = "0123456789abcdef0123456789abcdef"


def test_unfinished_multipart_uploads_are_aborted(moto_server_endpoint):
    """They hold storage without showing up as objects."""
    s3 = _s3(moto_server_endpoint)
    s3.create_bucket(Bucket="parts")
    s3.create_multipart_upload(Bucket="parts", Key=f"qwcert/{RUN}/half.split")
    s3.create_multipart_upload(Bucket="parts", Key="other/half.split")
    summary = clean_run(_client(moto_server_endpoint), "parts", RUN)
    assert summary["multipart_uploads_aborted"] == 1
    remaining = [u["Key"] for u in s3.list_multipart_uploads(Bucket="parts").get("Uploads", [])]
    assert remaining == ["other/half.split"]


def test_old_versions_are_removed_in_a_versioned_bucket(moto_server_endpoint):
    s3 = _s3(moto_server_endpoint)
    s3.create_bucket(Bucket="versioned")
    s3.put_bucket_versioning(Bucket="versioned", VersioningConfiguration={"Status": "Enabled"})
    for body in (b"one", b"two"):
        s3.put_object(Bucket="versioned", Key=f"qwcert/{RUN}/file", Body=body)
    s3.put_object(Bucket="versioned", Key="keep/file", Body=b"keep")
    summary = clean_run(_client(moto_server_endpoint), "versioned", RUN)
    assert summary["complete"]
    versions = s3.list_object_versions(Bucket="versioned")
    left = {v["Key"] for v in versions.get("Versions", []) + versions.get("DeleteMarkers", [])}
    assert left == {"keep/file"}


@pytest.mark.parametrize("bad", ["", "abc", "../", "*", RUN + "/..", RUN.upper()])
def test_a_wrong_run_id_is_refused_rather_than_widening_the_prefix(bad):
    """An empty or short id would turn the prefix into every run, or the bucket."""
    with pytest.raises(ValueError, match="not a run id"):
        run_prefix(bad)


def test_cleanup_refuses_a_run_that_is_still_going(tmp_path):
    from run_validation import cleanup_run

    (tmp_path / ".running").write_text("")
    with pytest.raises(ValueError, match="still running"):
        cleanup_run(tmp_path, "testing", "testing")


def test_old_compat_objects_are_removed_only_when_their_name_matches(moto_server_endpoint):
    """Versions before per-run folders wrote into a shared compat/ folder."""
    from src.cleanup import clean_old_compat_objects

    s3 = _s3(moto_server_endpoint)
    s3.create_bucket(Bucket="legacy")
    generated = [
        "compat/path-style-0123456789abcdef0123456789abcdef.txt",
        "compat/multipart-0123456789abcdef0123456789abcdef.split",
        "compat/checksum-fedcba9876543210fedcba9876543210.split",
    ]
    look_alikes = [
        "compat/path-style-notarandomid.txt",
        "compat/multipart-0123456789abcdef0123456789abcdef.split.backup",
        "compat/report.txt",
        "compatibility/range-0123456789abcdef0123456789abcdef.txt",
    ]
    for key in generated + look_alikes:
        s3.put_object(Bucket="legacy", Key=key, Body=b"x")
    result = clean_old_compat_objects(_client(moto_server_endpoint), "legacy")
    assert result["old_compat_objects_deleted"] == 3
    assert sorted(_keys(s3, "legacy")) == sorted(look_alikes)
