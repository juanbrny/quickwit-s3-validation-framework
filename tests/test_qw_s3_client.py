"""
Verifies qw_s3_client.py's flavor-aware behavior actually branches the way
docs/01_s3_interaction_analysis.md section 2 says it should -- single PUT vs
multipart, bulk delete vs per-object fallback, path-style addressing, and
range-GET semantics -- against moto's in-memory S3 emulator.
"""
from src.qw_s3_client import QwS3Client, QwS3Config

TEST_BUCKET = "qw-cert-test"  # must match tests/conftest.py's moto_s3 fixture


def _client(**overrides):
    cfg = QwS3Config(
        endpoint_url=None, access_key="testing", secret_key="testing",
        region="us-east-1", **overrides,
    )
    return QwS3Client(cfg)


def test_put_and_get_full_roundtrip(moto_s3):
    c = _client()
    payload = b"hello quickwit" * 100
    put_res = c.put_split(TEST_BUCKET, "test/roundtrip.split", payload)
    assert put_res["ok"], put_res.get("error")
    assert put_res["op"] == "put_object"  # below the multipart threshold

    got = c.get_full(TEST_BUCKET, "test/roundtrip.split")
    assert got["ok"]
    assert got["bytes"] == len(payload)


def test_multipart_upload_completes(moto_s3):
    c = _client()
    payload = b"x" * (12 * 1024 * 1024)  # 12MB
    resp = c._multipart_put(TEST_BUCKET, "test/multipart.split", payload,
                             part_size=5 * 1024 * 1024)
    assert "ETag" in resp

    got = c.get_full(TEST_BUCKET, "test/multipart.split")
    assert got["ok"]
    assert got["bytes"] == len(payload)


def test_disable_multipart_upload_falls_back_to_single_put(moto_s3):
    """Mirrors the `gcs` flavor: disable_multipart_upload: true."""
    c = _client(disable_multipart_upload=True)
    payload = b"y" * (6 * 1024 * 1024)  # would trigger multipart if enabled
    res = c.put_split(TEST_BUCKET, "test/no-multipart.split", payload)
    assert res["ok"], res.get("error")
    assert res["op"] == "put_object"


def test_bulk_delete_removes_all_keys(moto_s3):
    c = _client()
    keys = [f"test/del-{i}.txt" for i in range(5)]
    for k in keys:
        c.put_split(TEST_BUCKET, k, b"x")
    res = c.delete_batch(TEST_BUCKET, keys)
    assert res["ok"]
    assert res["op"] == "delete_objects_bulk"
    for k in keys:
        assert not c.get_full(TEST_BUCKET, k)["ok"]


def test_disable_multi_object_delete_uses_per_object_fallback(moto_s3):
    """Mirrors the `gcs`/`digital_ocean` flavors: disable_multi_object_delete: true."""
    c = _client(disable_multi_object_delete=True)
    keys = [f"test/del2-{i}.txt" for i in range(3)]
    for k in keys:
        c.put_split(TEST_BUCKET, k, b"x")
    res = c.delete_batch(TEST_BUCKET, keys)
    assert res["ok"]
    assert res["op"] == "delete_object_loop"
    for k in keys:
        assert not c.get_full(TEST_BUCKET, k)["ok"]


def test_range_get_start_end_and_open_ended(moto_s3):
    c = _client()
    payload = bytes(range(256)) * 50  # 12,800 bytes, content is checkable
    c.put_split(TEST_BUCKET, "test/range.bin", payload)

    r1 = c.get_range(TEST_BUCKET, "test/range.bin", 0, 99)
    assert r1["ok"] and r1["bytes"] == 100

    r2 = c.get_range(TEST_BUCKET, "test/range.bin", len(payload) - 50, None)
    assert r2["ok"] and r2["bytes"] == 50


def test_path_style_addressing_still_reaches_the_bucket(moto_s3):
    """Mirrors the `minio`/`garage`/`digital_ocean` flavors: force_path_style_access: true."""
    c = _client(force_path_style=True)
    res = c.put_split(TEST_BUCKET, "test/path-style.txt", b"ok")
    assert res["ok"], res.get("error")


def test_list_prefix_returns_exactly_what_was_written(moto_s3):
    c = _client()
    written = [f"test/list-prefix/{i}.txt" for i in range(3)]
    for k in written:
        c.put_split(TEST_BUCKET, k, b"x")
    res = c.list_prefix(TEST_BUCKET, "test/list-prefix/")
    assert res["ok"]
    assert sorted(res["keys"]) == sorted(written)


def test_checksum_algorithms_all_succeed(moto_s3):
    """
    Exercises all three checksum_algorithm code paths from
    docs/01 section 2 -- crc32c (SDK-native), md5 (legacy Content-MD5),
    and disabled. A vendor that only supports one of these is exactly what
    the `checksum_algorithm` compat check (compat_checks.py) is meant to
    catch when run against a real endpoint instead of moto.
    """
    for algo in ("crc32c", "md5", "disabled"):
        c = _client(checksum_algorithm=algo)
        res = c.put_split(TEST_BUCKET, f"test/checksum-{algo}.txt", b"payload")
        assert res["ok"], f"{algo}: {res.get('error')}"
