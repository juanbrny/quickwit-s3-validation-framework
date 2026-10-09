"""
Verifies qw_s3_client.py's flavor-aware behavior actually branches the way
docs/background/01_s3_interaction_analysis.md section 2 says it should -- single PUT vs
multipart, bulk delete vs per-object fallback, path-style addressing, and
range-GET semantics -- against moto's in-memory S3 emulator.
"""
import pytest

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


def test_put_split_routes_to_multipart_above_the_threshold(moto_s3):
    """put_split()'s own size check, not _multipart_put() called directly --
    confirms the 128 MiB threshold (MULTIPART_THRESHOLD_BYTES) actually
    decides the route, matching Pomsky's real MultiPartPolicy default."""
    c = _client()
    payload = b"z" * (130 * 1024 * 1024)  # above the 128 MiB multipart threshold
    res = c.put_split(TEST_BUCKET, "test/above-threshold.split", payload)
    assert res["ok"], res.get("error")
    assert res["op"] == "multipart_upload"

    got = c.get_full(TEST_BUCKET, "test/above-threshold.split")
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
    """Mirrors the `gcs` flavor: disable_multipart_upload: true. Payload is
    above MULTIPART_THRESHOLD_BYTES (128 MiB) so this actually exercises the
    flag -- a smaller payload would take the single-PUT path regardless."""
    c = _client(disable_multipart_upload=True)
    payload = b"y" * (130 * 1024 * 1024)  # above the 128 MiB multipart threshold
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


def _sent_checksum_headers(cfg, payload=b"checksum-payload"):
    """Capture the checksum headers a real PutObject would carry."""
    client = QwS3Client(cfg)
    captured = {}

    def record(request, **kwargs):
        captured.update(
            {
                name.lower(): value
                for name, value in request.headers.items()
                if "checksum" in name.lower() or name.lower() in ("x-amz-trailer", "content-md5")
            }
        )

    client._client.meta.events.register("before-send.s3.PutObject", record)
    client.ensure_bucket(TEST_BUCKET)
    assert client.put_split(TEST_BUCKET, "compat/checksum-headers", payload)["ok"]
    return captured


@pytest.mark.parametrize(
    "algorithm,expected_trailer",
    [("crc32c", b"x-amz-checksum-crc32c"), ("md5", None), ("disabled", None)],
)
def test_checksum_setting_decides_what_goes_on_the_wire(
    moto_s3, algorithm, expected_trailer
):
    """
    botocore 1.36 and later attach their own CRC32 trailer to every upload
    unless `request_checksum_calculation` says otherwise. Without that
    option, the `md5` and `disabled` settings still sent
    `x-amz-trailer: x-amz-checksum-crc32`, so they did not avoid the
    trailing checksum they exist to avoid. SeaweedFS writes that trailer
    into the stored object (seaweedfs issue 6548) and Scality CloudServer
    rejects it with 400 BadRequest (cloudserver issue 5553), so the
    seaweedfs and scality flavors would have failed for the original reason
    while reporting a different configuration.
    """
    cfg = QwS3Config(
        endpoint_url=None, access_key="testing", secret_key="testing",
        region="us-east-1", checksum_algorithm=algorithm,
    )
    headers = _sent_checksum_headers(cfg)
    assert headers.get("x-amz-trailer") == expected_trailer
    assert ("content-md5" in headers) is (algorithm == "md5")


def test_descriptor_limit_is_raised_when_the_system_allows_it(monkeypatch):
    """
    A concurrency sweep needs one socket per in-flight request. Raise the soft
    limit automatically, so a default macOS shell can still run the sweep.
    """
    import resource

    from src import qw_s3_client

    state = {"soft": 256, "hard": resource.RLIM_INFINITY}

    def getrlimit(which):
        return state["soft"], state["hard"]

    def setrlimit(which, limits):
        state["soft"] = limits[0]

    monkeypatch.setattr(resource, "getrlimit", getrlimit)
    monkeypatch.setattr(resource, "setrlimit", setrlimit)
    assert qw_s3_client.ensure_file_descriptors(2112) == 2112


def test_an_unraisable_descriptor_limit_stops_the_sweep_with_guidance(monkeypatch):
    """
    Running anyway would exhaust the descriptor table part way through. The
    sweep reads any error at a concurrency level as the backend failing, so a
    runner limit would be recorded as a vendor fault.
    """
    import resource

    from src import qw_s3_client

    monkeypatch.setattr(resource, "getrlimit", lambda which: (256, 256))
    monkeypatch.setattr(
        resource, "setrlimit", lambda which, limits: (_ for _ in ()).throw(OSError())
    )
    with pytest.raises(ValueError) as error:
        qw_s3_client.ensure_file_descriptors(2112)
    message = str(error.value)
    assert "256 open files" in message
    assert "ulimit -n 2112" in message
    assert "--levels" in message


@pytest.mark.parametrize(
    "verify,expected",
    [(True, True), (False, False), ("/etc/ssl/private-ca.pem", "/etc/ssl/private-ca.pem")],
)
def test_tls_verification_setting_reaches_the_client(moto_s3, verify, expected):
    """
    On-premises appliances present certificates from a private authority. With
    no way to supply one, a run can only use plain HTTP, which is not how the
    endpoint serves production traffic.
    """
    cfg = QwS3Config(
        endpoint_url=None, access_key="testing", secret_key="testing",
        region="us-east-1", verify_tls=verify,
    )
    client = QwS3Client(cfg)
    assert client._client.meta.endpoint_url is not None
    # botocore stores the caller's choice on the endpoint's TLS context.
    assert client._client._endpoint.http_session._verify == expected


def test_a_dropped_connection_counts_as_one_failed_request(moto_s3, monkeypatch):
    """
    A dropped connection or a timeout is a failed request, which is what the
    failed-requests check counts. It used to escape instead, which stopped a
    whole 30-minute workload over one request and recorded nothing.
    """
    from botocore.exceptions import EndpointConnectionError

    client = _client()
    client.ensure_bucket(TEST_BUCKET)

    def dropped(**kwargs):
        raise EndpointConnectionError(endpoint_url="http://storage.test")

    monkeypatch.setattr(client._client, "get_object", dropped)
    result = client.get_range(TEST_BUCKET, "any", 0, 99)
    assert result["ok"] is False
    assert result["error"] == "EndpointConnectionError"


def test_running_out_of_files_is_blamed_on_the_machine(moto_s3, monkeypatch):
    """
    macOS gives a program 256 open files by default. Running out is a limit of
    the test machine. Counting it as a failed request would blame the storage.
    """
    import errno

    from botocore.exceptions import EndpointConnectionError

    from src.qw_s3_client import RunnerLimitError

    client = _client()
    client.ensure_bucket(TEST_BUCKET)

    def out_of_files(**kwargs):
        try:
            raise OSError(errno.EMFILE, "Too many open files")
        except OSError as cause:
            raise EndpointConnectionError(endpoint_url="http://storage.test") from cause

    monkeypatch.setattr(client._client, "get_object", out_of_files)
    with pytest.raises(RunnerLimitError, match="not of the storage"):
        client.get_range(TEST_BUCKET, "any", 0, 99)


def test_the_workload_step_asks_for_enough_open_files():
    """Four search workers can need 620 sockets at once, far above macOS's 256."""
    from run_validation import load_file_descriptors
    from src.workload_model import load_config

    assert load_file_descriptors(load_config()) >= 4 * 155 + 256
