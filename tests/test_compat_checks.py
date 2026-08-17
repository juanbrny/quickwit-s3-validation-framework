"""
Tests the exact five checks presented in the customer-facing deck
("Five Checks, One Command" / "A Pass/Fail Table, Not a Guess") --
path-style addressing, multipart upload, multi-object delete, range-GET
semantics, and checksum handling.

These run compat_checks.py's real functions (run_all_checks, probe_flavor)
against moto instead of a live vendor endpoint, to confirm the checks
themselves are implemented correctly -- e.g. that check_multipart_upload
really does route through the flavor-aware client wrapper and not raw
boto3 (see the bug this caught during development: two checks originally
bypassed cfg.disable_multipart_upload / cfg.disable_multi_object_delete by
calling the raw client directly, which would have made probe_flavor never
succeed for a vendor whose whole reason for needing that flavor override is
that the raw operation doesn't work).
"""
from src import compat_checks
from src.qw_s3_client import QwS3Client, QwS3Config

TEST_BUCKET = "qw-cert-test"  # must match tests/conftest.py's moto_s3 fixture


def _client(**overrides):
    cfg = QwS3Config(
        endpoint_url=None, access_key="testing", secret_key="testing",
        region="us-east-1", **overrides,
    )
    return QwS3Client(cfg)


def test_all_five_checks_pass_against_a_compliant_backend(moto_s3):
    """
    moto emulates AWS S3 closely enough that the default (flavor: none)
    configuration should pass every check -- this is the same "flavor: none"
    column shown in the deck's sample results table.
    """
    c = _client()
    results = compat_checks.run_all_checks(c, TEST_BUCKET)

    expected_checks = {
        "path_style_addressing", "multipart_upload", "multi_object_delete",
        "range_get_semantics", "checksum_algorithm",
    }
    assert set(results.keys()) == expected_checks

    for name, r in results.items():
        assert r["passed"], f"{name} unexpectedly failed against moto: {r['detail']}"


def test_multipart_check_respects_disable_multipart_upload_flag(moto_s3):
    """
    Regression test for the bug described in the module docstring: this
    check must go through client.put_split() (which honors
    cfg.disable_multipart_upload), not call CreateMultipartUpload directly --
    otherwise a `gcs`-flavor config (which disables multipart) would always
    fail this check even when the single-PutObject fallback works fine.
    """
    c = _client(disable_multipart_upload=True)
    passed, detail = compat_checks.check_multipart_upload(c, TEST_BUCKET)
    assert passed, detail
    assert "multipart disabled" in detail


def test_multi_object_delete_check_respects_disable_flag(moto_s3):
    """Same regression class as above, for the bulk-delete fallback path."""
    c = _client(disable_multi_object_delete=True)
    passed, detail = compat_checks.check_multi_object_delete(c, TEST_BUCKET)
    assert passed, detail
    assert "delete_object_loop" in detail


def test_range_get_check_validates_actual_byte_content(moto_s3):
    """
    Not just "did the request succeed" -- check_range_get compares the
    returned bytes against the known payload for start-end, open-ended, and
    suffix ranges, since a backend can return 200 OK with the wrong slice.
    """
    c = _client()
    passed, detail = compat_checks.check_range_get(c, TEST_BUCKET)
    assert passed, detail
    assert "start-end=True" in detail
    assert "open-ended=True" in detail
    assert "suffix=True" in detail


def test_probe_flavor_recommends_none_for_a_fully_compliant_endpoint(moto_s3, monkeypatch):
    """
    Restrict the probe order to just `none` so the test isn't sensitive to
    moto's handling of the artificial region strings the `minio`/`garage`
    flavor presets use (region overridden to the literal string "minio" or
    "garage") -- that's a real-vendor concern, not something worth coupling
    this unit test to. The control flow being tested here is: try flavor,
    run all checks, short-circuit on full pass, return the recommended
    flavor and its generated storage.s3.yaml block.
    """
    monkeypatch.setattr(compat_checks, "AUTO_PROBE_ORDER", ["none"])
    result = compat_checks.probe_flavor(
        endpoint_url=None, access_key="testing", secret_key="testing",
        bucket=TEST_BUCKET, region="us-east-1",
    )
    assert result["recommended_flavor"] == "none"
    assert result["attempts"]["none"]["all_passed"] is True
    # `none` has no overrides, so its yaml block is just the bare storage.s3 stanza
    assert result["attempts"]["none"]["yaml"].strip() == "storage:\n  s3:"
