"""
Sanity-checks consistency_probes.py against a backend that is, by
construction, fully consistent (moto). If these fail, the bug is in the
probe code, not in whatever endpoint you'd eventually point it at -- a
probe that reports failure against a known-good backend is worse than no
probe at all, because it teaches you to distrust real failures too.
"""
from src.consistency_probes import (
    probe_delete_visibility,
    probe_list_after_write,
    probe_read_after_write,
)
from src.qw_s3_client import QwS3Client, QwS3Config

TEST_BUCKET = "qw-cert-test"  # must match tests/conftest.py's moto_s3 fixture


def _client():
    cfg = QwS3Config(endpoint_url=None, access_key="testing", secret_key="testing",
                      region="us-east-1")
    return QwS3Client(cfg)


def test_read_after_write_probe_succeeds(moto_s3):
    c = _client()
    result = probe_read_after_write(c, TEST_BUCKET, "test/consistency")
    assert result["success"], result


def test_list_after_write_probe_succeeds(moto_s3):
    c = _client()
    result = probe_list_after_write(c, TEST_BUCKET, "test/consistency")
    assert result["success"], result


def test_delete_visibility_probe_succeeds(moto_s3):
    c = _client()
    result = probe_delete_visibility(c, TEST_BUCKET, "test/consistency")
    assert result["success"], result
