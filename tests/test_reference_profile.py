"""
Tests the bundled AWS S3 reference profile, which is what the framework grades
latency against when the operator supplies no measured AWS run.

Most vendors have no AWS account. Without a bundled reference, every latency
criterion would stay INCONCLUSIVE for them, and the report would be unable to
answer "is it as fast as AWS S3". These tests cover the model that turns two
published numbers into a per-operation limit.
"""
import math

import pytest

from src.reference_profile import (
    DEFAULT_PROFILE,
    describe,
    load_profile,
    reference_p99,
)

MIB = 1024 * 1024
GIB = 1024 * MIB


@pytest.fixture
def profile():
    return load_profile(DEFAULT_PROFILE)


def test_the_bundled_profile_loads_and_declares_its_provenance(profile):
    """
    A published bar must be auditable. The report prints this block, so a
    vendor can see what they are being graded against and challenge it.
    """
    assert profile["id"] == DEFAULT_PROFILE
    assert profile["status"] in ("provisional", "published")
    context = profile["context"]
    assert context["storage_class"] and context["runner"] and context["source"]
    assert len(context["references"]) >= 2
    assert set(describe(profile)) == {
        "id", "version", "status", "title", "context", "latency", "throughput"
    }


def test_small_operations_cost_one_round_trip(profile):
    """
    A 2 KiB document read and an 8 KiB term read move almost no data, so the
    reference is the round trip itself. Transfer time must not distort it.
    """
    latency = profile["latency"]["first_byte_p99_s"]
    for size in (0, 2 * 1024, 8 * 1024, 64 * 1024):
        predicted = reference_p99(profile, "get_term_or_field", size)
        assert latency <= predicted < latency * 1.01


def test_large_reads_are_bound_by_transfer_rate(profile):
    """
    An 8 GB mature split read takes about 91 seconds at 90 MB/s. The round trip
    contributes a tenth of a percent, so this criterion is a throughput test.
    """
    predicted = reference_p99(profile, "get_object_full", 8 * GIB)
    expected = 8 * GIB / (90 * MIB)
    assert predicted == pytest.approx(expected + 0.085, rel=0.001)
    assert predicted > 90


def test_multipart_uploads_use_concurrent_parts(profile):
    """
    Quickwit uploads above 128 MiB as multipart with a 5 GiB target part size,
    and the parts upload at the same time. Modelling an 8 GB split as a single
    stream would make the limit roughly twice too lenient.
    """
    single = reference_p99(profile, "put_object", 100 * MIB)
    assert single == pytest.approx(0.085 + 100 / 90, rel=0.001)

    two_parts = reference_p99(profile, "multipart_upload", 8 * GIB)
    one_stream = 8 * GIB / (90 * MIB)
    assert two_parts == pytest.approx(0.085 + one_stream / 2, rel=0.001)

    # The number of parts is capped, so a huge object does not predict
    # unlimited parallelism.
    huge = reference_p99(profile, "multipart_upload", 500 * GIB)
    parts = min(math.ceil(500 / 5), profile["throughput"]["max_concurrent_parts"])
    assert huge == pytest.approx(0.085 + 500 * GIB / (90 * MIB * parts), rel=0.001)


def test_query_fan_out_allows_for_the_slowest_read(profile):
    """
    A query finishes when its slowest concurrent read returns. The p99 of that
    maximum sits above the p99 of one read, so the profile applies a factor.
    """
    one_read = profile["latency"]["first_byte_p99_s"]
    predicted = reference_p99(profile, "query_wall_clock", 0)
    assert predicted == pytest.approx(one_read * profile["latency"]["fanout_tail_factor"])
    assert predicted > one_read


def test_profile_selection_and_validation():
    assert load_profile("none") is None
    assert load_profile(None) is None
    assert reference_p99(None, "put_object", 1024) is None
    with pytest.raises(ValueError, match="Unknown reference profile"):
        load_profile("not-a-profile")


def test_a_profile_missing_a_required_number_is_rejected(tmp_path):
    """A profile with no throughput figure would silently produce no limits."""
    path = tmp_path / "broken.yaml"
    path.write_text(
        "id: broken\nversion: 1\n"
        "latency: {first_byte_p99_s: 0.085, fanout_tail_factor: 2.0}\n"
    )
    with pytest.raises(ValueError, match="throughput.per_stream_mb_s"):
        load_profile(str(path))
