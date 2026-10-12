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
    PROFILE_DIR,
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


# ---- deriving the profile from a measured AWS run --------------------------

import json
import random

import yaml

from src.reference_profile import derive_profile, render_profile

MIB = 1024 * 1024


def _aws_run(tmp_path, download_mib_s=90.0, upload_mib_s=70.0, small_p50=0.020,
             endpoint="https://s3.us-east-1.amazonaws.com", pauses=None,
             idle_p50=0.025, searches=2000, deletes=300, delete_s=0.25,
             round_trip_ms=None):
    """A finished AWS run with known speeds, so the derived numbers are checkable."""
    from examples.make_sample_report import make_bundle
    from src.run_store import digest, read_json, write_json

    run = make_bundle(tmp_path / "aws", True)
    rng = random.Random(7)
    t = 1_000_000.0
    query = []
    for i in range(3000):
        latency = small_p50 * (1 + rng.expovariate(1 / 0.15))
        query.append({"ts": t + i, "worker": "searcher", "op": "get_term_or_field" if i % 2 else "get_doc",
                      "ok": True, "latency_s": latency, "bytes": 8192, "error": None})
    for i in range(searches):  # the sample run lasts 180 s at a target of 10 per second
        query.append({"ts": t + i, "worker": "searcher", "op": "query_wall_clock", "ok": True,
                      "latency_s": small_p50 * (2 + rng.random()), "bytes": 0, "error": None})
    size = 132 * MIB
    ingest = []
    for i in range(40):
        for op, rate in (("get_object_full", download_mib_s), ("put_object", upload_mib_s)):
            ingest.append({"ts": t + i, "worker": "indexer-0", "op": op, "ok": True,
                           "latency_s": small_p50 + size / MIB / rate, "bytes": size, "error": None})
    probes = [{"probe": "delete_visibility", "ts": t + i, "delete_ok": True, "is_gone": True,
               "success": True, "within_deadline": True, "elapsed_s": delete_s + 0.05,
               "delete_latency_s": delete_s * (0.8 + 0.4 * rng.random())}
              for i in range(deletes)]
    (run / "consistency.jsonl").write_text("".join(json.dumps(r) + "\n" for r in probes))
    (run / "query.jsonl").write_text("".join(json.dumps(r) + "\n" for r in query))
    (run / "ingest_merge.jsonl").write_text("".join(json.dumps(r) + "\n" for r in ingest))
    manifest = read_json(run / "manifest.json")
    manifest["identity"]["endpoint"] = endpoint
    load = manifest["stages"]["load"]
    load["options"]["flavor"] = "aws"
    load["options"]["runner_location"] = "ec2 c5n.2xlarge us-east-1a"
    if pauses:
        load["pauses"] = pauses
    if round_trip_ms:
        manifest["stages"]["machine"]["machine"]["round_trip"].update(
            min_ms=round_trip_ms, median_ms=round_trip_ms
        )
    # The idle read test: the same small reads with nothing else running.
    fanout = read_json(run / "fanout.json")
    for level in fanout["levels"]:
        if level["concurrency"] == 1:
            level["p50_per_request_s"] = idle_p50
    write_json(run / "fanout.json", fanout)
    manifest["stages"]["fanout"]["artifacts"]["fanout.json"] = digest(run / "fanout.json")
    for name in ("query.jsonl", "ingest_merge.jsonl", "consistency.jsonl"):
        load["artifacts"][name] = digest(run / name)
    write_json(run / "manifest.json", manifest)
    return run


def test_the_profile_recovers_the_speeds_of_a_known_run(tmp_path):
    values, errors, warnings = derive_profile(_aws_run(tmp_path))
    assert errors == [] and warnings == []
    assert values["per_stream_mb_s"] == pytest.approx(90.0, rel=0.01)
    assert values["upload_per_stream_mb_s"] == pytest.approx(70.0, rel=0.01)
    assert 0.020 < values["first_byte_p99_s"] < 0.2
    assert values["fanout_tail_factor"] > 0


def test_a_written_profile_loads_and_grades_uploads_with_their_own_speed(tmp_path):
    values, _, _ = derive_profile(_aws_run(tmp_path, download_mib_s=90.0, upload_mib_s=45.0))
    path = tmp_path / "profile.yaml"
    path.write_text(render_profile(values, version=2))
    profile = load_profile(str(path))
    assert profile["status"] == "published" and profile["version"] == 2
    size = 100 * MIB
    download = reference_p99(profile, "get_object_full", size)
    upload = reference_p99(profile, "put_object", size)
    assert upload > download  # 45 MiB/s up is slower than 90 MiB/s down


@pytest.mark.parametrize("change,message", [
    (dict(endpoint="http://192.168.0.10:10444"), "not AWS S3"),
    (dict(pauses=[{"started_epoch": 1.0, "seconds": 90}]), "paused"),
    (dict(round_trip_ms=40.0), "40.0 ms from AWS S3"),
])
def test_runs_that_cannot_give_honest_numbers_are_refused(tmp_path, change, message):
    values, errors, _ = derive_profile(_aws_run(tmp_path, **change))
    assert values is None
    assert any(message in e for e in errors)


def test_a_runner_outside_the_region_is_flagged(tmp_path):
    """From a laptop, small reads took 140 ms in the median; in region, about 20."""
    _, errors, warnings = derive_profile(_aws_run(tmp_path, small_p50=0.140, idle_p50=0.130))
    assert errors == []
    assert any("outside" in w for w in warnings)


def test_small_transfers_are_never_used_to_measure_speed(tmp_path):
    """A small transfer finishes at first-byte speed: dividing by its tiny
    remaining time once gave an upload speed of 2,879 MiB/s."""
    from examples.make_sample_report import make_bundle

    values, errors, _ = derive_profile(make_bundle(tmp_path / "sample", True))
    assert values is None
    assert any("large downloads" in e for e in errors)


def test_the_profile_command_raises_the_version(tmp_path, capsys):
    from run_validation import main

    out = tmp_path / "aws-s3-ec2-same-region.yaml"
    out.write_text((PROFILE_DIR / "aws-s3-ec2-same-region.yaml").read_text())
    before = yaml.safe_load(out.read_text())["version"]
    assert main(["profile", "--run-dir", str(_aws_run(tmp_path)), "--out", str(out)]) == 0
    written = load_profile(str(out))
    assert written["version"] == before + 1
    assert written["status"] == "published"
    assert "first_byte_p99_s" in capsys.readouterr().out


def test_an_overloaded_test_machine_is_refused(tmp_path):
    """
    Recorded on an m5a.8xlarge in us-east-1 at 1 TB/day: small reads took 30 ms
    with nothing else running, and 71 ms during the workload. One Python process
    could not send 294 requests per second without slowing every one of them.
    A profile from that run would have been about 20 times too lenient.
    """
    values, errors, _ = derive_profile(_aws_run(tmp_path, small_p50=0.071, idle_p50=0.030))
    assert values is None
    assert any("overloaded" in e for e in errors)


def test_a_run_that_missed_its_search_rate_is_refused(tmp_path):
    """The same run reached 5.7 searches per second of the 10 it should send."""
    values, errors, _ = derive_profile(_aws_run(tmp_path, searches=1000))
    assert values is None
    assert any("could not generate its own workload" in e for e in errors)


def test_deletes_are_graded_against_their_own_figure():
    """
    Graded as small reads, AWS S3 itself failed: from an m5a.8xlarge in
    us-east-1, bulk deletes of 10 keys took about 300 ms, against a limit of
    255 ms (85 ms x 3).
    """
    profile = load_profile()
    assert reference_p99(profile, "delete_objects_bulk", 0) == profile["latency"]["delete_p99_s"]
    assert reference_p99(profile, "delete_objects_bulk", 0) * 3.0 > 0.301
    # A flavor without bulk delete sends one request for each key.
    assert reference_p99(profile, "delete_object_loop", 0, 10) == pytest.approx(
        10 * profile["latency"]["delete_p99_s"])


def test_the_profile_measures_deletes_from_the_visibility_checks(tmp_path):
    values, errors, _ = derive_profile(_aws_run(tmp_path, delete_s=0.25))
    assert errors == []
    assert 0.25 < values["delete_p99_s"] <= 0.30
    assert values["counts"]["deletes"] == 300
    path = tmp_path / "profile.yaml"
    path.write_text(render_profile(values, version=3))
    assert load_profile(str(path))["latency"]["delete_p99_s"] == values["delete_p99_s"]


def test_a_run_without_timed_deletes_is_refused(tmp_path):
    """The first AWS run on the load engine had 4 merge deletes and no timed probes."""
    values, errors, _ = derive_profile(_aws_run(tmp_path, deletes=0))
    assert values is None
    assert any("deletes" in e and "Record a new run" in e for e in errors)
