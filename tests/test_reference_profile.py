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
    for size in (0, 2 * 1024, 8 * 1024):
        predicted = reference_p99(profile, "get_term_or_field", size)
        assert latency <= predicted < latency * 1.01


def test_large_reads_are_bound_by_transfer_rate(profile):
    """
    An 8 GB mature split read takes minutes at one stream's speed. The round
    trip contributes a tiny fraction, so this check is a throughput test.
    """
    first_byte = profile["latency"]["first_byte_p99_s"]
    speed = profile["throughput"]["per_stream_mb_s"] * MIB
    predicted = reference_p99(profile, "get_object_full", 8 * GIB)
    assert predicted == pytest.approx(first_byte + 8 * GIB / speed, rel=0.001)
    assert first_byte / predicted < 0.001


def test_multipart_uploads_use_concurrent_parts(profile):
    """
    Quickwit uploads above 128 MiB as multipart with a 5 GiB target part size,
    and the parts upload at the same time. Modelling an 8 GB split as a single
    stream would make the limit roughly twice too lenient.
    """
    first_byte = profile["latency"]["first_byte_p99_s"]
    rates = profile["throughput"]
    speed = rates.get("upload_per_stream_mb_s", rates["per_stream_mb_s"]) * MIB
    single = reference_p99(profile, "put_object", 100 * MIB)
    assert single == pytest.approx(first_byte + 100 * MIB / speed, rel=0.001)

    two_parts = reference_p99(profile, "multipart_upload", 8 * GIB)
    assert two_parts == pytest.approx(first_byte + 8 * GIB / speed / 2, rel=0.001)

    # The number of parts is capped, so a huge object does not predict
    # unlimited parallelism.
    huge = reference_p99(profile, "multipart_upload", 500 * GIB)
    parts = min(math.ceil(500 / 5), rates["max_concurrent_parts"])
    assert huge == pytest.approx(first_byte + 500 * GIB / (speed * parts), rel=0.001)


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
             round_trip_ms=None, transfers=150, slow_fraction=0.0, slow_factor=0.2,
             merge_deletes=0, merge_delete_s=0.45, footers=300, footer_factor=2.0):
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
    for i in range(footers):  # the first read of a new file, slower on S3
        query.append({"ts": t + i, "worker": "searcher", "op": "get_footer", "ok": True,
                      "latency_s": footer_factor * small_p50 * (1 + rng.expovariate(1 / 0.15)),
                      "bytes": 65536, "error": None})
    for i in range(searches):  # the sample run lasts 180 s at a target of 10 per second
        query.append({"ts": t + i, "worker": "searcher", "op": "query_wall_clock", "ok": True,
                      "latency_s": small_p50 * (2 + rng.random()), "bytes": 0, "error": None})
    size = 132 * MIB
    ingest = []
    for i in range(transfers):
        for op, rate in (("get_object_full", download_mib_s), ("put_object", upload_mib_s)):
            # A few transfers run far slower, as real S3 transfers do.
            if rng.random() < slow_fraction:
                rate *= slow_factor
            ingest.append({"ts": t + i, "worker": "indexer-0", "op": op, "ok": True,
                           "latency_s": small_p50 + size / MIB / rate, "bytes": size, "error": None})
    probes = [{"probe": "delete_visibility", "ts": t + i, "delete_ok": True, "is_gone": True,
               "success": True, "within_deadline": True, "elapsed_s": delete_s + 0.05,
               "delete_latency_s": delete_s * (0.8 + 0.4 * rng.random())}
              for i in range(deletes)]
    for i in range(merge_deletes):
        ingest.append({"ts": t + i, "worker": "merger-0", "op": "delete_objects_bulk", "ok": True,
                       "latency_s": merge_delete_s * (0.8 + 0.4 * rng.random()), "bytes": 0,
                       "error": None})
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


def test_an_aws_run_passes_against_a_profile_made_from_itself(tmp_path, monkeypatch):
    """
    The reference run must agree with itself. A 90-minute AWS run at 10TB
    failed its merge reads: 4,610 ms against a limit of 4,373 ms. The profile
    took the median speed, so its "p99" described a typical transfer, and the
    slowest 1% of S3 transfers ran at less than half that speed.
    """
    from src import reference_profile
    from src.report_model import build_report

    run = _aws_run(tmp_path, slow_fraction=0.03, slow_factor=0.3)

    def grade(name):
        values, errors, _ = derive_profile(run)
        assert errors == []
        path = tmp_path / name
        path.write_text(render_profile(values, version=3))
        report = build_report(run, reference=str(path))
        return {c["id"]: c["status"] for c in report["checks"] if c["id"].endswith("_p99")}

    statuses = grade("slow-case.yaml")
    assert statuses and set(statuses.values()) == {"PASS"}, statuses
    # The median speed, as before, fails AWS S3 against its own figures.
    monkeypatch.setattr(reference_profile, "SLOW_SPEED_PCT", 50)
    assert grade("median.yaml")["get_object_full_p99"] == "FAIL"


def test_a_profile_needs_100_large_transfers_of_each_kind(tmp_path):
    """The speed 99 of 100 transfers beat is not measurable from 40 transfers."""
    values, errors, _ = derive_profile(_aws_run(tmp_path, transfers=40))
    assert values is None
    assert any("Only 40 large downloads" in e and "--tier 10TB" in e for e in errors)


def test_deletes_come_from_the_merges_when_the_run_has_enough(tmp_path):
    """
    The report grades the merges' 10-key deletes. A single-key delete from the
    visibility checks is faster, so it only fills in when merges are too few.
    """
    run = _aws_run(tmp_path / "long", merge_deletes=120, merge_delete_s=0.45)
    values, errors, _ = derive_profile(run)
    assert errors == []
    assert values["delete_p99_s"] > 0.45 and values["counts"]["deletes"] == 120
    assert "the merges' bulk deletes" == values["delete_source"]
    short = _aws_run(tmp_path / "short", merge_deletes=4)
    values, _, _ = derive_profile(short)
    assert values["counts"]["deletes"] == 304 and "single-key" in values["delete_source"]


def test_footer_reads_have_their_own_figure(tmp_path):
    """
    From an EC2 instance in us-east-1, footer reads had a p99 of 122 ms and
    other small reads 65 ms. Graded as small reads, AWS S3 passed with 8% to
    spare, and an independent run could fail by chance.
    """
    from src.report_model import build_report

    run = _aws_run(tmp_path, footer_factor=2.0)
    values, errors, _ = derive_profile(run)
    assert errors == [] and values["footer_p99_s"] > 1.5 * values["first_byte_p99_s"]
    path = tmp_path / "profile.yaml"
    path.write_text(render_profile(values, version=3))
    profile = load_profile(str(path))
    assert reference_p99(profile, "get_footer", 65536) > reference_p99(profile, "get_doc", 65536)
    footer = {c["id"]: c for c in build_report(run, reference=str(path))["checks"]}["get_footer_p99"]
    assert footer["status"] == "PASS" and footer["ratio"] < 0.6
    # An older profile without the figure still loads, and grades footers as small reads.
    del profile["latency"]["footer_p99_s"]
    assert reference_p99(profile, "get_footer", 65536) == reference_p99(profile, "get_doc", 65536)
