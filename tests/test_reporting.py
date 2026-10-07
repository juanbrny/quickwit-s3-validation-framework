import json
import threading
from argparse import Namespace

import pytest

from src.report import _percentile, summarize_ops, compare_to_baseline
from src.report_model import (
    QUESTIONS,
    build_report,
    check,
    limit_ratio,
    overall,
    time_windows,
)
from src.report_render import render_html, render_markdown, write_reports
from src.run_store import RunSession, load_bundle, read_json, write_json, digest
from src.workload_model import load_config
from src.ingest_merge_sim import ResultSink, _synthetic_payload
from src.concurrency_fanout import FanoutLevelResult, summarize_fanout
from examples.make_sample_report import make_bundle
from run_certification import main


@pytest.fixture
def bundle(tmp_path):
    return make_bundle(tmp_path / "vendor")


def indexed(report):
    return {c["id"]: c for c in report["checks"]}


@pytest.mark.parametrize(
    "percentile,expected", [(0, 0.1), (50, 0.25), (90, 0.37), (99, 0.397), (100, 0.4)]
)
def test_percentiles(percentile, expected):
    assert _percentile([0.4, 0.2, 0.1, 0.3], percentile) == pytest.approx(expected)


def test_empty_and_invalid_percentiles():
    assert _percentile([], 99) is None
    assert _percentile([0.2], 99) == 0.2
    for value in (float("nan"), float("inf"), -0.1):
        with pytest.raises(ValueError):
            _percentile([value], 99)


def test_missing_baseline_never_passes():
    s = summarize_ops([dict(op="put_object", ok=True, latency_s=0.1)])
    verdict = compare_to_baseline(s, None, load_config()["pass_fail_bands"])
    assert verdict["put_object"]["passed"] is False


def test_throttles_are_separate_from_non_throttle_errors():
    rows = [
        dict(op="put_object", ok=False, error="SlowDown", latency_s=0.1),
        dict(op="put_object", ok=True, latency_s=0.2),
    ]
    result = summarize_ops(rows)["put_object"]
    assert result["error_pct"] == 50
    assert result["throttle_pct"] == 50
    assert result["non_throttle_error_pct"] == 0


@pytest.mark.parametrize(
    "statuses,expected",
    [
        ([], "INCONCLUSIVE"),
        (["PASS"], "CERTIFIED"),
        (["PASS", "NOT RUN"], "INCONCLUSIVE"),
        (["PASS", "INCONCLUSIVE"], "INCONCLUSIVE"),
        (["FAIL", "NOT RUN"], "NOT CERTIFIED"),
    ],
)
def test_overall_requires_complete_evidence(statuses, expected):
    checks = [
        check(str(i), "check", state, "", "", "") for i, state in enumerate(statuses)
    ]
    assert overall(checks, "none") == expected
    if statuses == ["PASS"]:
        assert overall(checks, "minio") == "CERTIFIED WITH DEVIATION"
        # AWS S3 is a target in its own right, not only the baseline. Its
        # flavor keeps every default, so it needs no deviation.
        assert overall(checks, "aws") == "CERTIFIED"


def test_aws_flavor_agrees_with_a_recommendation_of_none(bundle):
    """`aws` and `none` hold the same settings, so one satisfies the other."""
    manifest = read_json(bundle / "manifest.json")
    for stage in ("fanout", "put-fanout", "load"):
        manifest["stages"][stage]["options"]["flavor"] = "aws"
    write_json(bundle / "manifest.json", manifest)
    compat = read_json(bundle / "compat.json")
    compat["recommended_flavor"] = "none"
    write_json(bundle / "compat.json", compat)
    assert indexed(build_report(bundle))["flavor"]["status"] == "PASS"


def test_zero_operations_never_certifies(bundle):
    (bundle / "query.jsonl").write_text("")
    (bundle / "ingest_merge.jsonl").write_text("")
    result = build_report(bundle)
    assert result["verdict"] == "INCONCLUSIVE"
    assert indexed(result)["missing_put_object"]["status"] == "NOT RUN"
    assert indexed(result)["integrity"]["status"] == "INCONCLUSIVE"


def test_report_uses_saved_thresholds_and_detects_flavor_mismatch(bundle, tmp_path):
    baseline = make_bundle(tmp_path / "aws", True)
    manifest = read_json(bundle / "manifest.json")
    manifest["config"]["pass_fail_bands"]["query_wall_clock_p99_multiplier_vs_aws"] = (
        3.5
    )
    manifest["stages"]["put-fanout"]["options"]["flavor"] = "none"
    write_json(bundle / "manifest.json", manifest)
    result = build_report(bundle, baseline)
    assert indexed(result)["query_wall_clock_p99"]["status"] == "PASS"
    assert indexed(result)["flavor"]["status"] == "INCONCLUSIVE"
    assert indexed(result)["merge_backlog"]["status"] == "NOT RUN"


def test_baseline_mismatch_is_inconclusive(bundle, tmp_path):
    baseline = make_bundle(tmp_path / "aws", True)
    m = read_json(baseline / "manifest.json")
    m["stages"]["load"]["options"]["tier"] = "10TB"
    write_json(baseline / "manifest.json", m)
    checks = indexed(build_report(bundle, baseline))
    assert checks["baseline"]["status"] == "INCONCLUSIVE"
    assert checks["query_wall_clock_p99"]["status"] == "INCONCLUSIVE"


def test_unrecorded_stage_evidence_is_not_trusted(bundle):
    m = read_json(bundle / "manifest.json")
    m["stages"]["compat"]["artifacts"] = {}
    write_json(bundle / "manifest.json", m)
    assert indexed(build_report(bundle))["integrity"]["status"] == "INCONCLUSIVE"


def test_missing_put_fanout_is_visible(bundle):
    (bundle / "put-fanout.json").unlink()
    result = build_report(bundle)
    assert indexed(result)["put-fanout_efficiency"]["status"] == "NOT RUN"


def test_failed_low_concurrency_sweep_not_hidden():
    result = summarize_fanout([FanoutLevelResult(1, 0.1, [0.09], 1, 0)])
    assert result["degrades_at_concurrency"] == 1
    assert result["levels"][0]["error_count"] == 1
    empty = summarize_fanout([FanoutLevelResult(8, 0.1, [], 0, 0)])
    assert empty["degrades_at_concurrency"] == 8
    json.dumps(empty, allow_nan=False)


def test_sweep_threshold_does_not_round_a_failure_into_pass(bundle):
    sweep = read_json(bundle / "fanout.json")
    sweep["levels"][1]["efficiency"] = 0.3999
    write_json(bundle / "fanout.json", sweep)
    assert indexed(build_report(bundle))["fanout_efficiency"]["status"] == "FAIL"


def test_windows_include_idle_intervals_exclude_failed_and_merge_bytes():
    def row(ts, worker="indexer-0", ok=True):
        return dict(ts=ts, worker=worker, ok=ok, op="put_object", bytes=60 * 1024**2)

    windows = time_windows(
        [row(101), row(102, "merger-0"), row(103, ok=False), row(281)],
        100,
        180,
        60,
        1,
        1,
    )
    assert [w["ingest_mib_s"] for w in windows] == [1, 0, 0]
    assert windows[0]["errors"] == 1
    assert len(windows) == 3


def test_consistency_checks_deadline_from_measurements(bundle):
    with (bundle / "consistency.jsonl").open("a") as f:
        f.write(
            json.dumps(
                dict(
                    probe="read_after_write",
                    success=True,
                    elapsed_s=31,
                    within_deadline=True,
                )
            )
            + "\n"
        )
    assert indexed(build_report(bundle))["read_after_write"]["status"] == "FAIL"


def args(path, stage="compat"):
    return Namespace(
        cmd=stage,
        run_dir=str(path),
        endpoint="https://s3.example.test",
        bucket="test",
        region="us-east-1",
        flavor="auto",
        access_key="SENTINEL_ACCESS",
        secret_key="SENTINEL_SECRET",
    )


def test_run_identity_write_once_and_credentials(tmp_path):
    directory = tmp_path / "run"
    with RunSession(args(directory), load_config()):
        write_json(
            directory / "compat.json", {"recommended_flavor": None, "attempts": {}}
        )
    text = (directory / "manifest.json").read_text()
    assert "SENTINEL" not in text
    assert (
        read_json(directory / "manifest.json")["stages"]["compat"]["status"]
        == "COMPLETED"
    )
    with pytest.raises(ValueError, match="already exists"):
        with RunSession(args(directory), load_config()):
            pass
    other = args(directory, "fanout")
    other.endpoint = "https://different.example.test"
    with pytest.raises(ValueError, match="differs"):
        with RunSession(other, load_config()):
            pass
    assert not (directory / ".running").exists()


def test_interrupt_keeps_partial_stage(tmp_path):
    directory = tmp_path / "interrupted"
    with pytest.raises(KeyboardInterrupt):
        with RunSession(args(directory), load_config()):
            raise KeyboardInterrupt()
    assert (
        read_json(directory / "manifest.json")["stages"]["compat"]["status"]
        == "INTERRUPTED"
    )
    assert not (directory / ".running").exists()


def test_worker_failure_is_captured_and_stops_other_workers(tmp_path):
    sink = ResultSink(tmp_path / "rows.jsonl")
    event = threading.Event()

    def broken():
        raise RuntimeError("secret exception text")

    sink.run(broken, (), event)
    sink.close()
    assert event.is_set() and sink.errors == ["RuntimeError"]
    with pytest.raises(FileExistsError):
        ResultSink(tmp_path / "rows.jsonl")


def test_synthetic_payload_size_is_exact():
    assert len(_synthetic_payload(4.5)) == int(4.5 * 1024**2)


def test_html_escapes_untrusted_results_and_has_no_remote_assets(bundle, tmp_path):
    report = build_report(bundle)
    report["run"]["identity"]["endpoint"] = (
        "https://example.test/<script>alert(1)</script>"
    )
    report["checks"][0]["observed"] = "</td><img src=x onerror=alert(1)>"
    html = render_html(report)
    assert "<script>alert(1)</script>" not in html
    assert "<img src=x" not in html
    assert "&lt;script&gt;" in html
    assert 'src="https://' not in html
    assert "Content-Security-Policy" in html
    assert "data-status=" in html
    paths = write_reports(report, tmp_path / "export" / "report.html", bundle)
    data = read_json(paths[2])
    assert data["verdict"] == report["verdict"]
    assert (
        report["verdict"] in paths[0].read_text()
        and report["verdict"] in paths[1].read_text()
    )
    assert (tmp_path / "export" / "report-evidence" / "manifest.json").exists()
    with pytest.raises(ValueError, match="overwrite"):
        write_reports(report, bundle / "manifest.json", bundle)


def test_previous_workload_mismatch_not_compared(bundle, tmp_path):
    report = build_report(bundle)
    report["run"]["config"]["model_constants"]["compression_ratio"] = 99
    previous = tmp_path / "previous.json"
    write_json(previous, report)
    result = build_report(bundle, previous=previous)
    assert not result["history"]["comparable"]
    assert result["history"]["comparisons"] == []


def test_compliance_requires_raw_evidence_and_reports_skips(bundle, tmp_path):
    raw = tmp_path / "external.txt"
    raw.write_text("100 passed, 1 skipped")
    data = dict(
        tool="s3-tests",
        version="example",
        executed_at="2026-10-07T09:00:00Z",
        selection="multipart, range, delete, list",
        passed=100,
        failed=0,
        skipped=1,
        evidence_file=raw.name,
        endpoint="https://s3.example.test",
        bucket="qw-cert-example",
    )
    manifest = tmp_path / "compliance.json"
    write_json(manifest, data)
    report = build_report(bundle, compliance=manifest)
    assert indexed(report)["compliance"]["status"] == "INCONCLUSIVE"
    raw.unlink()
    with pytest.raises(ValueError, match="does not exist"):
        build_report(bundle, compliance=manifest)


def test_cli_report_and_strict_exit(bundle, tmp_path, capsys):
    assert (
        main(
            [
                "report",
                "--run-dir",
                str(bundle),
                "--out",
                str(tmp_path / "result.html"),
                "--strict",
            ]
        )
        == 1
    )
    assert (tmp_path / "result.html").exists()
    assert "Overall:" in capsys.readouterr().out


@pytest.mark.parametrize("duration", ["0", "-1", "nan", "inf"])
def test_cli_rejects_invalid_duration(duration):
    with pytest.raises(SystemExit):
        main(
            [
                "load",
                "--endpoint",
                "https://s3.example.test",
                "--bucket",
                "test",
                "--access-key",
                "test",
                "--secret-key",
                "test",
                "--tier",
                "1TB",
                "--duration-min",
                duration,
            ]
        )


def test_real_cli_flow_against_local_emulator(moto_server_endpoint, tmp_path):
    run = tmp_path / "integration"
    common = [
        "--endpoint",
        moto_server_endpoint,
        "--bucket",
        "qw-report-integration",
        "--access-key",
        "testing",
        "--secret-key",
        "testing",
        "--run-dir",
        str(run),
        "--runner-location",
        "local emulator",
    ]
    assert main(["compat", *common]) == 0
    assert (
        main(
            [
                "fanout",
                *common,
                "--levels",
                "1,2",
                "--repeats",
                "1",
                "--object-size-mb",
                "1",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "put-fanout",
                *common,
                "--levels",
                "1,2",
                "--repeats",
                "1",
                "--object-size-kb",
                "8",
            ]
        )
        == 0
    )
    assert main(["load", *common, "--tier", "100GB", "--duration-min", "0.01"]) == 0
    assert main(["report", "--run-dir", str(run)]) == 0
    report = read_json(run / "report.json")
    assert report["verdict"] not in ("CERTIFIED", "CERTIFIED WITH DEVIATION")
    assert len(report["measurements"]) > 0
    assert len(report["sweeps"]["put-fanout"]["levels"]) == 2
    assert load_bundle(run)[1] == []
    assert "testing" not in (run / "manifest.json").read_text()


def test_partial_compatibility_cannot_pass(bundle):
    data = read_json(bundle / "compat.json")
    del data["attempts"]["minio"]["results"]["checksum_algorithm"]
    write_json(bundle / "compat.json", data)
    assert indexed(build_report(bundle))["compatibility"]["status"] == "INCONCLUSIVE"


def test_sparse_latency_and_zero_baseline_remain_inconclusive(bundle, tmp_path):
    baseline = make_bundle(tmp_path / "aws", True)
    path = baseline / "query.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    for row in rows:
        if row["op"] == "query_wall_clock":
            row["latency_s"] = 0
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    manifest = read_json(baseline / "manifest.json")
    manifest["stages"]["load"]["artifacts"]["query.jsonl"] = digest(path)
    write_json(baseline / "manifest.json", manifest)
    checks = indexed(build_report(bundle, baseline))
    assert checks["query_wall_clock_p99"]["status"] == "INCONCLUSIVE"
    path = bundle / "ingest_merge.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    seen = False
    sparse = []
    for row in rows:
        if row["op"] != "put_object" or not seen:
            sparse.append(row)
        if row["op"] == "put_object":
            seen = True
    path.write_text("".join(json.dumps(row) + "\n" for row in sparse))
    assert (
        indexed(build_report(bundle, baseline))["put_object_p99"]["status"]
        == "INCONCLUSIVE"
    )


CONNECTION_ENV = (
    "QW_S3_ENDPOINT", "QW_S3_BUCKET", "QW_S3_ACCESS_KEY", "QW_S3_SECRET_KEY",
    "QW_S3_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION",
    "AWS_DEFAULT_REGION",
)


def test_certify_runs_every_stage_with_the_recommended_flavor(
    moto_server_endpoint, tmp_path
):
    """
    One command covers the whole sequence. The flavor the compatibility probe
    recommends is carried into the performance stages automatically, which is
    the step that used to be copied by hand.
    """
    run = tmp_path / "certify"
    assert (
        main(
            [
                "certify",
                "--endpoint", moto_server_endpoint,
                "--bucket", "qw-certify",
                "--access-key", "testing",
                "--secret-key", "testing",
                "--run-dir", str(run),
                "--runner-location", "local emulator",
                "--tier", "100GB",
                "--duration-min", "0.01",
                "--levels", "1,2",
                "--repeats", "1",
            ]
        )
        == 0
    )
    manifest = read_json(run / "manifest.json")
    assert set(manifest["stages"]) == {"compat", "fanout", "put-fanout", "load"}
    recommended = read_json(run / "compat.json")["recommended_flavor"]
    for stage in ("fanout", "put-fanout", "load"):
        assert manifest["stages"][stage]["options"]["flavor"] == recommended
    assert indexed(read_json(run / "report.json"))["flavor"]["status"] == "PASS"


def test_certify_takes_connection_settings_from_the_environment(
    moto_server_endpoint, tmp_path, monkeypatch
):
    """The usual command should be short, so every connection setting has an
    environment variable. Operators put the exports in a file and source it."""
    for name in CONNECTION_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("QW_S3_ENDPOINT", moto_server_endpoint)
    monkeypatch.setenv("QW_S3_BUCKET", "qw-certify-env")
    monkeypatch.setenv("QW_S3_ACCESS_KEY", "testing")
    monkeypatch.setenv("QW_S3_SECRET_KEY", "testing")
    run = tmp_path / "env"
    assert (
        main(["certify", "--run-dir", str(run), "--tier", "100GB",
              "--duration-min", "0.01", "--levels", "1,2", "--repeats", "1"])
        == 0
    )
    assert read_json(run / "manifest.json")["identity"]["bucket"] == "qw-certify-env"


def test_missing_connection_settings_name_their_environment_variable(monkeypatch):
    for name in CONNECTION_ENV:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit):
        main(["load", "--tier", "1TB"])


def test_certify_stops_when_no_flavor_works(moto_server_endpoint, tmp_path, monkeypatch):
    """
    Running the performance stages after a failed compatibility probe would
    measure a configuration that does not work. Stop instead.
    """
    from src import compat_checks

    monkeypatch.setattr(
        compat_checks, "probe_flavor",
        lambda *a, **k: {"recommended_flavor": None, "attempts": {}},
    )
    run = tmp_path / "nogo"
    with pytest.raises(SystemExit):
        main(["certify", "--endpoint", moto_server_endpoint, "--bucket", "qw-nogo",
              "--access-key", "testing", "--secret-key", "testing",
              "--run-dir", str(run), "--tier", "100GB", "--duration-min", "0.01"])
    assert set(read_json(run / "manifest.json")["stages"]) == {"compat"}


def test_certify_runs_the_baseline_leg_with_matching_metadata(
    moto_server_endpoint, tmp_path
):
    """
    Baseline comparability compares tier, duration, workload, runner location
    and host environment. Running both legs from one command makes them match
    by construction. Only the AWS hostname check can still object, as it does
    here against the emulator.
    """
    run, baseline = tmp_path / "vendor", tmp_path / "reference"
    assert (
        main(
            [
                "certify",
                "--endpoint", moto_server_endpoint,
                "--bucket", "qw-vendor-leg",
                "--access-key", "testing",
                "--secret-key", "testing",
                "--run-dir", str(run),
                "--runner-location", "local emulator",
                "--tier", "100GB",
                "--duration-min", "0.01",
                "--levels", "1,2",
                "--repeats", "1",
                "--with-aws-baseline",
                "--aws-endpoint", moto_server_endpoint,
                "--aws-bucket", "qw-reference-leg",
                "--aws-access-key", "testing",
                "--aws-secret-key", "testing",
                "--baseline-run-dir", str(baseline),
            ]
        )
        == 0
    )
    reference = read_json(baseline / "manifest.json")["stages"]["load"]
    assert reference["status"] == "COMPLETED"
    assert reference["options"]["flavor"] == "aws"
    assert reference["options"]["runner_location"] == "local emulator"
    observed = indexed(read_json(run / "report.json"))["baseline"]["observed"]
    assert "AWS service hostname" in observed
    assert "differs" not in observed


def test_stage_aliases_keep_working(moto_server_endpoint, tmp_path):
    """`fanout` and `put-fanout` are the original names. Scripts still use them."""
    run = tmp_path / "alias"
    assert (
        main(["fanout", "--endpoint", moto_server_endpoint, "--bucket", "qw-alias",
              "--access-key", "testing", "--secret-key", "testing",
              "--run-dir", str(run), "--levels", "1,2", "--repeats", "1",
              "--object-size-mb", "1"])
        == 0
    )
    assert "fanout" in read_json(run / "manifest.json")["stages"]


def test_every_criterion_belongs_to_one_question(bundle):
    """
    The report answers four questions. A criterion with no question, or with an
    unknown one, would never appear in the grouped tables.
    """
    report = build_report(bundle)
    known = {key for key, _, _ in QUESTIONS}
    assert known == {g["id"] for g in report["groups"]}
    for c in report["checks"]:
        assert c["group"] in known, c["id"]
    for group in report["groups"]:
        members = [
            c for c in report["checks"] if c["required"] and c["group"] == group["id"]
        ]
        worst = next(
            s
            for s in ("FAIL", "INCONCLUSIVE", "NOT RUN", "PASS")
            if any(c["status"] == s for c in members)
        )
        assert group["status"] == worst
        assert sum(group["counts"].values()) == len(members)


@pytest.mark.parametrize(
    "observed,limit,at_most,expected",
    [
        (50, 100, True, 0.5),      # half the allowed latency
        (100, 100, True, 1.0),     # exactly at the limit
        (120, 100, True, 1.2),     # over the limit
        (100, 95, False, 0.95),    # above an "at least" requirement
        (88, 95, False, 95 / 88),  # below an "at least" requirement
        (0, 100, False, None),     # nothing measured
        (50, None, True, None),    # no limit available
    ],
)
def test_headroom_always_fails_above_one(observed, limit, at_most, expected):
    """
    One column has to be scannable down the page, so every criterion reports
    headroom the same way: at or below 1.00x passes, whichever direction the
    underlying threshold points.
    """
    result = limit_ratio(observed, limit, at_most=at_most)
    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


def test_report_states_the_certification_ceiling_prominently(bundle):
    """
    Merge backlog is a required criterion with no measurement, so CERTIFIED is
    unreachable today. The report must say that once, where it cannot be
    missed, instead of leaving the reader to infer it from a NOT RUN row.
    """
    report = build_report(bundle)
    assert report["best_possible_verdict"] == "INCONCLUSIVE"
    headline = report["headline_limits"][0]
    assert "certification" in headline["title"].lower()
    assert "merge backlog" in headline["detail"].lower()
    assert "never CERTIFIED" in headline["detail"]
    html = render_html(report)
    assert "What this report can conclude" in html
    assert headline["detail"] in html
    # The same statement must not also sit in the detailed scope list.
    assert not any("backlog" in note for note in report["limitations"])


def test_per_operation_criteria_are_shown_once_as_a_matrix(bundle):
    """
    27 of the 39 criteria are one operation times latency, errors and
    throttling. Listing them as individual rows buries the few criteria that
    decide the verdict, so the criteria tables summarize them in one row and
    the matrix carries the detail.
    """
    report = build_report(bundle)
    per_op = [
        c
        for c in report["checks"]
        if c["id"].endswith(("_p99", "_errors", "_throttles"))
    ]
    assert len(per_op) == 3 * len(report["measurements"])
    html = render_html(report)
    for c in per_op:
        assert f'id="check-{c["id"]}"' not in html
    assert html.count("Per-operation results") == 2  # capacity and latency
    for m in report["measurements"]:
        assert m["latency_status"] and m["error_status"] and m["throttle_status"]
    markdown = render_markdown(report)
    assert "## Operations matrix" in markdown
    assert markdown.count("| Split upload |") == 1


def test_baseline_credentials_are_checked_before_the_soak(tmp_path, monkeypatch):
    """
    Discovering a missing reference credential after a 30-minute soak wastes
    the run. Check it first, and default the AWS endpoint from the region.
    """
    for name in CONNECTION_ENV + ("QW_AWS_ENDPOINT", "QW_AWS_BUCKET",
                                   "QW_AWS_ACCESS_KEY", "QW_AWS_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    run = tmp_path / "early"
    with pytest.raises(SystemExit):
        main(["certify", "--endpoint", "https://s3.vendor.test", "--bucket", "b",
              "--access-key", "a", "--secret-key", "s", "--tier", "1TB",
              "--run-dir", str(run), "--with-aws-baseline"])
    assert not run.exists()
