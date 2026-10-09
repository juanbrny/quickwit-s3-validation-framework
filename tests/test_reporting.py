import json
import threading
from argparse import Namespace

import pytest

from src.report import _percentile, summarize_ops, compare_to_baseline
from src.report_model import (
    QUESTIONS,
    build_report,
    roll_up,
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
from pathlib import Path

ROOT_REPORTS = Path(__file__).resolve().parent.parent / "reports"
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
    # A consistent record: the recommended settings were tested and passed.
    compat["attempts"]["none"] = compat["attempts"]["minio"]
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
    """An error fails any level, including concurrency 1, which has no
    speedup to judge."""
    result = summarize_fanout([FanoutLevelResult(1, 0.1, [0.09], 1, 0)])
    assert result["serializes_at_concurrency"] == 1
    assert result["levels"][0]["error_count"] == 1
    empty = summarize_fanout([FanoutLevelResult(8, 0.1, [], 0, 0)])
    assert empty["serializes_at_concurrency"] == 8
    json.dumps(empty, allow_nan=False)


def test_sweep_threshold_does_not_round_a_failure_into_pass(bundle):
    """Just under the required speedup must fail, not round up to a pass."""
    sweep = read_json(bundle / "fanout.json")
    required = sweep["min_speedup_required"]
    sweep["levels"][1]["speedup"] = required - 0.0001
    write_json(bundle / "fanout.json", sweep)
    assert indexed(build_report(bundle))["fanout_efficiency"]["status"] == "FAIL"


def test_a_spread_of_latencies_alone_never_fails_the_sweep(bundle):
    """
    The old gate compared median request latency to batch wall clock. Wall
    clock tracks the slowest request in the batch, so that ratio fell as
    concurrency rose for every backend. Measured from one laptop, AWS S3
    scored 0.09 at concurrency 256 with zero errors. A gate that fails the
    reference implementation is not a gate, so this is a diagnostic now.
    """
    sweep = read_json(bundle / "fanout.json")
    for level in sweep["levels"]:
        level["efficiency"] = 0.01
    write_json(bundle / "fanout.json", sweep)
    assert indexed(build_report(bundle))["fanout_efficiency"]["status"] == "PASS"


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

    sink.run(broken, (), event, "searcher-0")
    sink.close()
    assert event.is_set()
    # The cause is kept. Credentials are removed later, before anything is
    # shown or saved; see the test below.
    assert sink.errors == [
        {"worker": "searcher-0", "type": "RuntimeError", "message": "secret exception text"}
    ]
    with pytest.raises(FileExistsError):
        ResultSink(tmp_path / "rows.jsonl")


def test_a_crashed_worker_reports_its_cause_without_credentials(
    moto_server_endpoint, tmp_path, monkeypatch, capsys
):
    """
    A 30-minute run against StorageGRID stopped at 18 minutes with "A workload
    worker failed. Inspect raw results". The raw results held no trace of
    the cause, because the tool kept only the error's type, in memory. Now the
    cause is printed and saved, and this run's own keys are removed from it.
    """
    from src import query_sim

    secret = "SUPER-SECRET-KEY-VALUE"

    def crash(*args, **kwargs):
        raise OSError(f"[Errno 24] Too many open files while signing with {secret}")

    monkeypatch.setattr(query_sim, "query_worker_loop", crash)
    run = tmp_path / "crash"
    code = main(["load", "--endpoint", moto_server_endpoint, "--bucket", "qw-crash",
                 "--access-key", "testing", "--secret-key", secret,
                 "--run-dir", str(run), "--tier", "100GB", "--duration-min", "0.05"])
    assert code == 1  # a clean failure, not a traceback
    error = capsys.readouterr().err
    assert "Traceback" not in error
    assert "searcher-0 worker stopped: OSError" in error
    assert "Too many open files" in error
    assert secret not in error
    saved = (run / "manifest.json").read_text()
    assert "Too many open files" in saved
    assert secret not in saved


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


def test_missing_connection_settings_name_their_environment_variable(monkeypatch, capsys):
    for name in CONNECTION_ENV:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit):
        main(["load", "--tier", "1TB"])
    assert "$QW_S3_ACCESS_KEY" in capsys.readouterr().err


def test_an_empty_shell_variable_is_reported_as_empty(monkeypatch, capsys):
    """
    An unset shell variable expands to an empty argument, so `--access-key
    "$AK"` looks present while carrying nothing. Saying "missing" there sends
    the operator looking for a flag they did pass.
    """
    for name in CONNECTION_ENV:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit):
        main(["load", "--tier", "1TB", "--endpoint", "https://s3.vendor.test",
              "--bucket", "b", "--access-key", "", "--secret-key", "secret"])
    error = capsys.readouterr().err
    assert "--access-key is empty" in error
    assert "--secret-key" not in error


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


def test_every_check_belongs_to_one_question(bundle):
    """
    The report must show the same five questions as
    docs/what_this_measures.md, and every check must sit under one of them.
    A check with no question would never appear on the page.
    """
    report = build_report(bundle)
    known = {key for key, _, _ in QUESTIONS}
    assert known == {g["id"] for g in report["groups"]}
    for c in report["checks"]:
        assert c["group"] in known, c["id"]
    for group in report["groups"]:
        # Results attached from other tools are shown, but never colour the
        # answer: most runs never attach them.
        members = [
            c for c in report["checks"]
            if c["group"] == group["id"] and c["id"] not in ("compliance", "warp")
        ]
        deciding = [c for c in members if c["required"]] or members
        worst = next(
            s
            for s in ("FAIL", "INCONCLUSIVE", "NOT RUN", "PASS")
            if any(c["status"] == s for c in deciding)
        )
        assert group["status"] == worst, group["id"]


def test_exactly_ten_checks_decide_the_result(bundle):
    """
    Thirty-eight checks decided the result before, and nobody could explain
    them. Ten do now. The rest stay in the report as information only, so no
    measurement is lost, only the noise in the result.
    """
    report = build_report(bundle)
    deciding = [c for c in report["checks"] if c["required"]]
    assert [c["id"] for c in deciding] == [
        "compatibility",
        "throughput",
        "query_rate",
        "merge_backlog",
        "response_time",
        "failed_requests",
        "slowed_requests",
        "object_visibility",
        "fanout_efficiency",
        "put-fanout_efficiency",
    ]
    assert len(report["checks"]) > len(deciding)


def test_every_deciding_check_says_where_its_number_came_from(bundle):
    """
    "Where does this number come from?" has to be answerable from the report
    alone, without reading the code.
    """
    report = build_report(bundle)
    for c in report["checks"]:
        if not c["required"]:
            continue
        source = c["source"]
        assert source.get("command") in (
            "compat", "read-concurrency", "write-concurrency", "load"
        ), c["id"]
        assert source.get("evidence"), c["id"]
    # Check the traceability line itself. An earlier version of this test
    # looked for the setting name anywhere on the page, and passed even
    # though the line was never rendered: the page also prints the full run
    # manifest, which contains every setting name.
    html = render_html(report)
    assert html.count('<p class="source">From command') == len(
        [c for c in report["checks"] if c["required"]]
    )
    assert (
        'From command <code>load</code>, evidence <code>consistency.jsonl</code>,'
        ' setting <code>consistency_probe_min_success_pct</code>.'
    ) in html


def test_one_failing_operation_fails_the_response_time_check(bundle):
    """
    Seven response-time checks became one. It must still fail when a single
    operation is too slow, or collapsing them would have hidden a failure.
    """
    report = build_report(bundle)
    summary = indexed(report)["response_time"]
    slow = [m for m in report["measurements"] if m["latency_status"] == "FAIL"]
    assert slow, "the sample bundle should contain one slow operation"
    assert summary["status"] == "FAIL"
    assert f"Of {len(report['measurements'])} operations: {len(slow)} failed" in summary["observed"]


def _drop_operation(bundle, op):
    """Remove every sample of one operation, as a short run would."""
    for name in ("query.jsonl", "ingest_merge.jsonl"):
        rows = [
            line for line in (bundle / name).read_text().splitlines()
            if f'"op": "{op}"' not in line
        ]
        (bundle / name).write_text("".join(line + "\n" for line in rows))
    manifest = read_json(bundle / "manifest.json")
    for name in ("query.jsonl", "ingest_merge.jsonl"):
        manifest["stages"]["load"]["artifacts"][name] = digest(bundle / name)
    write_json(bundle / "manifest.json", manifest)


def test_a_missing_operation_never_hides_a_failure(bundle):
    """
    Regression test. A one-minute run against AWS S3 had two reads over their
    limit, and no merge reads at all, because no merge happens in one minute.
    The summary said NOT RUN, which hid the two failures and turned the
    result from NOT CERTIFIED into INCONCLUSIVE.
    """
    _drop_operation(bundle, "get_object_full")
    report = build_report(bundle)
    result = indexed(report)["response_time"]
    assert result["status"] == "FAIL"
    assert "never ran: Merge object read" in result["observed"]
    assert report["verdict"] == "NOT CERTIFIED"


def test_a_missing_operation_makes_a_clean_summary_inconclusive():
    """
    Something measured, but not everything, is INCONCLUSIVE. NOT RUN means
    nothing at all was measured. The report defines the two words that way.
    """
    measured = [check("a", "a", "PASS", "", "", ""), check("b", "b", "PASS", "", "", "")]
    assert roll_up(measured, missing_expected=True) == "INCONCLUSIVE"
    assert roll_up(measured, missing_expected=False) == "PASS"
    assert roll_up([], missing_expected=True) == "NOT RUN"


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
    # Found by title, not by position: other notices, such as a short-run
    # warning, may come first.
    headline = next(
        h for h in report["headline_limits"] if "certification" in h["title"].lower()
    )
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


def test_a_vendor_without_aws_still_gets_graded_latency(bundle):
    """
    The point of the bundled profile: no AWS account, still a real verdict.
    Without it, every latency criterion would read INCONCLUSIVE and the report
    could not answer "is it as fast as AWS S3".
    """
    report = build_report(bundle)  # no --baseline
    assert report["reference"]["basis"] == "reference profile"
    assert report["reference"]["profile"]["id"] == "aws-s3-ec2-same-region"
    latency = [c for c in report["checks"] if c["id"].endswith("_p99")]
    assert latency and all(c["status"] in ("PASS", "FAIL") for c in latency)
    assert indexed(report)["baseline"]["status"] == "PASS"
    # The report must say the bar is published, not measured next to this run.
    titles = [item["title"] for item in report["headline_limits"]]
    assert "Latency is graded against published figures" in titles
    assert "reference profile" in render_html(report)


def test_a_measured_baseline_overrides_the_bundled_profile(bundle, tmp_path):
    baseline = make_bundle(tmp_path / "aws", True)
    report = build_report(bundle, baseline)
    assert report["reference"]["basis"] == "measured baseline"
    assert report["reference"]["profile"] is None
    titles = [item["title"] for item in report["headline_limits"]]
    assert "Latency is graded against published figures" not in titles


def test_reference_none_keeps_latency_inconclusive(bundle):
    """An operator who rejects the published bar can still opt out."""
    report = build_report(bundle, reference="none")
    assert report["reference"]["basis"] == "none"
    latency = [c for c in report["checks"] if c["id"].endswith("_p99")]
    assert all(c["status"] == "INCONCLUSIVE" for c in latency)
    assert indexed(report)["baseline"]["status"] == "INCONCLUSIVE"


def test_profile_limits_follow_the_recorded_payload_size(bundle):
    """
    The profile holds two numbers, not a table. Each operation's limit comes
    from its own median payload, so one profile covers every tier.
    """
    report = build_report(bundle)
    by_op = {m["op"]: m for m in report["measurements"]}
    small, large = by_op["get_term_or_field"], by_op["put_object"]
    assert large["median_bytes"] > small["median_bytes"]
    assert large["limit_p99_s"] > small["limit_p99_s"]
    for m in report["measurements"]:
        assert m["median_bytes"] >= 0


def test_a_connection_failure_is_not_reported_as_incompatibility(monkeypatch, capsys,
                                                                  moto_server_endpoint,
                                                                  tmp_path):
    """
    When every flavor fails before any check runs, nothing was tested. The old
    message said "no flavor passed every compatibility check", and sent the
    operator looking for a compatibility problem that did not exist. The real
    cause was the keys.
    """
    from src import compat_checks

    error = ("bucket setup failed: An error occurred (SignatureDoesNotMatch) when"
             " calling the CreateBucket operation")
    monkeypatch.setattr(
        compat_checks, "probe_flavor",
        lambda *a, **k: {
            "recommended_flavor": None,
            "equivalent_flavors": [],
            "attempts": {"none": {"error": error, "all_passed": False}},
        },
    )
    with pytest.raises(SystemExit):
        main(["certify", "--endpoint", moto_server_endpoint, "--bucket", "b",
              "--access-key", "AKIAEXAMPLE", "--secret-key", "s",
              "--run-dir", str(tmp_path / "run"), "--tier", "100GB"])
    out = capsys.readouterr()
    assert "Could not connect" in out.err
    assert "secret key does not belong" in out.err
    assert "no flavor passed" not in out.err.lower()


def test_the_run_says_where_the_keys_came_from(monkeypatch, capsys):
    """
    Keys can come from a flag or from several environment variables. A run
    that silently picks up the wrong ones fails with a signature error and no
    clue. Say the source, and never the secret.
    """
    import run_certification

    for name in CONNECTION_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ASIAEXAMPLEKEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "do-not-print-me")
    args = Namespace(access_key="ASIAEXAMPLEKEY", secret_key="do-not-print-me",
                     session_token=None)
    run_certification.describe_credentials(args, [])
    out = capsys.readouterr().out
    assert "$AWS_ACCESS_KEY_ID" in out
    assert "ASIA" in out and "WITHOUT a session token" in out
    assert "do-not-print-me" not in out
    assert "EXAMPLEKEY" not in out


def test_the_session_token_reaches_the_client(moto_s3):
    """Temporary keys are rejected without their session token."""
    from src.qw_s3_client import QwS3Client, QwS3Config

    cfg = QwS3Config(endpoint_url=None, access_key="ASIA", secret_key="s",
                     session_token="the-token")
    credentials = QwS3Client(cfg)._client._request_signer._credentials
    assert credentials.token == "the-token"


def test_the_session_token_is_never_written_to_the_manifest():
    from src.qw_s3_client import QwS3Config
    from src.run_store import public_config

    cfg = QwS3Config(endpoint_url="https://x.test", access_key="a",
                     secret_key="b", session_token="the-token")
    assert "the-token" not in json.dumps(public_config(cfg))


def test_a_short_run_says_why_checks_are_unanswered():
    """
    A one-minute run leaves checks NOT RUN or INCONCLUSIVE for reasons that
    are arithmetic, not faults: too few full minutes, too few samples, and no
    merge yet. The report must say so first, with the numbers, so nobody
    hunts for a problem in the storage.
    """
    report = build_report(ROOT_REPORTS / "20261008T214821Z-3b45e258") if (
        ROOT_REPORTS / "20261008T214821Z-3b45e258"
    ).exists() else None
    if report is None:
        pytest.skip("the recorded AWS run is not present")
    first = report["headline_limits"][0]
    assert first["title"].startswith("This run was too short to judge")
    assert "a merge needs 10 uploaded files" in first["detail"]
    assert "Run for at least" in first["detail"]


def test_a_run_long_enough_gets_no_short_run_warning(bundle):
    """The sample run answers every check, so it must not be warned."""
    titles = [h["title"] for h in build_report(bundle)["headline_limits"]]
    assert not any(t.startswith("This run was too short") for t in titles)


def test_missing_attachments_do_not_make_the_run_look_untrustworthy(bundle):
    """
    Most runs never attach s3-tests or warp results. Their absence read as
    "Can we trust this result? NOT RUN" even when every check about the run
    itself passed. They stay visible as rows, and stop deciding the answer.
    """
    report = build_report(bundle)
    trust = next(g for g in report["groups"] if g["id"] == "trust")
    assert indexed(report)["compliance"]["status"] == "NOT RUN"
    assert trust["status"] == "PASS"


def test_a_plain_http_run_warns_that_https_may_differ(bundle):
    """
    Over HTTP the client sends an upload checksum as an ordinary header. Over
    HTTPS it uses an x-amz-trailer header, which StorageGRID lists as
    unsupported. A StorageGRID run over HTTP passed with Quickwit's default
    settings, a result that may not hold in production over HTTPS.
    """
    manifest = read_json(bundle / "manifest.json")
    manifest["identity"]["endpoint"] = "http://192.168.0.10:10444"
    manifest["stages"]["load"]["effective_config"] = {"checksum_algorithm": "crc32c"}
    write_json(bundle / "manifest.json", manifest)
    first = build_report(bundle)["headline_limits"][0]
    assert first["title"] == "This run used plain HTTP, not HTTPS"
    assert "x-amz-trailer" in first["detail"]


def test_an_https_run_gets_no_http_warning(bundle):
    titles = [h["title"] for h in build_report(bundle)["headline_limits"]]
    assert "This run used plain HTTP, not HTTPS" not in titles


def test_the_default_settings_are_named_not_shown_as_none(bundle):
    """ "flavor: none" read like a missing value. It means Quickwit's defaults."""
    from src.qw_s3_client import flavor_label

    assert flavor_label("none") == "Quickwit defaults (no flavor setting)"
    manifest = read_json(bundle / "manifest.json")
    for stage in ("fanout", "put-fanout", "load"):
        manifest["stages"][stage]["options"]["flavor"] = "none"
    write_json(bundle / "manifest.json", manifest)
    html = render_html(build_report(bundle))
    assert "Quickwit defaults (no flavor setting)" in html
    assert "actual flavor" not in html


def test_certify_uses_the_flavor_you_choose(moto_server_endpoint, tmp_path):
    """
    The probe picks the mildest settings that work, so against a compliant
    system it picks Quickwit's defaults and never tests storagegrid. An
    operator who will deploy storagegrid's settings must be able to test
    those, and the report must grade those, not the defaults.
    """
    run = tmp_path / "chosen"
    assert main([
        "certify", "--endpoint", moto_server_endpoint, "--bucket", "qw-chosen",
        "--access-key", "testing", "--secret-key", "testing",
        "--run-dir", str(run), "--runner-location", "local",
        "--tier", "100GB", "--duration-min", "0.01", "--levels", "1,2", "--repeats", "1",
        "--flavor", "storagegrid",
    ]) == 0
    manifest = read_json(run / "manifest.json")
    for stage in ("fanout", "put-fanout", "load"):
        assert manifest["stages"][stage]["options"]["flavor"] == "storagegrid"
    compat = read_json(run / "compat.json")
    assert compat["recommended_flavor"] == "none"          # the mildest that works
    assert compat["attempts"]["storagegrid"]["all_passed"]  # and the choice was tested
    report = indexed(read_json(run / "report.json"))
    assert report["compatibility"]["status"] == "PASS"
    assert report["compatibility"]["observed"].startswith("storagegrid: every check passed")
    assert report["flavor"]["status"] == "PASS"


def test_certify_stops_when_the_chosen_flavor_fails(moto_server_endpoint, tmp_path,
                                                     monkeypatch, capsys):
    from src import compat_checks

    monkeypatch.setattr(compat_checks, "probe_flavor", lambda *a, **k: {
        "recommended_flavor": "none",
        "equivalent_flavors": [],
        "attempts": {
            "none": {"results": {}, "all_passed": True},
            "storagegrid": {
                "all_passed": False,
                "results": {"checksum_algorithm": {"passed": False, "detail": "rejected"}},
            },
        },
    })
    with pytest.raises(SystemExit):
        main(["certify", "--endpoint", moto_server_endpoint, "--bucket", "b",
              "--access-key", "a", "--secret-key", "s", "--run-dir", str(tmp_path / "r"),
              "--tier", "100GB", "--flavor", "storagegrid"])
    error = capsys.readouterr().err
    assert "did not pass every compatibility check: checksum_algorithm" in error


def test_a_pause_is_measured_from_the_run_clocks():
    """
    The wall clock keeps counting while a machine sleeps; the process clock
    does not. A MacBook's 99-second idle sleep showed up as 88 seconds between
    the two, with no help from outside the run.
    """
    from src.report_model import paused_time

    stage = {"started_epoch": 1000.0, "finished_epoch": 2122.0, "actual_duration_s": 1033.6}
    assert paused_time(stage) == pytest.approx(88.4)
    assert paused_time({"started_epoch": 1000.0, "finished_epoch": 1100.0,
                        "actual_duration_s": 100.0}) == 0


def test_an_unplaced_pause_never_stands_as_a_storage_failure(bundle):
    """
    Runs recorded before pause tracking know only that the machine stopped,
    not when. A time-based failure then cannot be told apart from the pause,
    so it reads INCONCLUSIVE, never FAIL.
    """
    manifest = read_json(bundle / "manifest.json")
    load = manifest["stages"]["load"]
    load["finished_epoch"] = load["started_epoch"] + load["actual_duration_s"] + 90
    load.pop("pauses", None)
    write_json(bundle / "manifest.json", manifest)
    report = build_report(bundle)
    checks = indexed(report)
    assert report["headline_limits"][0]["title"] == "The test machine stopped for 90 seconds"
    assert checks["throughput"]["status"] == "INCONCLUSIVE"   # the sample fails this one
    assert checks["response_time"]["status"] == "INCONCLUSIVE"
    assert report["verdict"] == "INCONCLUSIVE"


def test_a_placed_pause_leaves_only_its_own_minutes_out(bundle):
    """
    When the run knows when the pause happened, it leaves out only those
    minutes and the recovery after them. The rest is judged normally.
    """
    manifest = read_json(bundle / "manifest.json")
    load = manifest["stages"]["load"]
    start = load["measurement_started_epoch"]
    load["pauses"] = [{"started_epoch": start + 30, "seconds": 20}]
    load["finished_epoch"] = load["started_epoch"] + load["actual_duration_s"] + 20
    write_json(bundle / "manifest.json", manifest)
    report = build_report(bundle)
    paused = [w for w in report["timeline"] if w["paused"]]
    judged = [w for w in report["timeline"] if w["complete"] and not w["paused"]]
    assert paused and judged
    assert "leaves out that time" in report["headline_limits"][0]["detail"]


def test_the_pause_watch_notices_a_sleep(monkeypatch):
    import run_certification

    clock = {"wall": 1000.0, "mono": 50.0}
    monkeypatch.setattr(run_certification.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(run_certification.time, "monotonic", lambda: clock["mono"])

    class Ticks:
        """Stop after three seconds; the second one hides a 40-second sleep."""
        def __init__(self):
            self.n = 0

        def wait(self, timeout):
            self.n += 1
            clock["mono"] += 1.0
            clock["wall"] += 41.0 if self.n == 2 else 1.0
            return self.n > 3

    watch = run_certification.PauseWatch(Ticks())
    watch.run()
    assert len(watch.pauses) == 1
    assert watch.pauses[0]["seconds"] == pytest.approx(40.0)


def test_a_run_keeps_the_mac_awake(monkeypatch):
    """`caffeinate -w <pid>` holds the machine awake and ends with the run."""
    import os
    import shutil
    import subprocess

    import run_certification

    calls = []
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/caffeinate")
    monkeypatch.setattr(subprocess, "Popen", lambda argv, **kw: calls.append(argv) or object())
    run_certification.keep_awake()
    assert calls == [["/usr/bin/caffeinate", "-i", "-s", "-w", str(os.getpid())]]
