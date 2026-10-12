"""
The load engines: which one runs, and the workload rules both must follow.

An AWS run at the 1TB tier failed for reasons that had nothing to do with
S3. One Python process could not send the workload, a slow merge stopped
uploads for whole minutes, and merged-away files were deleted while searches
still read them. These tests pin the fixes. The Rust engine tests run only
when the engine is built, with
`cargo build --release --manifest-path loadgen/Cargo.toml`.
"""
import json
import os
import stat
import sys
import threading
import time

import boto3
import pytest

from run_validation import main
from src import load_engine
from src.ingest_merge_sim import ResultSink, SplitKeyRegistry, _do_merge, run_ingest_merge_sim
from src.qw_s3_client import QwS3Config
from src.report_model import _rows, machine_limit
from src.run_store import read_json
from src.workload_model import compute_op_mix, load_config

KEYS = ["--access-key", "testing", "--secret-key", "testing"]
built = pytest.mark.skipif(
    load_engine.find_engine() is None, reason="the Rust load engine is not built"
)


# ---- which engine runs ------------------------------------------------------

def test_a_large_tier_never_falls_back_to_python(monkeypatch):
    """Falling back would measure the test machine, as the AWS run did."""
    monkeypatch.setattr(load_engine, "find_engine", lambda: None)
    for tier in ("1TB", "10TB", "100TB"):
        with pytest.raises(ValueError, match="needs the Rust load engine"):
            load_engine.choose_engine("auto", tier)
    assert load_engine.choose_engine("auto", "100GB") == ("python", None)
    with pytest.raises(ValueError, match="cargo build --release"):
        load_engine.choose_engine("rust", "100GB")
    assert load_engine.choose_engine("python", "100TB") == ("python", None)


def test_auto_uses_the_rust_engine_when_it_is_built(monkeypatch, tmp_path):
    binary = tmp_path / "engine"
    monkeypatch.setattr(load_engine, "find_engine", lambda: binary)
    assert load_engine.choose_engine("auto", "100GB") == ("rust", binary)


def test_validate_refuses_a_large_tier_before_any_request(monkeypatch, capsys):
    monkeypatch.setattr(load_engine, "find_engine", lambda: None)
    with pytest.raises(SystemExit):
        main(["validate", "--endpoint", "http://127.0.0.1:9", "--bucket", "b", *KEYS,
              "--tier", "1TB"])
    assert "needs the Rust load engine" in capsys.readouterr().err


def _settings(**overrides):
    cfg = QwS3Config.from_flavor("none", "https://s3.example.test", "AK", "SECRET")
    for name, value in overrides.items():
        setattr(cfg, name, value)
    config = load_config()
    return load_engine.engine_settings(
        cfg, "bucket", "qwcert/" + "0" * 32 + "/1TB", compute_op_mix("1TB", config),
        {**config["model_constants"], "gc_grace_period_s": 60}, config["query_profiles"],
        60, "/tmp/run", 160 * 1024**2, 128 * 1024**2, 5 * 1024**3,
    )


def test_the_engine_settings_follow_the_flavor_and_the_tier():
    settings = _settings(checksum_algorithm="md5", force_path_style=True, verify_tls="/ca.pem")
    assert settings["storage"]["checksum_algorithm"] == "md5"
    assert settings["storage"]["force_path_style"] is True
    assert settings["connection"]["ca_bundle"] == "/ca.pem"
    assert settings["workload"]["indexers"] == 2
    assert settings["workload"]["query_qps"] == 10


def test_the_rust_engine_refuses_to_skip_certificate_checks():
    with pytest.raises(ValueError, match="--ca-bundle"):
        _settings(verify_tls=False)


def _fake_engine(tmp_path, body):
    script = tmp_path / "fake-engine"
    script.write_text(f"#!{sys.executable}\n{body}")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return script


def test_credentials_reach_the_engine_on_standard_input_only(tmp_path):
    """The command line is visible to every user of the machine."""
    seen = tmp_path / "seen.json"
    script = _fake_engine(tmp_path, f"""
import json, sys
settings = json.load(sys.stdin)
json.dump({{"argv": sys.argv, "secret": settings["connection"]["secret_key"]}},
          open({str(seen)!r}, "w"))
print(json.dumps({{"event": "started", "epoch": 1.0}}))
print(json.dumps({{"event": "finished", "measurement_duration_s": 1.0}}))
""")
    started = []
    summary = load_engine.run_engine(script, _settings(), threading.Event(), started.append)
    assert summary["measurement_duration_s"] == 1.0
    assert started == [{"event": "started", "epoch": 1.0}]
    record = json.loads(seen.read_text())
    assert record["secret"] == "SECRET"
    assert not any("SECRET" in arg for arg in record["argv"])


def test_an_engine_error_stops_the_run_with_its_message(tmp_path):
    script = _fake_engine(tmp_path, """
import json
print(json.dumps({"event": "error", "message": "cannot read the CA bundle /x"}))
raise SystemExit(2)
""")
    with pytest.raises(load_engine.EngineError, match="cannot read the CA bundle"):
        load_engine.run_engine(script, _settings(), threading.Event())


# ---- the workload rules, in the Python engine --------------------------------

class _Client:
    """Records the order of calls, and what searches could see at each one."""

    def __init__(self, registry):
        self.registry, self.calls = registry, []

    def get_full(self, bucket, key):
        self.calls.append(("get", key, time.monotonic()))
        return {"ok": True, "op": "get_object_full", "latency_s": 0.0, "bytes": 1024}

    def put_split(self, bucket, key, data):
        self.calls.append(("put", key, time.monotonic()))
        return {"ok": True, "op": "put_object", "latency_s": 0.0}

    def delete_batch(self, bucket, keys):
        self.calls.append(("delete", list(self.registry.snapshot()), time.monotonic()))
        return {"ok": True, "op": "delete_objects_bulk", "latency_s": 0.0}


def test_a_merge_unpublishes_its_inputs_before_it_deletes_them(tmp_path):
    """
    Deleting first made searches read files that were already gone: 440
    NoSuchKey errors in one AWS run, every one of them counted against S3.
    """
    registry = SplitKeyRegistry()
    inputs = [f"p/node0/immature-{i}.split" for i in range(3)]
    for key in inputs:
        registry.add(key)
    client, sink = _Client(registry), ResultSink(tmp_path / "rows.jsonl")
    mix = compute_op_mix("100GB", load_config())
    started = time.monotonic()
    _do_merge(0, client, "b", "p", inputs, mix, sink, registry, threading.Event(), 0.3)
    sink.close()
    kind, visible, when = client.calls[-1]
    assert kind == "delete"
    assert not set(inputs) & set(visible), "searches could still choose a deleted split"
    assert any("merged-" in k for k in visible)
    assert when - started >= 0.3, "the delete must wait for the grace period"


def test_a_merge_cut_off_by_the_end_of_the_run_deletes_nothing(tmp_path):
    registry = SplitKeyRegistry()
    client, sink = _Client(registry), ResultSink(tmp_path / "rows.jsonl")
    stop = threading.Event()
    stop.set()
    _do_merge(0, client, "b", "p", ["p/a.split"], compute_op_mix("100GB", load_config()),
              sink, registry, stop, 60)
    sink.close()
    assert [c[0] for c in client.calls] == ["get", "put"]


class _SlowMerges(_Client):
    def get_full(self, bucket, key):
        time.sleep(0.5)
        return super().get_full(bucket, key)


def test_a_slow_merge_does_not_delay_the_next_upload(tmp_path):
    """An inline merge stopped every upload of its node for minutes at a time."""
    registry = SplitKeyRegistry()
    client = _SlowMerges(registry)
    mix = compute_op_mix("100GB", load_config())
    run_ingest_merge_sim(client, "b", "p", mix, 2.2 / 60, tmp_path / "rows.jsonl", registry,
                         merge_factor=2, commit_timeout_s=0.2, gc_grace_s=0)
    puts = [when for kind, key, when in client.calls if kind == "put" and "immature" in key]
    gaps = [b - a for a, b in zip(puts, puts[1:])]
    # Merges take 1 second each here. Run inline, they would open gaps of
    # 1.2 seconds between uploads. The interval is 0.2 seconds.
    assert len(puts) >= 8 and max(gaps) < 0.6, gaps


def test_indexers_start_spread_over_one_commit_interval(tmp_path):
    """
    Started together, the 135 indexers of the 100TB tier uploaded 26 GB in
    the same second of every minute. The test then measured the network card.
    """
    registry = SplitKeyRegistry()
    client = _Client(registry)
    mix = compute_op_mix("100GB", load_config())
    mix.num_indexer_nodes = 4
    run_ingest_merge_sim(client, "b", "p", mix, 0.9 / 60, tmp_path / "rows.jsonl", registry,
                         commit_timeout_s=0.8, gc_grace_s=0)
    first = sorted(when for kind, key, when in client.calls if kind == "put")[:4]
    gaps = [b - a for a, b in zip(first, first[1:])]
    assert all(0.15 < g < 0.35 for g in gaps), gaps  # 0.8 s / 4 nodes


# ---- the report on the test machine ------------------------------------------

def test_a_machine_that_fell_behind_is_named_in_the_report():
    load = {"status": "COMPLETED", "options": {"tier": "100TB"}, "engine": {
        "name": "rust",
        "searches": {"started": 1000, "late_over_100ms": 50},
        "machine": {"cores": 16, "average_cores_used": 2.0},
    }}
    note = machine_limit(load)
    assert note["title"] == "The test machine was near its limit"
    assert "50 of 1000 searches" in note["detail"]


def test_a_busy_machine_is_named_in_the_report():
    load = {"status": "COMPLETED", "options": {"tier": "100TB"}, "engine": {
        "name": "rust", "searches": {"started": 1000, "late_over_100ms": 0},
        "machine": {"cores": 4, "average_cores_used": 3.6},
    }}
    assert "3.6 of 4 cores" in machine_limit(load)["detail"]


def test_a_large_tier_on_the_python_engine_is_named_in_the_report():
    """Runs recorded before the Rust engine have no engine record at all."""
    note = machine_limit({"status": "COMPLETED", "options": {"tier": "1TB"}})
    assert note["title"] == "The Python load engine sent this workload"
    assert machine_limit({"status": "COMPLETED", "options": {"tier": "100GB"}}) is None


def test_a_clean_rust_run_needs_no_note():
    load = {"status": "COMPLETED", "options": {"tier": "100TB"}, "engine": {
        "name": "rust", "searches": {"started": 1000, "late_over_100ms": 2},
        "machine": {"cores": 16, "average_cores_used": 1.1},
    }}
    assert machine_limit(load) is None


# ---- the Rust engine itself ----------------------------------------------------

@built
def test_the_rust_engine_writes_what_the_report_reads(moto_server_endpoint, tmp_path):
    run = tmp_path / "rust"
    assert main(["load", "--endpoint", moto_server_endpoint, "--bucket", "rust-engine", *KEYS,
                 "--run-dir", str(run), "--tier", "100GB", "--duration-min", "0.1",
                 "--engine", "rust"]) == 0
    load = read_json(run / "manifest.json")["stages"]["load"]
    assert load["status"] == "COMPLETED"
    assert load["engine"]["name"] == "rust"
    assert load["engine"]["searches"]["started"] > 0
    assert len(load["engine"]["binary_sha256"]) == 64
    rows = _rows(run / "ingest_merge.jsonl") + _rows(run / "query.jsonl")
    ops = {r["op"] for r in rows}
    assert {"put_object", "get_term_or_field", "query_wall_clock"} <= ops
    assert all(r["ok"] for r in rows), [r["error"] for r in rows if not r["ok"]]
    start = load["measurement_started_epoch"]
    assert all(start <= r["ts"] <= start + 6 + 5 for r in rows)


@built
def test_the_rust_engine_writes_only_inside_its_run_folder(moto_server_endpoint, tmp_path):
    """The rule that makes cleanup safe in a shared bucket."""
    run = tmp_path / "rust"
    main(["load", "--endpoint", moto_server_endpoint, "--bucket", "rust-folder", *KEYS,
          "--run-dir", str(run), "--tier", "100GB", "--duration-min", "0.05",
          "--engine", "rust"])
    run_id = read_json(run / "manifest.json")["run_id"]
    s3 = boto3.client("s3", endpoint_url=moto_server_endpoint, aws_access_key_id="testing",
                      aws_secret_access_key="testing", region_name="us-east-1")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="rust-folder").get("Contents", [])]
    assert keys and all(k.startswith(f"qwcert/{run_id}/") for k in keys), keys
