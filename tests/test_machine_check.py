import socket
import threading

import pytest

from examples.make_sample_report import make_bundle
from src import machine_check
from src.machine_check import GIB, problems, round_trip, summary_lines
from src.report_model import build_report
from src.run_store import read_json, write_json
from src.workload_model import load_config


def indexed(report):
    return {c["id"]: c for c in report["checks"]}


@pytest.fixture
def listener():
    """A local TCP server that accepts connections and does nothing else."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(64)
    stop = threading.Event()

    def accept():
        server.settimeout(0.1)
        while not stop.is_set():
            try:
                server.accept()[0].close()
            except OSError:
                pass

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    yield server.getsockname()[1]
    stop.set()
    thread.join()
    server.close()


def test_the_round_trip_is_the_fastest_new_connection(listener):
    trip = round_trip(f"http://127.0.0.1:{listener}", attempts=5, pause_s=0)
    assert trip["failures"] == 0 and "error" not in trip
    assert 0 < trip["min_ms"] <= trip["median_ms"] <= trip["max_ms"]


def test_an_unreachable_endpoint_is_recorded_not_raised():
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    port = closed.getsockname()[1]
    closed.close()
    trip = round_trip(f"http://127.0.0.1:{port}", attempts=3, pause_s=0, timeout_s=1)
    assert trip["failures"] == 3
    assert "min_ms" not in trip and "No connection opened" in trip["error"]


def facts(cores=8, memory_gb=32, disk_gb=100, files=65536, needed=1100, rtt=0.5):
    return {
        "cores": cores,
        "memory_bytes": memory_gb * GIB,
        "free_disk_bytes": disk_gb * GIB,
        "file_limit": files,
        "files_needed": needed,
        "round_trip": {"host": "s3.example.test", "attempts": 20, "failures": 0,
                       "min_ms": rtt, "median_ms": rtt, "max_ms": rtt},
    }


def test_a_machine_that_meets_the_recommendation_has_no_problem():
    assert problems(facts(), load_config(), "10TB") == []


def test_a_small_or_distant_machine_is_named():
    found = problems(facts(cores=2, memory_gb=4, files=1024, rtt=95.0), load_config(), "10TB")
    kinds = [kind for kind, _ in found]
    assert kinds.count("size") == 3 and kinds.count("distance") == 1
    text = " ".join(message for _, message in found)
    assert "2 cores, and the 10TB tier needs 8" in text
    assert "95.0 ms" in text


def test_memory_the_system_keeps_for_itself_is_tolerated():
    """A machine sold with 8 GB reports about 7.6 GB."""
    assert problems(facts(memory_gb=7.6), load_config(), "1TB") == []


def test_no_single_machine_can_send_the_largest_tiers():
    found = problems(facts(), load_config(), "1PB")
    assert found == [("size", "No single test machine can send the 1PB workload.")]


def test_a_container_quota_limits_the_cores(monkeypatch):
    """Docker's --cpus sets a quota. os.cpu_count() still shows the host."""
    monkeypatch.setattr(
        machine_check, "_read",
        lambda path: "200000 100000" if path == "/sys/fs/cgroup/cpu.max" else None,
    )
    assert machine_check.available_cores() <= 2


def test_the_summary_says_what_the_tier_needs():
    lines = summary_lines(facts(), load_config(), "1TB")
    assert lines[0] == "Cores: 8 (the 1TB tier needs 4)"
    assert any("0.5 ms at best" in line and "limit 10 ms" in line for line in lines)


def _set_machine(bundle, **change):
    manifest = read_json(bundle / "manifest.json")
    if change:
        manifest["stages"]["machine"]["machine"]["round_trip"].update(change)
    else:
        del manifest["stages"]["machine"]
    write_json(bundle / "manifest.json", manifest)


def test_a_close_machine_passes_and_adds_no_note(tmp_path):
    report = build_report(make_bundle(tmp_path / "vendor"))
    checks = indexed(report)
    assert checks["machine_size"]["status"] == "PASS"
    assert checks["network_distance"]["status"] == "PASS"
    assert not any("ms from the storage" in h["title"] for h in report["headline_limits"])


def test_a_run_without_a_machine_check_reads_not_run(tmp_path):
    bundle = make_bundle(tmp_path / "vendor")
    _set_machine(bundle)
    checks = indexed(build_report(bundle))
    assert checks["machine_size"]["status"] == "NOT RUN"
    assert checks["network_distance"]["status"] == "NOT RUN"


def test_a_distant_machine_is_named_as_the_likely_reason_for_a_failure(tmp_path):
    """
    The report never subtracts the round trip. It compares it with the amount
    by which each operation missed its limit.
    """
    bundle = make_bundle(tmp_path / "vendor")
    _set_machine(bundle, min_ms=5000.0, median_ms=5000.0)
    report = build_report(bundle)
    assert indexed(report)["network_distance"]["status"] == "INCONCLUSIVE"
    assert report["verdict"] == "FAIL"  # the note explains, it never changes a result
    note = report["headline_limits"][0]
    assert note["title"] == "The test machine is 5000 ms from the storage"
    assert "can explain every response-time failure" in note["detail"]


def test_a_failure_larger_than_the_round_trip_is_not_blamed_on_it(tmp_path):
    bundle = make_bundle(tmp_path / "vendor")
    slow = [m for m in build_report(bundle)["measurements"] if m["latency_status"] == "FAIL"]
    smallest = min(m["p99_latency_s"] - m["limit_p99_s"] for m in slow)
    rtt = max(11.0, smallest * 1000 / 2)
    _set_machine(bundle, min_ms=rtt, median_ms=rtt)
    note = build_report(bundle)["headline_limits"][0]
    assert "ms from the storage" in note["title"]
    assert "the distance alone does not explain the failures" in note["detail"]


def test_a_small_machine_gets_a_headline_note(tmp_path):
    bundle = make_bundle(tmp_path / "vendor")
    manifest = read_json(bundle / "manifest.json")
    manifest["stages"]["machine"]["machine"]["cores"] = 2
    write_json(bundle / "manifest.json", manifest)
    report = build_report(bundle)
    assert indexed(report)["machine_size"]["status"] == "INCONCLUSIVE"
    titles = [h["title"] for h in report["headline_limits"]]
    assert "The test machine is smaller than recommended" in titles


def test_a_run_older_than_the_machine_check_says_so(tmp_path):
    """Its saved settings have no machine sizes. That is not a 10TB problem."""
    bundle = make_bundle(tmp_path / "vendor")
    manifest = read_json(bundle / "manifest.json")
    del manifest["stages"]["machine"]
    del manifest["config"]["test_machine"]
    write_json(bundle / "manifest.json", manifest)
    checks = indexed(build_report(bundle))
    size = checks["machine_size"]
    assert size["status"] == "NOT RUN"
    assert "older than the machine check" in size["observed"]
    assert size["requirement"] == "Machine sizes were not saved with this run"
    assert "older than the machine check" in checks["network_distance"]["observed"]
