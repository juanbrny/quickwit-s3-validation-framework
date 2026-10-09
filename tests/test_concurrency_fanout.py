"""
Structural tests for concurrency_fanout.py against a local moto server.

IMPORTANT LIMITATION: moto is an in-memory emulator with no real network
latency, so it cannot demonstrate genuine concurrency degradation the way a
real vendor endpoint might (a backend that serializes "concurrent"
connections, throttles under load, or has a small connection pool will show
it here; moto essentially never will, since there's no real I/O being
serialized). These tests only confirm the sweep executes correctly at every
concurrency level and produces well-formed results -- they deliberately do
NOT assert anything about efficiency thresholds or degrades_at_concurrency,
since those numbers aren't meaningful against moto. Detecting a real
vendor's concurrency ceiling is what `run_certification.py fanout` against
a real endpoint is for.

Uses the moto_server_endpoint fixture (a real local HTTP server), not the
moto_s3 mock_aws() fixture used elsewhere -- the sweep runs on aiobotocore,
which talks to S3 over real HTTP via aiohttp, and mock_aws()'s monkeypatch
of botocore internals doesn't implement the response interface aiobotocore
expects.
"""
import pytest

from src.concurrency_fanout import (
    FanoutLevelResult,
    prepare_fanout_object,
    run_fanout_sweep,
    summarize_fanout,
)
from src.qw_s3_client import QwS3Client, QwS3Config

TEST_BUCKET = "qw-cert-test"


def _client_for(endpoint):
    cfg = QwS3Config(endpoint_url=endpoint, access_key="testing", secret_key="testing",
                      region="us-east-1", force_path_style=True)
    client = QwS3Client(cfg)
    client.ensure_bucket(TEST_BUCKET)
    return cfg, client


def test_prepare_fanout_object_uploads_requested_size(moto_server_endpoint):
    _, client = _client_for(moto_server_endpoint)
    size = prepare_fanout_object(client, TEST_BUCKET, "test/fanout-obj.bin", size_mb=2.0)
    assert size == 2 * 1024 * 1024

    got = client.get_full(TEST_BUCKET, "test/fanout-obj.bin")
    assert got["ok"]
    assert got["bytes"] == size


def test_fanout_sweep_runs_every_level_and_reports_no_errors(moto_server_endpoint):
    cfg, client = _client_for(moto_server_endpoint)
    levels = [1, 4, 8]
    size = prepare_fanout_object(client, TEST_BUCKET, "test/fanout-sweep.bin", size_mb=1.0)

    results = run_fanout_sweep(cfg, TEST_BUCKET, "test/fanout-sweep.bin", size,
                                concurrency_levels=levels)

    assert [r.concurrency for r in results] == levels
    for r in results:
        assert len(r.per_request_latencies_s) == r.concurrency
        assert r.error_count == 0  # moto should serve every range-GET successfully
        assert r.wall_clock_s > 0


def test_summarize_fanout_produces_well_formed_rows(moto_server_endpoint):
    cfg, client = _client_for(moto_server_endpoint)
    levels = [1, 4, 8]
    size = prepare_fanout_object(client, TEST_BUCKET, "test/fanout-summary.bin", size_mb=1.0)
    results = run_fanout_sweep(cfg, TEST_BUCKET, "test/fanout-summary.bin", size,
                                concurrency_levels=levels)

    summary = summarize_fanout(results, efficiency_floor=0.4)
    assert len(summary["levels"]) == len(levels)
    assert summary["efficiency_floor"] == 0.4
    for row in summary["levels"]:
        assert row["error_count"] == 0
        assert row["throttle_count"] == 0
        assert row["wall_clock_s"] >= 0


# Measured against AWS S3 us-east-1 from one laptop, and against a NetApp
# StorageGRID appliance on a local network. Both runs recorded zero errors and
# zero throttles at every level, so both must pass the sweep. Columns are
# concurrency, batch wall clock in seconds, and median request latency.
AWS_S3_REFERENCE = [
    (1, 0.661, 0.660), (8, 0.609, 0.556), (16, 0.889, 0.461), (32, 0.724, 0.440),
    (64, 0.837, 0.391), (128, 0.991, 0.567), (256, 5.370, 0.503),
]
STORAGEGRID_MEASURED = [
    (1, 0.053, 0.050), (8, 0.056, 0.022), (16, 0.063, 0.019), (32, 0.066, 0.020),
    (64, 0.081, 0.031), (128, 0.117, 0.051), (256, 0.280, 0.114),
    (512, 0.662, 0.280), (1024, 1.724, 0.469),
]


def _levels(rows):
    return [
        FanoutLevelResult(
            concurrency=k,
            wall_clock_s=wall,
            per_request_latencies_s=[p50] * max(k, 1),
            error_count=0,
            throttle_count=0,
        )
        for k, wall, p50 in rows
    ]


@pytest.mark.parametrize(
    "name,rows", [("aws", AWS_S3_REFERENCE), ("storagegrid", STORAGEGRID_MEASURED)]
)
def test_backends_with_no_errors_pass_the_serialization_gate(name, rows):
    """
    Regression test for a gate that failed its own reference implementation.

    The sweep used to score median request latency over batch wall clock, and
    fail anything under 0.4. Wall clock is bounded by the slowest request in
    the batch, so that ratio falls as concurrency rises for every backend. AWS
    S3 scored 0.09 at concurrency 256 here, and StorageGRID scored 0.27 at
    1024 while absorbing 278 requests' worth of latency at once. Both were
    marked as serializing. Neither was.
    """
    summary = summarize_fanout(_levels(rows), min_speedup=2.0)
    assert summary["serializes_at_concurrency"] is None
    assert summary["min_speedup"] >= 2.0
    # The old diagnostic still shows the latency spread, and still dips below
    # the old floor. It must no longer decide anything.
    assert min(level["efficiency"] for level in summary["levels"]) < 0.4


def test_a_serializing_backend_is_detected():
    """
    A backend that serves one request at a time gives a batch wall clock of
    concurrency x latency, so its speedup stays near 1 however many requests
    the client offers at once.
    """
    serialized = [(k, 0.05 * k, 0.05) for k in (1, 8, 16, 32)]
    summary = summarize_fanout(_levels(serialized), min_speedup=2.0)
    assert summary["serializes_at_concurrency"] == 8
    assert summary["min_speedup"] == pytest.approx(1.0, abs=0.01)
