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
from src.concurrency_fanout import (
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
