"""
Structural tests for concurrency_fanout.py against moto.

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
"""
from src.concurrency_fanout import (
    build_fanout_client,
    prepare_fanout_object,
    run_fanout_sweep,
    summarize_fanout,
)
from src.qw_s3_client import QwS3Config

TEST_BUCKET = "qw-cert-test"  # must match tests/conftest.py's moto_s3 fixture


def _base_cfg(**overrides):
    return QwS3Config(endpoint_url=None, access_key="testing", secret_key="testing",
                       region="us-east-1", **overrides)


def test_build_fanout_client_sizes_pool_above_the_sweep(moto_s3):
    base_cfg = _base_cfg(max_concurrency=50)
    levels = [1, 8, 16, 32, 64]
    client = build_fanout_client(base_cfg, levels)
    assert client.cfg.max_concurrency > max(levels)
    # dataclasses.replace() must return a copy -- base_cfg itself is untouched
    assert base_cfg.max_concurrency == 50


def test_prepare_fanout_object_uploads_requested_size(moto_s3):
    client = build_fanout_client(_base_cfg(), [1, 8])
    size = prepare_fanout_object(client, TEST_BUCKET, "test/fanout-obj.bin", size_mb=2.0)
    assert size == 2 * 1024 * 1024

    got = client.get_full(TEST_BUCKET, "test/fanout-obj.bin")
    assert got["ok"]
    assert got["bytes"] == size


def test_fanout_sweep_runs_every_level_and_reports_no_errors(moto_s3):
    levels = [1, 4, 8]
    client = build_fanout_client(_base_cfg(), levels)
    size = prepare_fanout_object(client, TEST_BUCKET, "test/fanout-sweep.bin", size_mb=1.0)

    results = run_fanout_sweep(client, TEST_BUCKET, "test/fanout-sweep.bin", size,
                                concurrency_levels=levels)

    assert [r.concurrency for r in results] == levels
    for r in results:
        assert len(r.per_request_latencies_s) == r.concurrency
        assert r.error_count == 0  # moto should serve every range-GET successfully
        assert r.wall_clock_s > 0


def test_summarize_fanout_produces_well_formed_rows(moto_s3):
    levels = [1, 4, 8]
    client = build_fanout_client(_base_cfg(), levels)
    size = prepare_fanout_object(client, TEST_BUCKET, "test/fanout-summary.bin", size_mb=1.0)
    results = run_fanout_sweep(client, TEST_BUCKET, "test/fanout-summary.bin", size,
                                concurrency_levels=levels)

    summary = summarize_fanout(results, efficiency_floor=0.4)
    assert len(summary["levels"]) == len(levels)
    assert summary["efficiency_floor"] == 0.4
    for row in summary["levels"]:
        assert row["error_count"] == 0
        assert row["throttle_count"] == 0
        assert row["wall_clock_s"] >= 0
