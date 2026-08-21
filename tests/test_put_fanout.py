"""
Structural tests for put_fanout.py, the write-side counterpart to
concurrency_fanout.py. See tests/test_concurrency_fanout.py's docstring for
why these run against a real local moto server rather than mock_aws() --
the same aiobotocore incompatibility applies here.

Same limitation as the read-side tests: moto has no real network latency,
so it can't demonstrate genuine PUT concurrency degradation. These only
confirm the sweep executes correctly and cleans up after itself.
"""
from src.put_fanout import run_put_fanout_sweep, summarize_put_fanout
from src.qw_s3_client import QwS3Client, QwS3Config

TEST_BUCKET = "qw-cert-test"


def _cfg_for(endpoint):
    cfg = QwS3Config(endpoint_url=endpoint, access_key="testing", secret_key="testing",
                      region="us-east-1", force_path_style=True)
    QwS3Client(cfg).ensure_bucket(TEST_BUCKET)
    return cfg


def test_put_fanout_sweep_runs_every_level_and_reports_no_errors(moto_server_endpoint):
    cfg = _cfg_for(moto_server_endpoint)
    levels = [1, 4, 8]

    results = run_put_fanout_sweep(cfg, TEST_BUCKET, "test/put-fanout", concurrency_levels=levels,
                                    object_size_kb=16, repeats=1)

    assert [r.concurrency for r in results] == levels
    for r in results:
        assert len(r.per_request_latencies_s) == r.concurrency
        assert r.error_count == 0
        assert r.wall_clock_s > 0


def test_put_fanout_sweep_deletes_its_own_objects(moto_server_endpoint):
    cfg = _cfg_for(moto_server_endpoint)
    client = QwS3Client(cfg)

    run_put_fanout_sweep(cfg, TEST_BUCKET, "test/put-fanout-cleanup", concurrency_levels=[1, 4],
                          object_size_kb=16, repeats=1)

    leftover = client.list_prefix(TEST_BUCKET, "test/put-fanout-cleanup")
    assert leftover["ok"]
    assert leftover["keys"] == []


def test_summarize_put_fanout_produces_well_formed_rows(moto_server_endpoint):
    cfg = _cfg_for(moto_server_endpoint)
    levels = [1, 4, 8]
    results = run_put_fanout_sweep(cfg, TEST_BUCKET, "test/put-fanout-summary", concurrency_levels=levels,
                                    object_size_kb=16, repeats=1)

    summary = summarize_put_fanout(results, efficiency_floor=0.4)
    assert len(summary["levels"]) == len(levels)
    assert summary["efficiency_floor"] == 0.4
    for row in summary["levels"]:
        assert row["error_count"] == 0
        assert row["throttle_count"] == 0
        assert row["wall_clock_s"] >= 0
