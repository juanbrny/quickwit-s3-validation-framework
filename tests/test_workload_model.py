"""
Pure-math tests for workload_model.py -- no S3, no moto, just verifying the
op-mix formulas in docs/03_throughput_tier_sizing.md behave as documented.
"""
import pytest

from src.workload_model import compute_op_mix, load_config


def test_all_tiers_produce_sane_positive_values():
    cfg = load_config()
    for tier_name in cfg["tiers"]:
        m = compute_op_mix(tier_name, cfg)
        assert m.avg_raw_mbps > 0
        assert m.peak_raw_mbps > m.avg_raw_mbps  # peak_multiplier > 1
        assert m.num_indexer_nodes >= 1
        assert m.merges_per_day > 0
        assert m.merge_get_ops_per_day == pytest.approx(
            m.merges_per_day * cfg["model_constants"]["merge_factor"]
        )


def test_node_count_is_monotonic_across_tiers():
    """More GB/day should never need *fewer* indexer nodes."""
    cfg = load_config()
    ordered_tiers = ["100GB", "1TB", "10TB", "100TB", "1PB"]
    node_counts = [compute_op_mix(t, cfg).num_indexer_nodes for t in ordered_tiers]
    assert node_counts == sorted(node_counts)


def test_op_counts_flat_within_a_fixed_node_count():
    """
    Documented, non-obvious finding (docs/03): because commit_timeout_secs
    is time-triggered, not size-triggered, op *counts* stay flat across
    tiers that share the same indexer node count -- only op *sizes* scale.
    Uses two synthetic sub-tiers guaranteed to share a node count, rather
    than depending on which of `config/tiers.yaml`'s named tiers happen to
    land on the same bracket.
    """
    cfg = load_config()
    cfg = {**cfg, "tiers": {
        "synthetic_small": {"raw_gb_per_day": 100, "query_qps": 1},
        "synthetic_larger": {"raw_gb_per_day": 500, "query_qps": 1},
    }}
    m_small = compute_op_mix("synthetic_small", cfg)
    m_larger = compute_op_mix("synthetic_larger", cfg)
    assert m_small.num_indexer_nodes == m_larger.num_indexer_nodes == 1
    assert m_small.merges_per_day == m_larger.merges_per_day
    assert m_small.immature_put_size_mb < m_larger.immature_put_size_mb


def test_gc_deletes_match_merge_reads():
    """Every split fully read during a merge is also the one deleted after publish."""
    cfg = load_config()
    m = compute_op_mix("10TB", cfg)
    assert m.gc_delete_ops_per_day == m.merge_get_ops_per_day
