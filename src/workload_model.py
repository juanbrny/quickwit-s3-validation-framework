"""
Implements the math in docs/background/03_throughput_tier_sizing.md.
Run `python -m src.workload_model --print-table` to regenerate the worked
examples table after changing config/tiers.yaml.
"""
from __future__ import annotations

import argparse
import dataclasses
import math
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "tiers.yaml"


@dataclasses.dataclass
class OpMix:
    tier_name: str
    raw_gb_per_day: float
    avg_raw_mbps: float
    peak_raw_mbps: float
    avg_s3_mbps: float
    peak_s3_mbps: float
    num_indexer_nodes: int
    immature_put_rate_per_s: float
    immature_put_size_mb: float
    merges_per_day: float
    merge_get_ops_per_day: float
    merge_put_ops_per_day: float
    merge_put_parts_per_upload: int
    gc_delete_ops_per_day: float
    query_qps: float


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def compute_op_mix(tier_name: str, cfg: dict) -> OpMix:
    mc = cfg["model_constants"]
    # extreme_tiers (e.g. 10PB/day) sit outside the default `tiers` map on
    # purpose -- see config/tiers.yaml. Callers into those tiers must gate
    # on the operator's explicit cost confirmation before reaching here.
    tier = cfg["tiers"].get(tier_name) or cfg.get("extreme_tiers", {}).get(tier_name)
    if tier is None:
        raise KeyError(f"Unknown tier: {tier_name!r}")

    T = tier["raw_gb_per_day"]
    avg_raw_mbps = T * 1024 / 86400.0
    peak_raw_mbps = avg_raw_mbps * mc["peak_multiplier"]

    avg_s3_mbps = avg_raw_mbps / mc["compression_ratio"]
    peak_s3_mbps = peak_raw_mbps / mc["compression_ratio"]

    num_indexer_nodes = max(1, math.ceil(peak_raw_mbps / mc["per_core_indexing_mbps"]))

    immature_put_rate_per_s = num_indexer_nodes / mc["commit_timeout_s"]
    immature_put_size_mb = (avg_s3_mbps * mc["commit_timeout_s"]) / num_indexer_nodes

    # splits produced per day, then grouped into merges of merge_factor
    if immature_put_size_mb > 0:
        splits_per_day = (avg_s3_mbps * 86400) / immature_put_size_mb
    else:
        splits_per_day = 0
    merges_per_day = splits_per_day / mc["merge_factor"]

    merge_get_ops_per_day = merges_per_day * mc["merge_factor"]
    merge_put_ops_per_day = merges_per_day  # one merged output split per merge
    merge_put_parts_per_upload = max(1, math.ceil(mc["mature_split_gb"] / mc["multipart_part_gb"]))

    gc_delete_ops_per_day = merges_per_day * mc["merge_factor"]

    return OpMix(
        tier_name=tier_name,
        raw_gb_per_day=T,
        avg_raw_mbps=avg_raw_mbps,
        peak_raw_mbps=peak_raw_mbps,
        avg_s3_mbps=avg_s3_mbps,
        peak_s3_mbps=peak_s3_mbps,
        num_indexer_nodes=num_indexer_nodes,
        immature_put_rate_per_s=immature_put_rate_per_s,
        immature_put_size_mb=immature_put_size_mb,
        merges_per_day=merges_per_day,
        merge_get_ops_per_day=merge_get_ops_per_day,
        merge_put_ops_per_day=merge_put_ops_per_day,
        merge_put_parts_per_upload=merge_put_parts_per_upload,
        gc_delete_ops_per_day=gc_delete_ops_per_day,
        query_qps=tier["query_qps"],
    )


def query_get_count(profile: dict) -> int:
    """Documented Quickwit formula (docs/01 section 5), per split hit."""
    fields = profile["num_search_fields"]
    terms = profile["num_terms"]
    fieldnorm = profile["fieldnorm_fields"]
    docs_returned = profile["docs_returned"]
    per_split = (fields * terms * 3) + fieldnorm + 1
    return per_split + docs_returned  # docs_returned added once per query in the doc's formula


def print_table(cfg: dict):
    header = ("Tier", "avg raw MB/s", "peak raw MB/s", "avg S3 MB/s", "nodes",
              "PUTs/min", "merges/day", "merge GETs/day", "GC deletes/day")
    rows = []
    for tier_name in cfg["tiers"]:
        m = compute_op_mix(tier_name, cfg)
        rows.append((
            tier_name,
            f"{m.avg_raw_mbps:.2f}", f"{m.peak_raw_mbps:.2f}", f"{m.avg_s3_mbps:.2f}",
            str(m.num_indexer_nodes), f"{m.immature_put_rate_per_s*60:.1f}",
            f"{m.merges_per_day:.0f}", f"{m.merge_get_ops_per_day:.0f}",
            f"{m.gc_delete_ops_per_day:.0f}",
        ))
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(header)]
    def fmt_row(r):
        return " | ".join(c.ljust(w) for c, w in zip(r, widths))
    print(fmt_row(header))
    print("-+-".join("-" * w for w in widths))
    for r in rows:
        print(fmt_row(r))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--print-table", action="store_true")
    ap.add_argument("--tier", default=None)
    args = ap.parse_args()

    cfg = load_config()
    if args.print_table:
        print_table(cfg)
    elif args.tier:
        m = compute_op_mix(args.tier, cfg)
        for k, v in dataclasses.asdict(m).items():
            print(f"{k}: {v}")
    else:
        print_table(cfg)
