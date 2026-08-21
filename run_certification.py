#!/usr/bin/env python3
"""
CLI entrypoint. See README.md for the full quick-start.

    run_certification.py compat  --endpoint ... --bucket ... --access-key ... --secret-key ...
    run_certification.py fanout  --endpoint ... --bucket ... --access-key ... --secret-key ...
    run_certification.py load    --endpoint ... --bucket ... --tier 1TB --duration-min 30
    run_certification.py report  --tier 1TB --out report_1TB.md
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.qw_s3_client import QwS3Client, QwS3Config
from src.compat_checks import probe_flavor
from src.workload_model import compute_op_mix, load_config
from src.ingest_merge_sim import run_ingest_merge_sim, SplitKeyRegistry
from src.query_sim import run_query_sim
from src.consistency_probes import run_consistency_probes
from src.concurrency_fanout import (
    prepare_fanout_object, run_fanout_sweep,
    summarize_fanout, render_fanout_markdown,
)
from src.report import (
    summarize_ops, compare_to_baseline, render_markdown_report,
    render_compat_markdown, _load_jsonl,
)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


def cmd_compat(args):
    result = probe_flavor(args.endpoint, args.access_key, args.secret_key,
                           args.bucket, args.region)
    tag = _safe_name(args.endpoint)
    out = REPORTS_DIR / f"compat_{tag}.json"
    out.write_text(json.dumps(result, indent=2))

    md_out = REPORTS_DIR / f"compat_{tag}.md"
    render_compat_markdown(result, md_out)

    print(f"Compat probe complete. Recommended flavor: {result['recommended_flavor']}")
    print(f"Full results (JSON): {out}")
    print(f"Comparison table (Markdown): {md_out}")
    if args.baseline_out:
        Path(args.baseline_out).write_text(json.dumps(result, indent=2))


def cmd_fanout(args):
    """
    Fast, targeted diagnostic (seconds, not the full duration-based `load`
    soak): can this backend actually sustain many concurrent range-GETs and
    deliver the throughput the "fire N requests to hide S3's per-request
    latency" strategy depends on? See concurrency_fanout.py and the design
    discussion in docs/02_test_methodology.md.
    """
    cfg_yaml = load_config()
    bands = cfg_yaml["pass_fail_bands"]
    levels = [int(x) for x in args.levels.split(",")] if args.levels else bands["fanout_concurrency_levels"]

    base_cfg = QwS3Config.from_flavor(args.flavor, args.endpoint, args.access_key,
                                       args.secret_key, args.region)
    client = QwS3Client(base_cfg)
    client.ensure_bucket(args.bucket)

    tag = _safe_name(args.endpoint)
    key = f"qwcert/fanout/{tag}-test-object.split"

    print(f"Uploading a {args.object_size_mb}MB test object and sweeping "
          f"concurrency levels {levels}...")
    obj_size = prepare_fanout_object(client, args.bucket, key, size_mb=args.object_size_mb)
    results = run_fanout_sweep(base_cfg, args.bucket, key, obj_size, concurrency_levels=levels)
    summary = summarize_fanout(results, efficiency_floor=bands["fanout_efficiency_min"])

    json_out = REPORTS_DIR / f"fanout_{tag}.json"
    json_out.write_text(json.dumps(summary, indent=2))
    md_out = REPORTS_DIR / f"fanout_{tag}.md"
    render_fanout_markdown(summary, md_out)

    print(f"Full results (JSON): {json_out}")
    print(f"Sweep table (Markdown): {md_out}")
    if summary["degrades_at_concurrency"]:
        print(f"Degrades at concurrency = {summary['degrades_at_concurrency']} "
              f"(efficiency floor {bands['fanout_efficiency_min']})")
    else:
        print("No degradation found within the tested concurrency range.")


def cmd_load(args):
    cfg_yaml = load_config()
    if args.tier in cfg_yaml.get("extreme_tiers", {}) and not args.confirm_extreme_cost:
        sys.exit(
            f"Refusing to run tier {args.tier} without --confirm-extreme-cost.\n"
            f"This tier moves petabytes of data during the soak. It can run up a "
            f"large, real cloud bill, and any objects or buckets left behind after "
            f"the run become a hidden ongoing cost. Re-run with "
            f"--confirm-extreme-cost only once you have sign-off and a guaranteed "
            f"teardown plan for everything this run creates."
        )
    op_mix = compute_op_mix(args.tier, cfg_yaml)

    qw_cfg = QwS3Config.from_flavor(args.flavor, args.endpoint, args.access_key,
                                     args.secret_key, args.region,
                                     max_concurrency=cfg_yaml["model_constants"]
                                     ["s3_max_concurrency_per_node"])
    client = QwS3Client(qw_cfg)
    client.ensure_bucket(args.bucket)

    tag = _safe_name(args.endpoint)
    prefix = f"qwcert/{args.tier}"
    ingest_out = REPORTS_DIR / f"{args.tier}_{tag}_ingest_merge.jsonl"
    query_out = REPORTS_DIR / f"{args.tier}_{tag}_query.jsonl"
    consistency_out = REPORTS_DIR / f"{args.tier}_{tag}_consistency.jsonl"
    bands = cfg_yaml["pass_fail_bands"]

    registry = SplitKeyRegistry()
    stop_event = threading.Event()

    print(f"Running ingest+merge, query, and consistency probes concurrently "
          f"for {args.duration_min} min ({op_mix.num_indexer_nodes} simulated "
          f"indexer node(s), {op_mix.query_qps} query QPS)...")

    # All three run concurrently against the shared bucket, mirroring a real
    # deployment where indexers, mergers, and searchers hit S3 at the same
    # time -- not sequentially. They share `stop_event` (single deadline)
    # and `registry` (queries only ever target splits the ingest/merge sim
    # has actually published and not yet GC'd).
    ingest_threads, ingest_sink = run_ingest_merge_sim(
        client, args.bucket, prefix, op_mix, args.duration_min, ingest_out,
        registry, stop_event=stop_event, block=False)

    query_threads, query_sink = run_query_sim(
        client, args.bucket, registry.snapshot, cfg_yaml["query_profiles"],
        op_mix.query_qps, args.duration_min, query_out,
        stop_event=stop_event, block=False)

    consistency_thread = threading.Thread(
        target=lambda: run_consistency_probes(
            client, args.bucket, prefix, args.duration_min, interval_s=5,
            deadline_s=bands["consistency_probe_deadline_s"], out_path=consistency_out),
        daemon=True)
    consistency_thread.start()

    try:
        for t in ingest_threads:
            t.join()
        for t in query_threads:
            t.join()
        consistency_thread.join()
    finally:
        stop_event.set()
        ingest_sink.close()
        query_sink.close()

    consistency_rows = _load_jsonl(consistency_out)
    success_pct = (100.0 * sum(1 for r in consistency_rows
                                if r.get("success") and r.get("within_deadline"))
                   / len(consistency_rows)) if consistency_rows else 0.0

    print(f"Raw results written to {REPORTS_DIR}/")
    print(f"Consistency: {success_pct:.2f}% success")
    print(f"Live splits remaining at end of run: {len(registry.snapshot())}")

    if args.baseline:
        print(f"Baseline file provided: {args.baseline} (used at report time)")


def cmd_report(args):
    tag = _safe_name(args.endpoint) if args.endpoint else "unknown"
    ingest_out = REPORTS_DIR / f"{args.tier}_{tag}_ingest_merge.jsonl"
    query_out = REPORTS_DIR / f"{args.tier}_{tag}_query.jsonl"
    consistency_out = REPORTS_DIR / f"{args.tier}_{tag}_consistency.jsonl"
    compat_path = REPORTS_DIR / f"compat_{tag}.json"
    fanout_path = REPORTS_DIR / f"fanout_{tag}.json"

    cfg_yaml = load_config()
    bands = cfg_yaml["pass_fail_bands"]

    rows = _load_jsonl(ingest_out) + _load_jsonl(query_out)
    vendor_summary = summarize_ops(rows)

    baseline_summary = None
    if args.baseline and Path(args.baseline).exists():
        baseline_summary = json.loads(Path(args.baseline).read_text())

    op_verdicts = compare_to_baseline(vendor_summary, baseline_summary, bands)

    consistency_rows = _load_jsonl(consistency_out)
    total = len(consistency_rows)
    successes = sum(1 for r in consistency_rows if r.get("success") and r.get("within_deadline"))
    consistency_result = {"total_probes": total, "successes": successes,
                           "success_pct": (100.0 * successes / total) if total else 0.0}

    compat_result = json.loads(compat_path.read_text()) if compat_path.exists() else \
        {"recommended_flavor": None, "attempts": {}}

    fanout_summary = json.loads(fanout_path.read_text()) if fanout_path.exists() else None
    if fanout_summary is None:
        print(f"Note: no {fanout_path.name} found -- run `fanout` first if you want the "
              f"concurrency sweep included in this report. Proceeding without it.")

    out_path = Path(args.out)
    final = render_markdown_report(args.tier, args.endpoint or tag, compat_result,
                                    op_verdicts, consistency_result, out_path,
                                    fanout_summary=fanout_summary)
    print(f"Report written to {out_path}. Overall: {'CERTIFIED-ish' if final else 'NOT CERTIFIED'}")


def _safe_name(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_compat = sub.add_parser("compat")
    p_compat.add_argument("--endpoint", required=True)
    p_compat.add_argument("--bucket", required=True)
    p_compat.add_argument("--access-key", required=True)
    p_compat.add_argument("--secret-key", required=True)
    p_compat.add_argument("--region", default="us-east-1")
    p_compat.add_argument("--baseline-out", default=None)
    p_compat.set_defaults(func=cmd_compat)

    p_fanout = sub.add_parser("fanout")
    p_fanout.add_argument("--endpoint", required=True)
    p_fanout.add_argument("--bucket", required=True)
    p_fanout.add_argument("--access-key", required=True)
    p_fanout.add_argument("--secret-key", required=True)
    p_fanout.add_argument("--region", default="us-east-1")
    p_fanout.add_argument("--flavor", default="none")
    p_fanout.add_argument("--object-size-mb", type=float, default=8.0)
    p_fanout.add_argument("--levels", default=None,
                           help="Comma-separated concurrency levels, e.g. 1,8,16,32,64,128,256 "
                                "(defaults to config/tiers.yaml pass_fail_bands.fanout_concurrency_levels)")
    p_fanout.set_defaults(func=cmd_fanout)

    p_load = sub.add_parser("load")
    p_load.add_argument("--endpoint", required=True)
    p_load.add_argument("--bucket", required=True)
    p_load.add_argument("--access-key", required=True)
    p_load.add_argument("--secret-key", required=True)
    p_load.add_argument("--region", default="us-east-1")
    p_load.add_argument("--flavor", default="none")
    p_load.add_argument("--tier", required=True,
                         choices=["100GB", "1TB", "10TB", "100TB", "1PB", "10PB"])
    p_load.add_argument("--duration-min", type=float, default=30.0)
    p_load.add_argument("--baseline", default=None)
    p_load.add_argument("--confirm-extreme-cost", action="store_true",
                         help="Required to run tiers listed under config/tiers.yaml's "
                              "extreme_tiers (currently 10PB). These soaks move petabytes "
                              "of data and can run up a large, real cloud bill -- only pass "
                              "this once you have cost sign-off and a guaranteed teardown "
                              "plan for everything the run creates.")
    p_load.set_defaults(func=cmd_load)

    p_report = sub.add_parser("report")
    p_report.add_argument("--tier", required=True)
    p_report.add_argument("--endpoint", default=None)
    p_report.add_argument("--baseline", default=None)
    p_report.add_argument("--out", required=True)
    p_report.set_defaults(func=cmd_report)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
