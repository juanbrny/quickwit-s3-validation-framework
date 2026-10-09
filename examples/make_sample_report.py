"""Generate a complete, clearly labeled HTML example without contacting S3."""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.concurrency_fanout import FanoutLevelResult, summarize_fanout
from src.report_model import build_report
from src.report_render import write_reports
from src.run_store import digest, write_json
from src.workload_model import load_config, compute_op_mix
from dataclasses import asdict


def make_bundle(path, aws=False):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    cfg = load_config()
    mix = asdict(compute_op_mix("1TB", cfg))
    flavor = "none" if aws else "minio"
    epoch = 1791363600.0
    env = {
        "commit": "illustrative-data",
        "source_sha256": "illustrative-data",
        "python": "3.12.0",
        "dependencies": {"boto3": "illustrative"},
        "cpu_count": 8,
        "architecture": "x86_64",
        "os": "Linux (illustrative)",
        "memory_bytes": 32 * 1024**3,
    }
    identity = {
        "endpoint": "https://s3.us-east-1.amazonaws.com"
        if aws
        else "https://s3.example.test",
        "bucket": "qw-cert-aws" if aws else "qw-cert-example",
        "region": "us-east-1",
    }
    base = {
        "status": "COMPLETED",
        "started_at": "2026-10-07T09:00:00+00:00",
        "finished_at": "2026-10-07T09:03:00+00:00",
        "started_epoch": epoch,
        "actual_duration_s": 180.0,
        "environment": env,
        "artifacts": {},
        "options": {"flavor": flavor, "runner_location": "Example runner · us-east-1"},
    }
    stages = {
        name: copy.deepcopy(base) for name in ("compat", "fanout", "put-fanout", "load")
    }
    load = stages["load"]
    load["options"].update(tier="1TB", duration_min=3.0)
    load.update(
        measurement_started_epoch=epoch,
        measurement_duration_s=180,
        op_mix=mix,
        effective_config={
            "force_path_style": not aws,
            "checksum_algorithm": "crc32c",
            "disable_multipart_upload": False,
            "max_concurrency": 50,
        },
    )
    manifest = {
        "schema_version": 1,
        "run_id": "example-aws-reference" if aws else "example-vendor-1tb",
        "created_at": "2026-10-07T09:00:00+00:00",
        "identity": identity,
        "config": cfg,
        "stages": stages,
    }
    ops = [
        "put_object",
        "get_object_full",
        "delete_objects_bulk",
        "get_footer",
        "get_term_or_field",
        "get_doc",
        "query_wall_clock",
    ]
    ingest, query = [], []
    for index, op in enumerate(ops):
        count = 1800 if op == "query_wall_clock" else 120
        for i in range(count):
            offset = (i + 0.5) * 180 / count
            latency = (0.045 if op != "query_wall_clock" else 0.15) * (
                0.9 + 0.2 * (i % 20) / 19
            )
            if not aws:
                latency *= 2.8 if op == "query_wall_clock" else 1.35
            worker = (
                "indexer-0"
                if op == "put_object"
                else "merger-0"
                if op == "get_object_full"
                else "gc-0"
                if "delete" in op
                else "searcher"
            )
            multiplier = 0.88 if not aws and offset >= 120 else 1.02
            size = (
                int(mix["avg_s3_mbps"] * 1024**2 * 180 / count * multiplier)
                if op == "put_object"
                else 8192
                if "get" in op
                else 0
            )
            row = dict(
                ts=epoch + offset,
                worker=worker,
                op=op,
                ok=True,
                latency_s=latency,
                bytes=size,
                error=None,
            )
            (ingest if index < 3 else query).append(row)
    for name, rows in [("ingest_merge.jsonl", ingest), ("query.jsonl", query)]:
        (path / name).write_text("".join(json.dumps(r) + "\n" for r in rows))
    probes = [
        dict(
            probe=p,
            success=True,
            elapsed_s=0.04,
            within_deadline=True,
            ts=epoch + i * 5,
        )
        for i in range(36)
        for p in ("read_after_write", "list_after_write", "delete_visibility")
    ]
    (path / "consistency.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in probes)
    )
    names = [
        "range_get_semantics",
        "multipart_upload",
        "checksum_algorithm",
        "multi_object_delete",
        "path_style_addressing",
    ]
    good = {
        "all_passed": True,
        "results": {
            n: {
                "passed": True,
                "detail": "Expected S3 behavior observed (illustrative).",
            }
            for n in names
        },
        "yaml": "storage:\n  s3:\n    endpoint: "
        + identity["endpoint"]
        + "\n    force_path_style_access: true",
    }
    attempts = {flavor: good}
    if not aws:
        attempts = {
            "none": {
                "all_passed": False,
                "results": {
                    "path_style_addressing": {
                        "passed": False,
                        "detail": "Path-style access required (illustrative).",
                    }
                },
            },
            **attempts,
        }
    write_json(
        path / "compat.json", {"recommended_flavor": flavor, "attempts": attempts}
    )
    for stage, key in [("fanout", "fanout"), ("put-fanout", "put_fanout")]:
        bands = cfg["pass_fail_bands"]
        levels = bands[key + "_concurrency_levels"]
        results = []
        for k in levels:
            # Wall clock grows with concurrency, but far more slowly than
            # concurrency itself, which is what a backend serving requests
            # together looks like.
            latencies = [0.045] * k
            latencies[-1] = 0.065
            results.append(
                FanoutLevelResult(
                    concurrency=k,
                    wall_clock_s=0.045 + k * 0.0034,
                    per_request_latencies_s=latencies,
                    error_count=0,
                    throttle_count=0,
                )
            )
        # Built by the real summarizer, so the sample can never describe a
        # shape the reporting code does not produce.
        write_json(
            path / (stage + ".json"),
            summarize_fanout(
                results,
                bands[key + "_efficiency_min"],
                bands[key + "_serialization_min_speedup"],
            ),
        )
        stages[stage]["tested_levels"] = levels
        stages[stage]["options"].update(repeats=3, levels=levels)
    for stage, files in {
        "compat": ["compat.json"],
        "fanout": ["fanout.json"],
        "put-fanout": ["put-fanout.json"],
        "load": ["ingest_merge.jsonl", "query.jsonl", "consistency.jsonl"],
    }.items():
        stages[stage]["artifacts"] = {name: digest(path / name) for name in files}
    write_json(path / "manifest.json", manifest)
    return path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", default="reports/example")
    args = p.parse_args()
    root = Path(args.out_dir)
    vendor = make_bundle(root / "vendor")
    baseline = make_bundle(root / "aws", aws=True)
    report = build_report(vendor, baseline)
    report["example"] = True
    write_reports(report, root / "sample-report.html", vendor)
    print(root / "sample-report.html")


if __name__ == "__main__":
    main()
