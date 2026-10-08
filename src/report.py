"""
Aggregates the raw JSONL result logs from ingest_merge_sim / query_sim /
consistency_probes into the pass/fail scorecard described in
docs/02_test_methodology.md, relative to an AWS S3 baseline run of the same
scripts (see README quick-start step 2).
"""
from __future__ import annotations

import json
import math
import statistics
from pathlib import Path
from typing import Optional

import yaml

from .qw_s3_client import DEFAULT_FLAVORS, flavor_note

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "tiers.yaml"


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return None
    if not 0 <= pct <= 100 or any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("Percentiles require finite, non-negative measurements and a percentile in [0, 100].")
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (k - f) * (s[c] - s[f])


def summarize_ops(rows: list[dict]) -> dict:
    by_op: dict[str, list[dict]] = {}
    for r in rows:
        by_op.setdefault(r["op"], []).append(r)

    summary = {}
    for op, op_rows in by_op.items():
        latencies = [r["latency_s"] for r in op_rows]
        errors = [r for r in op_rows if not r["ok"]]
        throttles = [r for r in errors if (r.get("error") or "").lower() in
                     ("slowdown", "requestlimitexceeded", "503", "throttlingexception")]
        summary[op] = {
            "count": len(op_rows),
            "error_count": len(errors),
            "error_pct": 100.0 * len(errors) / len(op_rows) if op_rows else 0.0,
            "non_throttle_error_pct": 100.0 * (len(errors) - len(throttles)) / len(op_rows),
            "throttle_count": len(throttles),
            "throttle_pct": 100.0 * len(throttles) / len(op_rows) if op_rows else 0.0,
            "p50_latency_s": _percentile(latencies, 50),
            "p90_latency_s": _percentile(latencies, 90),
            "p99_latency_s": _percentile(latencies, 99),
        }
    return summary


def compare_to_baseline(vendor_summary: dict, baseline_summary: Optional[dict],
                         bands: dict) -> dict:
    verdicts = {}
    for op, v in vendor_summary.items():
        b = (baseline_summary or {}).get(op)
        checks = []

        error_pct = v.get("non_throttle_error_pct", v["error_pct"])
        checks.append(("error_rate", error_pct <= bands["error_rate_max_pct"],
                        f"{error_pct:.3f}% non-throttle (max {bands['error_rate_max_pct']}%)"))
        checks.append(("throttle_rate", v["throttle_pct"] <= bands["throttle_rate_sustained_max_pct"],
                        f"{v['throttle_pct']:.3f}% (max {bands['throttle_rate_sustained_max_pct']}%)"))

        if b:
            multiplier_key = None
            if "wall_clock" in op:
                multiplier_key = "query_wall_clock_p99_multiplier_vs_aws"
            elif "delete" in op:
                multiplier_key = "bulk_delete_p99_multiplier_vs_aws"
            elif "range" in op or "footer" in op or "term" in op or "doc" in op:
                multiplier_key = "range_get_p99_multiplier_vs_aws"
            elif "put" in op or "multipart" in op:
                multiplier_key = "put_p99_multiplier_vs_aws"

            if multiplier_key and b.get("p99_latency_s") is not None and b["p99_latency_s"] > 0:
                ratio = v["p99_latency_s"] / b["p99_latency_s"]
                max_mult = bands[multiplier_key]
                checks.append((f"p99_vs_aws_{multiplier_key}", ratio <= max_mult,
                                f"{ratio:.2f}x AWS baseline (max {max_mult}x); "
                                f"vendor p99={v['p99_latency_s']*1000:.1f}ms, "
                                f"aws p99={b['p99_latency_s']*1000:.1f}ms"))
            elif multiplier_key:
                checks.append(("baseline_comparison", None, "invalid or zero baseline p99"))
        else:
            checks.append(("baseline_comparison", None, "no AWS baseline provided for this op"))

        passed = bool(checks) and all(c[1] is True for c in checks)
        verdicts[op] = {"passed": passed, "checks": checks, "raw": v}
    return verdicts


def render_markdown_report(tier: str, endpoint: str, compat_result: dict,
                            op_verdicts: dict, consistency_result: dict,
                            out_path: Path, fanout_summary: Optional[dict] = None):
    lines = [f"# Quickwit S3 Compatibility & Performance Report",
             f"", f"**Tier:** {tier}  ", f"**Endpoint under test:** {endpoint}  ", ""]

    lines.append("## Layer 2 — Compatibility knobs")
    rec = compat_result.get("recommended_flavor")
    if rec:
        lines.append(f"**Recommended flavor:** `{rec}`")
        note = flavor_note(rec)
        if note:
            lines.append("")
            lines.append(note)
        yaml_block = compat_result["attempts"][rec].get("yaml")
        if yaml_block:
            lines.append("\n```yaml\n" + yaml_block + "\n```")
        lines.append("")
        verdict = "PASS" if rec in DEFAULT_FLAVORS else "PASS WITH DEVIATION"
        lines.append(f"**Verdict: {verdict}**\n")
    else:
        lines.append("**No working flavor/config combination found. Verdict: FAIL**\n")

    for flavor, attempt in compat_result.get("attempts", {}).items():
        lines.append(f"<details><summary>{flavor}: "
                      f"{'all checks passed' if attempt.get('all_passed') else 'failed'}</summary>\n")
        if attempt.get("same_settings_as"):
            lines.append(f"- same settings as `{attempt['same_settings_as']}`, "
                          "so its result is reused")
        if "results" in attempt:
            for name, r in attempt["results"].items():
                mark = "✅" if r["passed"] else "❌"
                lines.append(f"- {mark} `{name}`: {r['detail']}")
        else:
            lines.append(f"- error: {attempt.get('error')}")
        lines.append("\n</details>\n")

    lines.append("## Layer 3 — Workload-shaped load test\n")
    lines.append("| Operation | Count | Error % | Throttle % | p50 (ms) | p90 (ms) | p99 (ms) | Verdict |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|")
    overall_pass = True
    for op, v in op_verdicts.items():
        raw = v["raw"]
        mark = "✅ PASS" if v["passed"] else "❌ FAIL"
        overall_pass = overall_pass and v["passed"]
        lines.append(f"| `{op}` | {raw['count']} | {raw['error_pct']:.3f} | "
                      f"{raw['throttle_pct']:.3f} | {raw['p50_latency_s']*1000:.1f} | "
                      f"{raw['p90_latency_s']*1000:.1f} | {raw['p99_latency_s']*1000:.1f} | {mark} |")

    lines.append("\n<details><summary>Per-operation check detail</summary>\n")
    for op, v in op_verdicts.items():
        lines.append(f"**{op}**")
        for name, passed, detail in v["checks"]:
            mark = "✅" if passed else ("⚪" if passed is None else "❌")
            lines.append(f"- {mark} {name}: {detail}")
    lines.append("\n</details>\n")

    lines.append("## Consistency probes\n")
    lines.append(f"- Total probes: {consistency_result.get('total_probes', 0)}")
    lines.append(f"- Successful within deadline: {consistency_result.get('successes', 0)}")
    lines.append(f"- Success rate: {consistency_result.get('success_pct', 0):.2f}%")
    consistency_pass = consistency_result.get("success_pct", 0) >= 100.0
    lines.append(f"- **Verdict: {'PASS' if consistency_pass else 'FAIL'}**\n")

    fanout_pass = True
    if fanout_summary is not None:
        lines.append("## Concurrency fan-out sweep\n")
        degrades_at = fanout_summary.get("degrades_at_concurrency")
        fanout_pass = degrades_at is None
        if degrades_at:
            lines.append(
                f"Degrades at concurrency = **{degrades_at}** (efficiency floor "
                f"{fanout_summary.get('efficiency_floor')}). See `fanout_*.md` for the "
                "full sweep -- this means the backend stops sustaining the concurrent "
                "range-GET fan-out this architecture depends on to hide S3-style "
                "per-request latency somewhere at or below this concurrency level."
            )
            lines.append(f"- **Verdict: FAIL**\n")
        else:
            lines.append("No degradation found within the tested concurrency range. "
                          "See `fanout_*.md` for the full sweep.")
            lines.append(f"- **Verdict: PASS**\n")

    lines.append("## Overall\n")
    # Legacy input has no run identity, complete gate coverage, or configuration
    # provenance. Only the versioned report pipeline may issue certification.
    final = False
    lines.append("Legacy diagnostic only: run metadata and required evidence are unavailable. "
                 "Use `report --run-dir` for the complete evaluated report.")
    if final and rec == "none":
        lines.append(f"### CERTIFIED for {tier} (default configuration)")
    elif final:
        lines.append(f"### CERTIFIED WITH DEVIATION for {tier} (requires `flavor: {rec}`)")
    else:
        lines.append(f"### NOT CERTIFIED at {tier}")
        failing = [op for op, v in op_verdicts.items() if not v["passed"]]
        if failing:
            lines.append(f"\nFirst metrics to investigate: {', '.join(failing)}")
        if not consistency_pass:
            lines.append("\nConsistency probes did not reach 100% success within deadline.")

    out_path.write_text("\n".join(lines))
    return final


def render_compat_markdown(compat_result: dict, out_path: Path):
    """
    Renders the flavor x check comparison table in the same format shown in
    the customer-facing deck (Quickwit-S3-Write-Read-Patterns.pptx, slide
    "A Pass/Fail Table, Not a Guess") -- one column per flavor attempted by
    probe_flavor(), one row per check, plus the recommended flavor's
    storage.s3.yaml block. This is the Layer 2 report; see
    docs/02_test_methodology.md for how it gates Layer 3.
    """
    attempts = compat_result.get("attempts", {})
    rec = compat_result.get("recommended_flavor")

    # Union of check names across attempts, in first-seen order, so a flavor
    # that errored out before running any checks doesn't reorder the table.
    check_names: list[str] = []
    for a in attempts.values():
        for name in a.get("results", {}):
            if name not in check_names:
                check_names.append(name)

    flavors = list(attempts.keys())
    lines = [
        "# S3 API Compatibility Check",
        "",
        "One row per check, one column per flavor attempted. "
        "See docs/01_s3_interaction_analysis.md section 2 for what each check "
        "corresponds to in Quickwit's own storage config.",
        "",
    ]

    if not flavors:
        lines.append("_No flavors were attempted (bucket setup likely failed for all of them)._")
        out_path.write_text("\n".join(lines))
        return

    header = "| Check | " + " | ".join(f"`{f}`" for f in flavors) + " |"
    sep = "|---|" + "---|" * len(flavors)
    lines += [header, sep]

    for name in check_names:
        row = [name]
        for f in flavors:
            r = attempts[f].get("results", {}).get(name)
            if r is None:
                row.append("\u2014")  # this flavor never got this far (e.g. bucket setup failed)
            else:
                row.append("\u2705" if r["passed"] else "\u274c")
        lines.append("| " + " | ".join(row) + " |")

    twins = {f: a["same_settings_as"] for f, a in attempts.items()
              if a.get("same_settings_as")}
    if twins:
        lines.append("")
        for f, twin in twins.items():
            lines.append(f"`{f}` holds the same settings as `{twin}`. "
                          "The probe runs the checks once and reuses the result.")
    lines.append("")
    if rec:
        lines.append(f"**Recommended flavor: `{rec}`**")
        same = compat_result.get("equivalent_flavors") or []
        if same:
            lines.append("")
            lines.append(
                "The same settings also carry these names: "
                + ", ".join(f"`{f}`" for f in same)
                + "."
            )
        note = flavor_note(rec)
        if note:
            lines.append("")
            lines.append(note)
        yaml_block = attempts[rec].get("yaml")
        if yaml_block:
            lines.append("\n```yaml\n" + yaml_block + "\n```")
        lines.append(
            "\nThis is a pass/fail gate, not a performance result. Passing it means it's "
            "worth running the throughput-tier load tests (Layer 3) -- not that the "
            "endpoint is certified at any particular ingestion tier. See "
            "docs/02_test_methodology.md."
        )
    else:
        lines.append(
            "**No working flavor/config combination found.** None of the "
            "flavors get every check passing against this endpoint -- see the "
            "per-flavor detail in the accompanying `compat_*.json` for which checks failed "
            "and why, since that's the starting point for a custom `storage.s3.*` override."
        )

    out_path.write_text("\n".join(lines))
