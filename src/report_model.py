"""Evaluate evidence once; all report formats consume the same verdicts."""

from __future__ import annotations

import math
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from .qw_s3_client import DEFAULT_FLAVORS, same_settings
from .report import _load_jsonl, _percentile, summarize_ops
from .run_store import load_bundle, read_json, utc_now, digest

PASS, FAIL, NOT_RUN, INCONCLUSIVE = "PASS", "FAIL", "NOT RUN", "INCONCLUSIVE"

# The four questions a reader brings to the report. Every criterion belongs to
# exactly one of them, so the report can show four roll-up answers instead of
# one flat list of several dozen equally weighted rows.
QUESTIONS = [
    ("evidence", "Can I trust this evidence?",
     "Whether the run is complete, unchanged and comparable to its reference."),
    ("behavior", "Does it behave like S3?",
     "Whether the API semantics Quickwit depends on are correct."),
    ("capacity", "Can it keep up?",
     "Whether the endpoint sustains the offered rate without errors or throttling."),
    ("latency", "Is it fast enough?",
     "Whether response times stay within the allowed multiple of AWS S3."),
]
# Worst first. A group reports the worst status among its required criteria.
STATUS_ORDER = (FAIL, INCONCLUSIVE, NOT_RUN, PASS)
OP_NAMES = {
    "put_object": "Split upload",
    "multipart_upload": "Multipart upload",
    "get_object_full": "Merge object read",
    "get_footer": "Split footer read",
    "get_term_or_field": "Term / field read",
    "get_doc": "Document read",
    "delete_objects_bulk": "Bulk deletion",
    "delete_object_loop": "Individual deletions",
    "query_wall_clock": "Simulated query completion",
}
# Stage keys in the manifest, and what to call them in the report. The command
# names changed to read-concurrency and write-concurrency; the manifest keys
# stay as they are so older bundles still load.
STAGE_NAMES = {
    "compat": "Compatibility probe",
    "fanout": "Read concurrency sweep",
    "put-fanout": "Write concurrency sweep",
    "load": "Workload soak",
}
PROBES = {
    "read_after_write": "Read after write",
    "list_after_write": "List after write",
    "delete_visibility": "Delete visibility",
}


def check(
    key,
    title,
    status,
    observed,
    requirement,
    explanation,
    action="",
    required=True,
    group="evidence",
    ratio=None,
):
    return dict(
        id=key,
        title=title,
        status=status,
        observed=observed,
        requirement=requirement,
        explanation=explanation,
        action=action if status != PASS else "",
        required=required,
        group=group,
        ratio=ratio,
    )


def limit_ratio(observed, limit, at_most=True):
    """How close one measurement sits to its own limit. At most 1.0 passes.

    Every criterion reports this the same way, so the column can be scanned
    down the page without reading units. For an "at most" limit this is
    observed divided by limit. For an "at least" requirement it is inverted,
    so a larger number always means less headroom.
    """
    if not _number(observed) or not _number(limit):
        return None
    if at_most:
        return observed / limit if limit > 0 else None
    return limit / observed if observed > 0 else None


def group_status(checks, group):
    """The worst status among one group's required criteria."""
    states = {c["status"] for c in checks if c["required"] and c["group"] == group}
    for status in STATUS_ORDER:
        if status in states:
            return status
    return NOT_RUN


def overall(checks, flavor):
    required = [c for c in checks if c["required"]]
    if any(c["status"] == FAIL for c in required):
        return "NOT CERTIFIED"
    if not required or flavor is None or any(c["status"] != PASS for c in required):
        return INCONCLUSIVE
    return "CERTIFIED" if flavor in DEFAULT_FLAVORS else "CERTIFIED WITH DEVIATION"


def _number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _rows(path):
    rows = _load_jsonl(path)
    for i, row in enumerate(rows, 1):
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("op"), str)
            or type(row.get("ok")) is not bool
            or not _number(row.get("latency_s"))
            or row["latency_s"] < 0
            or not _number(row.get("ts"))
            or not _number(row.get("bytes", 0))
            or row.get("bytes", 0) < 0
        ):
            raise ValueError(f"Invalid measurement in {path.name}, line {i}")
    return rows


def time_windows(rows, start, duration, seconds, target_mib_s, target_qps):
    windows = []
    if duration <= 0:
        return windows
    groups = [[] for _ in range(math.ceil(duration / seconds))]
    for row in rows:
        offset = row["ts"] - start
        if 0 <= offset < duration:
            groups[min(int(offset / seconds), len(groups) - 1)].append(row)
    for i, subset in enumerate(groups):
        lower, upper = (
            start + i * seconds,
            min(start + (i + 1) * seconds, start + duration),
        )
        requests = [r for r in subset if r["op"] != "query_wall_clock"]
        # Merge rewrites and reads are not original ingestion.
        byte_count = sum(
            r.get("bytes", 0)
            for r in subset
            if r["ok"]
            and r.get("worker", "").startswith("indexer-")
            and r["op"] in ("put_object", "multipart_upload")
        )
        actual = byte_count / (1024**2) / (upper - lower)
        qps = sum(r["ok"] for r in subset if r["op"] == "query_wall_clock") / (
            upper - lower
        )
        windows.append(
            dict(
                offset_s=i * seconds,
                duration_s=upper - lower,
                complete=upper - lower >= seconds - 1e-6,
                ingest_mib_s=actual,
                target_mib_s=target_mib_s,
                ingest_pct=100 * actual / target_mib_s if target_mib_s else None,
                query_qps=qps,
                target_qps=target_qps,
                errors=sum(not r["ok"] for r in requests),
                requests=len(requests),
                s3_success_bytes=sum(r.get("bytes", 0) for r in requests if r["ok"]),
            )
        )
    return windows


def _baseline(path, manifest, load):
    if not path:
        return None, ["No baseline run supplied."]
    path = Path(path)
    if not path.is_dir():
        raise ValueError(
            "--baseline must identify a versioned AWS run directory, not a legacy summary JSON file."
        )
    bm, issues = load_bundle(path)
    bl = bm["stages"].get("load", {})
    if bl.get("status") != "COMPLETED":
        issues.append("Baseline load stage did not complete.")
    if bl.get("options", {}).get("flavor") not in DEFAULT_FLAVORS:
        issues.append("AWS baseline must use flavor none or aws.")
    for name in ("tier", "duration_min"):
        if bl.get("options", {}).get(name) != load.get("options", {}).get(name):
            issues.append(f"Baseline {name} differs.")
    for name in ("model_constants", "query_profiles"):
        if bm["config"].get(name) != manifest["config"].get(name):
            issues.append(f"Baseline {name} differs.")
    if bl.get("op_mix") != load.get("op_mix"):
        issues.append("Baseline modeled workload differs.")
    for name in (
        "source_sha256",
        "python",
        "dependencies",
        "cpu_count",
        "architecture",
        "os",
    ):
        if bl.get("environment", {}).get(name) != load.get("environment", {}).get(name):
            issues.append(f"Baseline runner {name} differs.")
    location = load.get("options", {}).get("runner_location")
    if (
        not location
        or location == "unspecified"
        or bl.get("options", {}).get("runner_location") != location
    ):
        issues.append("Matching runner network locations were not recorded.")
    host = urlsplit(bm["identity"]["endpoint"]).hostname or ""
    if not (host.endswith(".amazonaws.com") or host.endswith(".amazonaws.com.cn")):
        issues.append("Baseline endpoint is not an AWS service hostname.")
    rows = _rows(path / "ingest_merge.jsonl") + _rows(path / "query.jsonl")
    if not rows:
        issues.append("Baseline contains no operation samples.")
    for filename in ("ingest_merge.jsonl", "query.jsonl"):
        if filename not in bl.get("artifacts", {}):
            issues.append(f"Baseline evidence is not recorded: {filename}")
    return {
        "run_id": bm["run_id"],
        "identity": bm["identity"],
        "load": bl,
        "summary": summarize_ops(rows),
        "issues": issues,
    }, issues


def build_report(run_dir, baseline=None, compliance=None, previous=None):
    path = Path(run_dir)
    manifest, issues = load_bundle(path)
    stages, cfg = manifest["stages"], manifest["config"]
    bands = cfg["pass_fail_bands"]
    policy = cfg.get("reporting", {})
    minimum = policy.get("minimum_p99_samples", 100)
    seconds = policy.get("window_seconds", 60)
    min_windows = policy.get("minimum_complete_windows", 3)
    if minimum < 2 or seconds <= 0 or min_windows < 1:
        raise ValueError("Invalid reporting sample or window policy.")
    load = stages.get("load", {})
    options = load.get("options", {})
    flavor = options.get("flavor")
    checks, measurements, errors = [], [], []

    def add(*args, **kwargs):
        checks.append(check(*args, **kwargs))
        return checks[-1]

    add(
        "integrity",
        "Evidence integrity",
        INCONCLUSIVE if issues else PASS,
        "; ".join(issues) if issues else "Recorded artifact checksums match",
        "Unchanged evidence",
        "Checks that measurements match the files recorded when each stage finished.",
        "Restore the original run bundle or run the affected stages in a new directory.",
        group="evidence",
    )
    for stage in ("compat", "fanout", "put-fanout", "load"):
        record = stages.get(stage, {})
        state = record.get("status")
        status = PASS if state == "COMPLETED" else INCONCLUSIVE if state else NOT_RUN
        add(
            "stage_" + stage,
            STAGE_NAMES[stage] + " execution",
            status,
            state or "No execution recorded",
            "Completed stage",
            "A stopped or failed test is incomplete evidence.",
            f"Run the {stage} stage and record its evidence in a complete run bundle.",
            group="evidence",
        )
    rows = _rows(path / "ingest_merge.jsonl") + _rows(path / "query.jsonl")
    summary = summarize_ops(rows)
    if rows and not load:
        issues.append("Operation files have no recorded load stage.")
    stage_files = {
        "load": ("ingest_merge.jsonl", "query.jsonl", "consistency.jsonl"),
        "compat": ("compat.json",),
        "fanout": ("fanout.json",),
        "put-fanout": ("put-fanout.json",),
    }
    for stage, names in stage_files.items():
        for name in names:
            if (stage in stages or (path / name).exists()) and name not in stages.get(
                stage, {}
            ).get("artifacts", {}):
                issues.append(f"Unrecorded {stage} artifact: {name}")
        if (
            stage in stages
            and load
            and stages[stage].get("environment", {}).get("source_sha256")
            != load.get("environment", {}).get("source_sha256")
        ):
            issues.append(f"Framework code changed between {stage} and load.")
    if issues:
        checks[0].update(status=INCONCLUSIVE, observed="; ".join(issues))
    baseline_data, baseline_issues = _baseline(baseline, manifest, load)
    add(
        "baseline",
        "AWS baseline comparability",
        INCONCLUSIVE if baseline_issues else PASS,
        "; ".join(baseline_issues)
        if baseline_issues
        else "Matching workload and runner metadata",
        "Completed AWS reference using the same workload and runner",
        "Relative latency is meaningful only when workload and measurement conditions are comparable.",
        "Run the same workload against AWS S3 from the same runner and supply --baseline.",
        group="evidence",
    )

    compat = read_json(path / "compat.json") if (path / "compat.json").exists() else {}
    rec = compat.get("recommended_flavor")
    attempt = compat.get("attempts", {}).get(rec, {})
    required_compat = stages.get("compat", {}).get(
        "required_checks",
        [
            "path_style_addressing",
            "multipart_upload",
            "multi_object_delete",
            "range_get_semantics",
            "checksum_algorithm",
        ],
    )
    compatibility_ok = bool(required_compat) and all(
        attempt.get("results", {}).get(name, {}).get("passed") is True
        for name in required_compat
    )
    compat_status = (
        PASS
        if compatibility_ok
        else INCONCLUSIVE
        if rec
        else FAIL
        if compat.get("attempts")
        else NOT_RUN
    )
    add(
        "compatibility",
        "S3 compatibility",
        compat_status,
        f"Recommended flavor: {rec}" if rec else "No working flavor recorded",
        "All compatibility checks pass",
        "Tests the S3 behavior exercised by Quickwit's storage settings.",
        "Inspect the flavor comparison and failing check details.",
        group="behavior",
    )
    matching = same_settings(rec, flavor)
    for stage in ("fanout", "put-fanout"):
        if stage in stages and not same_settings(
            stages[stage].get("options", {}).get("flavor"), flavor
        ):
            matching = False
    add(
        "flavor",
        "Configuration used for testing",
        PASS if matching else INCONCLUSIVE,
        f"Load: {flavor or 'not recorded'}; recommended: {rec or 'not recorded'}",
        "The compatibility recommendation and all three measured stages used the same flavor",
        "A passing result under one configuration does not certify a different configuration.",
        "Repeat performance stages with the recommended flavor in a new run bundle.",
        group="evidence",
    )

    start = load.get("measurement_started_epoch", load.get("started_epoch", 0))
    duration = load.get("measurement_duration_s", 0)
    mix = load.get("op_mix", {})
    windows = time_windows(
        rows,
        start,
        duration,
        seconds,
        mix.get("avg_s3_mbps", 0),
        mix.get("query_qps", 0),
    )
    full = [w for w in windows if w["complete"]]
    sufficient = len(full) >= min_windows
    for key, title, field, requirement in (
        (
            "throughput",
            "Sustained original ingestion",
            "ingest_pct",
            bands["sustained_throughput_min_pct_of_target"],
        ),
        (
            "query_rate",
            "Sustained simulated query rate",
            "query_qps",
            mix.get("query_qps", 0)
            * bands["sustained_throughput_min_pct_of_target"]
            / 100,
        ),
    ):
        values = [w[field] for w in full if w[field] is not None]
        observed = min(values) if values else None
        has_samples = (
            any(r.get("worker", "").startswith("indexer-") for r in rows)
            if key == "throughput"
            else "query_wall_clock" in summary
        )
        state = (
            NOT_RUN
            if not load or not has_samples
            else INCONCLUSIVE
            if not sufficient or observed is None
            else PASS
            if observed >= requirement
            else FAIL
        )
        unit = "% of target" if key == "throughput" else " queries/s"
        add(
            key,
            title,
            state,
            f"Worst complete window: {observed:.2f}{unit}"
            if observed is not None
            else "No complete measurement windows",
            f"At least {requirement:.2f}{'% of target' if key == 'throughput' else ' queries/s'} in every {seconds}s complete window",
            f"Requires {min_windows} complete windows. Ingestion counts successful original indexer writes only; merge rewrites are excluded. Partial final windows are displayed but not gated.",
            "Check the timeline for stalls and verify that the load generator can sustain the requested rate.",
            group="capacity",
            ratio=limit_ratio(observed, requirement, at_most=False),
        )
    add(
        "merge_backlog",
        "Merge backlog growth",
        NOT_RUN,
        "No independent merge queue is measured",
        "Flat or decreasing backlog under sustained offered ingestion",
        "Merges run synchronously inside indexer workers. This simulation cannot establish the documented merge-backlog criterion.",
        "Instrument an independently scheduled merger before claiming full tier certification.",
        group="capacity",
    )

    expected_groups = [
        ("put_object", "multipart_upload"),
        ("get_object_full",),
        ("get_footer",),
        ("get_term_or_field",),
        ("query_wall_clock",),
        ("delete_objects_bulk", "delete_object_loop"),
    ]
    if any(p.get("docs_returned", 0) > 0 for p in cfg.get("query_profiles", [])):
        expected_groups.append(("get_doc",))
    for group in expected_groups:
        if not any(op in summary for op in group):
            add(
                "missing_" + group[0],
                OP_NAMES[group[0]] + " coverage",
                NOT_RUN,
                "No samples",
                "At least one applicable operation measured",
                "A missing operation is not a passing operation.",
                "Run long enough to exercise ingestion, queries, merges and garbage collection.",
                group="capacity",
            )
    for op, raw in summary.items():
        title = OP_NAMES.get(op, op)
        relevant = [r for r in rows if r["op"] == op]
        if "wall_clock" in op:
            multiplier = bands["query_wall_clock_p99_multiplier_vs_aws"]
        elif "delete" in op:
            multiplier = bands["bulk_delete_p99_multiplier_vs_aws"]
        elif "put" in op or "multipart" in op:
            multiplier = bands["put_p99_multiplier_vs_aws"]
        elif op == "get_object_full":
            multiplier = bands.get("full_get_p99_multiplier_vs_aws", 2.0)
        else:
            multiplier = bands["range_get_p99_multiplier_vs_aws"]
        reference = (baseline_data or {}).get("summary", {}).get(op, {})
        bp99 = reference.get("p99_latency_s")
        limit = bp99 * multiplier if _number(bp99) and bp99 > 0 else None
        valid = (
            not baseline_issues
            and limit is not None
            and raw["count"] >= minimum
            and reference.get("count", 0) >= minimum
        )
        state = (
            PASS
            if valid and raw["p99_latency_s"] <= limit
            else FAIL
            if valid
            else INCONCLUSIVE
        )
        explanation = (
            "Storage-read fan-out completion under mixed load; this is not end-to-end Quickwit search latency. "
            if op == "query_wall_clock"
            else "Elapsed operation latency includes client retries. "
        )
        explanation += f"p99 uses linear interpolation across all outcomes; requires {minimum} vendor and baseline samples."
        latency_check = add(
            op + "_p99",
            title + " p99",
            state,
            f"{raw['p99_latency_s'] * 1000:.1f} ms; n={raw['count']}",
            f"≤ {multiplier:g}× AWS"
            + (
                f" = {limit * 1000:.1f} ms"
                if limit is not None
                else " (baseline unavailable)"
            ),
            explanation,
            "Check sample counts and baseline comparability, then inspect concurrency and backend contention.",
            group="latency",
            ratio=limit_ratio(raw["p99_latency_s"], limit),
        )
        error_check = add(
            op + "_errors",
            title + " error rate",
            PASS
            if raw["non_throttle_error_pct"] <= bands["error_rate_max_pct"]
            else FAIL,
            f"{raw['non_throttle_error_pct']:.3f}%",
            f"≤ {bands['error_rate_max_pct']}% non-throttle errors",
            "Throttling is evaluated separately. Rates describe final client outcomes after retries, not all wire attempts.",
            "Inspect the error codes and compatibility settings.",
            group="capacity",
            ratio=limit_ratio(
                raw["non_throttle_error_pct"], bands["error_rate_max_pct"]
            ),
        )
        throttle_windows = []
        for w in full:
            sub = [
                r
                for r in relevant
                if start + w["offset_s"] <= r["ts"] < start + w["offset_s"] + seconds
            ]
            if sub:
                throttle_windows.append(summarize_ops(sub)[op]["throttle_pct"])
        worst = max(throttle_windows, default=None)
        median = _percentile(throttle_windows, 50)
        state = (
            INCONCLUSIVE
            if not sufficient or worst is None
            else PASS
            if worst <= bands["throttle_rate_sustained_max_pct"] and median == 0
            else FAIL
        )
        throttle_check = add(
            op + "_throttles",
            title + " throttling",
            state,
            f"Worst {worst:.3f}%; median {median:.3f}%"
            if worst is not None
            else "No complete windows with samples",
            f"Worst window ≤ {bands['throttle_rate_sustained_max_pct']}%; median window 0%",
            "Evaluated in complete time windows with samples; empty operation windows are excluded, and coverage is checked separately.",
            "Inspect throttling and connection limits during the affected intervals.",
            group="capacity",
            ratio=limit_ratio(worst, bands["throttle_rate_sustained_max_pct"]),
        )
        measurements.append(
            dict(
                op=op,
                name=title,
                **raw,
                baseline_p99_s=bp99,
                limit_p99_s=limit,
                # One row per operation carries all three of its verdicts, so
                # the report can show a 9-row matrix instead of 27 separate
                # criteria rows.
                status=latency_check["status"],
                latency_status=latency_check["status"],
                latency_ratio=latency_check["ratio"],
                error_status=error_check["status"],
                error_ratio=error_check["ratio"],
                throttle_status=throttle_check["status"],
                throttle_ratio=throttle_check["ratio"],
            )
        )
        for code, count in Counter(
            str(r.get("error") or "unspecified") for r in relevant if not r["ok"]
        ).most_common():
            errors.append(dict(operation=title, code=code, count=count))

    consistency = _load_jsonl(path / "consistency.jsonl")
    consistency_summary = []
    for probe, title in PROBES.items():
        subset = [r for r in consistency if r.get("probe") == probe]
        valid = all(
            _number(r.get("elapsed_s"))
            and r["elapsed_s"] >= 0
            and type(r.get("success")) is bool
            for r in subset
        )
        successes = sum(
            r.get("success") is True
            and _number(r.get("elapsed_s"))
            and r["elapsed_s"] <= bands["consistency_probe_deadline_s"]
            for r in subset
        )
        percent = 100 * successes / len(subset) if subset else None
        state = (
            NOT_RUN
            if not subset
            else INCONCLUSIVE
            if not valid
            else PASS
            if percent >= bands["consistency_probe_min_success_pct"]
            else FAIL
        )
        add(
            probe,
            title,
            state,
            f"{successes}/{len(subset)} within deadline",
            f"{bands['consistency_probe_min_success_pct']}% within {bands['consistency_probe_deadline_s']} s",
            "Each probe performs an immediate follow-up operation; the deadline covers the complete probe, not repeated polling until visible.",
            "Inspect failed probe details and object visibility behavior.",
            group="behavior",
            ratio=limit_ratio(
                percent, bands["consistency_probe_min_success_pct"], at_most=False
            ),
        )
        consistency_summary.append(
            dict(
                name=title,
                count=len(subset),
                successes=successes,
                status=state,
                max_elapsed_s=max(
                    (r["elapsed_s"] for r in subset if _number(r.get("elapsed_s"))),
                    default=None,
                ),
            )
        )

    sweeps = {}
    for stage, title, key in (
        ("fanout", "Read concurrency", "fanout"),
        ("put-fanout", "Write concurrency", "put_fanout"),
    ):
        file = path / (stage + ".json")
        sweep = read_json(file) if file.exists() else {}
        levels = sweep.get("levels", [])
        floor = bands[key + "_efficiency_min"]
        expected = stages.get(stage, {}).get(
            "tested_levels", bands[key + "_concurrency_levels"]
        )
        valid = (
            bool(levels)
            and {r.get("concurrency") for r in levels} == set(expected)
            and any(k > 1 for k in expected)
        )
        bad = [
            r
            for r in levels
            if r.get("error_count", 0)
            or r.get("throttle_count", 0)
            or (_number(r.get("efficiency")) and r["efficiency"] < floor)
        ]
        if any(
            not _number(r.get("efficiency"))
            or not _number(r.get("wall_clock_s"))
            or r["wall_clock_s"] <= 0
            for r in levels
        ):
            valid = False
        state = (
            NOT_RUN if not sweep else FAIL if bad else PASS if valid else INCONCLUSIVE
        )
        add(
            stage + "_efficiency",
            title + " sweep",
            state,
            "Failed at concurrency " + ", ".join(str(r["concurrency"]) for r in bad)
            if bad
            else f"{len(levels)} levels measured",
            f"Efficiency ≥ {floor:g}; zero errors and throttles at every tested level",
            "Efficiency is typical request latency divided by batch wall-clock time. The median-duration trial is retained; results apply only to the listed concurrency range.",
            "Inspect latency and errors at each concurrency level; consider both runner and backend limits.",
            group="capacity",
            ratio=limit_ratio(
                min(
                    (r["efficiency"] for r in levels if _number(r.get("efficiency"))),
                    default=None,
                ),
                floor,
                at_most=False,
            ),
        )
        sweeps[stage] = {
            **sweep,
            "status": state,
            "efficiency_floor": floor,
            "options": stages.get(stage, {}).get("options", {}),
        }

    external = None
    if compliance:
        file = Path(compliance)
        external = read_json(file)
        required_fields = (
            "tool",
            "version",
            "executed_at",
            "selection",
            "passed",
            "failed",
            "skipped",
            "evidence_file",
        )
        if any(k not in external for k in required_fields):
            raise ValueError(
                "Compliance evidence must contain: " + ", ".join(required_fields)
            )
        evidence = file.parent / external["evidence_file"]
        if not evidence.is_file():
            raise ValueError("Compliance evidence_file does not exist.")
        counts = [external[k] for k in ("passed", "failed", "skipped")]
        if any(type(v) is not int or v < 0 for v in counts):
            raise ValueError("Compliance counts must be non-negative integers.")
        external = {
            **external,
            "evidence_sha256": digest(evidence),
            "source_sha256": digest(file),
        }
        same = (
            external.get("endpoint") == manifest["identity"]["endpoint"]
            and external.get("bucket") == manifest["identity"]["bucket"]
        )
        state = (
            INCONCLUSIVE
            if not same
            else FAIL
            if external["failed"]
            else INCONCLUSIVE
            if external["skipped"] or not external["passed"]
            else PASS
        )
    else:
        state = NOT_RUN
    add(
        "compliance",
        "External API compliance",
        state,
        f"{external['tool']}: {external['passed']} passed, {external['failed']} failed, {external['skipped']} skipped"
        if external
        else "No external compliance evidence supplied",
        "Zero failures or omissions in the relevant multipart, range, delete and list subset",
        "Operator-supplied evidence from s3-tests or mint; raw evidence is fingerprinted, not independently re-executed by the report.",
        "Supply a compliance evidence manifest with --compliance; see docs/measurement_policy.md.",
        group="behavior",
    )
    add(
        "warp",
        "Raw throughput cross-check",
        NOT_RUN,
        "External warp results are not imported",
        "Optional diagnostic",
        "A raw benchmark can distinguish general storage limits from workload-specific effects.",
        required=False,
        group="evidence",
    )

    history = None
    verdict = overall(checks, flavor)
    indexed = {c["id"]: c for c in checks}
    # Say once, in plain words, what this report cannot conclude. These used to
    # be scattered through the criteria text, where the practical consequence
    # was easy to miss.
    headline_limits = []
    if indexed["merge_backlog"]["status"] == NOT_RUN:
        headline_limits.append(
            {
                "title": "Full tier certification is not available yet",
                "detail": "Merge backlog has no measurement in this simulator, and"
                " it is a required criterion. The overall result can therefore"
                " reach INCONCLUSIVE at best, never CERTIFIED, however well the"
                " endpoint performs. Every other criterion is measured, and a"
                " failure in any of them is still a real failure.",
            }
        )
    headline_limits.append(
        {
            "title": "Error and latency figures are post-retry",
            "detail": "The client retries failed requests. Recorded rates describe"
            " the final outcome, and latency includes the retry time. A backend"
            " that fails often but recovers can still look clean here.",
        }
    )
    headline_limits.append(
        {
            "title": "Results apply to the tested range only",
            "detail": "Concurrency conclusions cover the levels actually swept, and"
            " performance conclusions cover the tier and duration actually run.",
        }
    )
    if previous:
        prior = read_json(previous)
        if prior.get("schema_version") != 1 or prior.get("kind") != "quickwit-report":
            raise ValueError("--previous must be a versioned report JSON.")
        history = {
            "run_id": prior["run"]["run_id"],
            "verdict": prior["verdict"],
            "comparisons": [],
        }
        previous_load = prior["run"]["stages"].get("load", {})
        comparable = (
            prior["run"]["identity"] == manifest["identity"]
            and prior.get("tier") == options.get("tier")
            and same_settings(prior.get("flavor"), flavor)
            and prior["run"]["config"] == cfg
            and previous_load.get("op_mix") == load.get("op_mix")
            and previous_load.get("options", {}).get("duration_min")
            == options.get("duration_min")
            and previous_load.get("options", {}).get("runner_location")
            == options.get("runner_location")
        )
        history["comparable"] = comparable
        old = {m["op"]: m for m in prior.get("measurements", [])}
        for m in measurements:
            if comparable and m["op"] in old and old[m["op"]]["p99_latency_s"] > 0:
                history["comparisons"].append(
                    {
                        "name": m["name"],
                        "p99_change_pct": 100
                        * (m["p99_latency_s"] / old[m["op"]]["p99_latency_s"] - 1),
                    }
                )
    return {
        "schema_version": 1,
        "kind": "quickwit-report",
        "generated_at": utc_now(),
        "run": manifest,
        "tier": options.get("tier", "Not run"),
        "flavor": flavor,
        "recommended_flavor": rec,
        "verdict": verdict,
        "best_possible_verdict": INCONCLUSIVE
        if indexed["merge_backlog"]["status"] == NOT_RUN
        else "CERTIFIED",
        "headline_limits": headline_limits,
        "groups": [
            {
                "id": key,
                "question": question,
                "summary": summary,
                "status": group_status(checks, key),
                "counts": dict(
                    Counter(
                        c["status"] for c in checks if c["required"] and c["group"] == key
                    )
                ),
            }
            for key, question, summary in QUESTIONS
        ],
        "checks": checks,
        "measurements": measurements,
        "baseline": baseline_data,
        "compatibility": compat,
        "consistency": consistency_summary,
        "sweeps": sweeps,
        "timeline": windows,
        "errors": errors,
        "external_compliance": external,
        "history": history,
        "limitations": [
            "Storage-layer simulation; not an end-to-end Quickwit functional or search benchmark.",
            "Merged synthetic payloads are capped at 64 MiB; this does not demonstrate production-size merge throughput.",
            "Ingestion is expressed in MiB/s of successful original writes. Raw-log equivalents use the configured compression assumption.",
            "Read-after-write currently verifies returned byte length, not full payload equality; it is not a corruption test.",
            "Partial final windows are shown but excluded from sustained-rate and throttle gates.",
        ],
    }
