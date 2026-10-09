"""Evaluate evidence once; all report formats consume the same verdicts."""

from __future__ import annotations

import math
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from .qw_s3_client import DEFAULT_FLAVORS, flavor_label, same_settings
from .reference_profile import DEFAULT_PROFILE, describe, load_profile, reference_p99
from .report import _load_jsonl, _percentile, summarize_ops
from .run_store import load_bundle, read_json, utc_now, digest

PASS, FAIL, NOT_RUN, INCONCLUSIVE = "PASS", "FAIL", "NOT RUN", "INCONCLUSIVE"

# The four questions a reader brings to the report. Every criterion belongs to
# exactly one of them, so the report can show four roll-up answers instead of
# one flat list of several dozen equally weighted rows.
# What this synthetic tool cannot test. These belong to the later stage: the
# performance tests run together with the BYOC modules. They are listed in the
# report and in docs/what_this_measures.md, and they never hold a result back.
LATER_STAGE = [
    {"title": "Merge backlog",
     "detail": "Whether merges keep up with incoming data over hours and days. That"
               " needs BYOC's own merge scheduler. This tool runs simple merges"
               " inside its upload workers."},
    {"title": "Full-size merges",
     "detail": "Real merged files reach 8 to 10 GB. To limit cost, this tool merges"
               " into files of at most 160 MB. That still tests multipart uploads,"
               " but not transfers that large."},
    {"title": "Indexing and search results",
     "detail": "Whether BYOC indexes documents and returns the right results. This"
               " tool tests storage only."},
    {"title": "Whole search time",
     "detail": "This tool times the storage reads of a search, not a complete BYOC"
               " search."},
]

# The five questions in docs/what_this_measures.md, in the same order and the
# same words. The report and that page must never drift apart. A sixth
# section covers whether the run itself can be trusted. Its checks never
# decide the result; they make an affected result inconclusive instead.
QUESTIONS = [
    ("compatibility", "Does the storage speak S3 the way BYOC needs?",
     "The five things BYOC needs from an S3 interface."),
    ("concurrency", "Does the storage handle many requests at the same time?",
     "Whether a group of requests sent together is served together."),
    ("keeps_up", "Can the storage keep up?",
     "Whether it sustains the write and search rate the daily volume needs."),
    ("speed", "Is the storage as fast as AWS S3?",
     "Whether response times stay under their limit."),
    ("correctness", "Is the storage correct and stable while busy?",
     "Failed requests, slowed-down requests, and when new objects appear."),
    ("trust", "Can we trust this result?",
     "Information about the run itself. These checks never decide the result."),
]
# Worst first. A section reports the worst status among its deciding checks.
STATUS_ORDER = (FAIL, INCONCLUSIVE, NOT_RUN, PASS)


def roll_up(members, missing_expected=False):
    """One status for a set of per-operation checks.

    Ten checks decide the result. Twenty-one of the old ones were the same
    three measurements repeated for each operation, which nobody could hold
    in their head. They are summarized here instead. The per-operation detail
    stays in the report, in the operations table.
    """
    if not members:
        return NOT_RUN
    states = {c["status"] for c in members}
    worst = next((s for s in STATUS_ORDER if s in states), NOT_RUN)
    # A failure always shows, even when other operations are missing. An
    # earlier version returned NOT RUN whenever an operation was missing,
    # which hid real failures and turned FAIL into INCONCLUSIVE.
    if worst == FAIL:
        return FAIL
    # Something was measured, but not everything. That is INCONCLUSIVE, by the
    # meaning the report gives each word, not NOT RUN.
    if missing_expected and worst == PASS:
        return INCONCLUSIVE
    return worst
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
    group="trust",
    ratio=None,
    source=None,
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
        # Where this number came from: the command, the evidence file, and
        # the setting that fixed the limit. So "where does this come from?"
        # is always answerable from the report itself.
        source=source or {},
    )


# Used for bundles recorded before the serialization band existed. A report
# normally uses the thresholds saved with its own run, but a band that was not
# saved has to come from somewhere.
DEFAULT_MIN_SPEEDUP = 2.0


# After a pause, connections must be re-made. A MacBook took 7 seconds to
# bring its network back after a 99-second sleep, and the next request failed
# 52 seconds after waking. This much time after each pause is left out too.
RECOVERY_S = 60.0
PAUSE_THRESHOLD_S = 5.0


# Checks whose result depends on when things happened. A pause of the test
# machine can make any of them fail without any fault in the storage.
PER_OPERATION_SUFFIXES = ("_p99", "_errors", "_throttles")
TIME_BASED = {
    "throughput", "query_rate", "response_time", "failed_requests",
    "slowed_requests", "object_visibility",
}


def paused_time(stage):
    """Seconds this machine was not running during one stage.

    The wall clock keeps counting while a machine sleeps; the process clock
    does not. Their difference is the time nothing ran.
    """
    started, finished = stage.get("started_epoch"), stage.get("finished_epoch")
    running = stage.get("actual_duration_s")
    if not _number(running):
        return 0.0
    if not _number(finished) and stage.get("started_at") and stage.get("finished_at"):
        from datetime import datetime

        wall = (
            datetime.fromisoformat(stage["finished_at"])
            - datetime.fromisoformat(stage["started_at"])
        ).total_seconds()
    elif _number(started) and _number(finished):
        wall = finished - started
    else:
        return 0.0
    return max(0.0, wall - running)


def pause_windows(stage):
    """When each pause happened, as wall-clock intervals including recovery.

    Returns None when the stage paused but the run predates pause tracking,
    so nobody can say which minutes were affected.
    """
    pauses = stage.get("pauses")
    if pauses:
        return [
            (p["started_epoch"], p["started_epoch"] + p["seconds"] + RECOVERY_S)
            for p in pauses
        ]
    return None if paused_time(stage) > PAUSE_THRESHOLD_S else []


def overlaps(begin, end, intervals):
    return any(begin < b and a < end for a, b in intervals or [])


def level_speedup(row):
    """Speedup for one sweep level, recomputed when the field is absent.

    Older bundles were summarized before `speedup` existed. The inputs are
    recorded, so the same number can be derived rather than lost.
    """
    if _number(row.get("speedup")):
        return row["speedup"]
    concurrency, p50, wall = (
        row.get("concurrency"),
        row.get("p50_per_request_s"),
        row.get("wall_clock_s"),
    )
    if _number(concurrency) and _number(p50) and _number(wall) and wall > 0:
        return concurrency * p50 / wall
    return None


# Below this many samples, nothing about the slowest requests can be said.
SMALL_SAMPLE_FLOOR = 3


def latency_result(count, p99, p50, slowest, limit, minimum):
    """Judge one operation's response time, even when it ran only a few times.

    With `minimum` samples or more, the p99 decides. Some operations are
    rare: at 100 GB/day a merge runs about every 10 minutes, so 100 merge
    samples would take about 17 hours. Waiting for them would stop such a run
    from ever passing. With fewer samples, only what the data proves decides:

    * PASS if even the slowest sample is within the limit. Every request we
      saw met it. This is weaker evidence than 100 samples, and the result
      says which rule decided.
    * FAIL if the median is over the limit. Then the p99 is over it too.
    * INCONCLUSIVE in between, and below SMALL_SAMPLE_FLOOR samples.

    Returns the status and a short note for the report.
    """
    if limit is None:
        return INCONCLUSIVE, ""
    if count >= minimum:
        return (PASS if p99 <= limit else FAIL), ""
    if count < SMALL_SAMPLE_FLOOR or not _number(slowest) or not _number(p50):
        return INCONCLUSIVE, f" (needs {minimum})"
    if slowest <= limit:
        return PASS, f" (few samples: all {count} within the limit)"
    if p50 > limit:
        return FAIL, f" (few samples: the median is over the limit)"
    return INCONCLUSIVE, f" (few samples: some over the limit, needs {minimum})"


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


# Results from other tools that the operator may attach. Most runs never do,
# so their absence must not make "Can we trust this result?" look like a
# problem. They are still shown as rows.
ATTACHED_EVIDENCE = ("compliance", "warp")


def group_status(checks, group):
    """The worst status in one section.

    Deciding checks set the status. The trust section has none of them, so it
    reports on its information-only checks instead.
    """
    members = [
        c for c in checks if c["group"] == group and c["id"] not in ATTACHED_EVIDENCE
    ]
    states = {c["status"] for c in members if c["required"]} or {
        c["status"] for c in members
    }
    for status in STATUS_ORDER:
        if status in states:
            return status
    return NOT_RUN


def overall(checks, flavor):
    """PASS, FAIL or INCONCLUSIVE: the same words every check uses.

    This is a synthetic test, not a formal certification. A run passes when
    every deciding check passes. Whether BYOC needs special storage settings
    is reported beside the result, not folded into it. Anything this tool
    cannot test is listed in LATER_STAGE and never holds a result back.
    """
    required = [c for c in checks if c["required"]]
    if any(c["status"] == FAIL for c in required):
        return FAIL
    if not required or flavor is None or any(c["status"] != PASS for c in required):
        return INCONCLUSIVE
    return PASS


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


def build_report(
    run_dir, baseline=None, compliance=None, previous=None, reference=DEFAULT_PROFILE
):
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
    flavor = options.get("flavor") or next(
        (
            record.get("options", {}).get("flavor")
            for name, record in stages.items()
            if name != "compat" and record.get("options", {}).get("flavor")
        ),
        None,
    )
    checks, measurements, errors = [], [], []
    # Latency is graded against one of two references. A measured AWS run wins
    # when the operator supplies one. Otherwise the bundled profile applies, so
    # a vendor with no AWS account still gets a verdict.
    profile = load_profile(reference) if not baseline else None

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
        group="trust",
        required=False,
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
            group="trust",
            required=False,
        )
    rows = _rows(path / "ingest_merge.jsonl") + _rows(path / "query.jsonl")
    paused_s = paused_time(load) if load else 0.0
    paused_at = pause_windows(load) if load else []
    if paused_at:
        # A request that ran across a pause measured the pause, not the
        # storage: its latency includes the sleep, and its failure is the
        # broken connection the sleep left behind.
        rows = [
            r for r in rows
            if not overlaps(r["ts"] - r["latency_s"], r["ts"], paused_at)
        ]
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
    if baseline:
        reference_state = INCONCLUSIVE if baseline_issues else PASS
        reference_observed = (
            "; ".join(baseline_issues)
            if baseline_issues
            else "Measured AWS run with matching workload and runner metadata"
        )
    elif profile:
        reference_state = PASS
        reference_observed = (
            f"Bundled reference profile {profile['id']} v{profile['version']}"
            f" ({profile.get('status', 'published')})"
        )
    else:
        reference_state = INCONCLUSIVE
        reference_observed = "No latency reference selected"
    add(
        "baseline",
        "Latency reference",
        reference_state,
        reference_observed,
        "A measured AWS run, or the bundled reference profile",
        "Latency checks compare against AWS S3. A measured run is the stronger"
        " evidence, because it shares this runner and network. The bundled"
        " profile is the published bar, and it applies when no measured run is"
        " supplied.",
        "Supply --baseline with a measured AWS run, or choose a profile with --reference.",
        group="trust",
        required=False,
    )

    compat = read_json(path / "compat.json") if (path / "compat.json").exists() else {}
    rec = compat.get("recommended_flavor")
    # Grade the settings the run actually used. Usually that is the
    # recommendation, but an operator may choose other settings with
    # `validate --flavor`. Grading the recommendation instead would pass
    # settings nobody measured.
    used = flavor or rec
    attempts = compat.get("attempts", {})
    attempt = attempts.get(used) or next(
        (a for name, a in attempts.items() if same_settings(name, used)), {}
    )
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
    results = attempt.get("results", {})
    # A check that ran and failed is a failure. A check with no result is
    # missing evidence, which is inconclusive, never a failure and never a pass.
    failed_compat = [
        name for name in required_compat if results.get(name, {}).get("passed") is False
    ]
    compat_status = (
        PASS
        if compatibility_ok
        else FAIL
        if failed_compat or (attempts and not rec and not results)
        else INCONCLUSIVE
        if results or rec
        else NOT_RUN
    )
    if used and attempt.get("results"):
        compat_seen = f"{flavor_label(used)}: " + (
            "every check passed" if compatibility_ok else "some checks failed"
        )
        if rec and not same_settings(rec, used):
            compat_seen += f". The mildest settings that work are {flavor_label(rec)}."
    elif rec:
        compat_seen = f"Recommended: {flavor_label(rec)}"
    else:
        compat_seen = "No working flavor recorded"
    add(
        "compatibility",
        "S3 compatibility",
        compat_status,
        compat_seen,
        "All compatibility checks pass",
        "Tests the S3 behavior that BYOC's storage settings depend on.",
        "Inspect the flavor comparison and failing check details.",
        group="compatibility",
        source={"command": "compat", "evidence": "compat.json"},
    )
    # Every measured stage must use one set of settings, and those settings
    # must have passed compatibility. Matching the recommendation is not the
    # point; an operator may choose other settings that also work.
    matching = flavor is not None and compatibility_ok
    for stage in ("fanout", "put-fanout"):
        if stage in stages and not same_settings(
            stages[stage].get("options", {}).get("flavor"), flavor
        ):
            matching = False
    add(
        "flavor",
        "Configuration used for testing",
        PASS if matching else INCONCLUSIVE,
        f"Used: {flavor_label(flavor)}; recommended: {flavor_label(rec)}",
        "All three measured steps used the same settings, and those settings passed compatibility",
        "A passing result under one configuration says nothing about a different configuration.",
        "Run again with one set of settings that passes compatibility, in a new run directory.",
        group="trust",
        required=False,
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
    for w in windows:
        w["paused"] = bool(paused_at) and overlaps(
            start + w["offset_s"], start + w["offset_s"] + w["duration_s"], paused_at
        )
    full = [w for w in windows if w["complete"] and not w["paused"]]
    sufficient = len(full) >= min_windows
    for key, title, field, requirement in (
        (
            "throughput",
            "Keeps up with writes",
            "ingest_pct",
            bands["sustained_throughput_min_pct_of_target"],
        ),
        (
            "query_rate",
            "Keeps up with searches",
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
        if observed is None:
            seen = "No complete 60-second window"
        else:
            seen = f"Worst complete window: {observed:.2f}{unit}"
            if not sufficient:
                seen += f" (only {len(full)} of {min_windows} needed windows)"
        add(
            key,
            title,
            state,
            seen,
            f"At least {requirement:.2f}{'% of target' if key == 'throughput' else ' queries/s'} in every {seconds}s complete window",
            f"Requires {min_windows} complete windows. Ingestion counts successful original indexer writes only; merge rewrites are excluded. A partial last window is shown, but does not count.",
            "Check the timeline for stalls and verify that the load generator can sustain the requested rate.",
            group="keeps_up",
            source={
                "command": "load",
                "evidence": "ingest_merge.jsonl" if key == "throughput" else "query.jsonl",
                "setting": "sustained_throughput_min_pct_of_target",
            },
            ratio=limit_ratio(observed, requirement, at_most=False),
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
    uncovered = [g for g in expected_groups if not any(op in summary for op in g)]
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
                group="correctness",
                required=False,
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
        sizes = sorted(r.get("bytes", 0) for r in relevant)
        median_bytes = sizes[len(sizes) // 2] if sizes else 0
        measured_reference = (baseline_data or {}).get("summary", {}).get(op, {})
        if baseline:
            bp99 = measured_reference.get("p99_latency_s")
            # A measured reference needs its own sample floor, the same as the
            # vendor side.
            enough_reference = measured_reference.get("count", 0) >= minimum
        else:
            bp99 = reference_p99(profile, op, median_bytes)
            # The profile is a published figure, so there is no second sample
            # set to qualify. Only the vendor's samples apply.
            enough_reference = True
        limit = bp99 * multiplier if _number(bp99) and bp99 > 0 else None
        comparable = (
            # A supplied baseline must be comparable. A bundled profile has no
            # comparability to establish, so "no baseline run supplied" must
            # not invalidate the criterion it replaces.
            (not baseline_issues if baseline else True)
            and limit is not None
            and enough_reference
        )
        latencies = [r["latency_s"] for r in relevant]
        state, rule = latency_result(
            raw["count"], raw["p99_latency_s"], raw["p50_latency_s"],
            max(latencies, default=None), limit if comparable else None, minimum,
        )
        explanation = (
            "Storage-read fan-out completion under mixed load; this is not end-to-end BYOC search latency. "
            if op == "query_wall_clock"
            else "Elapsed operation latency includes client retries. "
        )
        explanation += (
            f"With {minimum} samples or more, the p99 decides"
            + (", and the measured reference needs as many." if baseline else ".")
            + f" With fewer, the slowest sample and the median decide: PASS if every"
            " sample is within the limit, FAIL if the median is over it."
        )
        if not baseline and profile and limit is not None:
            explanation += (
                f" The limit comes from reference profile {profile['id']}"
                f" v{profile['version']}, for a median payload of"
                f" {median_bytes / (1024 * 1024):.2f} MiB."
            )
        latency_check = add(
            op + "_p99",
            title + " p99",
            state,
            f"{raw['p99_latency_s'] * 1000:.1f} ms; n={raw['count']}" + rule,
            f"≤ {multiplier:g}× AWS"
            + (
                f" = {limit * 1000:.1f} ms"
                if limit is not None
                else " (no latency reference)"
            ),
            explanation,
            "Check sample counts and baseline comparability, then inspect concurrency and backend contention.",
            group="speed",
            required=False,
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
            group="correctness",
            required=False,
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
            group="correctness",
            required=False,
            ratio=limit_ratio(worst, bands["throttle_rate_sustained_max_pct"]),
        )
        measurements.append(
            dict(
                op=op,
                name=title,
                **raw,
                baseline_p99_s=bp99,
                limit_p99_s=limit,
                median_bytes=median_bytes,
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

    # Three checks replace twenty-one. Each one fails when any operation in it
    # fails, so no detail is lost from the result, only from the reading.
    for key, title, suffix, explanation, action, setting in (
        (
            "response_time",
            "Response time",
            "_p99",
            "The time 99 out of 100 requests beat, for every operation, against"
            " its own limit. The operations table lists each one.",
            "Open the operations table and start with the operation furthest past its limit.",
            "put_p99_multiplier_vs_aws, and the other p99 multipliers",
        ),
        (
            "failed_requests",
            "Failed requests",
            "_errors",
            "The share of requests that failed, for every operation. Throttled"
            " requests are counted separately.",
            "Open the operations table, then check the error codes listed below it.",
            "error_rate_max_pct",
        ),
        (
            "slowed_requests",
            "Slowed-down requests",
            "_throttles",
            "The share of requests the storage asked us to retry, for every"
            " operation, in each full minute.",
            "Check the request limits on the storage system during the affected minutes.",
            "throttle_rate_sustained_max_pct",
        ),
    ):
        members = [c for c in checks if c["id"].endswith(suffix) and c["group"] != "trust"]
        failing = [c for c in members if c["status"] not in (PASS,)]
        status = roll_up(members, missing_expected=bool(uncovered))
        # Count each outcome separately. "Did not pass" lumped failures with
        # operations that were only too short to judge, and read as failure.
        tally = Counter(c["status"] for c in members)
        words = {FAIL: "failed", INCONCLUSIVE: "not enough data", PASS: "passed"}
        parts = [
            f"{tally[state]} {words[state]}"
            for state in (FAIL, INCONCLUSIVE, PASS)
            if tally.get(state)
        ]
        observed = f"Of {len(members)} operations: " + ", ".join(parts)
        if uncovered:
            observed += "; never ran: " + ", ".join(OP_NAMES[g[0]] for g in uncovered)
        add(
            key,
            title,
            status,
            observed,
            "Every measured operation stays within its limit",
            explanation,
            action,
            group="speed" if suffix == "_p99" else "correctness",
            source={
                "command": "load",
                "evidence": "ingest_merge.jsonl and query.jsonl",
                "setting": setting,
            },
            ratio=max(
                (c["ratio"] for c in members if _number(c.get("ratio"))), default=None
            ),
        )

    consistency = _load_jsonl(path / "consistency.jsonl")
    if paused_at:
        consistency = [
            r for r in consistency
            if not (
                _number(r.get("ts"))
                and _number(r.get("elapsed_s"))
                and overlaps(r["ts"] - r["elapsed_s"], r["ts"], paused_at)
            )
        ]
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
            group="correctness",
            required=False,
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

    probes = [c for c in checks if c["id"] in PROBES]
    failing = [c for c in probes if c["status"] != PASS]
    add(
        "object_visibility",
        "Objects appear at once",
        roll_up(probes),
        f"{len(probes) - len(failing)} of {len(probes)} checks passed"
        if probes
        else "No visibility checks recorded",
        "A new object is readable, listed and gone when it should be",
        "After a write, a list or a delete, we immediately look again. BYOC"
        " assumes the change is already visible.",
        "Open the three visibility checks below for the failing one.",
        group="correctness",
        source={
            "command": "load",
            "evidence": "consistency.jsonl",
            "setting": "consistency_probe_min_success_pct",
        },
        ratio=min((c["ratio"] for c in probes if _number(c.get("ratio"))), default=None),
    )

    sweeps = {}
    for stage, title, key in (
        ("fanout", "Read concurrency", "fanout"),
        ("put-fanout", "Write concurrency", "put_fanout"),
    ):
        file = path / (stage + ".json")
        sweep = read_json(file) if file.exists() else {}
        levels = sweep.get("levels", [])
        required_speedup = bands.get(
            key + "_serialization_min_speedup", DEFAULT_MIN_SPEEDUP
        )
        expected = stages.get(stage, {}).get(
            "tested_levels", bands[key + "_concurrency_levels"]
        )
        valid = (
            bool(levels)
            and {r.get("concurrency") for r in levels} == set(expected)
            and any(k > 1 for k in expected)
        )
        # The gate is serialization, not latency spread. A backend that serves
        # concurrent requests one at a time shows a speedup near 1 whatever
        # the client offers. Errors and throttles still fail outright.
        bad = [
            r
            for r in levels
            if r.get("error_count", 0)
            or r.get("throttle_count", 0)
            or (
                r.get("concurrency", 1) > 1
                and (
                    not _number(level_speedup(r))
                    or level_speedup(r) < required_speedup
                )
            )
        ]
        if any(
            not _number(r.get("wall_clock_s")) or r["wall_clock_s"] <= 0 for r in levels
        ):
            valid = False
        state = (
            NOT_RUN if not sweep else FAIL if bad else PASS if valid else INCONCLUSIVE
        )
        measured = sweep.get("min_speedup")
        if not _number(measured):
            measured = min(
                (
                    value
                    for value in (
                        level_speedup(r) for r in levels if r.get("concurrency", 1) > 1
                    )
                    if _number(value)
                ),
                default=None,
            )
        add(
            stage + "_efficiency",
            title,
            state,
            "Served requests one at a time at concurrency "
            + ", ".join(str(r["concurrency"]) for r in bad)
            if bad
            else f"{len(levels)} levels measured; worst speedup "
            + (f"{measured:.1f}x" if _number(measured) else "not measured"),
            f"Speedup of at least {required_speedup:g}x at every level above 1,"
            " with no errors or throttles",
            "Speedup is how many requests' worth of latency the batch absorbed"
            " at once. About 1 means the backend served them one after another."
            " About the concurrency level means it served them together. Batch"
            " throughput peaks where something saturates, and that may be this"
            " runner rather than the backend.",
            "Compare the speedup curve against the throughput peak, and check"
            " whether the runner's own network or CPU was the limit.",
            group="concurrency",
            source={
                "command": "read-concurrency" if stage == "fanout" else "write-concurrency",
                "evidence": stage + ".json",
                "setting": key + "_serialization_min_speedup",
            },
            ratio=limit_ratio(measured, required_speedup, at_most=False),
        )
        sweeps[stage] = {
            **sweep,
            "status": state,
            "min_speedup": measured,
            "min_speedup_required": required_speedup,
            "efficiency_floor": bands.get(key + "_efficiency_min"),
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
        group="trust",
        required=False,
    )
    add(
        "warp",
        "Raw throughput cross-check",
        NOT_RUN,
        "External warp results are not imported",
        "Information only",
        "A raw benchmark can distinguish general storage limits from workload-specific effects.",
        required=False,
        group="trust",
    )

    history = None
    if paused_at is None:
        for c in checks:
            time_based = c["id"] in TIME_BASED or c["id"].endswith(PER_OPERATION_SUFFIXES)
            if c["status"] == FAIL and time_based:
                c["status"] = INCONCLUSIVE
                c["observed"] += (
                    f" (this machine stopped for {paused_s:.0f} s at an unknown"
                    " moment, so this cannot be judged)"
                )
        # The operations table copies each status; keep it in step.
        by_id = {c["id"]: c["status"] for c in checks}
        for m in measurements:
            for field, suffix in (
                ("latency_status", "_p99"),
                ("error_status", "_errors"),
                ("throttle_status", "_throttles"),
            ):
                m[field] = by_id.get(m["op"] + suffix, m[field])
            m["status"] = m["latency_status"]
    verdict = overall(checks, flavor)
    indexed = {c["id"]: c for c in checks}
    # Say once, in plain words, what this report cannot conclude. These used to
    # be scattered through the criteria text, where the practical consequence
    # was easy to miss.
    headline_limits = []
    ran = [s for s in ("compat", "fanout", "put-fanout", "load") if s in stages]
    if len(ran) < 4:
        headline_limits.insert(
            0,
            {
                "title": f"Only {len(ran)} of the 4 stages ran",
                "detail": "This report covers "
                + ", ".join(STAGE_NAMES[s] for s in ran)
                + ". Everything the missing stages would measure reads NOT RUN"
                " below, which is not a pass and not a failure. Run the"
                " remaining stages into the same run directory, or use"
                " `validate` to run all of them in order.",
            },
        )
    if paused_s > PAUSE_THRESHOLD_S:
        headline_limits.insert(
            0,
            {
                "title": f"The test machine stopped for {paused_s:.0f} seconds",
                "detail": "The computer running the test went to sleep or was"
                " suspended. Nothing was measured during that time, and the"
                " open connections broke. "
                + (
                    "The report leaves out that time and the following minute,"
                    " so the storage is not blamed for it."
                    if paused_at
                    else "This run was recorded before the tool tracked when a"
                    " pause happens, so the report cannot tell which minutes it"
                    " affected. Checks that depend on time read INCONCLUSIVE"
                    " instead of FAIL."
                )
                + " The tool now keeps a Mac awake during a run. On other"
                " systems, make sure the machine cannot sleep.",
            },
        )
    # A short run leaves checks unanswered for reasons that are arithmetic, not
    # faults. Say so first, in numbers, so nobody hunts for a problem.
    if load.get("status") == "COMPLETED":
        constants = cfg.get("model_constants", {})
        nodes = max(1, mix.get("num_indexer_nodes", 1))
        merge_minutes = math.ceil(
            constants.get("merge_factor", 10)
            * constants.get("commit_timeout_s", 60)
            / 60
            / nodes
        )
        needed = max(merge_minutes + 5, min_windows * seconds / 60)
        ran = duration / 60
        uploads = sum(
            summary.get(op, {}).get("count", 0) for op in ("put_object", "multipart_upload")
        )
        if ran < needed:
            reasons = []
            if len(full) < min_windows:
                reasons.append(
                    f"the rate checks need {min_windows} full minutes, and got {len(full)}"
                )
            if "get_object_full" not in summary:
                reasons.append(
                    f"a merge needs {constants.get('merge_factor', 10)} uploaded files,"
                    f" and this run uploaded {uploads}, so no merge and no merge"
                    " delete happened"
                )
            # Only operations the small-sample rule could not judge either.
            undecided = {c["id"] for c in checks if c["status"] == INCONCLUSIVE}
            thin = [
                OP_NAMES.get(op, op)
                for op, raw in summary.items()
                if raw["count"] < minimum and op + "_p99" in undecided
            ]
            if thin:
                reasons.append(
                    ", ".join(thin)
                    + " had too few samples to judge"
                )
            # A short run that still answered every check needs no warning.
            if reasons:
                headline_limits.insert(
                    0,
                    {
                        "title": "This run was too short to judge"
                        f" ({ran:g} minute{'' if ran == 1 else 's'})",
                        "detail": "Many checks read NOT RUN or INCONCLUSIVE for"
                        " that reason alone: " + "; ".join(reasons) + "."
                        f" Run for at least {math.ceil(needed)} minutes at this"
                        " daily volume to answer every check that can be measured.",
                    },
                )
    # Over plain HTTP the client sends an upload checksum as an ordinary
    # header. Over HTTPS it sends it as a trailer after the body, with an
    # `x-amz-trailer` header that some storage systems reject; StorageGRID
    # lists it as unsupported. So an HTTP run can pass compatibility with
    # settings that would fail in production over HTTPS.
    effective = load.get("effective_config") or next(
        (r.get("effective_config") for r in stages.values() if r.get("effective_config")),
        {},
    )
    if urlsplit(manifest["identity"]["endpoint"]).scheme == "http":
        trailer_risk = effective.get("checksum_algorithm", "crc32c") == "crc32c"
        headline_limits.insert(
            0,
            {
                "title": "This run used plain HTTP, not HTTPS",
                "detail": (
                    "Over HTTP, uploads send their checksum as an ordinary"
                    " header. Over HTTPS they send it after the body, with an"
                    " x-amz-trailer header that some storage systems reject."
                    " So the compatibility result may not hold over HTTPS."
                    if trailer_risk
                    else "These settings send checksums the same way over HTTP"
                    " and HTTPS, so compatibility holds either way."
                )
                + " HTTP also skips the cost of encryption, so response times"
                " can look better than in production. Run over HTTPS, with"
                " --ca-bundle if needed, to test what production uses.",
            },
        )
    if profile:
        headline_limits.append(
            {
                "title": "Latency is graded against published figures",
                "detail": "No measured AWS run was supplied, so latency limits"
                f" come from reference profile {profile['id']} v{profile['version']}."
                " The profile states what AWS S3 delivers from an in-region"
                " instance. It cannot account for this runner's own network"
                " distance to the endpoint. Supply --baseline with a measured"
                " AWS run for a side-by-side comparison.",
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
    if load.get("effective_config", {}).get("verify_tls") is False:
        headline_limits.append(
            {
                "title": "The endpoint certificate was not verified",
                "detail": "This run used --insecure-skip-tls-verify. The"
                " measurements are still valid, because verification costs"
                " nothing at run time. The connection was not authenticated"
                " though, so this run does not show that clients can trust"
                " this endpoint. Supply --ca-bundle for a trusted run.",
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
    # Most important first: why the run itself is unreliable, then why it is
    # incomplete, then how far its results carry.
    order = (
        "The test machine stopped",
        "Only ",
        "This run was too short",
        "This run used plain HTTP",
    )
    headline_limits.sort(
        key=lambda h: next(
            (i for i, prefix in enumerate(order) if h["title"].startswith(prefix)),
            len(order),
        )
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
        "headline_limits": headline_limits,
        "later_stage": LATER_STAGE,
        "groups": [
            {
                "id": key,
                "question": question,
                "summary": summary,
                "status": group_status(checks, key),
                # Deciding checks when the section has them, otherwise every
                # check in it, so the trust section still reports something.
                "counts": dict(
                    Counter(
                        c["status"]
                        for c in checks
                        if c["group"] == key
                        and c["id"] not in ATTACHED_EVIDENCE
                        and (c["required"] or not any(
                            m["required"] for m in checks if m["group"] == key
                        ))
                    )
                ),
                "decides_result": any(
                    c["required"] for c in checks if c["group"] == key
                ),
            }
            for key, question, summary in QUESTIONS
        ],
        "checks": checks,
        "measurements": measurements,
        "baseline": baseline_data,
        "reference": {
            "basis": "measured baseline" if baseline else "reference profile" if profile else "none",
            "profile": describe(profile),
        },
        "compatibility": compat,
        "consistency": consistency_summary,
        "sweeps": sweeps,
        "timeline": windows,
        "errors": errors,
        "external_compliance": external,
        "history": history,
        "limitations": [
            "Ingestion is expressed in MiB/s of successful original writes. Raw-log equivalents use the configured compression assumption.",
            "Read-after-write currently verifies returned byte length, not full payload equality; it is not a corruption test.",
            "A partial last window is shown, but does not count toward the rate and throttling checks.",
        ],
    }
