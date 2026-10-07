#!/usr/bin/env python3
"""Validate an S3-compatible endpoint against Quickwit's workload, then report.

The short path is one command:

    python run_certification.py certify --tier 1TB --duration-min 30

`certify` runs every stage in order, uses the flavor the compatibility probe
recommends, and writes the report. Add --with-aws-baseline to measure AWS S3
from the same machine, which is what makes the latency comparison valid.

Run the stages one by one when you need to control each step. Each stage writes
into one --run-dir and can run once. Repeat an experiment in a new directory.
See docs/run_a_validation.md.
"""

from __future__ import annotations

import argparse
import dataclasses
import math
import os
import sys
import threading
import time
from pathlib import Path

from src.run_store import RunSession, default_run_dir, public_config, write_json
from src.workload_model import compute_op_mix, load_config


def cmd_compat(args, run):
    from src.compat_checks import probe_flavor, CHECKS
    from src.qw_s3_client import flavor_note
    from src.report import render_compat_markdown

    result = probe_flavor(
        args.endpoint, args.access_key, args.secret_key, args.bucket, args.region
    )
    write_json(run.path / "compat.json", result)
    render_compat_markdown(result, run.path / "compat.md")
    run.record["recommended_flavor"] = result["recommended_flavor"]
    run.record["required_checks"] = [name for name, _ in CHECKS]
    print(f"Recommended flavor: {result['recommended_flavor']}")
    note = flavor_note(result["recommended_flavor"])
    if note:
        print(note)


def client_config(args, run, concurrency=50):
    from src.qw_s3_client import QwS3Config

    cfg = QwS3Config.from_flavor(
        args.flavor,
        args.endpoint,
        args.access_key,
        args.secret_key,
        args.region,
        max_concurrency=concurrency,
    )
    run.record["effective_config"] = public_config(cfg)
    run.save()
    return cfg


def cmd_sweep(args, run):
    from src.qw_s3_client import QwS3Client
    from src.concurrency_fanout import (
        prepare_fanout_object,
        run_fanout_sweep,
        summarize_fanout,
    )
    from src.put_fanout import run_put_fanout_sweep

    key = "put_fanout" if args.cmd == "put-fanout" else "fanout"
    bands = run.config["pass_fail_bands"]
    levels = args.levels or bands[key + "_concurrency_levels"]
    if (
        not levels
        or any(type(k) is not int or k < 1 for k in levels)
        or len(set(levels)) != len(levels)
    ):
        raise ValueError("Concurrency levels must be unique positive integers.")
    cfg = client_config(args, run)
    client = QwS3Client(cfg)
    client.ensure_bucket(args.bucket)
    run.record["tested_levels"] = levels
    run.record["connection_pool_size"] = max(max(levels) * 2, 20)
    run.save()
    prefix = f"qwcert/{run.manifest['run_id']}/{args.cmd}"
    print(f"Sweeping concurrency levels {levels}, {args.repeats} trials per level.")
    if args.cmd == "fanout":
        size = prepare_fanout_object(
            client, args.bucket, prefix + ".split", args.object_size_mb
        )
        results = run_fanout_sweep(
            cfg, args.bucket, prefix + ".split", size, levels, args.repeats
        )
    else:
        results = run_put_fanout_sweep(
            cfg,
            args.bucket,
            prefix,
            concurrency_levels=levels,
            object_size_kb=args.object_size_kb,
            repeats=args.repeats,
        )
    write_json(
        run.path / (args.cmd + ".json"),
        summarize_fanout(results, bands[key + "_efficiency_min"]),
    )


def cmd_load(args, run):
    from src.qw_s3_client import QwS3Client
    from src.ingest_merge_sim import run_ingest_merge_sim, SplitKeyRegistry
    from src.query_sim import run_query_sim
    from src.consistency_probes import run_consistency_probes

    constants = run.config["model_constants"]
    op_mix = compute_op_mix(args.tier, run.config)
    cfg = client_config(args, run, constants["s3_max_concurrency_per_node"])
    client = QwS3Client(cfg)
    client.ensure_bucket(args.bucket)
    run.record["op_mix"] = dataclasses.asdict(op_mix)
    registry, stop = SplitKeyRegistry(), threading.Event()
    prefix = f"qwcert/{run.manifest['run_id']}/{args.tier}"
    threads, sinks, probe_errors = [], [], []
    start = time.time()
    run.record["measurement_started_epoch"] = start
    run.save()
    print(
        f"Running {args.tier} for {args.duration_min:g} minutes, flavor {args.flavor}."
    )
    try:
        workers, sink = run_ingest_merge_sim(
            client,
            args.bucket,
            prefix,
            op_mix,
            args.duration_min,
            run.path / "ingest_merge.jsonl",
            registry,
            merge_factor=constants["merge_factor"],
            commit_timeout_s=constants["commit_timeout_s"],
            stop_event=stop,
            block=False,
        )
        threads.extend(workers)
        sinks.append(sink)
        workers, sink = run_query_sim(
            client,
            args.bucket,
            registry.snapshot,
            run.config["query_profiles"],
            op_mix.query_qps,
            args.duration_min,
            run.path / "query.jsonl",
            stop_event=stop,
            block=False,
        )
        threads.extend(workers)
        sinks.append(sink)

        def probes():
            try:
                run_consistency_probes(
                    client,
                    args.bucket,
                    prefix,
                    args.duration_min,
                    interval_s=5,
                    deadline_s=run.config["pass_fail_bands"][
                        "consistency_probe_deadline_s"
                    ],
                    out_path=run.path / "consistency.jsonl",
                    stop_event=stop,
                )
            except Exception as error:
                probe_errors.append(type(error).__name__)
                stop.set()

        worker = threading.Thread(target=probes, daemon=True)
        threads.append(worker)
        worker.start()
        for worker in threads:
            worker.join()
        if probe_errors or any(s.errors for s in sinks):
            raise RuntimeError(
                "A workload worker failed; the run is incomplete. Inspect raw results."
            )
    finally:
        stop.set()
        # Let in-flight writes finish before closing their sinks, including on Ctrl-C.
        for worker in threads:
            worker.join()
        for sink in sinks:
            sink.close()
        run.record["measurement_duration_s"] = min(
            time.time() - start, args.duration_min * 60
        )
        run.record["live_splits_at_end"] = len(registry.snapshot())
        run.save()


def run_stage(args, config, announce=True):
    """Open a write-once stage in the bundle and execute it."""
    with RunSession(args, config) as run:
        if announce:
            print(f"Run directory: {run.path}")
        args.func(args, run)
    return run.path


# CLI name -> stage name stored in the manifest. The clear names are the ones
# users type; the original names stay as aliases so existing scripts work.
STAGE_ALIASES = {"read-concurrency": "fanout", "write-concurrency": "put-fanout"}

STAGES = {
    "compat": (
        "Check S3 behavior and recommend a flavor",
        "Tries each storage flavor against the endpoint and reports the first one"
        " that passes every compatibility check. Run this first.",
    ),
    "read-concurrency": (
        "Measure whether concurrent reads stay concurrent",
        "Fires a growing number of concurrent range reads at one object. A backend"
        " that serializes them fails Quickwit's query fan-out.",
    ),
    "write-concurrency": (
        "Measure whether concurrent writes stay concurrent",
        "The same sweep for uploads, which is how the indexer writes splits.",
    ),
    "load": (
        "Run the workload-shaped soak at one throughput tier",
        "Generates the indexer, merger, searcher and janitor operation mix for the"
        " chosen tier, then records every request.",
    ),
}

# Connection settings, and the environment variables that can supply them.
ENV_DEFAULTS = {
    "endpoint": ("QW_S3_ENDPOINT",),
    "bucket": ("QW_S3_BUCKET",),
    "access_key": ("QW_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"),
    "secret_key": ("QW_S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"),
    "region": ("QW_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"),
}
AWS_ENV_DEFAULTS = {
    "aws_endpoint": ("QW_AWS_ENDPOINT",),
    "aws_bucket": ("QW_AWS_BUCKET",),
    "aws_access_key": ("QW_AWS_ACCESS_KEY", "AWS_ACCESS_KEY_ID"),
    "aws_secret_key": ("QW_AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"),
    "aws_region": ("QW_AWS_REGION",),
}


def env_default(names):
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def add_connection_options(parser, prefix="", defaults=ENV_DEFAULTS):
    """Add endpoint, bucket, credential and region options.

    Every one of them can come from an environment variable instead, so the
    usual command is short. Put the exports in a file and source it.
    """
    for option in ("endpoint", "bucket", "access-key", "secret-key"):
        key = prefix + option.replace("-", "_")
        names = defaults[key]
        parser.add_argument(
            "--" + prefix.replace("_", "-") + option,
            default=env_default(names),
            help="Defaults to $" + ", $".join(names),
        )
    names = defaults[prefix + "region"]
    parser.add_argument(
        "--" + prefix.replace("_", "-") + "region",
        default=env_default(names) or "us-east-1",
        help="Defaults to $" + ", $".join(names) + ", then us-east-1",
    )


def require_connection(parser, args, prefix="", label="the endpoint under test"):
    """Fail early, naming the flag and the environment variable for each gap."""
    defaults = AWS_ENV_DEFAULTS if prefix else ENV_DEFAULTS
    missing = [
        "--" + (prefix + option).replace("_", "-")
        + " (or $" + defaults[prefix + option][0] + ")"
        for option in ("endpoint", "bucket", "access_key", "secret_key")
        if not getattr(args, prefix + option, None)
    ]
    if missing:
        parser.error(f"Missing connection settings for {label}: " + ", ".join(missing))


def stage_namespace(args, cmd, flavor, **extra):
    """Build the arguments one stage needs, from the certify arguments."""
    return argparse.Namespace(
        cmd=cmd,
        endpoint=args.endpoint,
        bucket=args.bucket,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region=args.region,
        run_dir=args.run_dir,
        runner_location=args.runner_location,
        flavor=flavor,
        **extra,
    )


def cmd_certify(args, config, parser):
    """Run every stage in order, then write the report.

    This exists because the stage-by-stage flow has two manual steps that are
    easy to get wrong: copying the recommended flavor out of the compatibility
    output, and reusing the same run directory. Both are automatic here.
    """
    # Check the reference credentials before the soak, not after it. Finding
    # them missing at the end would waste the whole run.
    if args.with_aws_baseline:
        if not args.aws_endpoint:
            args.aws_endpoint = f"https://s3.{args.aws_region}.amazonaws.com"
        require_connection(parser, args, "aws_", "the AWS S3 baseline")
    if args.run_dir is None:
        args.run_dir = str(default_run_dir())
    steps = 6 if args.with_aws_baseline else 5
    print(f"Run directory: {args.run_dir}")

    print(f"\nStep 1 of {steps}: compatibility")
    compat = stage_namespace(args, "compat", "auto")
    compat.func = cmd_compat
    run_stage(compat, config, announce=False)
    from src.run_store import read_json

    flavor = read_json(Path(args.run_dir) / "compat.json")["recommended_flavor"]
    if not flavor:
        parser.error(
            "No flavor passed every compatibility check, so the performance stages"
            " would measure a configuration that does not work. Read compat.md in"
            " the run directory, then fix the endpoint or supply a custom"
            " configuration."
        )
    print(f"Using flavor {flavor} for the remaining stages.")

    print(f"\nStep 2 of {steps}: read concurrency")
    sweep = stage_namespace(
        args, "fanout", flavor, levels=args.levels, repeats=args.repeats,
        object_size_mb=8.0,
    )
    sweep.func = cmd_sweep
    run_stage(sweep, config, announce=False)

    print(f"\nStep 3 of {steps}: write concurrency")
    sweep = stage_namespace(
        args, "put-fanout", flavor, levels=args.levels, repeats=args.repeats,
        object_size_kb=512.0,
    )
    sweep.func = cmd_sweep
    run_stage(sweep, config, announce=False)

    print(f"\nStep 4 of {steps}: workload soak, {args.tier}")
    load = stage_namespace(
        args, "load", flavor, tier=args.tier, duration_min=args.duration_min
    )
    load.func = cmd_load
    run_stage(load, config, announce=False)

    baseline = None
    if args.with_aws_baseline:
        print(f"\nStep 5 of {steps}: the same workload against AWS S3")
        baseline = args.baseline_run_dir or str(default_run_dir())
        reference = argparse.Namespace(
            cmd="load",
            endpoint=args.aws_endpoint,
            bucket=args.aws_bucket,
            access_key=args.aws_access_key,
            secret_key=args.aws_secret_key,
            region=args.aws_region,
            run_dir=baseline,
            # The baseline must match the vendor run on these three, or the
            # report cannot compare them. Running both legs here guarantees it.
            runner_location=args.runner_location,
            flavor="aws",
            tier=args.tier,
            duration_min=args.duration_min,
            func=cmd_load,
        )
        print(f"Reference directory: {baseline}")
        run_stage(reference, config, announce=False)

    print(f"\nStep {steps} of {steps}: report")
    return cmd_report(
        argparse.Namespace(
            run_dir=args.run_dir,
            baseline=baseline,
            compliance=args.compliance,
            previous=args.previous,
            out=args.out,
            strict=args.strict,
        )
    )


def cmd_report(args):
    from src.report_model import build_report
    from src.report_render import write_reports

    report = build_report(args.run_dir, args.baseline, args.compliance, args.previous)
    out = args.out or str(Path(args.run_dir) / "report.html")
    for file in write_reports(report, out, args.run_dir):
        print(f"Report written: {file}")
    print(f"Overall: {report['verdict']}")
    if args.strict and report["verdict"] not in (
        "CERTIFIED",
        "CERTIFIED WITH DEVIATION",
    ):
        return 1
    return 0


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Must be finite and greater than zero.")
    return number


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("Must be greater than zero.")
    return number


def parse_levels(value):
    try:
        values = [int(v) for v in value.split(",")]
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Use comma-separated positive integers."
        ) from None
    if not values or min(values) < 1 or len(values) != len(set(values)):
        raise argparse.ArgumentTypeError("Use unique positive concurrency levels.")
    return values


def add_report_options(parser):
    parser.add_argument(
        "--compliance",
        help="External s3-tests or mint evidence; see docs/measurement_policy.md.",
    )
    parser.add_argument(
        "--previous", help="Prior report.json, for an informational comparison."
    )
    parser.add_argument(
        "--out",
        help="Output path ending in .html, .md or .json; all three are written.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 when the verdict is not certified, after saving the report.",
    )


def main(argv=None):
    # Imported here, not at module level, so the flavor list has one source
    # of truth while the other commands keep loading boto3 only when needed.
    from src.qw_s3_client import FLAVOR_PRESETS

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    one_shot = sub.add_parser(
        "certify",
        help="Run every stage in order, then write the report (start here)",
        description=cmd_certify.__doc__,
    )
    add_connection_options(one_shot)
    one_shot.add_argument(
        "--tier",
        required=True,
        choices=["100GB", "1TB", "10TB", "100TB", "1PB", "10PB"],
        help="Daily log ingestion volume to simulate.",
    )
    one_shot.add_argument(
        "--duration-min",
        type=positive,
        default=30.0,
        help="Soak length in minutes. Use 1 for a first smoke run.",
    )
    one_shot.add_argument(
        "--levels",
        type=parse_levels,
        help="Concurrency levels for both sweeps, comma separated. Keep this"
        " small on a smoke run.",
    )
    one_shot.add_argument("--repeats", type=positive_int, default=3)
    one_shot.add_argument(
        "--run-dir", help="Bundle directory. A new timestamped one is made by default."
    )
    one_shot.add_argument(
        "--runner-location",
        default="unspecified",
        help="Label for where this machine runs, such as ec2-us-east-1a-runner-01.",
    )
    one_shot.add_argument(
        "--with-aws-baseline",
        action="store_true",
        help="Also run the same workload against AWS S3 from this machine, and"
        " compare. Latency criteria stay inconclusive without it.",
    )
    add_connection_options(one_shot, "aws_", AWS_ENV_DEFAULTS)
    one_shot.add_argument(
        "--baseline-run-dir", help="Where to write the AWS baseline bundle."
    )
    one_shot.add_argument("--confirm-extreme-cost", action="store_true")
    add_report_options(one_shot)
    one_shot.set_defaults(func=None)

    for name, (summary, description) in STAGES.items():
        stage = STAGE_ALIASES.get(name, name)
        p = sub.add_parser(
            name,
            aliases=[stage] if stage != name else [],
            help=summary,
            description=description,
        )
        add_connection_options(p)
        p.add_argument(
            "--run-dir",
            help="Bundle directory. Reuse it across stages, never for a repeated stage.",
        )
        p.add_argument(
            "--runner-location",
            default="unspecified",
            help="Label for where this machine runs; the baseline must match it.",
        )
        if name == "compat":
            p.add_argument(
                "--flavor",
                default="auto",
                choices=["auto"],
                help="This stage always probes every flavor.",
            )
            p.set_defaults(func=cmd_compat)
        else:
            p.add_argument(
                "--flavor",
                default="none",
                choices=list(FLAVOR_PRESETS),
                help="Storage flavor to test with. Use the one compat recommends,"
                " or aws for AWS S3 itself.",
            )
        if name in ("read-concurrency", "write-concurrency"):
            p.add_argument(
                "--levels", type=parse_levels, help="Concurrency levels, comma separated."
            )
            p.add_argument("--repeats", type=positive_int, default=3)
            if name == "read-concurrency":
                p.add_argument("--object-size-mb", type=positive, default=8.0)
            else:
                p.add_argument("--object-size-kb", type=positive, default=512.0)
            p.set_defaults(func=cmd_sweep)
        if name == "load":
            p.add_argument(
                "--tier",
                required=True,
                choices=["100GB", "1TB", "10TB", "100TB", "1PB", "10PB"],
                help="Daily log ingestion volume to simulate.",
            )
            p.add_argument(
                "--duration-min",
                type=positive,
                default=30.0,
                help="Soak length in minutes. Use 1 for a first smoke run.",
            )
            p.add_argument("--confirm-extreme-cost", action="store_true")
            p.set_defaults(func=cmd_load)

    p = sub.add_parser(
        "report",
        help="Build the HTML, JSON and Markdown report from a finished bundle",
        description="Reads only the recorded evidence. It needs no credentials and"
        " makes no S3 requests.",
    )
    p.add_argument("--run-dir", required=True)
    p.add_argument(
        "--baseline", help="AWS baseline run directory, for the latency comparison."
    )
    add_report_options(p)
    p.set_defaults(func=cmd_report)

    args = parser.parse_args(argv)
    args.cmd = STAGE_ALIASES.get(args.cmd, args.cmd)
    try:
        if args.cmd == "report":
            return cmd_report(args)
        require_connection(parser, args)
        config = load_config()
        if (
            getattr(args, "tier", None) in config.get("extreme_tiers", {})
            and not args.confirm_extreme_cost
        ):
            parser.error(
                "Extreme tiers require --confirm-extreme-cost because they can incur substantial storage and transfer costs."
            )
        if args.cmd == "certify":
            return cmd_certify(args, config, parser)
        path = run_stage(args, config)
        print(f"\nGenerate the report: python run_certification.py report --run-dir {path}")
        return 0
    except (ValueError, OSError, KeyError) as error:
        parser.error(str(error))
    except KeyboardInterrupt:
        print(
            "Interrupted. Partial evidence and execution status were preserved.",
            file=sys.stderr,
        )
        return 130


if __name__ == "__main__":
    sys.exit(main())
