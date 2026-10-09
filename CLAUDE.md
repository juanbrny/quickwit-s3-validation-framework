# CLAUDE.md

## Current workflow

## Documentation rules

`docs/what_this_measures.md` is the definition the code must match: five
questions, ten deciding checks. If the code and that page disagree, the page
wins. Change the page first, with the user's agreement, then the code.

Each document has one job. Do not repeat content across them:

- `README.md` — entry point. One starter command, and a map of the documents.
- `docs/what_this_measures.md` — the five questions and the ten checks.
- `docs/run_a_validation.md` — every command, and the only place with commands.
- `docs/read_the_report.md` — what the report shows, section by section.
- `docs/measurement_policy.md` — exact rules and numbers, for reviewers.
- `docs/background/` — design notes. Not needed to run the tool.

`tests/test_docs.py` parses every documented command and fails if a flag is
wrong, a required option is missing, or the README shows a command that is not
in `run_a_validation.md`. Keep it passing.

Write in simple English for non-native readers: one idea per sentence, short
sentences, no jargon. Use the words in the glossary of
`docs/what_this_measures.md`: check, result, rule, information only. Do not
use gate, criterion, criteria, verdict or diagnostic in anything a reader
sees. Code identifiers such as `verdict` in `report.json` keep their names.

`certify` is the primary entry point. It chains compat, read-concurrency,
write-concurrency, load and report into one versioned `--run-dir`, carries the
recommended flavor forward, and optionally runs the AWS baseline leg from the
same host. The per-stage commands still exist for manual control.

Connection settings fall back to environment variables (`QW_S3_*`, then
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`), and `--session-token` falls back
to `AWS_SESSION_TOKEN`. Every run prints which source supplied the keys, with
only the first four letters of the key ID. Never print a secret. When every
flavor fails before a check runs, `setup_failure()` reports a connection
problem, not an incompatibility. `src/run_store.py` owns
write-once metadata, `src/report_model.py` evaluates required evidence, and
`src/report_render.py` renders HTML/JSON/Markdown.

Report structure: `QUESTIONS` in `src/report_model.py` holds the five
questions plus a sixth `trust` section, word for word as the page. Every check
carries a `group`, a `ratio` (headroom, at most 1.0 passes) and a `source`
(command, evidence file, setting). Exactly ten checks have `required=True`;
`tests/test_reporting.py::test_exactly_ten_checks_decide_the_result` pins them.
The 21 per-operation checks are information only, summarized by three
deciding checks (`response_time`, `failed_requests`, `slowed_requests`) and
shown in full in the operations table. `headline_limits` states the
certification ceiling in one place; do not restate it elsewhere.

Concurrency sweeps decide on serialization, not on latency spread. The metric is
`FanoutLevelResult.speedup` = concurrency x median latency / batch wall clock,
floor 2.0 from `*_serialization_min_speedup`. The older `efficiency` ratio
(median latency / wall clock) is information only; it falls with concurrency
for every backend and once failed AWS S3 itself at 0.09. Never decide on it.
`tests/test_concurrency_fanout.py` holds the real AWS and StorageGRID numbers
as a regression test.

Latency reference: `src/reference_profile.py` plus
`config/reference_profiles/*.yaml`. A profile holds `first_byte_p99_s` and
`per_stream_mb_s`, and the per-operation limit follows the recorded median
payload, with multipart parts treated as concurrent streams. A measured
`--baseline` wins when supplied; otherwise the default profile applies, so a
vendor with no AWS account still gets a graded verdict. `--reference none`
restores the inconclusive behavior. Never add a per-operation latency table;
object sizes change per tier, and the two-number model already covers them.

A pause of the test machine is never the storage's fault. `keep_awake()` runs
`caffeinate` on macOS, `PauseWatch` records each pause during `load`, and the
report leaves paused time and 60 s of recovery out of every rate, latency and
visibility check. A worker crash keeps its message in `worker_errors`, with
credentials removed by `redact()`. Running out of open files raises
`RunnerLimitError`, never a failed request against the storage.

Do not edit the code while the user has a run going. Commands import `src/`
modules lazily, so a long run picks up half-edited files, and its traceback
shows the new lines next to the old line numbers.

Every object a run writes must live under `qwcert/<run id>/`. That rule is
what makes `src/cleanup.py` safe in a shared bucket: it deletes only that
prefix, never the bucket. `run_prefix()` refuses anything that is not a
32-character run id, so the prefix can never widen. Never write test objects
anywhere else. `certify` cleans up after the report unless `--keep-objects`.

Missing evidence is NOT RUN or INCONCLUSIVE, never PASS. The synchronous
simulator cannot measure merge backlog, so `best_possible_verdict` is
INCONCLUSIVE and full certification stays blocked.

Use one word per concept: `flavor`, never "preset". `FLAVOR_PRESETS` keeps its
name because it holds the preset values of a flavor.

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

This is a self-certification framework for S3-compatible storage vendors.
It lets a vendor prove their endpoint behaves like AWS S3 (Amazon Simple
Storage Service), for the specific way Quickwit uses S3. The framework
tests across five log-ingestion throughput tiers, from 100GB/day to 1PB/day,
sized around real BYOC (bring your own cloud) usage. A 10PB/day tier also
exists, gated behind `--confirm-extreme-cost` because of the real cloud
cost a soak at that scale can run up.

The framework is workload-shaped, not a generic S3 benchmark. The `src/`
directory simulates Quickwit's actual indexer/merger/searcher/janitor
operation mix. It does not reimplement generic S3 compliance or
raw-throughput testing. Those tests are delegated to `s3-tests`, `mint`,
and `warp` — see `external_tools/README.md`.

The framework has three layers. Run them in sequence, and gate each layer
on the previous one passing. There is no point load-testing an endpoint
that fails basic multipart or range-GET (range GET request) semantics.
`certify` enforces this order.
1. **Compliance + compatibility knobs** (`compat_checks.py`, fast)
2. **Concurrency fan-out sweep** (`concurrency_fanout.py`, `put_fanout.py`, seconds)
3. **Workload-shaped load test at a throughput tier** (`ingest_merge_sim.py` + `query_sim.py`, longer soak)

## Commands

```bash
# Setup
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt          # runtime deps (boto3, botocore, PyYAML)
pip install -r requirements-dev.txt      # + pytest, moto[s3] for the framework's own tests

# Run the framework's own test suite (moto-mocked S3, no real endpoint needed)
pytest tests/ -v
pytest tests/test_compat_checks.py -v                       # single file
pytest tests/test_compat_checks.py::test_probe_flavor -v    # single test

# Preview the report layout with synthetic data, no S3 calls
python examples/make_sample_report.py --out-dir reports/example

# Validate a real endpoint. QW_S3_ENDPOINT, QW_S3_BUCKET, QW_S3_ACCESS_KEY and
# QW_S3_SECRET_KEY replace the connection flags.
python run_certification.py certify --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1
python run_certification.py certify --tier 1TB --duration-min 30 \
  --runner-location <label> --with-aws-baseline --aws-bucket <b> \
  --aws-access-key $AWS_AK --aws-secret-key $AWS_SK

# Or stage by stage, sharing one --run-dir
python run_certification.py compat            --run-dir <dir>
python run_certification.py read-concurrency  --run-dir <dir> --flavor <f>
python run_certification.py write-concurrency --run-dir <dir> --flavor <f>
python run_certification.py load              --run-dir <dir> --flavor <f> --tier 1TB --duration-min 30
python run_certification.py report            --run-dir <dir> --baseline <aws-dir>
```

This framework requires `moto[s3]>=5.0`'s unified `mock_aws` API. Older
moto versions use a per-service `mock_s3` decorator and need different
imports. If tests fail with import or decorator errors, check the
installed moto version first. Do not assume a real bug.

## Architecture

**Flow through `run_certification.py`**: this is the only entry point.
The `src/` modules are not meant to run standalone. Each subcommand
(`certify`, `compat`, `read-concurrency`, `write-concurrency`, `load`,
`report`) builds a `QwS3Client` from CLI
(command-line interface) arguments. It then runs the corresponding `src/`
simulation or probe, and writes raw JSON (JavaScript Object Notation) to
`reports/`. Optionally, it renders a Markdown scorecard.

- **`qw_s3_client.py`** — a boto3 wrapper around Quickwit's `storage.s3.*`
  settings (`force_path_style`, `disable_multi_object_delete`,
  `disable_multipart_upload`, `checksum_algorithm`, region override).
  `FLAVOR_PRESETS` transcribes the upstream flavors (minio, garage,
  digital_ocean, gcs) from Quickwit's `storage-config.md`. It adds local
  flavors Quickwit does not cover (seaweedfs, scality, storagegrid) and
  `aws`, which
  keeps every AWS default so AWS S3 can be the endpoint under test, not
  only the baseline. `UPSTREAM_FLAVORS`, `DEFAULT_FLAVORS` and
  `flavor_note()` keep those groups apart: only `none` and `aws` certify
  without deviation, and a local flavor's report tells the user to ship the
  explicit `storage.s3.*` block, since Quickwit will not accept the flavor
  name. This module does not reimplement Quickwit's Rust storage layer. Instead, it
  reproduces the same behavioral choices through boto3, so the compat
  probe can find which setting combination makes an endpoint work. That
  combination becomes the recommended configuration shipped to the
  vendor.
- **`compat_checks.py`** — `probe_flavor()` tries each flavor in
  `AUTO_PROBE_ORDER` against the endpoint, then recommends one. All
  checks must go through the flavor-aware `QwS3Client`. Never call raw
  boto3 directly. A past bug had two checks call raw boto3 directly,
  which silently defeated the purpose of flavor probing.
  `tests/test_compat_checks.py` now regression-tests this.
- **`workload_model.py`** — pure math that converts GB/day into an S3
  operation mix. It uses `config/tiers.yaml` (`model_constants`, plus
  per-tier `raw_gb_per_day`, `query_qps`, and `query_profiles`). It makes
  no S3 calls. See `docs/background/03_throughput_tier_sizing.md` for details.
- **`ingest_merge_sim.py`** — simulates indexer commit, staged merge, and
  garbage collection (GC) against the computed operation mix.
  `SplitKeyRegistry` tracks the split lifecycle.
- **`query_sim.py`** — simulates the documented searcher GET formula
  (`docs/background/01_s3_interaction_analysis.md` §5). A single query needs several
  GET requests: footer, term/field lookups, and document fetches. The
  simulation dispatches all of them concurrently through a shared thread
  pool, and times them as one `query_wall_clock` metric. This must stay
  concurrent. Two rules keep it faithful to real traffic, and both once
  broke: document reads are added once per query, not once per split
  (`GETs = splits x (fields x terms x 3 + fieldnorm + 1) + docs_returned`),
  and `worker_offsets()` staggers the search workers so queries arrive evenly
  instead of in simultaneous bursts. `tests/test_query_sim.py` pins both. Sequential GETs here previously masked a backend's
  inability to handle Quickwit's real concurrent fan-out, the mechanism
  that hides S3's per-request latency.
- **`concurrency_fanout.py`** — a standalone sweep (Layer 2.5). It fires
  an increasing number of concurrent range-GET requests (1 to 256 by
  default) against one split-sized object. It checks whether wall-clock
  time stays near a single request's latency (good), or scales toward
  `concurrency × single-request latency` (bad, meaning the backend
  serializes requests it should run concurrently). `build_fanout_client()`
  sizes the connection pool, so the client itself never becomes the
  bottleneck being measured.
- **`consistency_probes.py`** — checks read-after-write, list-after-write,
  and delete-visibility consistency.
- **`report.py`** — turns raw JSONL (JSON Lines) results into scorecards.
  It compares vendor metrics against an AWS S3 baseline, using
  `compare_to_baseline`, `render_markdown_report`, and
  `render_compat_markdown`.

**Results**: each check is PASS, FAIL, NOT RUN or INCONCLUSIVE. The
overall result is CERTIFIED, CERTIFIED WITH DEVIATION, NOT CERTIFIED or
INCONCLUSIVE, decided by the ten deciding checks only. The rules are in
`docs/measurement_policy.md`. The limits, such as p99 (99th percentile)
latency at most 2x AWS, or failed requests at most 0.1%, live in
`config/tiers.yaml` under `pass_fail_bands`.

**Config-driven, not hardcoded**: throughput tiers, model constants, and
pass/fail bands all live in `config/tiers.yaml`. Check there before
changing simulation behavior. Tuning often means editing the config file,
not the `src/` code.

## Docs worth reading before changing simulation logic

- `docs/background/01_s3_interaction_analysis.md` — how Quickwit actually talks to S3, sourced from Quickwit's code and docs. The GET-count formula and concurrency model live here.
- `docs/background/02_test_methodology.md` — the three-layer test plan and the exact pass/fail bars.
- `docs/background/03_throughput_tier_sizing.md` — how GB/day maps to S3 operations per second, with worked examples backing `workload_model.py`.
