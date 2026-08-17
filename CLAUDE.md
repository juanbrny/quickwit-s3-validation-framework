# CLAUDE.md

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
1. **Compliance + compatibility knobs** (`compat_checks.py`, fast)
2. **Concurrency fan-out sweep** (`concurrency_fanout.py`, fast, seconds)
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

# Run certification against a real vendor endpoint (see README.md Quick Start for full flow)
python run_certification.py compat  --endpoint <url> --bucket <b> --access-key $AK --secret-key $SK --flavor auto
python run_certification.py fanout  --endpoint <url> --bucket <b> --access-key $AK --secret-key $SK
python run_certification.py load    --endpoint <url> --bucket <b> --access-key $AK --secret-key $SK --tier 1TB --duration-min 30 --baseline baseline_1TB_aws.json
python run_certification.py report  --tier 1TB --out report_1TB.md
```

This framework requires `moto[s3]>=5.0`'s unified `mock_aws` API. Older
moto versions use a per-service `mock_s3` decorator and need different
imports. If tests fail with import or decorator errors, check the
installed moto version first. Do not assume a real bug.

## Architecture

**Flow through `run_certification.py`**: this is the only entry point.
The `src/` modules are not meant to run standalone. Each subcommand
(`compat`, `fanout`, `load`, `report`) builds a `QwS3Client` from CLI
(command-line interface) arguments. It then runs the corresponding `src/`
simulation or probe, and writes raw JSON (JavaScript Object Notation) to
`reports/`. Optionally, it renders a Markdown scorecard.

- **`qw_s3_client.py`** — a boto3 wrapper around Quickwit's `storage.s3.*`
  settings (`force_path_style`, `disable_multi_object_delete`,
  `disable_multipart_upload`, `checksum_algorithm`, region override).
  `FLAVOR_PRESETS` transcribes known vendor presets (minio, garage,
  digital_ocean, gcs) from Quickwit's `storage-config.md`. This module
  does not reimplement Quickwit's Rust storage layer. Instead, it
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
  no S3 calls. See `docs/03_throughput_tier_sizing.md` for details.
- **`ingest_merge_sim.py`** — simulates indexer commit, staged merge, and
  garbage collection (GC) against the computed operation mix.
  `SplitKeyRegistry` tracks the split lifecycle.
- **`query_sim.py`** — simulates the documented searcher GET formula
  (`docs/01_s3_interaction_analysis.md` §5). A single query needs several
  GET requests: footer, term/field lookups, and document fetches. The
  simulation dispatches all of them concurrently through a shared thread
  pool, and times them as one `query_wall_clock` metric. This must stay
  concurrent. Sequential GETs here previously masked a backend's
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

**Verdicts**: every check yields one of three results. PASS means it
works with default settings. PASS WITH DEVIATION means it works, but
needs a non-default `storage.s3.*` configuration, recorded verbatim in
the report. FAIL means it does not meet the bar. Pass/fail bands, such as
p99 (99th percentile) latency at most 2x the AWS baseline, or error rate
at most 0.1%, live in `config/tiers.yaml`'s `pass_fail_bands` and in
`docs/02_test_methodology.md`.

**Config-driven, not hardcoded**: throughput tiers, model constants, and
pass/fail bands all live in `config/tiers.yaml`. Check there before
changing simulation behavior. Tuning often means editing the config file,
not the `src/` code.

## Docs worth reading before changing simulation logic

- `docs/01_s3_interaction_analysis.md` — how Quickwit actually talks to S3, sourced from Quickwit's code and docs. The GET-count formula and concurrency model live here.
- `docs/02_test_methodology.md` — the three-layer test plan and the exact pass/fail bars.
- `docs/03_throughput_tier_sizing.md` — how GB/day maps to S3 operations per second, with worked examples backing `workload_model.py`.
