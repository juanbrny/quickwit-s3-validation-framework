# CLAUDE.md

## Current workflow

## Documentation rules

Name the product **Datadog BYOC Logs**, spelled out as "Datadog BYOC Logs (BYOC)" at its first use in each document, then "BYOC". BYOC is built on Quickwit, its open-source upstream project. Say "Quickwit" only when citing Quickwit's own documentation, source code, benchmarks or configuration format, as the background notes do. Never write "Quickwit" in report text or command help. The `storage.s3` and `flavor` settings come from Quickwit's configuration format; the report presents them as BYOC settings.

Do not hard-wrap Markdown. Write each paragraph and each list item on one line, and let the editor wrap it.

The report uses Datadog purple (`--brand: #632ca6`, `--brand-dark: #2b124c`) for brand elements and for the measured series in charts. Reference lines (targets, minimums, limits) are dashed neutral grey, never a second hue. Status colours (PASS green, FAIL red, INCONCLUSIVE amber, NOT RUN grey) are reserved for status and are never brand colours. Chart colours were checked with the dataviz validator, and text pairs for 4.5:1 contrast; re-check any change.

`docs/what_this_measures.md` is the definition the code must match: five areas tested, nine deciding checks. If the code and that page disagree, the page wins. Change the page first, with the user's agreement, then the code.

Each document has one job. Do not repeat content across them:

- `README.md` — entry point. One starter command, and a map of the documents.
- `docs/what_this_measures.md` — the five areas tested, the nine checks, and what is tested at a later stage.
- `docs/run_a_validation.md` — every command, and the only place with commands.
- `docs/read_the_report.md` — what the report shows, section by section.
- `docs/measurement_policy.md` — exact rules and numbers, for reviewers.
- `docs/background/` — design notes. Not needed to run the tool.

`tests/test_docs.py` parses every documented command and fails if a flag is wrong, a required option is missing, or the README shows a command that is not in `run_a_validation.md`. Keep it passing.

Write in simple English for non-native readers: one idea per sentence, short sentences, no jargon. Use the words in the glossary of `docs/what_this_measures.md`: check, result, rule, information only. Do not use gate, criterion, criteria, verdict or diagnostic in anything a reader sees. Code identifiers such as `verdict` in `report.json` keep their names.

`validate` is the primary entry point. It chains check-machine, compat, read-concurrency, write-concurrency, load and report into one versioned `--run-dir`, carries the recommended flavor forward, and optionally runs the AWS baseline leg from the same host. The per-stage commands still exist for manual control.

Connection settings fall back to environment variables (`QW_S3_*`, then `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`), and `--session-token` falls back to `AWS_SESSION_TOKEN`. Every run prints which source supplied the keys, with only the first four letters of the key ID. Never print a secret. When every flavor fails before a check runs, `setup_failure()` reports a connection problem, not an incompatibility. `src/run_store.py` owns write-once metadata, `src/report_model.py` evaluates required evidence, and `src/report_render.py` renders HTML/JSON/Markdown.

Report structure: `QUESTIONS` in `src/report_model.py` holds the five areas plus a sixth `trust` section, word for word as the page. Each area's name is phrased as a question; the data key stays `question`. Every check carries a `group`, a `ratio` (headroom, at most 1.0 passes) and a `source` (command, evidence file, setting). Exactly nine checks have `required=True`; `tests/test_reporting.py::test_exactly_nine_checks_decide_the_result` pins them, and `test_nothing_untestable_holds_a_result_back` checks that a clean run can PASS. The 21 per-operation checks are information only, summarized by three deciding checks (`response_time`, `failed_requests`, `slowed_requests`) and shown in full in the operations table. `headline_limits` holds the notes that change how to read one run, such as a pause or a plain-HTTP endpoint.

Concurrency sweeps decide on serialization, not on latency spread. The metric is `FanoutLevelResult.speedup` = concurrency x median latency / batch wall clock, floor 2.0 from `*_serialization_min_speedup`. The older `efficiency` ratio (median latency / wall clock) is information only; it falls with concurrency for every backend and once failed AWS S3 itself at 0.09. Never decide on it. `tests/test_concurrency_fanout.py` holds the real AWS and StorageGRID numbers as a regression test.

Latency reference: `src/reference_profile.py` plus `config/reference_profiles/*.yaml`. A profile holds `first_byte_p99_s`, `footer_p99_s`, `fanout_tail_factor`, `per_stream_mb_s`, `upload_per_stream_mb_s` and `delete_p99_s`. The bundled one (version 3 and later) is measured from a 90-minute 10TB AWS run, `9325c7ec…`, from an m5a.8xlarge in us-east-1. Split footer reads, the first read of a file just after its upload, use `footer_p99_s`: their p99 was 122 ms against 65 ms for other small reads, and graded as small reads AWS S3 passed with only 8% to spare. The limit of a read or upload follows the recorded median payload, with multipart parts treated as concurrent streams. A bulk delete uses `delete_p99_s`, and a per-key delete loop uses `delete_p99_s x merge_factor`: graded as small reads, AWS S3 itself failed its 10-key bulk deletes (about 300 ms against 255 ms). The profile command measures `delete_p99_s` from the merges' bulk deletes when the run has at least 100, and otherwise adds the `delete_latency_s` that every delete-visibility probe records, because a 30-minute run has only a few merge deletes. A measured `--baseline` wins when supplied; otherwise the default profile applies, so a vendor with no AWS account still gets a graded verdict. `--reference none` restores the inconclusive behavior. `run_validation.py profile` writes the profile from a measured AWS run (`derive_profile()` in `src/reference_profile.py`); it refuses non-AWS, unfinished, paused, overloaded or thin runs (including fewer than 100 timed deletes), and only measures speed from transfers of 16 MiB or more (at least 100 downloads and 100 uploads). Every profile value is a slow-case figure, because the report compares p99 with p99: `per_stream_mb_s` and `upload_per_stream_mb_s` are the speed 99 of 100 transfers beat (`SLOW_SPEED_PCT`), not the median. With the median, AWS S3 failed its own 189 MiB merge reads at 10TB (4,610 ms against 4,373). `test_an_aws_run_passes_against_a_profile_made_from_itself` pins the rule that matters most: an AWS run graded against a profile made from itself must pass. The search rate counts only the time after the first upload, because both engines skip searches while there is nothing to search; at 10TB that once failed AWS S3 at 37.72 against 38.0 searches per second. Never add a per-operation latency table; object sizes change per tier, and the profile's numbers already cover them.

The machine check (`src/machine_check.py`, stage `machine`, command `check-machine`, step 1 of `validate`) records cores and memory as the container sees them, free disk, the open-file limit, and the network round trip: the fastest of 20 new TCP connections to the endpoint, which the storage software takes no part in. The report compares them with `test_machine` in `config/tiers.yaml` (sizes per tier, `max_round_trip_ms: 10`) as two information-only trust checks, `machine_size` and `network_distance`, plus headline notes. It only warns; a run continues on any machine. Never subtract the round trip from a latency: `distance_note()` compares the median round trip with how far each operation missed its limit, and says whether the distance can explain the failure. From a Mac, AWS S3 in us-east-1 was 101 ms away. `profile` refuses a run over the round-trip limit. Network bandwidth is not measured yet.

A pause of the test machine is never the storage's fault. `keep_awake()` runs `caffeinate` on macOS, `PauseWatch` records each pause during `load`, and the report leaves paused time and 60 s of recovery out of every rate, latency and visibility check. A worker crash keeps its message in `worker_errors`, with credentials removed by `redact()`. Running out of open files raises `RunnerLimitError`, never a failed request against the storage.

The workload step runs on the Rust load engine in `loadgen/` (`aws-sdk-s3`, the same S3 library as BYOC). One Python process could not send the 1TB workload: in an AWS run it made small reads take 71 ms instead of 30, and the report blamed S3. `src/load_engine.py` builds the engine settings, passes them and the credentials on standard input (never the command line), and records the engine summary in the load stage under `engine`. `--engine auto` uses Rust when `loadgen/target/release/byoc-loadgen` exists, and falls back to Python only for `100GB`; every larger tier refuses to run on Python. The engine writes the same `ingest_merge.jsonl` and `query.jsonl` rows as the Python engine, so the report does not care which one ran. Both engines must follow the same workload rules, listed in `docs/measurement_policy.md` under "The workload": indexers start spread over one commit interval, merges never block uploads, a merge unpublishes its inputs before the garbage collector deletes them after `gc_grace_period_s`, and searches are open loop. Uploads and searches run on separate thread pools in the engine, because hashing a 196 MB upload holds a thread for about 120 ms and once delayed 2% of searches. `machine_limit()` in `src/report_model.py` adds a headline note when the engine fell behind its schedule or kept most cores busy. Build with `cargo build --release --manifest-path loadgen/Cargo.toml`; test with `cargo test --release --manifest-path loadgen/Cargo.toml`. `cargo run --release --example null_s3` starts a server that stores nothing, to measure the engine's own cost. The test machine sizes per tier live in `docs/run_a_validation.md` under "Choose a test machine", and again in `test_machine` in `config/tiers.yaml` for the machine check; keep the two in step, and re-measure before changing them.

The tool ships as a container image, published as `ghcr.io/juanbrny/byoc-s3-validation` (`:latest` plus a commit tag, linux/amd64 and linux/arm64), run through `bin/byoc-s3`. Users type `byoc-s3 validate ...`, not `python run_validation.py ...`; every documented command uses `byoc-s3`, and `tests/test_docs.py` parses both forms. The wrapper mounts the current folder at the same path, passes connection variables by name only (never values on a command line), mounts an outside `--ca-bundle`, runs as the caller's user, uses `--network host` on Linux and `caffeinate` on macOS. `tests/test_docs.py::test_the_wrapper_passes_every_connection_setting` fails if a new connection variable is not passed. Inside the image, `BYOC_CONTAINER=1`, `BYOC_LOADGEN` points at the engine, `BYOC_REPORTS_DIR` (set by the wrapper) replaces the default `reports/` folder, and `BYOC_SOURCE_COMMIT` replaces git in the run record. `docker/entrypoint.sh` also handles `print-wrapper` (writes the wrapper with the image name filled in) and `sample-report`. The engine stage cross-compiles on `$BUILDPLATFORM`: rustc crashes under QEMU emulation, so never build it emulated.

Bucket addressing must match BYOC's S3 library: `addressing_style()` in `src/qw_s3_client.py` uses virtual-hosted style unless `force_path_style` is set or the endpoint is an IP address. boto3's "auto" style used paths for every custom endpoint, so the compatibility check once passed BYOC defaults on an endpoint where the Rust engine, like BYOC, failed every request.

Do not edit the code while the user has a run going. Commands import `src/` modules lazily, so a long run picks up half-edited files, and its traceback shows the new lines next to the old line numbers.

Every object a run writes must live under `qwcert/<run id>/`. That rule is what makes `src/cleanup.py` safe in a shared bucket: it deletes only that prefix, never the bucket. `run_prefix()` refuses anything that is not a 32-character run id, so the prefix can never widen. Never write test objects anywhere else. `validate` cleans up after the report unless `--keep-objects`.

Missing evidence is NOT RUN or INCONCLUSIVE, never PASS. This is a synthetic test, not a formal certification, and a clean run must be able to PASS. Never make a deciding check of something this tool cannot measure: it would block every result. List it in `LATER_STAGE` in `src/report_model.py` and in the "Tested at a later stage" section of `docs/what_this_measures.md` instead. Merge backlog and full-size merges live there; they are tested later, in the BYOC performance tests.

Use one word per concept: `flavor`, never "preset". `FLAVOR_PRESETS` keeps its name because it holds the preset values of a flavor.

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

This is a synthetic validation tool for S3-compatible storage vendors, not a formal certification. It lets a vendor check that their endpoint behaves like AWS S3 (Amazon Simple Storage Service), for the specific way Datadog BYOC Logs (BYOC) uses S3. BYOC is built on Quickwit, so the tests apply to Quickwit too. The framework tests across five log-ingestion throughput tiers, from 100GB/day to 1PB/day, sized around real BYOC (bring your own cloud) usage. A 10PB/day tier also exists, gated behind `--confirm-extreme-cost` because of the real cloud cost a soak at that scale can run up.

The framework is workload-shaped, not a generic S3 benchmark. The `src/` directory simulates Quickwit's actual indexer/merger/searcher/janitor operation mix. It does not reimplement generic S3 compliance or raw-throughput testing. Those tests are delegated to `s3-tests`, `mint`, and `warp` — see `external_tools/README.md`.

The framework has three layers. Run them in sequence, and gate each layer on the previous one passing. There is no point load-testing an endpoint that fails basic multipart or range-GET (range GET request) semantics. `validate` enforces this order.
1. **Compliance + compatibility knobs** (`compat_checks.py`, fast)
2. **Concurrency fan-out sweep** (`concurrency_fanout.py`, `put_fanout.py`, seconds)
3. **Workload-shaped load test at a throughput tier** (`ingest_merge_sim.py` + `query_sim.py`, longer soak)

## Commands

```bash
# Container (what users run)
docker run --rm ghcr.io/juanbrny/byoc-s3-validation print-wrapper > ~/.local/bin/byoc-s3 && chmod +x ~/.local/bin/byoc-s3
docker build -t ghcr.io/juanbrny/byoc-s3-validation .                 # local build, under the published name
# publish both platforms: see docs/run_a_validation.md, "Build the image"
byoc-s3 validate --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1

# Setup from source
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
python run_validation.py validate --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1
python run_validation.py validate --tier 1TB --duration-min 30 \
  --runner-location <label> --with-aws-baseline --aws-bucket <b> \
  --aws-access-key $AWS_AK --aws-secret-key $AWS_SK

# Or stage by stage, sharing one --run-dir
python run_validation.py compat            --run-dir <dir>
python run_validation.py read-concurrency  --run-dir <dir> --flavor <f>
python run_validation.py write-concurrency --run-dir <dir> --flavor <f>
python run_validation.py load              --run-dir <dir> --flavor <f> --tier 1TB --duration-min 30
python run_validation.py report            --run-dir <dir> --baseline <aws-dir>
```

This framework requires `moto[s3]>=5.0`'s unified `mock_aws` API. Older moto versions use a per-service `mock_s3` decorator and need different imports. If tests fail with import or decorator errors, check the installed moto version first. Do not assume a real bug.

## Architecture

**Flow through `run_validation.py`**: this is the only entry point. The `src/` modules are not meant to run standalone. Each subcommand (`validate`, `compat`, `read-concurrency`, `write-concurrency`, `load`, `report`) builds a `QwS3Client` from CLI (command-line interface) arguments. It then runs the corresponding `src/` simulation or probe, and writes raw JSON (JavaScript Object Notation) to `reports/`. Optionally, it renders a Markdown scorecard.

- **`qw_s3_client.py`** — a boto3 wrapper around Quickwit's `storage.s3.*` settings (`force_path_style`, `disable_multi_object_delete`, `disable_multipart_upload`, `checksum_algorithm`, region override). `FLAVOR_PRESETS` transcribes the upstream flavors (minio, garage, digital_ocean, gcs) from Quickwit's `storage-config.md`. It adds local flavors Quickwit does not cover (seaweedfs, scality, storagegrid) and `aws`, which keeps every AWS default so AWS S3 can be the endpoint under test, not only the baseline. `UPSTREAM_FLAVORS`, `DEFAULT_FLAVORS` and `flavor_note()` keep those groups apart: only `none` and `aws` validate without deviation, and a local flavor's report tells the user to ship the explicit `storage.s3.*` block, since Quickwit will not accept the flavor name. This module does not reimplement Quickwit's Rust storage layer. Instead, it reproduces the same behavioral choices through boto3, so the compat probe can find which setting combination makes an endpoint work. That combination becomes the recommended configuration shipped to the vendor.
- **`compat_checks.py`** — `probe_flavor()` tries each flavor in `AUTO_PROBE_ORDER` against the endpoint, then recommends one. All checks must go through the flavor-aware `QwS3Client`. Never call raw boto3 directly. A past bug had two checks call raw boto3 directly, which silently defeated the purpose of flavor probing. `tests/test_compat_checks.py` now regression-tests this.
- **`workload_model.py`** — pure math that converts GB/day into an S3 operation mix. It uses `config/tiers.yaml` (`model_constants`, plus per-tier `raw_gb_per_day`, `query_qps`, and `query_profiles`). It makes no S3 calls. See `docs/background/03_throughput_tier_sizing.md` for details.
- **`ingest_merge_sim.py`** — simulates indexer commit, staged merge, and garbage collection (GC) against the computed operation mix. `SplitKeyRegistry` tracks the split lifecycle.
- **`query_sim.py`** — simulates the documented searcher GET formula (`docs/background/01_s3_interaction_analysis.md` §5). A single query needs several GET requests: footer, term/field lookups, and document fetches. The simulation dispatches all of them concurrently through a shared thread pool, and times them as one `query_wall_clock` metric. This must stay concurrent. Two rules keep it faithful to real traffic, and both once broke: document reads are added once per query, not once per split (`GETs = splits x (fields x terms x 3 + fieldnorm + 1) + docs_returned`), and `worker_offsets()` staggers the search workers so queries arrive evenly instead of in simultaneous bursts. `tests/test_query_sim.py` pins both. Sequential GETs here previously masked a backend's inability to handle Quickwit's real concurrent fan-out, the mechanism that hides S3's per-request latency.
- **`concurrency_fanout.py`** — a standalone sweep (Layer 2.5). It fires an increasing number of concurrent range-GET requests (1 to 256 by default) against one split-sized object. It checks whether wall-clock time stays near a single request's latency (good), or scales toward `concurrency × single-request latency` (bad, meaning the backend serializes requests it should run concurrently). `build_fanout_client()` sizes the connection pool, so the client itself never becomes the bottleneck being measured.
- **`consistency_probes.py`** — checks read-after-write, list-after-write, and delete-visibility consistency.
- **`report.py`** — turns raw JSONL (JSON Lines) results into scorecards. It compares vendor metrics against an AWS S3 baseline, using `compare_to_baseline`, `render_markdown_report`, and `render_compat_markdown`.

**Results**: each check is PASS, FAIL, NOT RUN or INCONCLUSIVE. The overall result is PASS, FAIL or INCONCLUSIVE, decided by the nine deciding checks only. Never use the word CERTIFIED in reader-facing text. Whether BYOC needs special storage settings is shown beside the result, never as a separate result. The rules are in `docs/measurement_policy.md`. The limits, such as p99 (99th percentile) latency at most 2x AWS, or failed requests at most 0.1%, live in `config/tiers.yaml` under `pass_fail_bands`.

**Config-driven, not hardcoded**: throughput tiers, model constants, and pass/fail bands all live in `config/tiers.yaml`. Check there before changing simulation behavior. Tuning often means editing the config file, not the `src/` code.

## Docs worth reading before changing simulation logic

- `docs/background/01_s3_interaction_analysis.md` — how Quickwit actually talks to S3, sourced from Quickwit's code and docs. The GET-count formula and concurrency model live here.
- `docs/background/02_test_methodology.md` — the three-layer test plan and the exact pass/fail bars.
- `docs/background/03_throughput_tier_sizing.md` — how GB/day maps to S3 operations per second, with worked examples backing `workload_model.py`.
