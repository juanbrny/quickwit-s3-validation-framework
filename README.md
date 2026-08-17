# Datadog BYOC/Quickwit S3 Storage Provider Validation Framework

This is a self-certification framework for storage vendors. Examples include SeaweedFS,
NetApp StorageGRID, Ceph/RGW, MinIO, Scality, and custom appliances.
It lets a vendor prove that their Amazon Simple Storage Service (S3)-compatible
endpoint behaves like AWS S3, for the specific way Quickwit uses S3.
The framework tests this across five log-ingestion throughput tiers, sized
around real bring-your-own-cloud (BYOC) usage, which mostly ranges from
1 TB/day to 1 PB/day:

`100 GB/day · 1 TB/day · 10 TB/day · 100 TB/day · 1 PB/day`

100 GB/day sits below that commercial range and exists mainly for
completeness. A 10 PB/day tier also exists for customers at that
scale, but it is opt-in only (`--confirm-extreme-cost`) because a soak at
that volume can run up a large, real cloud bill — see
`docs/03_throughput_tier_sizing.md`.

It is not a generic S3 benchmark. It is a **workload-shaped** benchmark.
The traffic it generates mirrors what an indexer/merger/searcher/janitor fleet
does on the wire. We derive this traffic pattern from Quickwit's public
source code and documentation, as closely as possible.

## Why not just run `warp` or `s3-tests` and stop there?

Those tools are still used here, but they answer different questions:

| Tool | Question it answers | Question it *cannot* answer |
|---|---|---|
| **Ceph `s3-tests`** / **MinIO `mint`** | Is the S3 application programming interface (API) implemented *correctly* (semantics, edge cases, error codes)? | Does it perform well under Quickwit's access pattern? |
| **MinIO `warp`** | What is the raw GET/PUT throughput and latency distribution? | Does it survive Quickwit's actual mix of operations? This includes small commit-interval PUTs, full-object merge GETs, byte-range hotcache and fast-field GETs, and bulk deletes. These operations run concurrently, at a given ingestion rate. |
| **This framework** | Given an ingestion rate of 1 TB/day of logs, does the endpoint sustain the resulting S3 operation mix at AWS-S3-like latency and error rates? Does it support every S3 feature that Quickwit's code path uses? | Whether the *documents* index and search correctly. This is a storage-layer test only, not a Quickwit functional test. |

Run the three layers in sequence: **compliance → compatibility knobs →
workload-shaped load test**. Gate each layer in that order. There is no
point in load-testing an endpoint that fails basic multipart or range-GET
semantics.

## Directory layout

```
docs/
  01_s3_interaction_analysis.md   <- how Quickwit actually talks to S3 (sourced)
  02_test_methodology.md          <- the 3-layer test plan, pass/fail bars
  03_throughput_tier_sizing.md    <- how GB/day maps to S3 ops/sec, worked examples
config/
  tiers.yaml                      <- the 5 throughput tiers + extreme_tiers + tunable assumptions
src/
  qw_s3_client.py                 <- boto3 wrapper mirroring Quickwit's `flavor` knobs
  workload_model.py               <- GB/day -> op-mix math (footnoted to docs/03)
  ingest_merge_sim.py             <- simulates indexer commit + staged merge + garbage collection (GC)
  query_sim.py                    <- simulates the documented searcher GET formula
  consistency_probes.py           <- read-after-write / list-after-write / delete-visibility
  compat_checks.py                <- path-style, multipart, checksum, multi-delete knobs
  report.py                       <- turns raw results into a scorecard vs. an AWS S3 baseline
external_tools/
  README.md                       <- exact commands for s3-tests, mint, warp
run_certification.py              <- CLI entrypoint, orchestrates everything
requirements.txt
```

## Quick start

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 1. Compliance + compatibility knobs (fast, ~5-10 min)
python run_certification.py compat \
  --endpoint https://s3.vendor.example.com \
  --bucket qw-cert --access-key $AK --secret-key $SK \
  --flavor auto   # or: minio | garage | digital_ocean | gcs | none

# 2. Concurrency fan-out sweep (fast, seconds) -- can this backend actually
#    sustain many concurrent range-GETs, which is what this whole
#    architecture depends on to hide S3's per-request latency? Run this
#    before investing in the full Layer 3 soak below.
python run_certification.py fanout \
  --endpoint https://s3.vendor.example.com \
  --bucket qw-cert --access-key $AK --secret-key $SK

# 3. Run the same two commands against AWS S3 to produce the reference baseline
python run_certification.py compat \
  --endpoint https://s3.us-east-1.amazonaws.com \
  --bucket qw-cert-baseline --access-key $AWS_AK --secret-key $AWS_SK \
  --flavor none --baseline-out baseline_compat.json

# 4. Workload-shaped load test at a given tier, against the vendor
python run_certification.py load \
  --endpoint https://s3.vendor.example.com \
  --bucket qw-cert --access-key $AK --secret-key $SK \
  --tier 1TB --duration-min 30 --baseline baseline_1TB_aws.json

# 5. Generate the certification report (folds in the fanout result if present)
python run_certification.py report --tier 1TB --out report_1TB.md
```

Repeat steps 4 and 5 for each of the tiers you want to certify.
A vendor does not need to pass the 1 PB/day tier to get certified for
100 GB/day use cases. The report is per-tier, not all-or-nothing.

## Testing the framework itself

`tests/` is separate from the sections above. It is a pytest suite that
verifies this framework's own code is correct. It uses `moto`, an in-memory
S3 emulator, instead of a real endpoint. Run it before you trust the tool
against a real vendor, and again after any change to `src/`:

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

This suite covers the five checks featured in the customer-facing deck
("Five Checks, One Command" / "A Pass/Fail Table, Not a Guess"):

- `tests/test_compat_checks.py` runs `compat_checks.run_all_checks()` and
  `probe_flavor()` against moto. It asserts every check passes against a
  known-compliant backend. It also adds two regression tests for a bug
  caught during development. Two checks originally bypassed the
  flavor-aware client wrapper and called raw boto3 directly. This would
  have made `probe_flavor()` never recommend a flavor, even though a
  flavor's entire purpose is to route around that exact raw operation.
- `tests/test_qw_s3_client.py` and `tests/test_consistency_probes.py` cover
  the lower-level client and probe behavior that the checks build on.
- `tests/test_workload_model.py` covers the GB/day to operation-mix math,
  with no S3 involved at all.

We wrote and statically reviewed these tests in an environment without
network access to install `moto`, `boto3`, and `pytest`. So we have not run
them end-to-end yet. Run them locally first. Treat any failure as either a
real bug or a moto-version quirk.

The suite targets `moto[s3]>=5.0` and its unified `mock_aws` API. Older
moto pins use a per-service `mock_s3` decorator instead, and need small
import changes.

`tests/test_concurrency_fanout.py` covers the newest addition,
`concurrency_fanout.py` (see "Does it handle concurrency?" below). It tests
structure only. moto has no real network latency, so a real vendor's
concurrency ceiling cannot show up in these tests.

## Does it actually test whether the vendor can handle concurrency?

This question came up directly in review. The honest answer was "not
fully". We fixed it now, in two places:

- **`query_sim.py` had a real bug.** Every GET request a simulated query
  needed (footer, term/field lookups, doc fetches) ran in a sequential
  `for` loop, one request at a time. Real Quickwit queries fire their whole
  fan-out concurrently. This concurrency is the entire mechanism that makes
  sub-second search possible, despite S3's higher per-request latency (see
  `docs/01_s3_interaction_analysis.md` §5a). Sequential GETs meant Layer
  3's simulated query latency did not reflect real behavior. So the load
  test could never reveal a backend that fails specifically at
  concurrency. Fix: every GET in a query now dispatches through a shared
  thread pool at once. Each query's wall-clock completion time is logged
  as its own metric (`query_wall_clock`), alongside each GET's individual
  latency.
- **New: `concurrency_fanout.py` / `run_certification.py fanout`.** This is
  a fast, standalone sweep. It fires an increasing number of concurrent
  range-GETs (1 to 256 by default) against a single split-sized object. It
  measures whether wall-clock time for the whole batch stays close to a
  single request's latency (good), or climbs toward `concurrency x
  single-request-latency` (bad — this means the backend serializes
  "concurrent" requests instead of running them in parallel). This is
  Layer 2.5 in `docs/02_test_methodology.md`. Run it before the full Layer
  3 soak. A backend that fails it badly will almost certainly fail Layer
  3's query-latency criteria too.

## What "pass" means

Every check produces one of three verdicts:

- **PASS** — the vendor's metric stays within the tolerance band of the AWS
  S3 baseline. See `docs/02_test_methodology.md` for exact bands, for
  example p99 latency at or below 2x the AWS S3 baseline, and error rate at
  or below 0.1%.
- **PASS WITH DEVIATION** — functionally correct, but it requires a
  non-default Quickwit storage config (e.g. `disable_multipart_upload:
  true`, `checksum_algorithm: md5`) to work. The report records the exact
  YAML needed, so it can be shipped as a "known configuration" for that
  vendor.
- **FAIL** — either incorrect S3 semantics, or throughput and latency that
  cause Quickwit to fall behind on ingestion. This also includes
  throughput or latency that exceeds commit timeouts, or stalls merges or
  garbage collection (GC), at the declared tier.

This mirrors how Quickwit itself already documents vendor differences (the
`flavor` system in `storage-config.md`), instead of inventing a new
certification vocabulary.
