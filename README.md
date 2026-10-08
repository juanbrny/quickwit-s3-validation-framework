# Datadog BYOC/Quickwit S3 Storage Provider Validation Framework

This is a self-certification framework for storage vendors. Examples include
SeaweedFS, NetApp StorageGRID, Ceph/RGW, MinIO, Scality, and custom appliances.
It lets a vendor prove that their Amazon Simple Storage Service (S3)-compatible
endpoint behaves like AWS S3, for the specific way Quickwit uses S3.

It is not a generic S3 benchmark. It is a **workload-shaped** benchmark. The
traffic it generates mirrors what an indexer, merger, searcher and janitor
fleet does on the wire. We derive that traffic pattern from Quickwit's public
source code and documentation, as closely as possible.

## Quick start

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export QW_S3_ENDPOINT=https://s3.vendor.example.com
export QW_S3_BUCKET=qw-cert
export QW_S3_ACCESS_KEY=... QW_S3_SECRET_KEY=...

# Smoke run first: about one minute, proves the credentials and the sequence.
python run_certification.py certify --tier 100GB --duration-min 1 \
  --levels 1,8,16 --repeats 1

# The real run, with the AWS reference measured from the same machine.
python run_certification.py certify --tier 1TB --duration-min 30 \
  --runner-location ec2-us-east-1a-runner-01 \
  --with-aws-baseline --aws-bucket qw-cert-baseline \
  --aws-access-key "$AWS_AK" --aws-secret-key "$AWS_SK"
```

`certify` runs every stage in order, uses the flavor the compatibility probe
recommends, and writes the report. Open the `report.html` it prints.

**You do not need an AWS account.** Latency is graded against a bundled
reference profile in `config/reference_profiles/`, which states what AWS S3
delivers from an instance in the same region as its bucket. We publish it
because we define the bar, and we grade ourselves against the same numbers.
Supply `--with-aws-baseline` when you do have an account: a run measured next
to yours is stronger evidence, and it wins over the profile.

Prefer to see the output before running anything?

```bash
python examples/make_sample_report.py --out-dir reports/example
```

Three guides cover the rest:

- **[Run a validation](docs/run_a_validation.md)** — the commands, including
  how to run the stages one at a time.
- **[Read the report](docs/read_the_report.md)** — what each section means, and
  what to do about a failure.
- **[Measurement policy](docs/measurement_policy.md)** — the exact rule behind
  every threshold and verdict.

## Throughput tiers

The framework tests five log-ingestion tiers, sized around real
bring-your-own-cloud (BYOC) usage, which mostly ranges from 1 TB/day to
1 PB/day:

`100 GB/day · 1 TB/day · 10 TB/day · 100 TB/day · 1 PB/day`

100 GB/day sits below that commercial range and exists mainly for
completeness. A 10 PB/day tier also exists for customers at that scale. It is
opt-in only, through `--confirm-extreme-cost`, because a soak at that volume
runs up a large cloud bill. See
[tier sizing](docs/03_throughput_tier_sizing.md).

## What "pass" means

A report answers four questions, and rolls up its criteria under each one: can
I trust this evidence, does it behave like S3, can it keep up, and is it fast
enough. Each criterion reports PASS, FAIL, NOT RUN or INCONCLUSIVE. Missing
evidence never counts as a pass.

The overall verdict is one of NOT CERTIFIED, INCONCLUSIVE, CERTIFIED, or
CERTIFIED WITH DEVIATION. The last one means the endpoint works but needs a
non-default `storage.s3.*` configuration, which the report prints verbatim so
it can ship as a known configuration for that vendor. This mirrors how Quickwit
already documents vendor differences, through the `flavor` system in
`storage-config.md`, instead of inventing a new certification vocabulary.

**One limit to know before you start.** Merge backlog is a required criterion
and this simulator cannot measure it, so it is always NOT RUN and the overall
verdict cannot reach CERTIFIED today. A clean run ends at INCONCLUSIVE. Every
other criterion is measured, and a FAIL anywhere is still a real failure. See
[measurement policy](docs/measurement_policy.md).

## Why not just run `warp` or `s3-tests` and stop there?

Those tools are still used here. They answer different questions.

| Tool | Question it answers | Question it *cannot* answer |
|---|---|---|
| **Ceph `s3-tests`** / **MinIO `mint`** | Is the S3 application programming interface (API) implemented *correctly* (semantics, edge cases, error codes)? | Does it perform well under Quickwit's access pattern? |
| **MinIO `warp`** | What is the raw GET/PUT throughput and latency distribution? | Does it survive Quickwit's actual mix of operations? This includes small commit-interval PUTs, full-object merge GETs, byte-range hotcache and fast-field GETs, and bulk deletes, running concurrently at a given ingestion rate. |
| **This framework** | Given an ingestion rate of 1 TB/day of logs, does the endpoint sustain the resulting S3 operation mix at AWS-S3-like latency and error rates? Does it support every S3 feature that Quickwit's code path uses? | Whether the *documents* index and search correctly. This is a storage-layer test only, not a Quickwit functional test. |

Run the layers in sequence: **compliance → compatibility → concurrency →
workload-shaped load test**. Gate each one on the previous. There is no point
load-testing an endpoint that fails basic multipart or range-GET semantics.
`certify` enforces that order. External tool commands live in
`external_tools/README.md`.

## How concurrency is tested

Concurrency is the mechanism that makes sub-second search possible on storage
with high per-request latency, so the framework tests it directly in two
places.

- **`query_sim.py`** dispatches every read a simulated query needs through a
  shared thread pool at once, the way a real Quickwit query does. It records
  each query's wall-clock completion as its own metric, `query_wall_clock`,
  alongside each individual read. A sequential simulation would hide a backend
  that cannot handle the fan-out.
- **`concurrency_fanout.py`** sweeps an increasing number of concurrent
  requests against one object, for reads and for writes. It measures whether
  the batch wall-clock time stays close to a single request's latency (good),
  or climbs toward `concurrency × single-request latency` (bad, meaning the
  backend serializes requests it should run in parallel). Run it before the
  soak. A backend that fails it will almost certainly fail the soak's query
  latency criteria too.

## Directory layout

```
docs/
  run_a_validation.md             <- how to run it
  read_the_report.md              <- how to read the output
  measurement_policy.md           <- exact thresholds, definitions, verdict rules
  01_s3_interaction_analysis.md   <- how Quickwit actually talks to S3 (sourced)
  02_test_methodology.md          <- the layered test plan
  03_throughput_tier_sizing.md    <- how GB/day maps to S3 ops/sec, worked examples
config/
  tiers.yaml                      <- tiers, model constants, pass/fail bands
src/
  qw_s3_client.py                 <- boto3 wrapper mirroring Quickwit's `flavor` knobs,
                                     plus flavors for aws, seaweedfs and scality
  workload_model.py               <- GB/day -> op-mix math (footnoted to docs/03)
  ingest_merge_sim.py             <- indexer commit, staged merge, garbage collection
  query_sim.py                    <- the documented searcher read fan-out
  concurrency_fanout.py           <- concurrent read sweep
  put_fanout.py                   <- concurrent write sweep
  consistency_probes.py           <- read-after-write, list-after-write, delete visibility
  compat_checks.py                <- path style, multipart, checksum, multi-delete
  run_store.py                    <- write-once run bundles and provenance
  report_model.py                 <- evaluates evidence into criteria and verdicts
  report_render.py                <- HTML, Markdown and JSON output
external_tools/
  README.md                       <- exact commands for s3-tests, mint, warp
run_certification.py              <- the only entry point
```

## Testing the framework itself

`tests/` verifies this framework's own code against `moto`, an in-memory S3
emulator. It needs no credentials and costs nothing. Run it before trusting the
tool against a real vendor, and again after any change to `src/`.

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

It covers the five compatibility checks, the storage client's flavor branching,
the GB/day to operation-mix math, the reporting rules, and a complete
command-line run against a local moto server. These tests validate the
framework. They do not replace a real vendor certification run, and moto has no
network latency, so a real backend's concurrency ceiling cannot appear in them.

The suite needs `moto[s3]>=5.0` and its unified `mock_aws` API.
