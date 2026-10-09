# Test Methodology

**Implementation and reporting status:** see
[Measurement policy](../measurement_policy.md), and
[Run a validation](../run_a_validation.md) for the commands.
The HTML report uses the evidence it has, and says clearly what is missing.
The current simulator does not measure an independent merge backlog and therefore
cannot issue full tier certification. The requirements below describe the intended
certification bar, not a claim that every measurement is already implemented.

This methodology uses three decided layers. A vendor must clear layer N before
layer N+1 is worth running. For example, a 1 PB/day soak test against an
endpoint that fails basic multipart semantics has no value.

## Layer 1 — API compliance (external tools, run once per endpoint)

No command in this repo. Run `s3-tests` or `mint`, then attach the evidence
with `report --compliance`.

This layer is not Quickwit-specific. It uses existing, mature open-source
test suites instead of building new ones:

- **Ceph `s3-tests`** — the most exhaustive open functional suite for Simple
  Storage Service (S3) semantics. It covers bucket operations, access
  control lists (ACLs), multipart edge cases, error codes, and encoding.
- **MinIO `mint`** — a containerized compliance runner. It wraps
  `s3-tests`, the `aws-sdk-*` client suites, and its own tests. It runs as a
  single container against any endpoint, so it is the easiest option to run.

To pass: **zero failures** in the subset of tests relevant to the operations
listed in `docs/background/01_s3_interaction_analysis.md` §7. These operations are
multipart, delete, range-GET, and list.

Failures outside that subset don't block certification. For example, bucket
ACL or versioning edge cases that Quickwit never touches. The framework
still logs these failures.

## Layer 2 — Quickwit compatibility knobs (`compat`, seconds)

`src/compat_checks.py` runs a scripted sequence for each knob in the table
above. It reports which Quickwit storage-config combination, if any, makes
the endpoint work:

1. Try the default configuration: path-style off, multi-delete on,
   multipart on, `checksum_algorithm: crc32c`.
2. If any sub-check fails, retry with the single most likely override. This
   mirrors Quickwit's own `flavor` values, plus the flavors this framework
   adds for SeaweedFS and Scality. Record which flag fixed the failure.
   Presets that hold the same settings are checked once, and the result is
   reused for the twin.
3. Output a minimal `storage.s3.*` YAML block for a user of that vendor to
   ship. This artifact is the most useful one for Quickwit's maintainers,
   because it is a candidate `flavor` value.

To pass: **at least one working configuration exists.** A vendor that only
works with three overrides set does not FAIL. It gets a "PASS WITH
DEVIATION" result. Upstream already handles `gcs` and `digital_ocean` the
same way. The `none` and `aws` flavors set no override, so they are the only
ones that can reach a plain PASS.

**Automated self-tests for this layer:** `tests/test_compat_checks.py` runs
the same five checks against `moto`, an in-memory Simple Storage Service
(S3) emulator. This confirms the check implementations are correct,
independent of any real vendor.

Running `run_certification.py compat` against a real endpoint also writes
`compat.md` in the run directory. This file is a flavor × check comparison
table, in the same format as the customer-facing deck. The command also
writes the raw JSON output.

## Layer 2.5 — Concurrency fan-out sweep (`read-concurrency`, `write-concurrency`, seconds)

Layers 1 and 2 confirm that the API is implemented correctly. Neither one
answers the real question: **can the backend sustain enough concurrent
requests to hide Simple Storage Service (S3)-style per-request latency
behind them?** This question determines whether the architecture works on
a given backend.

Per `docs/background/01_s3_interaction_analysis.md` §5a, Quickwit has no way to make a
single S3 request faster. This applies to any bring-your-own-cloud (BYOC)
deployment of it, too.

Its sub-second query claim only holds because a query fires roughly dozens
of small range-GETs *concurrently*. So wall-clock time tracks roughly one
round-trip's latency, instead of the sum of all requests. This follows
Little's Law: throughput ≈ concurrency ÷ latency.

A backend can have excellent single-request latency and still fail this
test in practice. Causes include a proxy that queues connections, a
connection pool sized too small, or per-connection throttling. In these
cases, query latency scales with the GET count, regardless of how fast any
individual request was.

`run_certification.py read-concurrency` tests this directly and in isolation. It
runs in seconds, rather than the minutes to hours a full Layer 3 soak test
takes:

1. Uploads one split-sized synthetic object.
2. Sweeps an increasing number of *concurrent* range-GETs against the
   object. The default levels are 1, 8, 16, 32, 64, 128, and 256; see
   `concurrency_fanout.py`.
3. At each level, measures wall-clock time for the whole batch. It also
   measures each individual request's own latency.
4. Computes an **efficiency** ratio per level: `p50_per_request_latency ÷
   wall_clock_time`. A ratio of 1.0 means the backend fully parallelized
   the batch. In that case, wall-clock time is about one request's
   latency, regardless of how many requests were fired. A ratio falling
   toward `1/concurrency` means the backend processed the "concurrent"
   requests essentially one at a time.

To pass: a batch of concurrent requests must finish at least twice as fast
as one-by-one handling, at every level. The setting is
`fanout_serialization_min_speedup` in `config/tiers.yaml`. No request may
fail, and no request may be throttled.

The framework reports the first level where efficiency drops below that
floor as `degrades_at_concurrency`. Past that point, the framework cannot
certify this backend for concurrency-dependent workloads without further
investigation. This applies independent of which throughput tier is under
test.

This is a pre-flight check, not a replacement for Layer 3. A backend that
fails the fan-out sweep badly will almost certainly also fail Layer 3's
query-latency checks. Failing fast here saves the cost of a full soak
run.

`run_certification.py load` (Layer 3) also measures the same property
under realistic mixed traffic. It uses the `query_wall_clock` metric in
`query_sim.py`. The fan-out sweep isolates the mechanism in isolation. The
Layer 3 number confirms the mechanism holds up under concurrent ingest,
merge, and garbage collection (GC) traffic too.

**A correctness note on the client's own connection pool:** the boto3
client's connection pool is fixed at construction time. Its size is set to
`cfg.max_concurrency`; see `qw_s3_client.py`.

Sweeping concurrency levels with a client sized for typical query traffic
(default 50) would measure *this tool's own* pool ceiling, not the
vendor's. To rule that out, `build_fanout_client()` sizes the pool well
above the top of the sweep.

**Automated self-tests for this layer:** `tests/test_concurrency_fanout.py`
confirms the sweep executes correctly at every level against `moto`. It
also confirms the sweep produces well-formed results. It deliberately does
not check efficiency thresholds or `degrades_at_concurrency`. Moto has no
real network latency to hide behind concurrency, so those numbers are only
meaningful against a real endpoint.

## Layer 3 — Workload-shaped load test (`load`, one throughput tier, a long soak)

For a chosen tier, from 100 GB to 1 PB per day, `run_certification.py load`
does the following. A 10 PB/day `extreme_tiers` entry also exists for the
rare customer at that scale, but it needs
`--confirm-extreme-cost` — see `03_throughput_tier_sizing.md` for why.

1. Computes the target operation mix using `src/workload_model.py`. See
   `03_throughput_tier_sizing.md` for the math.
2. Runs four concurrent workers for `--duration-min` minutes:
   - `ingest_merge_sim.py`: PUTs immature splits on the
     `commit_timeout_secs` cadence. It triggers staged merges at the
     `merge_factor` cadence: a full-object GET for each of the
     `merge_factor` splits, a PUT of the merged split, and a bulk DELETE of
     the old splits.
   - `query_sim.py`: fires range-GET bursts per split, at the tier's
     modeled queries per second (QPS), using the documented GET-count
     formula. Every GET in a given query is dispatched *concurrently*,
     bounded by the shared client's connection pool, rather than one at a
     time. See §5a in the analysis doc for why this detail matters, not
     just for style. Each query's wall-clock completion time is logged as
     its own metric, `query_wall_clock`. This lets the scorecard show
     whether concurrency actually delivers the latency this architecture
     depends on, instead of showing just each individual GET's latency in
     isolation.
   - `consistency_probes.py`: runs continuously interleaved
     read-after-write, list-after-write, and delete-visibility probes.
     Volume is low, but the probes are *latency-sensitive*. They must not
     just eventually pass; they must pass within the interval Quickwit
     actually waits.
   - A raw-throughput baseline pass, using `warp` (see `external_tools/`).
     This separates a generally slow endpoint from an endpoint that is
     slow specifically under Quickwit's operation mix.
3. Logs every request's latency, HTTP status, and error code (if the
   request failed) to `reports/<tier>_<endpoint>_raw.jsonl`.

### Pass/fail bands (relative to an AWS S3 baseline run with the same script)

| Metric | Threshold |
|---|---|
| Error rate (non-throttle) | ≤ 0.1% |
| Throttling rate (503 `SlowDown` / equivalent) | ≤ 1% sustained; 0% p50 |
| PUT p99 latency | ≤ 2x AWS S3 baseline |
| Range-GET p99 latency | ≤ 2x AWS S3 baseline |
| Bulk DELETE (1000 keys) p99 latency | ≤ 3x AWS S3 baseline |
| **Query wall-clock p99 latency** (`query_wall_clock`) | ≤ 2.5x AWS S3 baseline — this is the one that actually reflects whether concurrent fan-out is delivering; see Layer 2.5 |
| Sustained achieved throughput vs. target tier | ≥ 95% of target for the full soak duration |
| Merge backlog growth | Flat or decreasing over the soak window (not growing — a growing backlog at fixed input rate means the endpoint can't keep up with merge-driven GET/PUT volume at that tier) |
| Consistency probes | 100% read-after-write and list-after-write success within one `polling_interval` (default 30s) |

A tier is **CERTIFIED** if every metric passes. It is **CERTIFIED WITH
DEVIATION** if it passes only with a non-default `storage.s3.*`
configuration from Layer 2. Otherwise, it is **NOT CERTIFIED AT THIS
TIER**.

In that case, the report names the first metric that broke. This detail
usually points to where to look next. For example, a growing merge backlog
with normal PUT and GET latency points to a compute-bound merger, not the
storage backend. A blown-out PUT p99 latency under concurrency points to
connection-handling or backend contention.

## Why relative-to-AWS rather than absolute thresholds

Absolute latency numbers are meaningless across regions, network paths, and
on-premises versus cloud deployments. Running the identical workload
generator against Amazon Web Services (AWS) Simple Storage Service (S3)
first, and using it as the denominator, controls for these differences.

The report then answers "how much worse than AWS S3 is this, for
Quickwit's specific traffic shape". This is the question a vendor
evaluation actually needs answered. It is not "is this fast in absolute
terms".

## Scope explicitly excluded

- Whether Quickwit's *documents* index or search correctly. That is a
  Quickwit functional or integration test, not a storage-layer test.
- Simple Storage Service (S3) features Quickwit does not use at all.
  Examples: bucket versioning, lifecycle rules, object lock, ACLs beyond
  basic authentication, and server-side encryption with AWS Key Management
  Service (SSE-KMS) specifics beyond whether it exists. Upstream issue
  #5399 flags this scope; the framework does not cover it here.
- Multi-writer metastore compare-and-swap (CAS) semantics; see analysis doc
  §6. Current Quickwit does not rely on this, so the framework does not
  decide the result on it. The framework still records the probe results, for
  forward-compatibility interest.
