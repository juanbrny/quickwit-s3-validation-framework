# How Quickwit Talks to S3

This document forms the basis for reverse-engineering the test suite. It
draws on Quickwit's own documentation, config schema, GitHub issues, and
engineering blog posts (see References). Where Quickwit's public material
does not give an exact internal implementation detail, this document flags
it as an assumption, not a fact.

## 1. The four phases and their S3 usage

Quickwit's architecture splits into four roles. Each role reads and writes
the same Simple Storage Service (S3) bucket. Each role has a different
access pattern.

| Phase | Component | S3 operations | Shape |
|---|---|---|---|
| **Ingest** | Indexer (in-memory buffering + commit) | `PutObject`, multipart (`CreateMultipartUpload`/`UploadPart`/`CompleteMultipartUpload`) | Frequent, small-to-medium sequential writes |
| **Index (compact within a pipeline)** | Indexer → split writer | Same as above, plus a full JavaScript Object Notation (JSON) metadata write or overwrite, if the metastore is file-backed on S3 (not the case for Bring Your Own Cloud (BYOC), which defaults to an external database as the metastore) | Bursty, tied to `commit_timeout_secs` |
| **Compact/Merge** | Merger (part of the indexing pipeline) | `GetObject` (full-object download of input splits), `PutObject`/multipart (merged output split), `DeleteObjects`/`DeleteObject` (old splits, post-publish) | Read-heavy, amplifies both GET and PUT volume |
| **Query** | Searcher | `GetObject` with `Range` (byte-range reads), `HeadObject` (implied by SDK range logic) | Very high fan-out of small, concurrent range GETs |
| **Garbage collection (GC) / retention** | Janitor | `ListObjectsV2` (to reconcile bucket contents vs. metastore — orphan detection), `DeleteObjects` (bulk) or `DeleteObject` (fallback) | Periodic, bursty |
| **Metastore (file-backed mode only)** | Any node | `GetObject` (full read on startup / poll interval), `PutObject` (full overwrite on every mutation) | Low volume, but consistency-sensitive |

## 2. The compatibility knobs Quickwit already ships

Quickwit's own `storage-config.md` file gives the best evidence of what
breaks against non-Amazon Web Services (AWS) S3 implementations. It
documents a `flavor` system, with options `digital_ocean`, `garage`, `gcs`,
and `minio`. Each flavor works around a behavior gap in a specific provider.

Quickwit also ships a set of manual override flags. Each knob below maps to
a concrete, testable requirement.

| Config knob | What it controls | Who needs it, per Quickwit's own docs |
|---|---|---|
| `force_path_style_access` | Path-style (`https://host/bucket/key`) vs. virtual-hosted (`https://bucket.host/key`) addressing | Ceph, MinIO |
| `disable_multi_object_delete` | Falls back from bulk `DeleteObjects` (up to 1000 keys/request) to per-object `DeleteObject` | Google Cloud Storage (GCS), Digital Ocean |
| `disable_multipart_upload` | Falls back to single-shot `PutObject` for large splits | GCS |
| `checksum_algorithm` (`crc32c` \| `md5` \| `disabled`) | Whether upload integrity uses the AWS software development kit (SDK)'s Cyclic Redundancy Check (CRC32C) trailer, the legacy `Content-MD5` header, or no checksum | Providers that came before `x-amz-checksum-*` support need `md5` or `disabled` |
| Region override (for example, forced to the literal string `garage` or `minio`) | Some providers require a specific, or dummy, region string to pass Signature Version 4 (SigV4) validation | Garage, MinIO |
| `endpoint` | Custom (non-AWS) endpoint URL | All non-AWS providers |
| `QW_S3_MAX_CONCURRENCY` | Caps concurrent in-flight S3 requests | Adjustable, based on each provider's connection-handling capacity |

**This is the highest-value part of the test suite.** Most "Quickwit doesn't
work with our S3" reports trace back to one of these five knobs. They rarely
point to a deep architectural mismatch.

The most common cause is checksum behavior. The AWS SDK for Rust sends a
`crc32c` trailer checksum by default. Older or partial S3 implementations do
not understand this checksum. They may reject it, or handle it incorrectly
without warning.

## 3. Ingest → split lifecycle (sizing and PUT behavior)

This section draws on Quickwit's AWS cost-optimization guide and its 23 TB
benchmark blog post:

- Indexers buffer documents and flush a new split about every
  `commit_timeout_secs` (default **60s**).
- A merge policy progressively merges splits, with a default `merge_factor`
  of **10**. A split becomes "mature" at roughly **10 million documents**,
  typically **1–10 GB** on disk.
- A mature split usually needs 2 PUT requests to upload (1 PUT request per
  5 GB). Quickwit uses multipart upload once a split passes S3's ~5 GB limit
  for a single PUT request.
- In the adversarial 23 TB benchmark, sustained indexing throughput reached
  **~27 MB/s per core** in the worst case. Typical structured logs index
  faster than this. Quickwit's team chose the `c5n.2xlarge` instance type
  for the indexer fleet. They chose it for its network throughput to S3, not
  for its compute power.
- Storage footprint after indexing was **~36%** of the raw input size in
  that benchmark. This is roughly a 2.75x compression ratio, once Quickwit
  builds the inverted index, columnar, and row-store representations. This
  ratio determines how many actual bytes reach S3 per GB of raw log data
  ingested.

**Assumption flagged:** Quickwit does not publish the exact per-pipeline
concurrency or multipart part size it uses internally. The test harness
models the following behavior instead: it uploads a mature split via
multipart upload in ~5 GB parts, and uploads an immature split via a single
PUT request. This model matches the documented PUT-count rule above.

## 4. Compact/Merge → read/write amplification

Each merge consolidates `merge_factor` (default **10**) input splits into
one output split. Every merge event includes:

- `merge_factor` full-object `GetObject` calls. Quickwit must read each
  input split fully to build the merged split; it does not use range reads.
- One `PutObject` or multipart upload sequence, for the merged split.
- `merge_factor` delete calls, once Quickwit publishes the merge and the old
  splits are safe to remove.

So write amplification and read amplification both scale with merge depth,
not only with raw ingest rate. A storage backend that handles sequential
PUT throughput well can still fail under merge-driven, full-object GET
fan-out at higher tiers. This happens because merge activity scales with the
number of splits produced. The number of splits produced scales with the
ingest rate divided by `commit_timeout_secs`.

## 5. Query → the documented GET-count formula

Quickwit's AWS cost page gives an explicit formula for the number of GET
requests a single query issues. This formula assumes Quickwit already
cached the per-split footer, known as the "hotcache," from a prior query.

```
GET requests ≈ num_splits_hit
             × ((num_search_fields × num_terms × 3) + num_fields_with_fieldnorms + 1)
             + num_docs_returned
```

One caveat: if term positions are disabled, term lookups cost 2 GET requests
instead of 3.

The first query against a "cold" split adds one more GET request. This
request fetches that split's footer, which embeds the hotcache. Quickwit's
storage-cache code keeps the hotcache in a long-lived, in-process cache,
because re-fetching it is expensive.

In other words, query performance depends mainly on many small, concurrent,
byte-range GET requests. The first-touch cost per split is much higher than
the cost of a warm-cache query.

**Assumption flagged:** Quickwit does not publish the precise byte ranges,
such as footer size or per-term posting-list block size. The harness instead
models representative small-range GET requests, from a few KB to a few
hundred KB, fired at high concurrency per split. This shape matters most for
a storage backend's range-GET path. The exact byte offsets do not change
what the test validates: correctness and latency of concurrent reads under
fan-out.

### 5a. Why concurrency matters here

**Assumption flagged, reasoned rather than directly documented:** Quickwit's
docs do not explicitly state that Quickwit issues these GET requests
concurrently. This document reasons it from the numbers already established
here.

S3's per-request latency is higher than local Non-Volatile Memory Express
(NVMe) storage. This latency includes the network round trip, Transport
Layer Security (TLS) overhead, Hypertext Transfer Protocol (HTTP) overhead,
and S3's own request handling. It commonly runs tens of milliseconds, not
sub-millisecond.

A query issues around dozens of GET requests, per the formula above. If
Quickwit issued these requests serially, query latency would scale linearly
with the GET count. Latency would then routinely reach hundreds of
milliseconds to seconds, not sub-second.

Quickwit's sub-second-search claim and the formula's GET count can both be
true only if most GET requests run at the same time. This follows Little's
Law (throughput ≈ concurrency ÷ latency). Quickwit cannot make each S3
request faster, so it keeps many requests in flight at once. A query's
wall-clock time then tracks roughly one round trip's latency, instead of the
sum of all round trips.

This has a direct, testable consequence for storage backend validation. A
backend's raw single-request latency matters little if the backend cannot
also sustain high concurrency.

A backend can have excellent single-request latency, but still serialize
"concurrent" connections silently. This can happen through a proxy that
queues requests, a connection pool that is too small, or a throttling policy
applied per connection instead of per account. Such a backend still produces
query latency that scales with GET count, which fails the property this
architecture depends on. A naive single-request latency benchmark would
never catch this failure.

See `concurrency_fanout.py` and the "Layer 2.5" section of
`docs/02_test_methodology.md` for how this suite tests for that directly.
See the `query_wall_clock` metric in `query_sim.py` for how the suite
monitors the same property under realistic mixed traffic.

## 6. Metastore consistency (file-backed-on-S3 mode only)

Quickwit's metastore docs state this clearly. The file-backed metastore
stores its full state as a single object. It does not implement any locking
mechanism.

For this reason, Quickwit documents the file-backed metastore as unsafe for
multiple concurrent writers. Quickwit recommends PostgreSQL whenever more
than one process needs to change metastore state.

Searchers can re-fetch that state with a `GetObject` call, on a
`polling_interval`. At a 30-second interval, this costs about $0.04 per
month per index, based on AWS pricing.

This has two direct consequences for the test suite:

1. Quickwit does not rely on S3 conditional-write or compare-and-swap
   semantics, such as `If-None-Match`, for multi-writer safety. Testing
   this is out of scope for current Quickwit versions. It is still worth
   flagging as a "nice to have," since future metastore-on-S3 designs may
   add it.
2. Quickwit does rely on plain read-after-write consistency. A searcher's
   next poll must see the indexer's last committed write, with no staleness
   window beyond `polling_interval`. This is the property the consistency
   probes in this suite check.

## 7. Summary table: what to test, mapped to what breaks in production

| Test category | Quickwit dependency | Real-world failure mode if absent |
|---|---|---|
| Path-style addressing | Bucket resolution | Indexer can't reach the bucket at all |
| Multipart upload (5 MB–5 GB parts, ≤5 TB object) | Splits > ~5 GB | Ingest fails once splits mature past 5 GB. Works in demos, breaks in production |
| Multi-Object Delete (≤1000 keys), with per-object fallback | Janitor/GC | GC fails outright, or silently falls back to slow per-key deletes; backlog grows |
| Checksum handling (crc32c trailer / MD5 / none) | Every upload | Uploads rejected, or silently mishandled by the SDK; most common real-world break |
| Byte-range GET (start-end, open-ended, suffix, out-of-range) | Every query | Query correctness or latency regressions, most visible under concurrency |
| Full-object GET at volume | Merges | Merge backlog, stalled compaction, growing small-split count |
| Read-after-write / list-after-write consistency | Publish visibility, GC reconciliation | Searchers miss just-published splits; GC deletes live data or leaves orphans |
| Concurrent range-GET fan-out (many requests at once, not raw single-request speed) | Every query, at the volume the sub-second-search claim depends on | Query latency scales with GET count instead of staying ~flat; single-request-latency benchmarks miss this entirely |
| Sustained PUT/GET/DELETE throughput & error rate at tier's derived op-mix | Everything, at scale | Throttling, backpressure, ingestion falling behind |

## References

- Quickwit storage configuration docs (`flavor`, override flags): `https://quickwit.io/docs/configuration/storage-config`
- Quickwit AWS cost optimization guide (PUT/GET formulas, commit/merge defaults): `https://quickwit.io/docs/operating/aws-costs`
- Quickwit metastore configuration docs (file-backed consistency/locking): `https://quickwit.io/docs/configuration/metastore-config`
- Quickwit GitHub issue #12 (original file-backed metastore design rationale): `https://github.com/quickwit-oss/quickwit/issues/12`
- "Scaling search to terabytes on a budget" (23 TB benchmark: throughput, compression ratio, instance choice): `https://quickwit.io/blog/benchmarking-quickwit-engine-on-an-adversarial-dataset`
- Quickwit storage cache module (footer/hotcache, fast-field cache rationale): `https://github.com/quickwit-oss/quickwit/blob/main/quickwit-storage/src/cache/mod.rs`
- Quickwit vs. Loki benchmark (typical "hundreds of GB/day" log workload framing): `https://quickwit.io/blog/benchmarking-quickwit-loki`
