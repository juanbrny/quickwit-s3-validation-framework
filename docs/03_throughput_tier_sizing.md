# Throughput Tier → S3 Op-Mix Math

Each constant below comes from one of three sources.

- Quickwit's published documentation, marked **[docs]**.
- Explicit, tunable modeling assumptions, marked **[assumption]**.
- Confirmed directly against Pomsky's (Datadog's Quickwit fork) source, marked **[pomsky]**.

`config/tiers.yaml` exposes all assumptions. Change them if your dataset or
configuration differs from Quickwit's own adversarial benchmark.

This is v1 of the suite: the **[pomsky]**-marked rows were checked against
real source during v1 hardening; the remaining **[docs]**/**[assumption]**
rows (`per_core_mbps`, `mature_split_gb`) are known to need a closer look
-- see the note below the table -- but are left as-is until real usage of
both this suite and the separate integration-test suite produces feedback
to correct them against.

## Inputs

| Symbol | Meaning | Default | Source |
|---|---|---|---|
| `T` | Raw log ingest, GB/day | 100 / 1,024 / 10,240 / 102,400 / 1,048,576 | user-selected tier |
| `peak_mult` | Peak-to-average burst ratio | 3.0 | **[assumption]** typical diurnal log traffic |
| `compression` | Raw bytes ÷ indexed (S3) bytes | 2.75 | **[docs]** 23 TB benchmark |
| `commit_timeout_s` | Indexer flush interval | 60s | **[pomsky]** `default_commit_timeout_secs()`, confirmed exact match |
| `merge_factor` | Splits consolidated per merge | 10 | **[pomsky]** `default_merge_factor()`, confirmed exact match |
| `mature_split_gb` | Target mature split size | 8 GB (typical BYOC size; documented range is 1-10GB) | **[docs]** -- **known issue**: Pomsky's actual split maturity is time-based (48h `maturation_period`), not size-based at all. This constant models PUT payload size for the ingest/merge pattern, not real "maturity"; needs reframing, not fixed in v1. |
| `per_core_mbps` | Worst-case per-core indexing throughput | 27 MB/s | **[docs]** adversarial benchmark -- **known issue**: Pomsky's real constant, `PIPELINE_THROUGHPUT`, is 20 MB/s **per pipeline** (~4 CPU threads), not per core, with real-world typically >30 MB/s. The per-core unit and the pipelines-per-node multiplier need correcting; not fixed in v1. |
| `s3_max_concurrency_per_node` | Concurrent in-flight S3 requests per node | 10,000 | **[pomsky]** `QW_S3_MAX_CONCURRENCY` env var default, confirmed (`s3_compatible_storage.rs`); previously modeled as 50, off by 200x |
| `multipart_threshold` | Object size that triggers multipart upload | 128 MiB | **[pomsky]** `MultiPartPolicy::default().multipart_threshold_num_bytes`, confirmed; previously modeled as ~5 GB (conflated with target part size), off by ~40x |
| `multipart_part_gb` | Target part size once multipart is in use | 5 GB | **[pomsky]** `MultiPartPolicy::default().target_part_num_bytes`, confirmed exact match |

## Step 1 — Raw → sustained/peak MB/s

```
avg_raw_MBps  = T * 1024 / 86400
peak_raw_MBps = avg_raw_MBps * peak_mult
```

## Step 2 — Raw → compressed (what actually crosses the wire to S3)

```
avg_s3_MBps  = avg_raw_MBps  / compression
peak_s3_MBps = peak_raw_MBps / compression
```

## Step 3 — Indexer fleet size (for concurrency modeling only)

```
num_indexer_nodes = ceil(peak_raw_MBps / per_core_mbps)
```

This step divides *raw* MB/s by a *per-core* number. The 27 MB/s figure is
Quickwit's own measured ingest throughput per core on an adversarial
dataset. This makes the figure deliberately conservative.

Typical structured logs index faster per core than this figure suggests.
So this calculation over-estimates fleet size, and therefore concurrency,
rather than under-estimating it. Over-estimating is the safer direction for
a stress test.

## Step 4 — PUT rate (ingest side)

Immature-split PUTs (one per indexer pipeline per commit interval):

```
immature_put_rate_per_s = num_indexer_nodes / commit_timeout_s
immature_put_size_MB    = avg_s3_MBps * commit_timeout_s / num_indexer_nodes
```

Merge-driven PUTs: a merge happens roughly every time `merge_factor`
immature splits accumulate. Their combined size must also approach
`mature_split_gb`:

```
splits_per_day        = avg_s3_MBps * 86400 / (immature_put_size_MB)
merges_per_day         = splits_per_day / merge_factor
merge_put_size_gb      = mature_split_gb
merge_parts_per_upload = ceil(merge_put_size_gb / multipart_part_gb)
```

## Step 5 — GET rate (merge reads + query reads)

Merge reads (full-object GET, not range):

```
merge_get_ops_per_day = merges_per_day * merge_factor
```

Query reads use the documented formula from `01_s3_interaction_analysis.md`
§5. This formula runs at a configurable queries per second (QPS) rate
(`config/tiers.yaml: query_qps`).

The default QPS scales mildly with tier, because more data usually
correlates with more query traffic. This relationship depends on the
workload. Override the default per evaluation as needed.

## Step 6 — DELETE rate (garbage collection, GC)

```
delete_ops_per_day = merges_per_day * merge_factor   # old splits retired
```

The model uses bulk `DeleteObjects` calls in batches of up to 1000 keys by
default. A fallback mode (`disable_multi_object_delete: true`) issues these
as individual `DeleteObject` calls instead. The tests exercise both paths.

## Worked examples

Output of `python3 src/workload_model.py --print-table` against the default
`config/tiers.yaml`:

| Tier | avg raw MB/s | peak raw MB/s | avg S3 MB/s | indexer nodes (@27MB/s/core, peak) | immature PUTs/min | merges/day | merge GETs/day | GC deletes/day |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 GB/day | 1.19 | 3.56 | 0.43 | 1 | 1.0 | 144 | 1,440 | 1,440 |
| 1 TB/day | 12.14 | 36.41 | 4.41 | 2 | 2.0 | 288 | 2,880 | 2,880 |
| 10 TB/day | 121.36 | 364.09 | 44.13 | 14 | 14.0 | 2,016 | 20,160 | 20,160 |
| 100 TB/day | 1,213.63 | 3,640.89 | 441.32 | 135 | 135.0 | 19,440 | 194,400 | 194,400 |
| 1 PB/day | 12,427.57 | 37,282.70 | 4,519.12 | 1,381 | 1,381.0 | 198,864 | 1,988,640 | 1,988,640 |

*(Regenerate this table from the script after changing any constant in
`config/tiers.yaml`. Do not hand-edit it out of sync with the code.)*

100GB/day sits below what a commercial BYOC (bring your own cloud)
deployment typically ingests. It stays in the tier list for completeness,
not because vendors need to be certified at that size. The 1TB–1PB/day
range covers the ingestion rates of real BYOC customers.

`config/tiers.yaml` also defines an `extreme_tiers` section (currently
10PB/day), for the rare customer that ingests at that scale. It is
deliberately excluded from `run_certification.py load`'s default `--tier`
choices' normal path and requires `--confirm-extreme-cost` to run. A soak
at 10PB/day moves petabytes of data, which can run up a large, real cloud
bill. Any objects or buckets left behind after the run become a hidden,
ongoing cost. Only run it with cost sign-off and a guaranteed teardown
plan for everything the run creates.

The model produces two results that are easy to miss at first glance.

1. **Merge, GET, and DELETE counts stay flat within a fixed indexer node
   count.** This is not a bug. It follows directly from
   `commit_timeout_secs`: a time-based flush, not a size-based one. One
   indexer node flushes 1,440 times/day (every 60s), regardless of how
   full each flush is. Op *counts* only step up when the tier needs
   another indexer node. Op *sizes* (bytes per PUT or GET) scale smoothly
   with ingest rate within a fixed node count.
   A storage backend that handles infrequent large objects well, but
   struggles with frequent small ones (or the reverse), shows this
   specifically when you compare tiers that share a node count. At the
   point where node count steps up, that PUT-size effect resets.
2. **Even at 1 PB/day, the model produces low millions of ops/day, not
   thousands of ops/sec.** This reflects Quickwit's cost-efficiency
   design. For this reason, op-mix and tail latency under *concurrent*
   ingest, merge, and query traffic matter more than raw ops/sec
   ceilings. These factors are what actually separate storage backends
   at these tiers.
   A backend that handles 10,000 uniform PUTs/sec in a `warp` benchmark
   can still fall behind at 1 TB/day. This happens if its range-GET path
   serializes under concurrent small reads. It can also happen if bulk
   `DeleteObjects` calls have multi-second tail latency, which stalls GC
   and lets the merge backlog grow without limit.
