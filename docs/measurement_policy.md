# Measurement policy

This page is the normative reference. It states exactly how each number is computed and how each result is decided. Read [Run a validation](run_a_validation.md) to run the tool, and [Read the report](read_the_report.md) to interpret the output.

Every threshold here lives in `config/tiers.yaml` and is copied into each run's `manifest.json`. A report uses the thresholds saved with its own run, never the current contents of the configuration file.

Abbreviations used below: Datadog BYOC Logs (BYOC), Amazon Web Services (AWS), Simple Storage Service (S3), JavaScript Object Notation (JSON), 99th percentile (p99).

## Results

Per check:

- **PASS:** the check has adequate evidence and meets its threshold.
- **FAIL:** an evaluated check violates its threshold.
- **NOT RUN:** the required measurement or stage is absent.
- **INCONCLUSIVE:** evidence is insufficient, invalid, interrupted or incompatible.

This is a synthetic test, not a formal certification. Nine checks decide the overall result. They are listed in [What this tool measures](what_this_measures.md#the-nine-checks-that-decide-the-result).

- If any deciding check fails, the result is FAIL.
- Otherwise, if any deciding check is not PASS, the result is INCONCLUSIVE.
- If all nine pass, the result is PASS.

The flavor does not change the result. With the `none` or `aws` flavor, BYOC can use its default storage settings. With any other flavor, BYOC needs the settings the report prints, and the report says so beside the result.

Every other check is information only. It is shown, and it never changes the result.

Three deciding checks summarize every operation: response time, failed requests, and slowed-down requests. Each takes the worst result of its operations. A single slow operation therefore fails the response time check. If an operation the workload should produce has no samples, a check that would otherwise pass is INCONCLUSIVE instead, so a missing operation never passes it. A failure always shows, whatever is missing.

Malformed input reports a validation error, never a passing result.

## Tested at a later stage

Some things need the real BYOC modules, so this tool does not test them. They are tested later, in the BYOC performance tests, and they never stop a run from passing here. The full list is in [What this tool measures](what_this_measures.md#tested-at-a-later-stage), and every report repeats it. Two of them affect how to read the numbers:

- **Merge backlog.** Merges run inside the upload workers here, not on BYOC's own merge scheduler. So this tool cannot show whether merges keep up over hours and days.
- **Full-size merges.** Merged files are capped at 160 MB to limit test cost. The cap sits just above BYOC's 128 MiB multipart threshold, so the workload does exercise multipart uploads. It does not move a real 8 GB merged file, where transfer speed decides everything.

## When the test machine stops

A run measures the storage only while the test machine is running. During a run the tool checks every second whether the wall clock and the process clock still agree. The wall clock keeps counting while the machine sleeps; the process clock does not. A difference of more than 5 seconds is a pause.

Each pause is saved with its start and length. The report then leaves out:

- every request that ran during the pause or the 60 seconds after it,
- every one-minute window that overlaps that time,
- every visibility check that ran during that time.

Runs recorded before pauses were tracked only show the total pause, not when it happened. For those runs, any time-based check that fails reads INCONCLUSIVE, because the failure cannot be told apart from the pause.

On a Mac, the tool runs `caffeinate` for as long as the run lasts, which stops idle sleep.

## When the test machine is the limit

The load engine starts each search on a fixed schedule. It records how late each search started, and how much processor time it used. The report names the test machine as a possible limit when either is true:

- more than 1% of searches started more than 100 ms late;
- the engine used more than 80% of the machine's cores on average.

A run of a tier above `100GB` with the Python engine gets the same note, because one Python process cannot send those workloads at full speed. The note changes no result. It tells the reader that response times and the search rate may describe the test machine.

## Checking the test machine before the run

The first step of `validate`, and the `check-machine` command, record what the test machine can use. The report compares the record with `test_machine` in `config/tiers.yaml`, for the tier of the workload:

| Measured | How | Rule |
|---|---|---|
| Cores | The processor affinity and the container's processor quota | At least the tier's `cores` |
| Memory | The machine's memory, or the container's memory limit when lower | At least 90% of the tier's `memory_gb`, because the system keeps part of it |
| Free disk | Free space in the run directory | At least the tier's `disk_gb` |
| Open files | The highest open-file limit the process can set | At least what the workload can need at its busiest |
| Network round trip | The fastest of 20 new TCP (Transmission Control Protocol) connections to the endpoint | At most `max_round_trip_ms`, 10 ms |

A TCP connection opens in one network round trip, and the storage software takes no part in it. So the fastest connection is the network's share of every request. A same-region EC2 (Amazon Elastic Compute Cloud) instance is about 1 ms from AWS S3, and the AWS figures come from there. At 10 ms, the network alone can use a third of the margin of a small read: its limit is 85 ms above the AWS figure, and a new HTTPS connection waits two or three round trips. `1PB` and `10PB` have no entry, because no single machine can send them. Network bandwidth is not measured.

The two checks are information only. A machine that misses a rule reads INCONCLUSIVE in "Can we trust this result?", and the report adds a note at the top. No other result changes.

The report never subtracts the round trip from a response time. The network adds more than one fixed delay: new connections, lost packets, and slower large transfers. A number with the network removed would not be a measurement. The report compares instead. For each operation over its limit, it takes the amount by which the operation missed the limit. When the median round trip is longer, the note says that the distance can explain that failure.

The `profile` command refuses a run whose round trip was over the limit. A reference profile describes AWS S3 from the same region.

## The workload

Both load engines follow these rules. The numbers come from `config/tiers.yaml`.

- **Indexers.** Each indexer uploads one split every `commit_timeout_s` (60 seconds). The indexers start spread evenly over that interval, as real indexers are not synchronized.
- **Merges.** When an indexer has uploaded `merge_factor` (10) splits, a merge starts in its own task, so it never delays the next upload. It reads each input in full, then uploads one merged split, capped at 160 MiB.
- **Publish and delete.** The merged split replaces its inputs for every new search, in one step. The inputs are deleted `gc_grace_period_s` (60 seconds) later, so searches that already chose them can finish. BYOC waits longer; one minute keeps the deletes inside a 30-minute run.
- **Searches.** Searches start on a fixed schedule at the tier's rate, whether or not earlier searches have finished, the way user queries arrive. All reads of one search go out at the same time. If more than 30 seconds of searches are still running, new searches are counted as skipped instead of piling up.
- **Client.** Each request is tried at most 3 times, in the S3 library's standard retry mode. A connection must open within 10 seconds, and a response must start within 60 seconds.

## Evidence policy

Stages are write-once. A lock prevents two commands from modifying one manifest at the same time. A crash may leave a `.running` file; start a fresh experiment rather than appending to interrupted measurements. Reports can still inspect partial bundles.

Checksums detect accidental change. They do not establish authenticity and do not detect deliberate tampering.

## Sampling

- `minimum_p99_samples`: 100 vendor and 100 reference observations per operation. With that many, the p99 decides the response time check. This is an evidence floor, not a statistical confidence guarantee.

Some operations are rare. At 100 GB/day a merge runs about every 10 minutes, so 100 merge samples would take about 17 hours. With fewer than 100 samples, only what the samples prove decides:

- **PASS** if every sample is within the limit. This is weaker evidence than 100 samples, so the report says "few samples: all N within the limit".
- **FAIL** if the median is over the limit. Then the p99 is over it too.
- **INCONCLUSIVE** otherwise, and always below 3 samples.
- `window_seconds`: 60 seconds, for sustained throughput and throttling.
- `minimum_complete_windows`: 3, before any sustained-rate check can pass.

## Latency

Percentiles use linear interpolation and include the latency of failed operations. The vendor and the AWS reference use the same definition.

Latency includes client retry time. The underlying client retries requests, so recorded error and throttle rates describe final outcomes and can hide intermediate retried failures. Every report prints this limit.

Full S3 object reads use the explicit `full_get_p99_multiplier_vs_aws` policy, 2× by default. Other latency multipliers and limits come from the rest of the pass/fail configuration.

Headroom is the measurement divided by its own limit. For an "at least" requirement the ratio is inverted, so a value above 1.00× always means the check failed.

## Throughput

Throughput counts successful original indexer writes only. It excludes merge rewrite bytes. The rate is measured in MiB/s and compared against the modelled indexed-byte ingestion rate, not against all S3 traffic and not against raw log volume.

Every complete window must reach 95% of its target. Idle windows count as zero throughput. A partial final window appears in the timeline but is not decided. Startup is included; mixed-workload measurements have no hidden warm-up exclusion.

Successful simulated query rate is evaluated separately, the same way.

## Throttling

Throttling uses each operation's non-empty complete windows. The worst window must be at or below the configured sustained maximum, and the median window must be zero. Empty operation windows are excluded, and coverage is checked as its own check.

Non-throttle errors exclude throttles, which are evaluated separately.

## Concurrency sweeps

Each sweep keeps the median-duration trial at every concurrency level. The report shows the actual levels, object sizes and repeat count. Both the read and the write sweep are evaluated.

The rule tests for serialization, set by `fanout_serialization_min_speedup` and `put_fanout_serialization_min_speedup`, both 2.0 by default:

```
speedup = concurrency x median request latency / batch wall clock
```

Every level above concurrency 1 must reach that speedup, and no level may record an error or a throttle. Level 1 is measured to establish the single-request latency, and has no speedup to judge.

An earlier rule compared median request latency to batch wall clock and required 0.4. That ratio cannot measure serialization. Batch wall clock is bounded below by the slowest request in the batch, while the numerator is the median, so the ratio falls as concurrency rises for every backend. Measured from one laptop, AWS S3 scored 0.09 at concurrency 256 with zero errors. The ratio is still reported, as a latency-spread measurement, and `fanout_efficiency_min` still describes it, but it never decides a result.

Batch throughput and its peak are also reported. A peak marks where something saturates, which may be the runner rather than the backend.

Missing or invalid measurements cannot pass, and neither can any non-throttle error. Conclusions apply only to the concurrency range actually tested.

## Compatibility

The probe tries flavors in order and stops at the first one where every check passes. Later flavors are NOT RUN. Flavors that hold identical settings are checked once, and the result is reused for the twin.

A flavor this framework adds, rather than one BYOC ships, cannot be selected by name in BYOC's configuration. The report says so and prints the explicit `storage.s3.*` block instead.

## Consistency

Deadlines are recomputed from the recorded `elapsed_s` using the policy saved with the run. Each probe performs one immediate follow-up operation. It does not poll repeatedly until an object becomes visible.

The read-after-write probe checks the returned length, not full payload equality. Do not read it as a corruption test.

## The latency reference

Latency checks compare against AWS S3. There are two ways to supply that comparison, and the report always names which one it used.

**Bundled reference profile (the default).** Most vendors have no AWS account. The framework therefore ships the bar it grades against, in `config/reference_profiles/`. Select one with `--reference <id>`, give a path to your own file, or pass `--reference none` to leave latency checks inconclusive.

A profile holds a few numbers, not a table of per-operation latencies. These three matter most:

| Field | Meaning |
|---|---|
| `latency.first_byte_p99_s` | The 99th percentile of one small request, measured in region. This is a tail figure, because the framework compares p99 to p99. |
| `throughput.per_stream_mb_s` | The rate one request transfers bytes at. AWS advises one concurrent request for each 85-90 MB/s wanted. |
| `latency.delete_p99_s` | The 99th percentile of one delete request, measured in region. |

The limit for one operation then follows its own recorded payload size:

```
parts      = ceil(bytes / multipart_part_gb)        # uploads above the threshold
throughput = min(parts, max_concurrent_parts) x per_stream_mb_s
reference  = first_byte_p99_s + bytes / throughput
limit      = reference x the operation's multiplier
```

Two numbers therefore cover every read and upload at every tier, including tiers nobody has measured. A small read costs one round trip. An 8 GB mature split read costs about 91 seconds, where the round trip is a tenth of a percent.

Queries are the exception. A query finishes when the slowest of its concurrent reads returns, and the 99th percentile of that maximum sits above the 99th percentile of one read. `latency.fanout_tail_factor` covers the difference. In a provisional profile it is an assumption. In a profile written by `run_validation.py profile` it is measured: the p99 of whole searches, divided by the p99 of small reads.

Deletes are the other exception. A delete moves no bytes, but S3 updates its index for every key, so it is slower than a small read. A bulk delete uses `latency.delete_p99_s` as its reference. A flavor without bulk delete sends one request per key, so its reference is `delete_p99_s` times the number of keys in one merge, `merge_factor`. Version 1 of the bundled profile graded deletes as small reads, and AWS S3 itself failed: its bulk deletes of 10 keys took about 300 ms, against a limit of 255 ms. A profile without `delete_p99_s` still grades deletes as small reads.

Uploads can be slower than downloads. A measured profile records `throughput.upload_per_stream_mb_s` for uploads. An older profile without it uses `per_stream_mb_s` for both. How to record a new profile is in [Run a validation](run_a_validation.md#refresh-the-aws-reference-profile).

A profile also carries its provenance: storage class, runner, date and the sources behind its numbers. The report prints all of it. A profile marked `status: provisional` holds published figures rather than a run recorded with this framework, and the report says so.

**Measured AWS run.** This is the stronger evidence, because it shares the runner, the network and the code version with the run under test. It always wins when supplied with `--baseline`.

## The measured AWS reference run

`--baseline` accepts a versioned AWS run directory. Supplying it switches off the bundled profile for that report. The evaluator checks the AWS hostname, completed load status, artifact checksums, a default flavor (`none` or `aws`), tier, duration, modelled workload, query profiles, source fingerprint, Python and dependency versions, CPU count, architecture, operating system, and an explicit runner network location.

Any difference keeps the relative latency checks inconclusive. The report shows the reference identity, dates and settings.

Matching metadata does not prove identical network conditions. Inspect the reference context when interpreting results. Running both legs with `validate --with-aws-baseline` satisfies every field except the hostname check by construction.

## Historical comparison

`--previous path/to/report.json` adds an informational p99 comparison against an earlier report. A change is shown only when the endpoint, bucket, region, tier, flavor, workload, configuration, requested duration and runner location all match. Positive percentages mean latency increased.

History never substitutes for the AWS reference and never changes the result.

## External compliance evidence

Run the relevant `s3-tests` or `mint` subset separately. Supply a JSON manifest and the matching raw result file:

```json
{
  "tool": "s3-tests",
  "version": "the exact tool commit or release",
  "executed_at": "2026-10-07T09:00:00Z",
  "endpoint": "https://s3.vendor.example.com",
  "bucket": "qw-cert",
  "selection": "multipart, range, delete_multi, list_objects",
  "passed": 120,
  "failed": 0,
  "skipped": 0,
  "evidence_file": "s3-tests-results.txt"
}
```

The evidence path is relative to that manifest. Add `--compliance compliance.json` to the report command. The endpoint and bucket must match the run, at least one check must pass, and any failure or skipped relevant test prevents a pass.

The report records the SHA-256 fingerprint of both the manifest and the raw file. This is an operator declaration with attached evidence. The report does not parse the suite output and does not re-execute it. Keep the original files with the report package.

Raw `warp` reports stay optional extra evidence from another tool. The report states explicitly when they are absent.

## Portability and credentials

Manifests use an allowlist of settings. Access keys and secret keys are never serialized. Endpoint URLs containing embedded credentials, query strings or fragments are rejected. When a workload worker stops, its error type and message are printed and saved under `worker_errors`, with this run's access key, secret key and session token removed from the message first.

The report HTML escapes all dynamic content, sets a restrictive content security policy, and uses no content delivery network, remote font or remote JavaScript. Raw evidence is intended for technical review. Avoid putting secrets into external tool output or free-text runner labels.

## Appendix: migrating from the legacy reports

Legacy endpoint and tier filenames do not carry enough provenance. They cannot establish a run identity, and they cannot reliably recover settings, dates and configuration. They are not imported into a validation automatically.

- Replace `report --tier ... --endpoint ...` with `report --run-dir ...`.
- Supply the baseline during report generation, not during `load`.
- The old compatibility `--baseline-out` format was never a performance baseline and is no longer used.
- The legacy Markdown renderer in `src/report.py` stays available for diagnosis. It cannot give a result for unversioned evidence.
