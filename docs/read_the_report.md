# Read the report

Open `report.html`. This page walks through it from top to bottom. Generate the
sample report first if you want to follow along:

```bash
python examples/make_sample_report.py --out-dir reports/example
```

The Markdown and JavaScript Object Notation (JSON) files carry the same
verdicts. The JSON file is the one to parse in automation.

## Start with the four questions

The **Summary by question** section is the part to read first. Every criterion
belongs to exactly one question, and each question reports the worst status
among its own criteria.

| Question | What it covers |
|---|---|
| Can I trust this evidence? | The run finished, the files are unchanged, and the reference run is comparable. |
| Does it behave like S3? | The application programming interface (API) semantics Quickwit depends on, plus object visibility. |
| Can it keep up? | Sustained rate, error rate, throttling and concurrency scaling. |
| Is it fast enough? | Response times, against the allowed multiple of the Amazon Web Services (AWS) Simple Storage Service (S3) reference. |

A question answered PASS means every required criterion under it passed. Click
through to its table for the detail.

## The verdict, and what it can be today

The headline verdict is one of four values.

- **CERTIFIED** — every required criterion passed, with no configuration
  deviation.
- **CERTIFIED WITH DEVIATION** — every required criterion passed, but the
  endpoint needs a non-default `storage.s3.*` configuration. The report prints
  the exact block to ship.
- **NOT CERTIFIED** — a required criterion failed.
- **INCONCLUSIVE** — nothing failed, but the evidence is incomplete.

**Today the verdict cannot reach CERTIFIED.** Merge backlog is a required
criterion and this simulator has no measurement for it, so it is always NOT
RUN. The report says this in the box under the verdict. A good run therefore
ends at INCONCLUSIVE.

That does not make the report empty. A FAIL anywhere is a real failure, and the
per-criterion detail is what you act on. Read the verdict as "did anything
fail", not as "did it pass".

## Status values

| Status | Meaning |
|---|---|
| PASS | The criterion has enough evidence and met its threshold. |
| FAIL | The criterion was evaluated and missed its threshold. |
| NOT RUN | The measurement or the stage is absent. |
| INCONCLUSIVE | The evidence exists but is insufficient, invalid or not comparable. |

Missing evidence never counts as a pass. An optional criterion is labelled as
such and never changes the verdict.

## Headroom: one number per criterion

Every criterion reports **headroom**: the measurement divided by its own limit.

- `0.68×` means the measurement used 68% of what it was allowed. Good.
- `1.00×` means it sits exactly on the limit.
- `1.12×` means it exceeded the limit by 12%. This criterion fails.

The direction is always the same. For "at least" requirements, such as
sustained throughput, the ratio is inverted so that a number above 1.00× still
means a problem. You can scan the column without reading units.

## The operations matrix

Three criteria apply to every operation the workload issues: latency, error
rate and throttling. With nine operation types, that is up to 27 criteria.
Listing them as individual rows buries everything else, so the criteria tables
summarize them in one row and the **Performance** section carries the detail:

| Operation | Samples | p99 ms | Limit ms | Headroom | Latency | Errors | Throttling |
|---|---|---|---|---|---|---|---|
| Split upload | 120 | 66.83 | 99.00 | 0.68× | PASS | PASS | PASS |
| Simulated query completion | 1800 | 462.00 | 412.50 | 1.12× | FAIL | PASS | PASS |

Read down the three status columns to find the failing operation, then read its
headroom to see how far off it is. The operation names map to Quickwit's work
like this:

| Operation | Where it comes from |
|---|---|
| Split upload, Multipart upload | The indexer writing a split at each commit. |
| Merge object read | The merger reading whole splits. |
| Split footer read, Term / field read, Document read | The searcher's byte-range reads. |
| Simulated query completion | One query's whole read fan-out, measured end to end. |
| Bulk deletion, Individual deletions | The janitor removing merged and expired splits. |

`p99` is the latency below which 99% of recorded outcomes fall. Limits are a
multiple of the AWS reference, not absolute numbers, so a slow network affects
both sides equally.

## Compatibility and the configuration to ship

This section holds a table of flavor against check. The probe stops at the
first flavor that passes everything, so later flavors show as NOT RUN. That is
expected, not a gap.

Below the table is the `storage.s3.*` block to ship. Copy it verbatim. When the
recommended flavor is one this framework adds rather than one Quickwit knows,
the report says so, because Quickwit will not accept that name in its
configuration.

## Performance over time

Two charts show successful ingestion and query rate per 60-second window,
against the target. Use them to tell a steady shortfall from a stall. A
sustained-rate criterion fails when any complete window drops below its
threshold, so one stall is enough to fail it.

The concurrency sweeps plot efficiency against concurrency. Efficiency is
typical request latency divided by the wall-clock time of the whole batch. A
flat line near 1.0 means the backend truly runs requests concurrently. A line
that falls as concurrency rises means it serializes them, which is what breaks
Quickwit's query fan-out.

## Consistency

Three probes check read-after-write, list-after-write and delete visibility.
Each one does an immediate follow-up operation. It does not poll until the
object appears, so the result describes immediate visibility only.

## Evidence and scope

The last section lists the interpretation limits, links the original
measurement files, and holds the full run manifest with the thresholds that
were in force. The download links need the `report-evidence/` directory next to
the HTML file.

## What to do next

| You see | Do this |
|---|---|
| A FAIL in "Is it fast enough?" | Open the matrix, find the operation, check its headroom and sample count. Then look at concurrency and contention on the backend. |
| A FAIL in "Can it keep up?" | Check the timeline charts for stalls, and the error breakdown for codes. Confirm the runner itself was not the bottleneck. |
| INCONCLUSIVE on latency criteria | Usually a missing or mismatched reference run. Re-run with `--with-aws-baseline`. |
| INCONCLUSIVE from sample counts | Run longer. Each operation needs at least 100 samples on both sides. |
| A compatibility FAIL | Read `compat.md`. It names the failing check under each flavor. |
