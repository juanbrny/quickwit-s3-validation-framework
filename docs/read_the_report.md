# Read the report

Open `report.html` in a browser. This page explains it from top to bottom.

To see a report without running anything, see the last section of [Run a validation](run_a_validation.md#see-a-report-without-running-anything).

The report also comes as `report.md` and `report.json`. They hold the same results. Use the JSON file in automated pipelines.

## 1. The result

The top of the report shows one result. This is a synthetic test of the storage that Datadog BYOC Logs (BYOC) would use, not a formal certification.

| Result | Meaning |
|---|---|
| PASS | All nine deciding checks passed. |
| FAIL | At least one deciding check failed. |
| INCONCLUSIVE | Nothing failed, but at least one deciding check was not measured, or not measured enough. |

The line under the result says whether BYOC can use its default storage settings, or needs the settings the report prints.

## 2. What this report can conclude

A box under the result lists what the report cannot tell you. For example:

- that the test machine stopped for a while, for example because it went to sleep — the report then leaves that time out, so the storage is not blamed for it;
- that only some of the steps ran;
- that response times were compared with published AWS figures, not with your own AWS run;
- that the certificate was not checked.

Read this box before you trust any number below it.

## 3. What needs attention

A short list of the checks that did not pass, with failures first. Each item links to its row further down.

## 4. The five questions

Six boxes. Five are the questions from [What this tool measures](what_this_measures.md). The sixth says whether the run itself can be trusted.

Each box shows the worst result among its checks. If a box says PASS, every check under it passed.

## 5. All checks

One table for each question. Each row is one check:

| Column | Meaning |
|---|---|
| Check | The name of the check. |
| What we measured | The number we got. |
| Rule | What the number had to be. |
| Headroom | The number divided by its limit. See below. |
| Result | PASS, FAIL, NOT RUN or INCONCLUSIVE. |

Click **What this means** on any row. It explains the check, says what to do if it failed, and names where the number came from:

```text
From command load, evidence consistency.jsonl, setting consistency_probe_min_success_pct.
```

That line tells you which step measured it, which file holds the raw data, and which line in `config/tiers.yaml` set the limit.

A row marked **(information only)** is shown, but never changes the result.

### The four results

| Result | Meaning |
|---|---|
| PASS | We measured enough, and the number met the rule. |
| FAIL | We measured it, and the number broke the rule. |
| NOT RUN | We could not measure it. That step did not run. |
| INCONCLUSIVE | We measured something, but not enough to decide. |

Missing evidence never counts as a pass.

### Headroom

Headroom is one number per check. It is the measurement divided by its limit.

- `0.68×` means the measurement used 68% of what it was allowed.
- `1.00×` means it is exactly on the limit.
- `1.12×` means it is 12% over the limit. The check fails.

**Above 1.00× always means a problem.** This holds even for rules like "at least 95% of target". There the tool turns the ratio around, so a bigger number still means worse.

## 6. Performance

### The operations table

Three deciding checks cover every operation: response time, failed requests, and slowed-down requests. Each one fails if any single operation fails. This table shows every operation on its own row, so you can find which one:

| Operation | Samples | Median payload | p99 ms | Limit ms | Headroom | Latency | Errors | Throttling |
|---|---|---|---|---|---|---|---|---|
| Split upload | 120 | 6.752 MiB | 66.83 | 320.05 | 0.21× | PASS | PASS | PASS |
| Simulated query completion | 1800 | 0.000 MiB | 462.00 | 425.00 | 1.09× | FAIL | PASS | PASS |

Read across the three result columns to find the failing operation. Then read its headroom to see how far off it is.

`p99` is the response time that 99 out of 100 requests are faster than.

**Median payload** is there because the limit depends on it. A small read is judged on how fast the first byte arrives. A large read is judged on transfer speed.

What the operation names mean in BYOC:

| Operation | What BYOC uses it for |
|---|---|
| Split upload | The indexer saves new data. |
| Merge object read | The merger reads whole files to combine them. |
| Split footer read, Term / field read, Document read | A search reads small parts of files. |
| Simulated query completion | One whole search, with all its reads together. |
| Bulk deletion | Old files are removed. |

### Where the AWS numbers come from

The run details name a **latency reference**. It is one of two things:

- **A reference profile.** Published AWS S3 figures that ship with the tool. You need no AWS account. This is the default.
- **A measured AWS run.** You ran the same test against AWS S3 yourself. This is stronger evidence, because it used your machine and your network.

A published profile cannot know how far your machine is from your storage. If your machine is far away, your times include that distance, and the AWS figures do not. A measured AWS run removes that difference.

### Over time

Two charts show writes and searches for each minute, against the target. A straight line under the target means the storage is too slow overall. A sudden dip means it stalled.

### Concurrency

Two charts show **speedup** against the number of requests sent at once. Speedup says how much faster a group finished than one-by-one handling would.

- Speedup near 1: the storage handled the requests one after another.
- Speedup that grows with the group size: it handled them together.

The rule is a speedup of at least 2 at every level, with no failed requests.

Requests per second also appears. Where it stops growing, something is full. That can be your own machine, not the storage. A laptop on the internet fills up long before a data center does.

## 7. Compatibility and the settings to use

A table shows each known set of settings against each of the five compatibility checks. The tool stops at the first set that passes everything. Later sets show NOT RUN. That is normal.

**BYOC defaults (no flavor setting)** means BYOC's own settings worked, and you need no special configuration. That is the best result.

The report grades the settings the run actually used. If you chose settings with `--flavor`, the report grades those, and also names the mildest settings that would work.

If the top of the report says **This run used plain HTTP**, the compatibility result may not hold over HTTPS. Uploads send their checksum differently over the two. Test again over HTTPS if production uses HTTPS.

Below the table are the exact `storage.s3` settings to copy into the BYOC storage configuration. If the settings carry a name BYOC does not know, the report says so. Then copy the settings, not the name.

Some storage systems need the same settings. The report lists every name that shares them, so you see your own product named.

## 8. Consistency

Three checks: after a write, after a list, and after a delete, we look again straight away. BYOC assumes the change is already visible.

## 9. Evidence and scope

The last section starts with **Tested at a later stage**: what this tool cannot test, because it needs the real BYOC modules. Those items are tested later, in the BYOC performance tests, and never change this result. The section then explains how to read some of the numbers, links the raw measurement files, and shows every setting that was in force for the run.

## What to do next

| You see | Do this |
|---|---|
| FAIL in "Is the storage as fast as AWS S3?" | Open the operations table. Find the operation over its limit. Check its sample count. |
| FAIL in "Can the storage keep up?" | Look at the charts for a dip or a stall. Check that your own machine was not the limit. |
| INCONCLUSIVE response times | Too few samples to decide, or no AWS reference. The row says which. Run longer, or check the latency reference. |
| FAIL in concurrency | Compare speedup with requests per second. If requests per second stopped growing early, test from a machine closer to the storage. |
| FAIL in compatibility | Open `compat.md` in the run directory. It names the failing check under each set of settings. |
| "Only 1 of the 4 stages ran" | The other questions read NOT RUN. Run the missing steps, or use `validate`. |
