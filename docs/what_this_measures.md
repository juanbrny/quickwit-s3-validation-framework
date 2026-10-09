# What this tool measures

One page. Read it before the report.

## In one sentence

The tool copies how Quickwit uses storage. It runs that traffic against your
storage system. Then it tells you if Quickwit would work well on it.

## Words used on this page

| Word | Meaning |
|---|---|
| S3 | Simple Storage Service. The storage interface Quickwit uses. |
| AWS S3 | Amazon's own S3 service. Every other system is compared to it. |
| Endpoint | The web address of the storage system under test. |
| Check | One measurement with a pass or fail answer. |
| p99 | The response time that 99 out of 100 requests are faster than. |

## The five questions

Every question has one command, one measurement, and one pass rule.

### 1. Does the storage speak S3 the way Quickwit needs?

- **We run:** `compat`
- **We measure:** five things Quickwit needs. How it addresses buckets. Large
  uploads. Deleting many objects in one request. Reading part of an object.
  Upload checksums.
- **It passes when:** all five work, with at least one known set of settings.
- **You also get:** the exact settings to copy into a Quickwit config file.
  For a storage vendor, this is the most useful part of the report.

### 2. Does the storage handle many requests at the same time?

- **We run:** `read-concurrency` and `write-concurrency`
- **We measure:** how long a group of requests takes when we send them all at
  once.
- **It passes when:** the group finishes at least twice as fast as one-by-one
  handling. No request may fail.
- **Why it matters:** one Quickwit search sends dozens of reads at the same
  time. If your storage answers them one by one, search becomes slow.

### 3. Can the storage keep up?

- **We run:** `load`, with a daily volume you choose
- **We measure:** how many bytes we wrote and how many searches we served, in
  every 60 seconds.
- **It passes when:** every full 60 seconds reaches 95% of the target rate.
- **Why it matters:** if the storage falls behind, Quickwit stops keeping up
  with incoming logs.

### 4. Is the storage as fast as AWS S3?

- **We run:** `load`
- **We measure:** the p99 response time of each operation.
- **It passes when:** each p99 stays under its limit. The limit is a multiple
  of the AWS S3 time. Most limits are 2 times. Deleting many objects is 3
  times. A full search read is 2.5 times.
- **Where the AWS number comes from:** we publish it, so you do not need an
  AWS account. If you have one, you can measure AWS yourself. Your own
  measurement is then used instead of ours.

### 5. Is the storage correct and stable while busy?

- **We run:** `load`
- **We measure:** failed requests, slowed-down requests, and whether a new
  object appears at once after you write it, list it, or delete it.
- **It passes when:** fewer than 0.1% of requests fail. Slowed-down requests
  stay under 1% in the worst minute, and at zero in the middle minute. Every
  object appears when it should.

## The daily volumes

Choose one. The tool works out all the storage traffic from it.

`100 GB/day · 1 TB/day · 10 TB/day · 100 TB/day · 1 PB/day`

Most customers sit between 1 TB and 1 PB per day. A 10 PB/day volume also
exists. It needs an extra flag, because a test that large costs real money.

## What the result means

| Result | Meaning |
|---|---|
| CERTIFIED | Everything passed, with no setting changes. |
| CERTIFIED WITH DEVIATION | Everything passed, but Quickwit needs the settings the report prints. |
| NOT CERTIFIED | Something failed. |
| INCONCLUSIVE | Nothing failed, but something was not measured. |

A check we could not measure says NOT RUN. That is not a pass and not a
failure. Missing evidence never counts as a pass.

**Today the result can never be CERTIFIED.** One required check, merge
backlog, has no measurement yet. So a clean run always ends at INCONCLUSIVE.
Read the result as "did anything fail". Then read the five questions for the
answer that matters.

## When a result cannot be trusted

Before anything else, the report checks its own evidence:

- Every stage finished.
- Nobody changed the measurement files after the run.
- There are at least 100 samples for each operation.
- Every stage used the same settings.

If one of these fails, the affected result says INCONCLUSIVE. It does not
pass.

## What this tool does not measure

- Whether search returns the right documents. This tests storage only.
- Whether your data survives a failure.
- Anything above the volume and the request counts you actually ran.
- Cost.

## The ten checks that decide the result

Ten checks decide the result. Each one answers part of a question above.

| Check | Question |
|---|---|
| S3 compatibility | 1 |
| Read concurrency | 2 |
| Write concurrency | 2 |
| Keeps up with writes | 3 |
| Keeps up with searches | 3 |
| Merge backlog | 3 |
| Response time | 4 |
| Failed requests | 5 |
| Slowed-down requests | 5 |
| Objects appear at once | 5 |

Response time, failed requests and slowed-down requests each cover every
operation. One check fails if any single operation fails. The report still
shows every operation on its own row, so you can see which one.

Every other check in the report is **information only**. It is shown, and it
never changes the result. Two kinds belong here:

- Checks about the run itself. Did each stage finish? Are the files unchanged?
  Did every stage use the same settings? A problem here already makes the
  result INCONCLUSIVE.
- Results from other tools that you attach yourself, such as `s3-tests`.

## How to run it

See [Run a validation](run_a_validation.md). Every command is on that page.
