# Run a validation

This page is the how-to. To understand the output, read
[Read the report](read_the_report.md). For the exact rules behind each
threshold, read [Measurement policy](measurement_policy.md).

## What you need

- Python 3.9 or later.
- An empty bucket on the endpoint under test. The run writes and deletes
  objects under the `qwcert/` prefix.
- An access key and a secret key with read, write and delete rights on that
  bucket.
- Optional, but strongly recommended: an Amazon Web Services (AWS) Simple
  Storage Service (S3) bucket for the reference run. Latency criteria stay
  inconclusive without one.

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## Step 1: set the connection once

Every connection setting reads from an environment variable, so the commands
stay short. Put the exports in a file and source it.

```bash
# vendor.env
export QW_S3_ENDPOINT=https://s3.vendor.example.com
export QW_S3_BUCKET=qw-cert
export QW_S3_ACCESS_KEY=...
export QW_S3_SECRET_KEY=...
export QW_S3_REGION=us-east-1
```

```bash
source vendor.env
```

Command-line flags override the environment. `--access-key` and `--secret-key`
also fall back to `$AWS_ACCESS_KEY_ID` and `$AWS_SECRET_ACCESS_KEY`.

## Step 2: smoke run, about one minute

Run this first, every time. It proves the credentials, the bucket rights and
the endpoint work before you commit to a long soak.

```bash
python run_certification.py certify --tier 100GB --duration-min 1 \
  --levels 1,8,16 --repeats 1
```

The report it writes is not a certification. A one-minute run collects too few
samples. You are checking that the sequence completes.

## Step 3: the real run

```bash
python run_certification.py certify --tier 1TB --duration-min 30 \
  --runner-location ec2-us-east-1a-runner-01 \
  --with-aws-baseline \
  --aws-bucket qw-cert-baseline \
  --aws-access-key "$AWS_AK" --aws-secret-key "$AWS_SK"
```

`certify` runs five steps in order:

1. **Compatibility.** Tries each flavor and recommends the first that passes
   every check.
2. **Read concurrency.** Sweeps concurrent range reads against one object.
3. **Write concurrency.** The same sweep for uploads.
4. **Workload soak.** Generates the indexer, merger, searcher and janitor
   operation mix for the tier.
5. **Report.** Writes HTML, JSON and Markdown into the run directory.

With `--with-aws-baseline` it adds the same soak against AWS S3 before the
report. Run both legs from one command whenever you can. The report only
compares latency when the tier, duration, modelled workload, runner location
and host environment all match, and one command makes them match.

`--runner-location` is a free-text label for where the machine sits. Use the
same label for both legs. Without it, the baseline comparison stays
inconclusive.

Useful options:

| Option | Why |
|---|---|
| `--run-dir PATH` | Choose the output directory. A timestamped one is created by default. |
| `--levels 1,8,16,32` | Shorten the concurrency sweeps. |
| `--out PATH` | Write the report somewhere other than the run directory. |
| `--strict` | Exit with status 1 when the verdict is not certified. Useful in continuous integration. |

The 10PB tier needs `--confirm-extreme-cost`. A soak at that volume runs up a
real cloud bill.

## Step 4: read the result

`certify` prints the paths it wrote. Open `report.html` in a browser. It works
offline and needs no network access.

See [Read the report](read_the_report.md).

## Running the stages one by one

Use the separate commands when you need to control a step, repeat only part of
the work, or run stages from different machines. Pass the same `--run-dir` to
each one.

```bash
RUN=reports/vendor-2026-10-07

python run_certification.py compat --run-dir "$RUN"
# Note the recommended flavor it prints, then pass it to the other stages.

python run_certification.py read-concurrency  --run-dir "$RUN" --flavor minio
python run_certification.py write-concurrency --run-dir "$RUN" --flavor minio
python run_certification.py load --run-dir "$RUN" --flavor minio \
  --tier 1TB --duration-min 30 --runner-location ec2-us-east-1a-runner-01

python run_certification.py report --run-dir "$RUN" \
  --baseline reports/aws-1tb-2026-10-07 --out "$RUN/report.html"
```

`read-concurrency` and `write-concurrency` were called `fanout` and
`put-fanout`. Both old names still work.

Each stage runs once per directory. To repeat an experiment, use a new
directory. The endpoint, bucket, region and configuration must stay the same
across the stages of one directory.

The baseline is its own run directory holding a completed `load` stage against
AWS S3. It does not need the vendor gates.

```bash
python run_certification.py load --run-dir reports/aws-1tb-2026-10-07 \
  --endpoint https://s3.us-east-1.amazonaws.com --bucket qw-cert-baseline \
  --access-key "$AWS_AK" --secret-key "$AWS_SK" --flavor aws \
  --tier 1TB --duration-min 30 --runner-location ec2-us-east-1a-runner-01
```

## What lands in the run directory

```text
manifest.json          # schema version, identity, stage status, settings, hashes
compat.json/.md        # flavor attempts and the recommended Quickwit configuration
fanout.json            # concurrent read measurements
put-fanout.json        # concurrent write measurements
ingest_merge.jsonl     # every ingest, merge and garbage-collection request
query.jsonl            # every read, and each simulated query completion
consistency.jsonl      # visibility probes
report.html/.json/.md  # the report, written by the report step
report-evidence/       # a copy of the evidence, for the download links
```

Keep `report-evidence/` next to the HTML file to preserve its download links.
The HTML still opens correctly on its own.

## When something goes wrong

| Message | What to do |
|---|---|
| `Missing connection settings` | Source your environment file, or pass the flags. The message names the variable for each missing setting. |
| `No flavor passed every compatibility check` | Read `compat.md` in the run directory. It shows which check failed under which flavor. Fix the endpoint, or build a custom `storage.s3.*` configuration. |
| `Stage ... already exists` | Each stage runs once per directory. Use a new `--run-dir`. |
| `This run is active or was interrupted` | A `.running` lock file remains. Start a fresh directory rather than appending to interrupted measurements. |

An interrupted run keeps its partial evidence and records the stage as
INTERRUPTED. You can still build a report from it.

## Test the framework itself

No credentials and no cloud charges. It runs against `moto`, an in-memory S3
emulator.

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
python examples/make_sample_report.py --out-dir reports/example
```

The sample report shows the layout with synthetic numbers, and says so on the
page.
