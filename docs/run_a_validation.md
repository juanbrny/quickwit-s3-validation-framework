# Run a validation

Every command for running the tool is on this page, and only here. To
understand the output, read [Read the report](read_the_report.md).

## What you need

- Python 3.9 or later.
- A bucket on the storage system you want to test. The run writes and deletes
  objects under the `qwcert/` prefix.
- An access key and a secret key that can read, write and delete in that
  bucket.

You do not need an Amazon Web Services (AWS) account. The tool compares your
storage with published AWS S3 figures. If you do have an AWS account, you can
measure AWS yourself. See [Compare with your own AWS run](#compare-with-your-own-aws-run).

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## Step 1: set the connection

Put these lines in a file, for example `vendor.env`:

```bash
export QW_S3_ENDPOINT=https://s3.example.com
export QW_S3_BUCKET=my-test-bucket
export QW_S3_ACCESS_KEY=...
export QW_S3_SECRET_KEY=...
export QW_S3_REGION=us-east-1
```

Then load it in your shell:

```bash
source vendor.env
```

### Where the keys come from

The tool looks for keys in this order, and uses the first it finds:

1. The `--access-key` and `--secret-key` flags.
2. `$QW_S3_ACCESS_KEY` and `$QW_S3_SECRET_KEY`.
3. `$AWS_ACCESS_KEY_ID` and `$AWS_SECRET_ACCESS_KEY`.

Step 3 matters. If your shell already has AWS keys from another tool, the
run can use them by mistake. Every run therefore prints where its keys came
from, before it sends any request:

```text
Endpoint under test: keys from $AWS_ACCESS_KEY_ID, access key AKIA…
```

If that line names the wrong source, set `QW_S3_ACCESS_KEY` and
`QW_S3_SECRET_KEY`. They win over the AWS variables.

Temporary keys start with `ASIA`. They only work together with a session
token. Set `$AWS_SESSION_TOKEN`, or pass `--session-token`.

## Step 2: a one-minute test

Run this first, every time. It proves the keys, the bucket and the address
all work, before you start a long test.

```bash
python run_certification.py certify --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1
```

This is not a real result. One minute collects too few samples. You only check
that every step finishes.

### Keep the machine awake

A long run stops being a test of the storage if the test machine sleeps. On a
Mac, the tool keeps the machine awake by itself while a run is going. Keep the
lid open, though: closing it can still put a MacBook to sleep. On other
systems, make sure the machine cannot sleep or suspend during the run.

If the machine does stop, the report says so at the top, and leaves that time
out of the results.

## Step 3: the real run

```bash
python run_certification.py certify --tier 1TB --duration-min 30
```

`certify` runs five steps, in this order:

1. **Compatibility.** Tries each known set of settings, and picks the first one
   that passes every check.
2. **Read concurrency.** Sends growing groups of reads at the same time.
3. **Write concurrency.** The same, for uploads.
4. **Workload.** Sends Quickwit's real storage traffic, at the daily volume you
   chose, for the minutes you chose.
5. **Report.** Writes `report.html`, `report.json` and `report.md`.
6. **Cleanup.** Deletes every object this run wrote to the bucket.

Each step uses the settings that step 1 picked. You do not copy anything by
hand.

### What cleanup deletes

Every object a run writes goes into the run's own folder in the bucket:
`qwcert/<run id>/`. Cleanup deletes only that folder. It never deletes the
bucket, and it never touches other data in it, so you can test in a bucket
that also holds other data.

Cleanup removes the objects, any unfinished multipart uploads, and old
versions if the bucket keeps versions. It runs after the report, and also when
the workload stopped early. It saves what it did in `cleanup.json`.

To keep the objects, for example to inspect them, add `--keep-objects`.

### Which settings are used

Step 1 tries known sets of settings, from the mildest to the strongest. It
stops at the first set that passes every check. If Quickwit's own defaults
pass, it picks those, and the report says **Quickwit defaults (no flavor
setting)**. That is the best result: Quickwit needs no special settings.

To test the settings you will actually deploy, choose them:

```bash
python run_certification.py certify --tier 1TB --duration-min 30 --flavor storagegrid
```

The tool still tests your choice first. If it fails a compatibility check, the
run stops, so it never measures settings that do not work.

### Use HTTPS if production uses HTTPS

Over plain `http://`, uploads send their checksum in a different way than
over `https://`. Some storage systems accept one way and reject the other.
StorageGRID's documentation lists the HTTPS way as unsupported. So a test over
HTTP can pass with settings that fail in production. Test over the same
protocol that production uses. If the certificate comes from a private
authority, add `--ca-bundle`.

### Choose the daily volume

| `--tier` | Daily volume |
|---|---|
| `100GB` | 100 GB per day |
| `1TB` | 1 TB per day |
| `10TB` | 10 TB per day |
| `100TB` | 100 TB per day |
| `1PB` | 1 PB per day |
| `10PB` | 10 PB per day. Also needs `--confirm-extreme-cost`, because it costs real money. |

### Useful options

| Option | Use it to |
|---|---|
| `--run-dir PATH` | Choose where the results go. By default, a new timestamped directory. |
| `--flavor NAME` | Test these settings instead of the ones step 1 picks. |
| `--keep-objects` | Leave the test objects in the bucket. |
| `--levels 1,8,16,32` | Test fewer concurrency levels. |
| `--ca-bundle PATH` | Trust a private certificate authority. On-premises storage usually needs this. |
| `--insecure-skip-tls-verify` | Skip the certificate check. The report records that you did. |
| `--strict` | Exit with status 1 when the result is not certified. Useful in automated pipelines. |

## Step 4: read the result

The command prints where it wrote the report. Open `report.html` in a browser.
It works offline.

See [Read the report](read_the_report.md).

## Compare with your own AWS run

By default, the tool compares response times with published AWS S3 figures. A
run that you measure yourself is stronger evidence, because it uses your
machine and your network.

Add the AWS details to your environment file:

```bash
export QW_AWS_BUCKET=my-aws-bucket
export QW_AWS_ACCESS_KEY=...
export QW_AWS_SECRET_KEY=...
```

Then add `--with-aws-baseline`:

```bash
python run_certification.py certify --tier 1TB --duration-min 30 --with-aws-baseline --runner-location office-laptop
```

The tool runs the same test against AWS S3, from the same machine, straight
after the first one. `--runner-location` is a free label for where your
machine is. Both runs must carry the same label, so give it every time.

## Run the steps one by one

Use this when you need to repeat one step, or run steps from different
machines. Give every step the same `--run-dir`.

```bash
python run_certification.py compat --run-dir reports/my-test
```

It prints the settings it picked, for example `Recommended flavor: storagegrid`.
Use that name in the next steps:

```bash
python run_certification.py read-concurrency --run-dir reports/my-test --flavor storagegrid
python run_certification.py write-concurrency --run-dir reports/my-test --flavor storagegrid
python run_certification.py load --run-dir reports/my-test --flavor storagegrid --tier 1TB --duration-min 30
python run_certification.py report --run-dir reports/my-test
```

Each step runs once per directory. To repeat a step, use a new directory.

When you have finished with a run, delete its objects:

```bash
python run_certification.py cleanup --run-dir reports/my-test
```

## Clean up runs from before this version

Earlier versions did not delete anything. Give `cleanup` every old run
directory at once. It reads the bucket and the address from each run, so you
only need the keys:

```bash
python run_certification.py cleanup --run-dir reports/run-a --run-dir reports/run-b --old-compat-objects
```

`--old-compat-objects` also removes the compatibility objects that older
versions left in a shared `compat/` folder. It removes only names the tool
generated, in the buckets those runs used.

## What lands in the run directory

```text
manifest.json          what ran, when, with which settings
compat.json, .md       the settings tried, and the ones picked
fanout.json            read concurrency measurements
put-fanout.json        write concurrency measurements
ingest_merge.jsonl     every write, merge and delete request
query.jsonl            every search read
consistency.jsonl      the object visibility checks
report.html, .json, .md   the report
cleanup.json           what cleanup deleted, and anything it could not
report-evidence/       a copy of the files above, for the report's download links
```

Keep `report-evidence/` next to `report.html`, or the download links break.

## When something goes wrong

| Message | What to do |
|---|---|
| `Missing connection settings` | Load your environment file, or pass the flags. The message names each missing variable. |
| `--access-key is empty` | The shell variable you passed is not set. Check it with `echo`. |
| `Could not connect to the endpoint, so no check ran` | Nothing was tested. Read the "Likely cause" line under the message. |
| `SignatureDoesNotMatch` | The secret key does not belong to the access key. Check the "keys from" line at the start of the run. |
| `InvalidToken` or `ExpiredToken` | Temporary keys need a valid session token. See [Where the keys come from](#where-the-keys-come-from). |
| `SSLError` or `CERTIFICATE_VERIFY_FAILED` | Pass `--ca-bundle /path/to/ca.pem`. |
| `No flavor passed every compatibility check` | The tool connected, but no known settings work. Open `compat.md` in the run directory to see which check failed. |
| `Too many open files`, or `This machine ran out of open files` | A limit of your machine, not of the storage. The tool raises it by itself where it can. If it cannot, run `ulimit -n 4096` and start again. |
| `The ... worker stopped: ...` | The workload stopped early. The message names the real cause. The measurements up to that point are kept, and `certify` still writes the report. |
| `The test machine stopped for N seconds` (in the report) | The machine slept or was suspended during the run. Run again, and keep it awake. |
| `Stage ... already exists` | Each step runs once per directory. Use a new `--run-dir`. |
| `This run is active or was interrupted` | A previous run stopped early. Start a new directory. |

## See a report without running anything

This writes an example report from made-up numbers. The page says that it is
an example.

```bash
python examples/make_sample_report.py --out-dir reports/example
```
