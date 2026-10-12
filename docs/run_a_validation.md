# Run a validation

Every command for running the tool is on this page, and only here. To understand the output, read [Read the report](read_the_report.md).

## What you need

- Docker or Podman, on Linux or macOS. The tool runs in a container, for `linux/amd64` and `linux/arm64`. You do not need Python or Rust. To run without a container, see [Run from source](#run-from-source).
- A test machine big enough for the daily volume you test. See [Choose a test machine](#choose-a-test-machine).
- A bucket on the storage system you want to test. The run writes and deletes objects under the `qwcert/` prefix.
- An access key and a secret key that can read, write and delete in that bucket. See [Permissions](#permissions) for the exact list.

You do not need an Amazon Web Services (AWS) account. The tool compares your storage with published AWS S3 figures. If you do have an AWS account, you can measure AWS yourself. See [Compare with your own AWS run](#compare-with-your-own-aws-run).

## Install

You need two things: the container image, and `byoc-s3`, a small script that runs it. The image is published as `ghcr.io/juanbrny/byoc-s3-validation`, for `linux/amd64` and `linux/arm64`. Docker picks the right one for your machine. These commands download the image, then write the script into a folder on your `PATH`:

```bash
mkdir -p ~/.local/bin
docker run --rm ghcr.io/juanbrny/byoc-s3-validation print-wrapper > ~/.local/bin/byoc-s3
chmod +x ~/.local/bin/byoc-s3
```

To update the tool, run `docker pull ghcr.io/juanbrny/byoc-s3-validation`. To run another image, for example one you built yourself, set `BYOC_S3_IMAGE`. To use Podman when both are installed, set `BYOC_S3_RUNTIME=podman`.

Every command on this page starts with `byoc-s3`. The script passes its arguments to the tool unchanged. It also sets up the container:

- **Results.** It mounts the current folder, so results go to `reports/` in the folder where you run the command. Run it in the same folder every time.
- **Keys.** It passes the connection settings of [Step 1](#step-1-set-the-connection) from your environment, by name. Their values never appear on a command line.
- **Certificates.** It mounts a `--ca-bundle` file, even outside the current folder.
- **Owner.** The container runs as your user, so you own the results.
- **Network.** On Linux, the container uses the host network. At high request rates, the container network would cost processor time.
- **Sleep.** On macOS, it keeps the Mac awake for as long as the run lasts.

Inside the container, `localhost` is the container itself. On Linux this does not matter, because of the host network. On macOS, use `host.docker.internal` to reach a storage system that runs on the Mac.

### Choose a test machine

The test machine must send the whole workload without becoming the limit itself. Otherwise the report measures the machine, not the storage. These are the smallest machines we recommend, for a 30-minute run:

| `--tier` | Network, sustained | Cores (vCPUs) | Memory | Free disk | Example on AWS |
|---|---|---|---|---|---|
| `100GB` | 1 Gbit/s | 2 | 4 GB | 1 GB | `c7i.large` |
| `1TB` | 2.5 Gbit/s | 4 | 8 GB | 1 GB | `c7i.xlarge` |
| `10TB` | 5 Gbit/s | 8 | 16 GB | 4 GB | `c7i.2xlarge` |
| `100TB` | 25 Gbit/s | 16 | 32 GB | 10 GB | `c6in.4xlarge` |

- **Network, sustained** is the speed the machine keeps for the whole run, not its short peak. Many cloud machines are sold as "up to" a speed, and keep only a fraction of it. Check the baseline figure. On premises, `100TB` needs a 25 Gbit/s network card and path to the storage: a 10 Gbit/s card would be 80% busy on average.
- **Memory and disk** are mostly for the results. A 30-minute `100TB` run records about 14 million requests, in about 2 GB of files. The report needs about 3.5 GB of memory to read them.
- **On macOS**, the container runs inside a virtual machine. Its cores and memory are the test machine, so give it at least the values above, in the settings of Docker Desktop or Rancher Desktop. Its network is also slower than the Mac's own. Use Linux for `10TB` and `100TB`.
- `1PB` and `10PB` need several test machines. Their write traffic alone is 36 Gbit/s on average. The tool does not run one workload from several machines yet.

How the table was sized:

| `--tier` | Average traffic | Requests per second | Main source |
|---|---|---|---|
| `100GB` | 12 Mbit/s | about 50 | searches |
| `1TB` | 0.1 Gbit/s | about 500 | searches |
| `10TB` | 0.9 Gbit/s | about 2,000 | searches |
| `100TB` | 8 Gbit/s | about 7,800 | uploads and merges: 2.2 uploads of 196 MB every second |

The engine itself is light. Against a local server that stores nothing, it sent the whole `100TB` workload for 13 minutes, merges and deletes included, with about 1 core busy and 350 MB of memory. No search started late. The recommendations leave room for encryption, the operating system's network work, and the report. A busy machine also delays searches, and the table keeps it well below that point.

The report checks the machine on every run. If searches started late, or the engine kept most cores busy, the report says so at the top. Then the results may describe the test machine, not the storage.

### Check the test machine

The machine must also be close to the storage. Every request waits for the network at least once, and a new HTTPS connection waits two or three times. The AWS figures come from a machine about 1 ms from AWS S3. Use a machine in the same data center or region as the storage: the network round trip must be 10 ms or less.

`validate` checks the machine first, in a few seconds. To check a machine before you start a run, use the same connection settings and the tier you plan to run:

```bash
byoc-s3 check-machine --tier 1TB --run-dir reports/machine-check
```

It prints what it found, and what the tier needs:

```text
Cores: 32 (the 1TB tier needs 4)
Memory: 124.4 GB (the 1TB tier needs 8 GB)
Free disk: 61.2 GB (the 1TB tier needs 1 GB)
Open files: 65,536 (the run needs about 1,496)
Network round trip to s3.us-east-1.amazonaws.com: 0.6 ms at best, 0.9 ms median (limit 10 ms)
```

- **Cores and memory** are the ones the container may use, not only the host's. On macOS, these are the values of the Docker Desktop or Rancher Desktop virtual machine.
- **The network round trip** is the time to open a new TCP (Transmission Control Protocol) connection to the storage. The storage software takes no part in it, so it shows the network's share of every request. The check opens 20 connections and keeps the fastest.
- **Network bandwidth** is not checked. Check it against the table above.

The check only warns. A run continues on any machine, and the report shows each warning at the top. A machine over 10 ms away also gets a note that says whether the distance can explain a failed response time.

### Permissions

The keys need these S3 permissions. Object permissions can be limited to the `qwcert/` folder, where every test object lives. On AWS, this is the IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:ListBucketVersions",
        "s3:GetBucketVersioning"
      ],
      "Resource": "arn:aws:s3:::your-test-bucket"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::your-test-bucket/qwcert/*"
    }
  ]
}
```

Without `s3:ListBucketMultipartUploads`, the test still runs, and cleanup still deletes every object. Cleanup only cannot check for unfinished multipart uploads, and says so. Add `s3:CreateBucket` only if the bucket does not exist yet. To remove objects from older versions of the tool with `--old-compat-objects`, also allow the object permissions on `your-test-bucket/compat/*`.

Most S3-compatible storage systems use the same permission names in their own access policies.

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

Step 3 matters. If your shell already has AWS keys from another tool, the run can use them by mistake. Every run therefore prints where its keys came from, before it sends any request:

```text
Endpoint under test: keys from $AWS_ACCESS_KEY_ID, access key AKIA…
```

If that line names the wrong source, set `QW_S3_ACCESS_KEY` and `QW_S3_SECRET_KEY`. They win over the AWS variables.

Temporary keys start with `ASIA`. They only work together with a session token. Set `$AWS_SESSION_TOKEN`, or pass `--session-token`.

## Step 2: a one-minute test

Run this first, every time. It proves the keys, the bucket and the address all work, before you start a long test.

```bash
byoc-s3 validate --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1
```

This is not a real result. One minute collects too few samples. You only check that every step finishes.

### Keep the machine awake

A long run stops being a test of the storage if the test machine sleeps. On a Mac, `byoc-s3` keeps the machine awake by itself while a run is going. Keep the lid open, though: closing it can still put a MacBook to sleep. On other systems, make sure the machine cannot sleep or suspend during the run.

If the machine does stop, the report says so at the top, and leaves that time out of the results.

## Step 3: the real run

```bash
byoc-s3 validate --tier 1TB --duration-min 30
```

`validate` runs seven steps, in this order:

1. **Test machine.** Measures this machine and its network round trip to the storage, and warns when either may limit the run. See [Check the test machine](#check-the-test-machine).
2. **Compatibility.** Tries each known set of settings, and picks the first one that passes every check.
3. **Read concurrency.** Sends growing groups of reads at the same time.
4. **Write concurrency.** The same, for uploads.
5. **Workload.** Sends the real storage traffic of Datadog BYOC Logs (BYOC), at the daily volume you chose, for the minutes you chose.
6. **Report.** Writes `report.html`, `report.json` and `report.md`.
7. **Cleanup.** Deletes every object this run wrote to the bucket.

Each step uses the settings that step 2 picked. You do not copy anything by hand.

### What cleanup deletes

Every object a run writes goes into the run's own folder in the bucket: `qwcert/<run id>/`. Cleanup deletes only that folder. It never deletes the bucket, and it never touches other data in it, so you can test in a bucket that also holds other data.

Cleanup removes the objects, any unfinished multipart uploads, and old versions if the bucket keeps versions. It runs after the report, and also when the workload stopped early. It saves what it did in `cleanup.json`.

To keep the objects, for example to inspect them, add `--keep-objects`.

### Which settings are used

Step 2 tries known sets of settings, from the mildest to the strongest. It stops at the first set that passes every check. If BYOC's own defaults pass, it picks those, and the report says **BYOC defaults (no flavor setting)**. That is the best result: BYOC needs no special settings.

To test the settings you will actually deploy, choose them:

```bash
byoc-s3 validate --tier 1TB --duration-min 30 --flavor storagegrid
```

The tool still tests your choice first. If it fails a compatibility check, the run stops, so it never measures settings that do not work.

### Use HTTPS if production uses HTTPS

Over plain `http://`, uploads send their checksum in a different way than over `https://`. Some storage systems accept one way and reject the other. StorageGRID's documentation lists the HTTPS way as unsupported. So a test over HTTP can pass with settings that fail in production. Test over the same protocol that production uses. If the certificate comes from a private authority, add `--ca-bundle`.

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
| `--flavor NAME` | Test these settings instead of the ones step 2 picks. |
| `--keep-objects` | Leave the test objects in the bucket. |
| `--levels 1,8,16,32` | Test fewer concurrency levels. |
| `--ca-bundle PATH` | Trust a private certificate authority. On-premises storage usually needs this. |
| `--insecure-skip-tls-verify` | Skip the certificate check. The report records that you did. Works only with `--engine python`. |
| `--engine rust` or `--engine python` | Choose the load engine. By default, the load engine when it is built, and Python for `100GB` otherwise. |
| `--strict` | Exit with status 1 when the result is not PASS. Useful in automated pipelines. |

## Step 4: read the result

The command prints where it wrote the report. Open `report.html` in a browser. It works offline.

See [Read the report](read_the_report.md).

## Compare with your own AWS run

By default, the tool compares response times with published AWS S3 figures. A run that you measure yourself is stronger evidence, because it uses your machine and your network.

Add the AWS details to your environment file:

```bash
export QW_AWS_BUCKET=my-aws-bucket
export QW_AWS_ACCESS_KEY=...
export QW_AWS_SECRET_KEY=...
```

Then add `--with-aws-baseline`:

```bash
byoc-s3 validate --tier 1TB --duration-min 30 --with-aws-baseline --runner-location office-laptop
```

The tool runs the same test against AWS S3, from the same machine, straight after the first one. `--runner-location` is a free label for where your machine is. Both runs must carry the same label, so give it every time.

## Refresh the AWS reference profile

Response times are graded against a reference profile of AWS S3, bundled with the tool. To replace its figures with your own measurement, record one run from inside AWS, then let the tool derive the profile from it.

1. **Start an EC2 instance in the same region as an S3 bucket**, for example `us-east-1`. Pick one with high network bandwidth, for example `c7i.2xlarge`. A small instance limits the transfer speed, and the profile would describe the instance instead of S3.

2. **On the instance, [install the tool](#install), then run the full validation against that bucket:**

```bash
export QW_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
export QW_S3_REGION=us-east-1
export QW_S3_BUCKET=your-aws-bucket
export QW_S3_ACCESS_KEY=...
export QW_S3_SECRET_KEY=...

byoc-s3 validate --flavor aws --tier 10TB --duration-min 90 --runner-location "ec2 c7i.2xlarge us-east-1" --run-dir reports/aws-profile-run
```

Use `10TB` for 90 minutes. The profile needs at least 100 downloads and 100 uploads of 16 MiB or more, and a `1TB` run of 30 minutes has far fewer. A 90-minute `10TB` run has more than 1,000 of each, and more than 100 merge deletes.


3. **Copy the run directory back to your own machine**, for example with `scp -r`. Keep it: it is the evidence behind the new figures.

4. **In the repository folder, write the profile from it:**

```bash
byoc-s3 profile --run-dir reports/aws-profile-run --out config/reference_profiles/aws-s3-ec2-same-region.yaml
```

The command prints each value before and after, and raises the profile's version. Review the change with `git diff`, then commit it.

It refuses a run that cannot give honest figures:

- the endpoint is not AWS S3;
- a step did not finish;
- the machine paused;
- there are too few samples: fewer than 100 small reads, searches, timed deletes, large downloads or large uploads. Runs recorded before the visibility checks timed their deletes have too few deletes;
- small reads were more than twice as slow during the workload as in the idle read test, so the test machine was overloaded;
- the run sent less than 90% of the searches per second its tier asks for.

It warns when small reads look too slow to come from inside the region.

It measures five values:

| Value | Measured as |
|---|---|
| `first_byte_p99_s` | The p99 of the small reads, 2 KiB and 8 KiB. |
| `fanout_tail_factor` | The p99 of whole searches, divided by the small-read p99. |
| `per_stream_mb_s` | The slow-case speed of whole-file downloads, one stream each: 99 of 100 downloads were faster. |
| `upload_per_stream_mb_s` | The slow-case speed of uploads, one stream each: 99 of 100 uploads were faster. |
| `delete_p99_s` | The p99 of the merges' bulk deletes, which the report grades. When the run has fewer than 100 of them, the visibility checks' single-key deletes, one every few seconds, fill the gap. |

Only transfers of at least 16 MiB count toward a speed. On a smaller transfer, the time after the first byte is too short to measure a speed from.

Every value describes the slow case, because the report compares the p99 of each operation with a reference p99. A run graded against a profile made from itself therefore passes. Check this before you commit a new profile:

```bash
byoc-s3 report --run-dir reports/aws-profile-run --reference config/reference_profiles/aws-s3-ec2-same-region.yaml
```

## Run the steps one by one

Use this when you need to repeat one step, or run steps from different machines. Give every step the same `--run-dir`.

```bash
byoc-s3 check-machine --run-dir reports/my-test --tier 1TB
byoc-s3 compat --run-dir reports/my-test
```

It prints the settings it picked, for example `Recommended flavor: storagegrid`. Use that name in the next steps:

```bash
byoc-s3 read-concurrency --run-dir reports/my-test --flavor storagegrid
byoc-s3 write-concurrency --run-dir reports/my-test --flavor storagegrid
byoc-s3 load --run-dir reports/my-test --flavor storagegrid --tier 1TB --duration-min 30
byoc-s3 report --run-dir reports/my-test
```

Each step runs once per directory. To repeat a step, use a new directory.

Older names still work, so existing scripts do not break: `run_certification.py` for `run_validation.py`, `certify` for `validate`, `fanout` for `read-concurrency`, and `put-fanout` for `write-concurrency`.

When you have finished with a run, delete its objects:

```bash
byoc-s3 cleanup --run-dir reports/my-test
```

## Clean up runs from before this version

Earlier versions did not delete anything. Give `cleanup` every old run directory at once. It reads the bucket and the address from each run, so you only need the keys:

```bash
byoc-s3 cleanup --run-dir reports/run-a --run-dir reports/run-b --old-compat-objects
```

`--old-compat-objects` also removes the compatibility objects that older versions left in a shared `compat/` folder. It removes only names the tool generated, in the buckets those runs used.

## What lands in the run directory

```text
manifest.json          what ran, when, with which settings, and the machine check
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
| `The 1TB tier needs the Rust load engine` | Only when you run from source. Build the engine. See [Run from source](#run-from-source). |
| `Name or service not known`, or no connection to `localhost` | On macOS, use `host.docker.internal` instead of `localhost`. If the storage system has no host name for each bucket, the compatibility step picks settings with path-style addresses. |
| `The test machine is N ms from the storage` (in the report) | The machine is far from the storage, so response times include network time. Run from a machine in the same data center or region. See [Check the test machine](#check-the-test-machine). |
| `The test machine is smaller than recommended` (in the report) | Use a bigger machine, or a smaller tier. See [Choose a test machine](#choose-a-test-machine). |
| `The test machine was near its limit` (in the report) | The machine could not keep to the workload's schedule. Use a bigger machine. See [Choose a test machine](#choose-a-test-machine). |
| `Too many open files`, or `This machine ran out of open files` | A limit of your machine, not of the storage. The tool raises it by itself where it can. If it cannot, run the `ulimit -n` command the message shows, and start again. The `100TB` tier needs about 16,500 open files. |
| `The ... worker stopped: ...` | The workload stopped early. The message names the real cause. The measurements up to that point are kept, and `validate` still writes the report. |
| `The test machine stopped for N seconds` (in the report) | The machine slept or was suspended during the run. Run again, and keep it awake. |
| `Stage ... already exists` | Each step runs once per directory. Use a new `--run-dir`. |
| `This run is active or was interrupted` | A previous run stopped early. Start a new directory. |

## See a report without running anything

This writes an example report from made-up numbers. The page says that it is an example.

```bash
byoc-s3 sample-report --out-dir reports/example
```

## Run from source

Use this to change the tool, or where you cannot run containers. You need Python 3.9 or later, and Rust from [rustup.rs](https://rustup.rs) for the load engine.

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cargo build --release --manifest-path loadgen/Cargo.toml
```

Then type `python run_validation.py` wherever this page says `byoc-s3`, for example:

```bash
python run_validation.py validate --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1
```

`byoc-s3 sample-report` becomes `python examples/make_sample_report.py`.

The load engine in `loadgen/` sends the workload. One Python process cannot: it runs on one core at a time, and it slows every request it sends. Build the engine again after you update the tool. If it is not built, only the `100GB` tier runs, with the slower Python engine. Every other tier stops with a message that tells you how to build it.

## Build the image

The image holds Python, its packages, the tool and the load engine. To build it for this machine only, under the published name, so that `byoc-s3` runs your build:

```bash
docker build -t ghcr.io/juanbrny/byoc-s3-validation .
```

To publish a new version for both `linux/amd64` and `linux/arm64`, log in to the GitHub container registry with a token that has the `write:packages` scope, then build with `buildx` and push. `SOURCE_COMMIT` goes into every run record, because the image has no git. Each version gets two tags: `latest`, which `byoc-s3` runs, and its commit, which never changes:

```bash
docker login ghcr.io -u <github-user>
docker buildx build --platform linux/amd64,linux/arm64 --build-arg SOURCE_COMMIT=$(git rev-parse HEAD) -t ghcr.io/juanbrny/byoc-s3-validation:latest -t ghcr.io/juanbrny/byoc-s3-validation:$(git rev-parse --short HEAD) --push .
```

Publish only from a commit that is pushed, so the commit in each run record can be found.

The load engine compiles on the build machine's own architecture for each target, so the build takes a few minutes for each platform.
