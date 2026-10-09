# Datadog BYOC/Quickwit S3 Storage Provider Validation Framework

This tool tests whether a storage system works well for Quickwit.

Quickwit keeps its data in S3, the Simple Storage Service interface from Amazon
Web Services (AWS). Many storage systems offer the same interface: SeaweedFS,
NetApp StorageGRID, Ceph, MinIO, Scality, and others. This tool sends
Quickwit's real storage traffic to one of them. Then it reports whether
Quickwit would work well there.

It tests storage only. It does not test search results, durability or cost.

## Try it in one minute

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export QW_S3_ENDPOINT=https://s3.example.com
export QW_S3_BUCKET=my-test-bucket
export QW_S3_ACCESS_KEY=...
export QW_S3_SECRET_KEY=...

python run_certification.py certify --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1
```

This short run proves the connection works. It is not a real result. The
command prints where it wrote `report.html`. Open that file in a browser.

You do not need an AWS account. The tool compares your storage with published
AWS S3 figures.

## Documents

Each document has one job. Read them in this order.

| Document | Read it to |
|---|---|
| [What this tool measures](docs/what_this_measures.md) | Understand the five questions and the ten checks. One page. Start here. |
| [Run a validation](docs/run_a_validation.md) | Run the tool. Every command is on this page, and only there. |
| [Read the report](docs/read_the_report.md) | Understand the result, and what to do next. |
| [Measurement policy](docs/measurement_policy.md) | See the exact rules and numbers. For reviewers. |

The [background notes](docs/background/) explain how the tool was designed.
You do not need them to run it.

## Test the tool itself

These tests check the tool's own code. They need no storage system and no
credentials.

```bash
pip install -r requirements-dev.txt
pytest tests/
```
