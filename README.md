# Datadog BYOC Logs S3 Storage Provider Validation Framework

This tool tests whether a storage system works well for [Datadog BYOC Logs](https://docs.datadoghq.com/byoc-logs/) (BYOC). BYOC is built on [Quickwit](https://github.com/quickwit-oss/quickwit), its open-source upstream project, so the same tests also apply to Quickwit.

BYOC keeps its data in object storage such as S3, the Simple Storage Service interface from Amazon Web Services (AWS). Many storage systems offer the same interface: SeaweedFS, NetApp StorageGRID, Ceph, MinIO, Scality, and others. This tool sends BYOC's real storage traffic to one of them. Then it reports whether BYOC would work well there, by comparing the results with a baseline from performance tests run on AWS S3.

This is a storage-only test. It does not test search results, data durability or cost.

## Quick start

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

export QW_S3_ENDPOINT=https://s3.example.com
export QW_S3_BUCKET=my-test-bucket
export QW_S3_ACCESS_KEY=...
export QW_S3_SECRET_KEY=...

python run_validation.py validate --tier 100GB --duration-min 1 --levels 1,8,16 --repeats 1
```

This short run proves the connection works. **It is not a real result.** The command prints where it wrote `report.html`. Open that file in a browser to see what a report looks like.

You do not need an AWS account. The tool compares your storage with published AWS S3 figures.

## Documents

Each document has one job. Read them in this order.

| Document                                              | Read it to                                                                    |
| ----------------------------------------------------- | ----------------------------------------------------------------------------- |
| [What this tool measures](docs/what_this_measures.md) | Understand the five main questions and the nine checks. You should start here. |
| [Run validations](docs/run_a_validation.md)           | Run the tool. Every command is covered in detail on this page.                |
| [Read the report](docs/read_the_report.md)            | Understand the result, and what to do next.                                   |
| [Measurement policy](docs/measurement_policy.md)      | See the exact rules and numbers. For reviewers.                               |

The [background notes](docs/background/) explain how the tool was designed, and how to test the tool itself. You do not need them to run it.
