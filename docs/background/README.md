# Background notes

These notes explain how the tool was designed, and why. You do not need them to run the tool or to read a report.

BYOC is built on Quickwit, its open-source upstream project. Quickwit publishes its documentation and source code, so these notes cite Quickwit when they quote those sources. What they say about storage traffic applies to BYOC.

| Note | It explains |
|---|---|
| [How BYOC uses S3](01_s3_interaction_analysis.md) | Which S3 operations BYOC sends, and when. Every simulated request comes from here. |
| [Test design](02_test_methodology.md) | Why the tests run in layers, and why each layer runs only after the one before it passes. |
| [Daily volume sizing](03_throughput_tier_sizing.md) | How a daily log volume turns into S3 requests per second. |

The documents for using the tool are one level up. Start with [What this tool measures](../what_this_measures.md).

## Test the tool itself

These tests check the tool's own code. They need no storage system and no credentials. They run against `moto`, an in-memory S3 emulator.

```bash
pip install -r requirements-dev.txt
pytest tests/
```

Run them before you trust the tool against a real storage system, and again after any change to `src/`. They check that the tool measures and reports correctly. They cannot show how a real storage system behaves, because `moto` has no network delay.
