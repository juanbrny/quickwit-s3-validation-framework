# Background notes

These notes explain how the tool was designed, and why. You do not need them
to run the tool or to read a report.

| Note | It explains |
|---|---|
| [How Quickwit uses S3](01_s3_interaction_analysis.md) | Which S3 operations Quickwit sends, and when. Every simulated request comes from here. |
| [Test design](02_test_methodology.md) | Why the tests run in layers, and why each layer runs only after the one before it passes. |
| [Daily volume sizing](03_throughput_tier_sizing.md) | How a daily log volume turns into S3 requests per second. |

The documents for using the tool are one level up. Start with
[What this tool measures](../what_this_measures.md).
