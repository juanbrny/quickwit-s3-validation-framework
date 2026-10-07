# Reporting

This page was split into three, so the how-to is no longer mixed with the
measurement rules.

- **[Run a validation](run_a_validation.md)** — the commands, from the
  one-minute smoke run to a full run with an Amazon Web Services (AWS)
  reference.
- **[Read the report](read_the_report.md)** — what each section and status
  means, and what to do about a failure.
- **[Measurement policy](measurement_policy.md)** — the exact definition of
  every threshold, percentile and verdict rule, plus migration notes.

The deliverable is a self-contained HyperText Markup Language (HTML) report,
with the same verdicts in JavaScript Object Notation (JSON) and Markdown. The
HTML file carries its own styles, script and charts. It opens offline, supports
search and status filtering, and prints cleanly. Copy the adjacent
`<report>-evidence/` directory with it to keep the download links working.
