"""Portable, escaped HTML with inline SVG, plus equivalent Markdown and JSON."""

from __future__ import annotations

import base64
import hashlib
import html
import json
import shutil
from collections import Counter
from pathlib import Path

from .run_store import write_json


def esc(value):
    return html.escape(str(value if value is not None else "Not recorded"), quote=True)


def fmt(value, digits=2):
    return f"{value:,.{digits}f}" if isinstance(value, (int, float)) else "—"


def badge(status):
    style = {
        "PASS": "pass",
        "FAIL": "fail",
        "NOT RUN": "missing",
        "INCONCLUSIVE": "uncertain",
    }.get(status, "uncertain")
    return f'<span class="badge {style}">{esc(status)}</span>'


def table(headers, rows):
    return (
        '<div class="table-wrap"><table><thead><tr>'
        + "".join(f'<th scope="col">{esc(h)}</th>' for h in headers)
        + "</tr></thead><tbody>"
        + "".join(
            "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows
        )
        + "</tbody></table></div>"
    )


def ratio_cell(value):
    """Headroom: how close the measurement sits to its own limit."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "—"
    return f"{value:.2f}×"


# Per-operation latency, error and throttling criteria. They are shown as one
# matrix in the performance section, not as three rows each in the criteria
# tables.
PER_OPERATION = ("_p99", "_errors", "_throttles")


def per_operation(c):
    return c["id"].endswith(PER_OPERATION)


def pretty(value):
    return "<pre>" + esc(json.dumps(value, indent=2, allow_nan=False)) + "</pre>"


def line_chart(title, xvalues, series, xlabel, ylabel):
    """Inline SVG with an explicit scale and a corresponding data table."""
    if not xvalues:
        return '<p class="empty">No samples available.</p>'
    colors = ["#087f8c", "#d98716", "#a54059"]
    values = [v for _, data in series for v in data if v is not None]
    high = max(values or [1]) * 1.12 or 1
    xmax = max(xvalues) or 1
    x = lambda v: 60 + 620 * v / xmax
    y = lambda v: 220 - 170 * v / high
    parts = [
        f'<svg viewBox="0 0 720 290" role="img" aria-label="{esc(title)}">',
        f"<title>{esc(title)}</title>",
    ]
    for step in range(5):
        v = high * step / 4
        parts.append(
            f'<line x1="60" x2="680" y1="{y(v):.2f}" y2="{y(v):.2f}" stroke="#dfe7e8"/>'
        )
        parts.append(
            f'<text x="50" y="{y(v) + 4:.2f}" text-anchor="end">{v:.2g}</text>'
        )
    for i in sorted({0, len(xvalues) // 2, len(xvalues) - 1}):
        v = xvalues[i]
        parts.append(f'<text x="{x(v):.2f}" y="242" text-anchor="middle">{v:g}</text>')
    for index, (label, data) in enumerate(series):
        color = colors[index % len(colors)]
        points = " ".join(
            f"{x(xv):.2f},{y(v):.2f}" for xv, v in zip(xvalues, data) if v is not None
        )
        parts.append(
            f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2.5"/>'
        )
        for xv, v in zip(xvalues, data):
            if v is not None:
                parts.append(
                    f'<circle cx="{x(xv):.2f}" cy="{y(v):.2f}" r="3" fill="{color}"><title>{esc(label)}: {v:.3g} at {xv:g}</title></circle>'
                )
        parts.append(
            f'<text x="{60 + index * 205}" y="280" fill="{color}">{esc(label)}</text>'
        )
    parts.extend(
        [
            f'<text x="370" y="258" text-anchor="middle">{esc(xlabel)}</text>',
            f'<text x="60" y="20">{esc(ylabel)}</text>',
            "</svg>",
        ]
    )
    return "".join(parts)


CSS = """
:root{--ink:#193239;--muted:#536b72;--line:#dce6e7;--teal:#087f8c}
*{box-sizing:border-box}body{margin:0;background:#f2f6f5;color:var(--ink);font:15px/1.55 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
a{color:#096b7b;text-underline-offset:3px}main{max-width:1220px;margin:auto;padding:28px}
.hero{background:#143e45;color:white;padding:36px;border-radius:18px}.eyebrow{font-size:12px;letter-spacing:2px;text-transform:uppercase;color:#a8dcdb;font-weight:650}
h1{font-size:34px;line-height:1.15;margin:12px 0}h2{font-size:23px;margin:0 0 8px}h3{font-size:17px;margin:22px 0 8px}p{margin:8px 0 14px}.hero p{color:#d0e7e8;max-width:850px}.verdict{font-size:20px;font-weight:750;margin:22px 0 8px}
nav{display:flex;gap:18px;flex-wrap:wrap;padding:18px 0;font-weight:600;font-size:13px}
section{background:white;border:1px solid var(--line);border-radius:14px;padding:28px;margin:0 0 20px;scroll-margin-top:15px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:22px}.stat{background:#ffffff12;border:1px solid #ffffff25;border-radius:10px;padding:12px 16px}.stat strong{display:block;font-size:25px}.stat span{font-size:12px;color:#d0e7e8}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}.meta{display:grid;grid-template-columns:160px 1fr;gap:8px 18px;margin:16px 0}.meta dt{color:var(--muted)}.meta dd{margin:0;overflow-wrap:anywhere}.muted,.empty{color:var(--muted)}.callout{background:#fff8e8;border-left:4px solid #d98716;padding:13px 16px;margin:16px 0}.sample{background:#e8f1ff;color:#244b78;border-radius:8px;padding:12px;margin-bottom:15px}
.badge{display:inline-block;white-space:nowrap;font-size:11px;font-weight:750;letter-spacing:.3px;padding:4px 8px;border-radius:5px}.pass{background:#e3f3ec;color:#18664b}.fail{background:#fbe8e8;color:#a02a35}.missing{background:#edf1f4;color:#516471}.uncertain{background:#fff1d8;color:#885b13}
.table-wrap{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}th{text-align:left;background:#f3f7f7;color:#476068;font-size:11px;text-transform:uppercase;letter-spacing:.5px}th,td{padding:12px 10px;border-bottom:1px solid var(--line);vertical-align:top}td{overflow-wrap:anywhere}td:first-child{font-weight:600}.criteria td:first-child{min-width:190px}.criteria td:nth-child(2){min-width:170px}.criteria td:nth-child(3){min-width:190px}
.questions{display:grid;grid-template-columns:repeat(2,1fr);gap:14px;margin:18px 0}.question{border:1px solid var(--line);border-radius:12px;padding:16px}.question h3{margin:0 0 6px}.question p{margin:6px 0 0;font-size:13px}
summary{cursor:pointer;color:#116e7b;font-weight:600}details{margin-top:10px}details p{font-weight:400}.tools{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin:18px 0}input,select{font:inherit;padding:8px 10px;border:1px solid #a8bcc0;border-radius:6px;max-width:100%}input{min-width:240px}pre{background:#f3f7f7;padding:16px;border-radius:8px;overflow:auto;font-size:12px;line-height:1.6;white-space:pre-wrap;overflow-wrap:anywhere}svg{width:100%;height:auto}svg text{font:12px system-ui,sans-serif}code{font-size:12px}.bars{display:grid;grid-template-columns:180px 1fr;align-items:center;gap:10px;font-size:13px;margin:20px 0}.bar-track{height:27px;background:#edf3f3;position:relative}.bar{height:100%;background:#087f8c;min-width:2px}.bar.fail-bar{background:#b54b59}.bar-limit{position:absolute;border-left:2px dashed #d98716;top:-4px;bottom:-4px}.bar-label{position:absolute;right:6px;top:3px;font-size:11px;background:#ffffffdc;padding:0 3px}footer{color:var(--muted);font-size:12px;padding:0 4px 25px}ul{padding-left:22px}li{margin:6px 0}
@media(max-width:760px){main{padding:12px}.questions{grid-template-columns:1fr}.hero,section{padding:20px}.stats{grid-template-columns:1fr 1fr}.grid{grid-template-columns:1fr}.meta{grid-template-columns:120px 1fr}h1{font-size:28px}.bars{grid-template-columns:115px 1fr}}
@media print{body{background:white;font-size:10pt}main{max-width:none;padding:0}.hero{background:#edf5f5;color:#193239}.hero p,.hero .eyebrow,.stat span{color:#34555c}section{padding:14px;border-radius:0;break-inside:auto}nav,.tools{display:none}details>*{display:block!important}summary{display:none}tr,svg,.callout{break-inside:avoid}.grid{display:block}a{color:inherit}.table-wrap{overflow:visible}pre{white-space:pre-wrap}}
"""
SCRIPT = """
const search=document.getElementById('search');
const filter=document.getElementById('status-filter');
function update(){for(const row of document.querySelectorAll('.criteria tbody tr')){
row.hidden=!(row.textContent.toLowerCase().includes(search.value.toLowerCase()) &&
(filter.value==='all'||(filter.value==='attention'?row.dataset.status!=='PASS':row.dataset.status===filter.value)));}}
search.addEventListener('input',update);filter.addEventListener('change',update);
let closed=[];window.addEventListener('beforeprint',()=>{closed=[...document.querySelectorAll('details:not([open])')];closed.forEach(d=>d.open=true);document.querySelectorAll('.criteria tr').forEach(r=>r.hidden=false)});
window.addEventListener('afterprint',()=>{closed.forEach(d=>d.open=false);update()});
"""


def render_html(report, links=None):
    links = links or {}
    run = report["run"]
    load = run["stages"].get("load", {})
    counts = Counter(c["status"] for c in report["checks"] if c["required"])
    script_hash = base64.b64encode(hashlib.sha256(SCRIPT.encode()).digest()).decode()
    parts = [
        '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">',
        f"<meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'unsafe-inline'; script-src 'sha256-{script_hash}'; base-uri 'none'\">",
        "<title>Quickwit S3 validation report</title><style>"
        + CSS
        + "</style></head><body><main>",
    ]
    if report.get("example"):
        parts.append(
            '<div class="sample"><strong>Illustrative report.</strong> Synthetic data demonstrates the layout; these are not vendor measurements.</div>'
        )
    explanation = {
        "INCONCLUSIVE": "The evidence is incomplete. Review the missing or inconclusive criteria below before making a certification decision.",
        "NOT CERTIFIED": "One or more required checks failed. The detailed results explain the measured gaps and what to investigate.",
        "CERTIFIED": "Every required criterion passed with the default configuration.",
        "CERTIFIED WITH DEVIATION": "Every required criterion passed with the configuration recorded below.",
    }[report["verdict"]]
    parts.append(
        f'<header class="hero"><div class="eyebrow">Quickwit · Storage validation</div><h1>S3 compatibility &amp; performance</h1><p>{esc(run["identity"]["endpoint"])} · {esc(report["tier"])} tier · actual flavor: <strong>{esc(report["flavor"])}</strong></p><div class="verdict">{esc(report["verdict"])}</div><p>{explanation}</p><div class="stats">'
    )
    for state in ("PASS", "FAIL", "INCONCLUSIVE", "NOT RUN"):
        parts.append(
            f'<div class="stat"><strong>{counts[state]}</strong><span>REQUIRED CHECKS · {state}</span></div>'
        )
    parts.append(
        '</div></header><nav aria-label="Report sections">'
        + "".join(
            f'<a href="#{key}">{label}</a>'
            for key, label in (
                ("summary", "Summary"),
                ("run", "Run details"),
                ("results", "Results"),
                ("performance", "Performance"),
                ("compat", "Compatibility"),
                ("consistency", "Consistency"),
                ("evidence", "Evidence"),
            )
        )
        + "</nav>"
    )
    limits = report.get("headline_limits", [])
    if limits:
        parts.append('<div class="callout"><strong>What this report can conclude</strong>')
        for item in limits:
            parts.append(
                f'<p><strong>{esc(item["title"])}.</strong> {esc(item["detail"])}</p>'
            )
        parts.append("</div>")
    attention = [c for c in report["checks"] if c["required"] and c["status"] != "PASS"]
    if attention:
        # Put failures first, then provenance and missing evidence. Every item links to the full criterion.
        ordered = sorted(attention, key=lambda c: c["status"] != "FAIL")
        parts.append(
            "<section><h2>What needs attention</h2><ul>"
            + "".join(
                f'<li><a href="#check-{esc(c["id"])}">{esc(c["title"])}</a> — {esc(c["observed"])}</li>'
                for c in ordered[:5]
            )
            + "</ul>"
        )
        if len(attention) > 5:
            parts.append(
                f'<p class="muted">{len(attention) - 5} more criteria need attention. All are listed in the results table.</p>'
            )
        parts.append("</section>")
    groups = report.get("groups", [])
    if groups:
        parts.append(
            '<section id="summary"><h2>Summary by question</h2><p>Each question'
            " rolls up its own criteria. A question answers with the worst status"
            ' among them.</p><div class="questions">'
        )
        for group in groups:
            counts = ", ".join(
                f"{count} {state.lower()}" for state, count in group["counts"].items()
            )
            parts.append(
                f'<div class="question"><h3>{esc(group["question"])}</h3>'
                + badge(group["status"])
                + f'<p>{esc(group["summary"])}</p>'
                + f'<p class="muted">{esc(counts) if counts else "No criteria"}</p>'
                + f'<p><a href="#group-{esc(group["id"])}">See the criteria</a></p></div>'
            )
        parts.append("</div></section>")
    metadata = [
        ("Run ID", run["run_id"]),
        ("Started", load.get("started_at", run["created_at"])),
        ("Finished", load.get("finished_at")),
        ("Execution status", load.get("status", "Load not run")),
        (
            "Requested duration",
            f"{load.get('options', {}).get('duration_min', '—')} minutes",
        ),
        ("Actual stage duration", fmt(load.get("actual_duration_s")) + " seconds"),
        ("Measurement window", fmt(load.get("measurement_duration_s")) + " seconds"),
        ("Report generated", report["generated_at"]),
        ("Bucket", run["identity"]["bucket"]),
        ("Requested region", run["identity"]["region"]),
        ("Actual flavor", report["flavor"]),
        ("Recommended flavor", report["recommended_flavor"]),
        ("Runner location", load.get("options", {}).get("runner_location")),
    ]
    parts.append(
        '<section id="run"><h2>Run details</h2><p class="muted">Times include their UTC offset. Setup, measured workload and report generation are recorded separately.</p><dl class="meta">'
        + "".join(f"<dt>{esc(k)}</dt><dd>{esc(v)}</dd>" for k, v in metadata)
        + "</dl>"
    )
    parts.append(
        '<details><summary>Effective settings, workload and runner</summary><div class="grid"><div><h3>Settings used</h3>'
        + pretty(load.get("effective_config", {}))
        + "<h3>Workload targets</h3>"
        + pretty(load.get("op_mix", {}))
        + "</div><div><h3>Runner and software versions</h3>"
        + pretty(load.get("environment", {}))
        + "</div></div></details></section>"
    )
    parts.append(
        '<section id="results"><h2>Results and explanations</h2><p>PASS means the measured criterion met its requirement. FAIL means it did not. NOT RUN means evidence is absent; INCONCLUSIVE means evidence is insufficient or not comparable. Headroom is the measurement divided by its own limit, so 1.00× or less passes.</p><div class="tools"><label>Search <input id="search" type="search" placeholder="Find an operation or criterion"></label><label>Show <select id="status-filter"><option value="all">All results</option><option value="attention">Needs attention</option><option>PASS</option><option>FAIL</option><option>INCONCLUSIVE</option><option>NOT RUN</option></select></label></div>'
    )
    headers = ["Criterion", "Observed", "Requirement", "Headroom", "Result"]
    for group in report.get("groups", []) or [
        {"id": "all", "question": "All criteria", "status": report["verdict"]}
    ]:
        members = [
            c
            for c in report["checks"]
            if c.get("group", "all") == group["id"] or group["id"] == "all"
        ]
        rows = []
        for c in members:
            if per_operation(c):
                continue
            detail = (
                f"<details><summary>Why this matters</summary><p>{esc(c['explanation'])}</p>"
                + (
                    f"<p><strong>Next step:</strong> {esc(c['action'])}</p>"
                    if c["action"]
                    else ""
                )
                + "</details>"
            )
            rows.append(
                f'<tr id="check-{esc(c["id"])}" data-status="{esc(c["status"])}"><td>{esc(c["title"])}'
                + (" <small>(optional)</small>" if not c["required"] else "")
                + f"</td><td>{esc(c['observed'])}{detail}</td><td>{esc(c['requirement'])}</td>"
                + f"<td>{ratio_cell(c.get('ratio'))}</td><td>{badge(c['status'])}</td></tr>"
            )
        per_op = [c for c in members if per_operation(c)]
        if per_op:
            worst = next(
                (
                    state
                    for state in ("FAIL", "INCONCLUSIVE", "NOT RUN", "PASS")
                    if any(c["status"] == state for c in per_op)
                ),
                "PASS",
            )
            failing = sorted(
                {c["title"].rsplit(" ", 1)[0] for c in per_op if c["status"] != "PASS"}
            )
            rows.append(
                f'<tr data-status="{esc(worst)}"><td>Per-operation results</td>'
                + f"<td>{len(per_op)} criteria across {len(report['measurements'])} operations"
                + (
                    "<details><summary>Which ones need attention</summary><p>"
                    + esc(", ".join(failing))
                    + "</p></details>"
                    if failing
                    else ""
                )
                + '</td><td>See the operations matrix under <a href="#performance">Performance</a></td>'
                + f"<td>—</td><td>{badge(worst)}</td></tr>"
            )
        parts.append(
            f'<h3 id="group-{esc(group["id"])}">{esc(group["question"])} {badge(group["status"])}</h3>'
            + (
                '<div class="table-wrap"><table class="criteria"><thead><tr>'
                + "".join(f'<th scope="col">{esc(h)}</th>' for h in headers)
                + "</tr></thead><tbody>"
                + "".join(rows)
                + "</tbody></table></div>"
                if rows
                else '<p class="empty">No criteria in this group.</p>'
            )
        )
    parts.append(
        '</section><section id="performance"><h2>Performance</h2><p>p99 is the estimated latency below which 99% of recorded outcomes fall. Sparse samples and incomparable baselines remain inconclusive.</p><h3>Latency against the allowed limit</h3>'
    )
    measured = report["measurements"]
    if measured:
        parts.append(
            '<p class="muted">Bars show vendor p99 as a fraction of its allowed limit. Dashed markers indicate the limit; the table includes the AWS reference.</p><div class="bars">'
        )
        scale = max(
            [1.25]
            + [
                m["p99_latency_s"] / m["limit_p99_s"] * 1.12
                for m in measured
                if m["limit_p99_s"]
            ]
        )
        for m in measured:
            if m["limit_p99_s"]:
                ratio = m["p99_latency_s"] / m["limit_p99_s"]
                parts.append(
                    f'<span>{esc(m["name"])}</span><div class="bar-track"><div class="bar {"fail-bar" if ratio > 1 else ""}" style="width:{100 * ratio / scale:.2f}%"></div><div class="bar-limit" style="left:{100 / scale:.2f}%"></div><span class="bar-label">{ratio:.2f}× limit</span></div>'
                )
        parts.append("</div>")
        parts.append(
            "<h3>Every operation, all three criteria</h3>"
            + table(
                [
                    "Operation",
                    "Samples",
                    "p99 ms",
                    "Limit ms",
                    "Headroom",
                    "Latency",
                    "Errors",
                    "Throttling",
                ],
                [
                    [
                        esc(m["name"]),
                        str(m["count"]),
                        fmt(m["p99_latency_s"] * 1000),
                        fmt(
                            m["limit_p99_s"] * 1000
                            if m["limit_p99_s"] is not None
                            else None
                        ),
                        ratio_cell(m.get("latency_ratio")),
                        badge(m.get("latency_status", m["status"])),
                        badge(m.get("error_status", "NOT RUN")),
                        badge(m.get("throttle_status", "NOT RUN")),
                    ]
                    for m in measured
                ],
            )
        )
        parts.append(
            "<details><summary>Full latency distribution and rates</summary>"
            + table(
                [
                    "Operation",
                    "p50 ms",
                    "p90 ms",
                    "p99 ms",
                    "AWS p99 ms",
                    "Errors %",
                    "Throttles %",
                ],
                [
                    [
                        esc(m["name"]),
                        fmt(m["p50_latency_s"] * 1000),
                        fmt(m["p90_latency_s"] * 1000),
                        fmt(m["p99_latency_s"] * 1000),
                        fmt(
                            m["baseline_p99_s"] * 1000
                            if m["baseline_p99_s"] is not None
                            else None
                        ),
                        fmt(m["non_throttle_error_pct"], 3),
                        fmt(m["throttle_pct"], 3),
                    ]
                    for m in measured
                ],
            )
            + "</details>"
        )
    else:
        parts.append('<p class="empty">No operation measurements were recorded.</p>')
    windows = report["timeline"]
    parts.append(
        '<h3>Workload over time</h3><div class="grid"><div>'
        + line_chart(
            "Successful original ingestion over time",
            [w["offset_s"] for w in windows],
            [
                ("Original ingest", [w["ingest_mib_s"] for w in windows]),
                ("Target", [w["target_mib_s"] for w in windows]),
            ],
            "Seconds from workload start",
            "MiB/s",
        )
        + "</div><div>"
        + line_chart(
            "Successful simulated queries over time",
            [w["offset_s"] for w in windows],
            [
                ("Queries/s", [w["query_qps"] for w in windows]),
                ("Target", [w["target_qps"] for w in windows]),
            ],
            "Seconds from workload start",
            "Queries/s",
        )
        + "</div></div>"
    )
    parts.append(
        "<details><summary>Window measurements and errors</summary>"
        + table(
            [
                "Start s",
                "Duration s",
                "Complete",
                "Ingest MiB/s",
                "Target %",
                "Queries/s",
                "Request errors",
                "Requests",
            ],
            [
                [
                    fmt(w["offset_s"]),
                    fmt(w["duration_s"]),
                    "Yes" if w["complete"] else "No",
                    fmt(w["ingest_mib_s"]),
                    fmt(w["ingest_pct"]),
                    fmt(w["query_qps"]),
                    str(w["errors"]),
                    str(w["requests"]),
                ]
                for w in windows
            ],
        )
        + "</details>"
    )
    parts.append('<div class="grid">')
    for name, title in [
        ("fanout", "Read concurrency sweep"),
        ("put-fanout", "Write concurrency sweep"),
    ]:
        sweep = report["sweeps"][name]
        levels = sweep.get("levels", [])
        parts.append(
            f"<div><h3>{title} {badge(sweep['status'])}</h3>"
            + line_chart(
                title,
                [r["concurrency"] for r in levels],
                [
                    ("Efficiency", [r["efficiency"] for r in levels]),
                    ("Minimum", [sweep["efficiency_floor"]] * len(levels)),
                ],
                "Concurrent requests",
                "Efficiency",
            )
        )
        parts.append(
            "<details><summary>Sweep settings and measurements</summary>"
            + pretty(sweep.get("options", {}))
            + table(
                [
                    "Concurrency",
                    "Batch ms",
                    "p50 ms",
                    "p99 ms",
                    "Efficiency",
                    "Errors",
                    "Throttles",
                ],
                [
                    [
                        str(r["concurrency"]),
                        fmt(
                            r["wall_clock_s"] * 1000
                            if r.get("wall_clock_s") is not None
                            else None
                        ),
                        fmt(
                            r["p50_per_request_s"] * 1000
                            if r.get("p50_per_request_s") is not None
                            else None
                        ),
                        fmt(
                            r["p99_per_request_s"] * 1000
                            if r.get("p99_per_request_s") is not None
                            else None
                        ),
                        fmt(r["efficiency"]),
                        str(r["error_count"]),
                        str(r["throttle_count"]),
                    ]
                    for r in levels
                ],
            )
            + "</details></div>"
        )
    parts.append("</div>")
    if report["errors"]:
        parts.append(
            "<h3>Error breakdown</h3>"
            + table(
                ["Operation", "Error code", "Count"],
                [
                    [esc(e["operation"]), esc(e["code"]), str(e["count"])]
                    for e in report["errors"]
                ],
            )
        )
    if report.get("history"):
        h = report["history"]
        parts.append(
            "<h3>Previous run comparison</h3><p>"
            + esc(h["run_id"])
            + " · "
            + esc(h["verdict"])
            + "</p>"
        )
        parts.append(
            table(
                ["Operation", "p99 change"],
                [
                    [esc(v["name"]), fmt(v["p99_change_pct"]) + "%"]
                    for v in h["comparisons"]
                ],
            )
            if h["comparable"]
            else "<p>Runs differ in endpoint, tier, flavor or workload. No numerical comparison is shown.</p>"
        )
    parts.append(
        '</section><section id="compat"><h2>Compatibility and recommended configuration</h2>'
    )
    attempts = report["compatibility"].get("attempts", {})
    from .qw_s3_client import AUTO_PROBE_ORDER, flavor_note

    flavors = list(dict.fromkeys([*AUTO_PROBE_ORDER, *attempts]))
    names = list(
        dict.fromkeys(n for a in attempts.values() for n in a.get("results", {}))
    )
    rows = []
    for name in names:
        row = [esc(name.replace("_", " ").title())]
        for f in flavors:
            result = attempts.get(f, {}).get("results", {}).get(name)
            row.append(
                badge("PASS" if result["passed"] else "FAIL")
                if result
                else badge("NOT RUN")
            )
        rows.append(row)
    parts.append(
        table(["Check", *flavors], rows)
        if names
        else '<p class="empty">No compatibility check results.</p>'
    )
    parts.append(
        '<p class="muted">The probe stops at the first working flavor. Later flavors are not run. '
        "Flavors that hold the same settings are checked once, and the result is reused.</p>"
    )
    rec = report["recommended_flavor"]
    yaml_block = attempts.get(rec, {}).get("yaml")
    if yaml_block:
        note = flavor_note(rec)
        parts.append(
            f"<h3>Recommended Quickwit configuration · {esc(rec)}</h3>"
            + (f'<p class="muted">{esc(note)}</p>' if note else "")
            + f"<pre>{esc(yaml_block)}</pre>"
        )
    descriptions = {
        "path_style_addressing": "Checks whether object requests can reach the bucket with this flavor's addressing settings.",
        "multipart_upload": "Exercises multipart upload, or the single-upload fallback when that flavor disables multipart.",
        "multi_object_delete": "Checks deletion through the flavor's bulk-delete or individual-delete path.",
        "range_get_semantics": "Checks that bounded, open-ended and suffix reads return the expected byte ranges.",
        "checksum_algorithm": "Checks whether an upload using the selected checksum setting is accepted.",
    }
    parts.append(
        "<details><summary>What each compatibility test checks</summary>"
        + table(
            ["Test", "Purpose"],
            [
                [esc(name.replace("_", " ").title()), esc(description)]
                for name, description in descriptions.items()
            ],
        )
        + "</details>"
    )
    for flavor, attempt in attempts.items():
        parts.append(f"<details><summary>{esc(flavor)} — measured results</summary>")
        if attempt.get("error"):
            parts.append("<p>" + esc(attempt["error"]) + "</p>")
        if attempt.get("same_settings_as"):
            parts.append(
                "<p>Same settings as "
                + esc(attempt["same_settings_as"])
                + ". The checks ran once and this result is reused.</p>"
            )
        parts.append(
            table(
                ["Test", "Result", "Detail"],
                [
                    [
                        esc(name.replace("_", " ").title()),
                        badge("PASS" if result.get("passed") is True else "FAIL"),
                        esc(result.get("detail")),
                    ]
                    for name, result in attempt.get("results", {}).items()
                ],
            )
            + "</details>"
        )
    parts.append("</section>")
    parts.append(
        '<section id="consistency"><h2>Consistency</h2>'
        + table(
            ["Probe", "Samples", "Within deadline", "Worst elapsed s", "Result"],
            [
                [
                    esc(r["name"]),
                    str(r["count"]),
                    str(r["successes"]),
                    fmt(r["max_elapsed_s"]),
                    badge(r["status"]),
                ]
                for r in report["consistency"]
            ],
        )
        + "</section>"
    )
    parts.append(
        '<section id="evidence"><h2>Evidence and scope</h2><div class="callout"><strong>Interpretation limits</strong><ul>'
        + "".join("<li>" + esc(v) + "</li>" for v in report["limitations"])
        + "</ul></div>"
    )
    if links:
        parts.append(
            "<h3>Download evidence</h3><ul>"
            + "".join(
                f'<li><a href="{esc(url)}" download>{esc(label)}</a></li>'
                for label, url in links.items()
            )
            + "</ul>"
        )
    parts.append(
        "<details><summary>AWS reference metadata and comparability</summary>"
        + pretty(report["baseline"])
        + "</details><details><summary>External compliance evidence</summary>"
        + pretty(report["external_compliance"])
        + "</details><details><summary>Full run manifest and recorded thresholds</summary>"
        + pretty(run)
        + "</details></section>"
    )
    parts.append(
        "<footer>Generated "
        + esc(report["generated_at"])
        + " · report schema 1 · portable HTML · no external network resources</footer></main><script>"
        + SCRIPT
        + "</script></body></html>"
    )
    return "".join(parts)


def render_markdown(report):
    def cell(v):
        return esc(v).replace("|", "&#124;").replace("\n", "<br>")

    lines = [
        "# Quickwit S3 validation report",
        "",
        f"**{report['verdict']}** · Tier {report['tier']}",
        "",
        f"Run: {report['run']['run_id']} · Generated: {report['generated_at']}",
        "",
        f"Flavor used: {report['flavor']}; recommended: {report['recommended_flavor']}",
        "",
    ]
    for item in report.get("headline_limits", []):
        lines += [f"**{item['title']}.** {item['detail']}", ""]
    groups = report.get("groups", [])
    if groups:
        lines += ["## Summary by question", "", "| Question | Answer | Criteria |",
                   "|---|---|---|"]
        for group in groups:
            counts = ", ".join(
                f"{count} {state.lower()}" for state, count in group["counts"].items()
            )
            lines.append(
                f"| {cell(group['question'])} | {group['status']} | {cell(counts)} |"
            )
        lines.append("")
    lines += ["## Criteria", ""]
    for group in groups or [{"id": "all", "question": "All criteria"}]:
        members = [
            c
            for c in report["checks"]
            if c.get("group", "all") == group["id"] or group["id"] == "all"
        ]
        shown = [c for c in members if not per_operation(c)]
        lines += [
            f"### {group['question']}",
            "",
            "| Criterion | Observed | Requirement | Headroom | Result |",
            "|---|---|---|---|---|",
        ]
        for c in shown:
            lines.append(
                "| "
                + " | ".join(
                    [
                        cell(c["title"]),
                        cell(c["observed"]),
                        cell(c["requirement"]),
                        ratio_cell(c.get("ratio")),
                        c["status"],
                    ]
                )
                + " |"
            )
        if len(members) > len(shown):
            lines.append(
                f"| Per-operation results | {len(members) - len(shown)} criteria |"
                " See the operations matrix | — |"
                f" {next((s for s in ('FAIL', 'INCONCLUSIVE', 'NOT RUN', 'PASS') if any(c['status'] == s for c in members if per_operation(c))), 'PASS')} |"
            )
        lines.append("")
    if report["measurements"]:
        lines += [
            "## Operations matrix",
            "",
            "| Operation | Samples | p99 ms | Limit ms | Headroom | Latency | Errors | Throttling |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for m in report["measurements"]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        cell(m["name"]),
                        str(m["count"]),
                        fmt(m["p99_latency_s"] * 1000),
                        fmt(
                            m["limit_p99_s"] * 1000
                            if m["limit_p99_s"] is not None
                            else None
                        ),
                        ratio_cell(m.get("latency_ratio")),
                        m.get("latency_status", m["status"]),
                        m.get("error_status", "NOT RUN"),
                        m.get("throttle_status", "NOT RUN"),
                    ]
                )
                + " |"
            )
        lines.append("")
    lines += ["## Explanations", ""]
    for c in report["checks"]:
        if c["status"] == "PASS" and not c["action"]:
            continue
        lines.extend([f"### {c['title']}", "", c["explanation"], "", c["action"], ""])
    lines += [
        "## Scope",
        "",
        *["- " + v for v in report["limitations"]],
        "",
        "## Run metadata",
        "",
        "```json",
        json.dumps(report["run"], indent=2),
        "```",
        "",
    ]
    return "\n".join(lines)


def write_reports(report, out, run_dir):
    out, run_dir = Path(out), Path(run_dir)
    if out.suffix.lower() not in (".html", ".md", ".json"):
        raise ValueError(
            "--out must end in .html, .md or .json. All three formats are written."
        )
    outputs = [out.with_suffix(suffix) for suffix in (".html", ".md", ".json")]
    protected = {
        p.resolve()
        for p in run_dir.iterdir()
        if p.name == "manifest.json"
        or p.suffix == ".jsonl"
        or p.name in ("compat.json", "fanout.json", "put-fanout.json")
    }
    if any(p.resolve() in protected for p in outputs):
        raise ValueError(
            "Report output would overwrite original evidence. Choose report.html or another distinct name."
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    evidence = out.parent / (out.stem + "-evidence")
    evidence.mkdir(exist_ok=True)
    links = {
        "Evaluated results (JSON)": out.with_suffix(".json").name,
        "Text report (Markdown)": out.with_suffix(".md").name,
    }
    names = ["manifest.json"] + [
        n for s in report["run"]["stages"].values() for n in s.get("artifacts", {})
    ]
    from urllib.parse import quote

    for name in dict.fromkeys(names):
        if Path(name).name != name:
            raise ValueError("Invalid evidence filename.")
        source = run_dir / name
        if source.exists():
            shutil.copyfile(source, evidence / name)
            links[name] = quote(evidence.name + "/" + name)
    write_json(out.with_suffix(".json"), report)
    out.with_suffix(".md").write_text(render_markdown(report), encoding="utf-8")
    out.with_suffix(".html").write_text(render_html(report, links), encoding="utf-8")
    return outputs
