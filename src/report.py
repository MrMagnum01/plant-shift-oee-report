"""
Generates the shift report (OEE breakdown, downtime Pareto by reason, alarm
summary) from DuckDB, and publishes it atomically.

Coherent reads: this module assumes ingestion for the reported window has
finished (a "shift report" runs after the shift closes) - it does not claim
to give a consistent snapshot against a concurrently-writing ingester. All
of the report's queries run against one DuckDB connection opened read-only,
so nothing else can be writing through that connection while the report is
built.

Completeness: a machine/shift's STATE telemetry is expected to fully tile
the shift window (see schedule.py) - RUN+DOWN+IDLE seconds should sum to
exactly the planned production time. If it doesn't (missing/partial STATE
events - e.g. a machine that never reported, or an empty database), that
machine/shift is marked incomplete (`complete: False`) and its
availability/performance/quality/OEE are withheld (`None`) rather than
computed and reported as an ordinary (mathematically valid but misleading)
zero. `data["completeness"]["all_complete"]` is the whole-report rollup;
render_html() renders incomplete cells distinctly and banners the report.

Atomic publish (per file, not per report-set): report.html and report.json
are each written to a temp file in the destination directory and moved into
place with os.replace, which is an atomic rename on the same filesystem
(POSIX) - a reader of either published path alone always sees either the
previous complete file or the new complete one, never a partial write.

This does NOT make the (html, json) *pair* atomic as a set: if the process
fails between the two renames (e.g. the second os.replace raises), the
directory can be left with the new html and the old json, or vice versa.
A reader that needs a verified matching pair must cross-check the
`generated_at` timestamp embedded in both files (present in the HTML body
and as data["generated_at"] in the JSON) and treat a mismatch as "no
consistent pair available yet" - this module does not implement a
report-set transaction (generation directory + atomic pointer switch).
"""
from __future__ import annotations

import html as html_lib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from schedule import IDEAL_CYCLE_S, MACHINES, SHIFTS

REPORT_ROLE_LINE = (
    "Synthetic portfolio demonstration, implemented with AI coding agents and "
    "independently reviewed by a separate AI reviewer. No client data or client work."
)

OEE_METHOD_NOTE = (
    "OEE here is computed on synthetic, planted data using the documented "
    "formulas below. It demonstrates the pipeline's mechanics, not a real "
    "production line's performance. No benchmark or industry-comparison claim is made."
)

COMPLETENESS_TOLERANCE_SECONDS = 1e-6

FORMULAS = {
    "availability": "Run Time / Planned Production Time (DOWN and IDLE both count as downtime)",
    "performance": "(Good Count + Reject Count) * Ideal Cycle Time / Run Time",
    "quality": "Good Count / (Good Count + Reject Count)",
    "oee": "Availability * Performance * Quality",
}


def _esc(v) -> str:
    """Escape any value for embedding as HTML text content. Used for every
    table cell whose value can trace back to ingested (MQTT-sourced) data -
    reason codes, alarm codes - and, defensively, for the fixed-vocabulary
    fields (machine, shift, ingest category) alongside them."""
    return html_lib.escape(str(v), quote=True)


def build_report_data(db_path: str) -> dict:
    con = duckdb.connect(db_path, read_only=True)
    try:
        con.execute("BEGIN TRANSACTION")
        machines_out = {}
        incomplete: list[dict] = []
        for machine in MACHINES:
            shifts_out = {}
            for shift_name, s_start_tz, s_end_tz in SHIFTS:
                # DuckDB TIMESTAMP is naive (UTC by construction throughout
                # this pipeline); strip tzinfo so comparisons against rows
                # read back from the DB are well-defined.
                s_start, s_end = s_start_tz.replace(tzinfo=None), s_end_tz.replace(tzinfo=None)
                planned = (s_end - s_start).total_seconds()
                run_s = _state_seconds(con, machine, shift_name, s_start, s_end, "RUN")
                down_s = _state_seconds(con, machine, shift_name, s_start, s_end, "DOWN")
                idle_s = _state_seconds(con, machine, shift_name, s_start, s_end, "IDLE")
                covered = run_s + down_s + idle_s
                complete = abs(covered - planned) < COMPLETENESS_TOLERANCE_SECONDS
                completeness_reason = None
                if not complete:
                    completeness_reason = (
                        f"STATE telemetry covers {covered:.1f}s of {planned:.1f}s planned "
                        f"production time for {machine}/{shift_name} "
                        f"(gap {planned - covered:.1f}s) - machine reported no/partial "
                        "state telemetry for this window."
                    )
                    incomplete.append({
                        "machine": machine,
                        "shift": shift_name,
                        "reason": completeness_reason,
                        "coverage_seconds": covered,
                        "gap_seconds": planned - covered,
                    })
                g, r = _counts(con, machine, s_start, s_end)
                total = g + r
                pareto = _downtime_pareto(con, machine, s_start, s_end)
                if complete:
                    availability = run_s / planned if planned else 0.0
                    performance = (total * IDEAL_CYCLE_S[machine]) / run_s if run_s > 0 else 0.0
                    quality = (g / total) if total > 0 else 0.0
                    oee = availability * performance * quality
                else:
                    # Withhold derived metrics rather than report a
                    # numerically "valid" zero/partial OEE computed from
                    # incomplete telemetry.
                    availability = performance = quality = oee = None
                shifts_out[shift_name] = {
                    "planned_production_seconds": planned,
                    "run_seconds": run_s,
                    "down_seconds": down_s,
                    "idle_seconds": idle_s,
                    "good_count": g,
                    "reject_count": r,
                    "total_count": total,
                    "availability": availability,
                    "performance": performance,
                    "quality": quality,
                    "oee": oee,
                    "downtime_pareto": pareto,
                    "complete": complete,
                    "completeness_reason": completeness_reason,
                }
            machines_out[machine] = shifts_out

        alarms = _alarm_summary(con)
        ingest_totals = dict(
            con.execute("SELECT category, count(*) FROM ingest_log GROUP BY 1").fetchall()
        )
        con.execute("COMMIT")
    finally:
        con.close()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "role": REPORT_ROLE_LINE,
        "method_note": OEE_METHOD_NOTE,
        "formulas": FORMULAS,
        "machines": machines_out,
        "alarms": alarms,
        "ingest_totals": ingest_totals,
        "completeness": {
            "all_complete": len(incomplete) == 0,
            "incomplete": incomplete,
        },
    }


def _state_seconds(con, machine, shift_name, s_start, s_end, state) -> float:
    # Each STATE event marks the *start* of a segment that runs until the
    # next STATE event for that machine (or the shift end). Segments here
    # are shift-aligned by construction (see schedule.py), so we compute
    # duration as MIN(next_ts, shift_end) - ts, clipped to the shift window.
    rows = con.execute(
        """
        WITH ordered AS (
            SELECT ts, state,
                   lead(ts) OVER (PARTITION BY machine ORDER BY ts) AS next_ts
            FROM state_events
            WHERE machine = ?
        )
        SELECT ts, state, COALESCE(next_ts, ?) AS seg_end
        FROM ordered
        WHERE ts < ? AND COALESCE(next_ts, ?) > ?
        """,
        [machine, s_end, s_end, s_end, s_start],
    ).fetchall()
    total = 0.0
    for ts, st, seg_end in rows:
        if st != state:
            continue
        clipped_start = max(ts, s_start)
        clipped_end = min(seg_end, s_end)
        if clipped_end > clipped_start:
            total += (clipped_end - clipped_start).total_seconds()
    return total


def _counts(con, machine, s_start, s_end) -> tuple[int, int]:
    row = con.execute(
        "SELECT COALESCE(SUM(good_delta),0), COALESCE(SUM(reject_delta),0) FROM count_ticks "
        "WHERE machine = ? AND ts >= ? AND ts < ?",
        [machine, s_start, s_end],
    ).fetchone()
    return int(row[0]), int(row[1])


def _downtime_pareto(con, machine, s_start, s_end) -> dict:
    # lead(ts) must be computed over ALL of the machine's state events (the
    # very next segment start, whatever its state), not just DOWN rows -
    # filtering to DOWN before windowing would make a DOWN segment's "end"
    # jump all the way to the *next* DOWN segment, skipping any RUN/IDLE in
    # between.
    rows = con.execute(
        """
        WITH ordered AS (
            SELECT ts, state, reason_code,
                   lead(ts) OVER (PARTITION BY machine ORDER BY ts) AS next_ts
            FROM state_events
            WHERE machine = ?
        )
        SELECT reason_code, ts, COALESCE(next_ts, ?) AS seg_end
        FROM ordered
        WHERE state = 'DOWN' AND ts < ? AND COALESCE(next_ts, ?) > ?
        """,
        [machine, s_end, s_end, s_end, s_start],
    ).fetchall()
    pareto: dict[str, float] = {}
    for reason, ts, seg_end in rows:
        clipped_start = max(ts, s_start)
        clipped_end = min(seg_end, s_end)
        if clipped_end > clipped_start:
            pareto[reason] = pareto.get(reason, 0.0) + (clipped_end - clipped_start).total_seconds()
    return pareto


def _alarm_summary(con) -> list[dict]:
    # Safe by construction: the ingester (ingester.py) enforces strictly
    # alternating RAISE/CLEAR phases per (machine, alarm_code) and rejects
    # (alarm_out_of_order_rejected) anything else before it reaches
    # alarm_events, so "the next event for this (machine, code)" is always
    # that RAISE's actual CLEAR, never a fabricated one.
    rows = con.execute(
        """
        WITH paired AS (
            SELECT machine, alarm_code, ts AS raised,
                   lead(ts) OVER (PARTITION BY machine, alarm_code ORDER BY ts) AS maybe_clear,
                   phase
            FROM alarm_events
        )
        SELECT machine, alarm_code, raised, maybe_clear
        FROM paired
        WHERE phase = 'RAISE'
        """
    ).fetchall()
    summary: dict[tuple, dict] = {}
    for machine, code, raised, cleared in rows:
        key = (machine, code)
        d = summary.setdefault(key, {"count": 0, "duration_seconds": 0.0})
        d["count"] += 1
        if cleared is not None:
            d["duration_seconds"] += (cleared - raised).total_seconds()
    return [
        {"machine": m, "alarm_code": c, "count": v["count"], "duration_seconds": v["duration_seconds"]}
        for (m, c), v in sorted(summary.items())
    ]


def render_html(data: dict) -> str:
    rows = []
    for machine, shifts in data["machines"].items():
        for shift, v in shifts.items():
            if v["complete"]:
                avail = f"{v['availability']*100:.1f}%"
                perf = f"{v['performance']*100:.1f}%"
                qual = f"{v['quality']*100:.1f}%"
                oee = f"<b>{v['oee']*100:.1f}%</b>"
            else:
                avail = perf = qual = oee = '<span class="incomplete">incomplete</span>'
            rows.append(
                f"<tr><td>{_esc(machine)}</td><td>{_esc(shift)}</td>"
                f"<td>{avail}</td><td>{perf}</td>"
                f"<td>{qual}</td><td>{oee}</td>"
                f"<td>{_esc(v['good_count'])}</td><td>{_esc(v['reject_count'])}</td></tr>"
            )
    pareto_rows = []
    for machine, shifts in data["machines"].items():
        for shift, v in shifts.items():
            for reason, secs in sorted(v["downtime_pareto"].items(), key=lambda kv: -kv[1]):
                pareto_rows.append(
                    f"<tr><td>{_esc(machine)}</td><td>{_esc(shift)}</td>"
                    f"<td>{_esc(reason)}</td><td>{secs:.0f}s</td></tr>"
                )
    alarm_rows = [
        f"<tr><td>{_esc(a['machine'])}</td><td>{_esc(a['alarm_code'])}</td>"
        f"<td>{_esc(a['count'])}</td><td>{a['duration_seconds']:.0f}s</td></tr>"
        for a in data["alarms"]
    ]
    ingest_rows = [
        f"<tr><td>{_esc(k)}</td><td>{_esc(v)}</td></tr>" for k, v in sorted(data["ingest_totals"].items())
    ]

    completeness = data["completeness"]
    if completeness["all_complete"]:
        completeness_banner = ""
    else:
        items = "".join(
            f"<li>{_esc(i['machine'])} / {_esc(i['shift'])}: {_esc(i['reason'])}</li>"
            for i in completeness["incomplete"]
        )
        completeness_banner = (
            '<p class="incomplete-banner"><b>INCOMPLETE DATA:</b> the following '
            "machine/shift rows are missing STATE telemetry coverage; their "
            "availability/performance/quality/OEE are withheld, not reported as "
            f"zero:</p><ul>{items}</ul>"
        )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Line A - synthetic shift OEE report</title>
<style>
body {{ font-family: -apple-system, Arial, sans-serif; margin: 2rem; color: #1a1a1a; }}
table {{ border-collapse: collapse; margin-bottom: 2rem; }}
td, th {{ border: 1px solid #ccc; padding: 0.4rem 0.8rem; text-align: left; }}
h1, h2 {{ margin-top: 2rem; }}
.role {{ background: #fffbcc; padding: 0.5rem 1rem; border: 1px solid #e0d060; }}
.note {{ color: #555; font-size: 0.9em; }}
.incomplete {{ color: #a00; font-style: italic; }}
.incomplete-banner {{ background: #ffe0e0; padding: 0.5rem 1rem; border: 1px solid #c00; }}
</style></head>
<body>
<h1>Line A - synthetic shift OEE report</h1>
<p class="role"><b>Role:</b> {_esc(data['role'])}</p>
<p class="note">{_esc(data['method_note'])}</p>
<p class="note">Generated at {_esc(data['generated_at'])}.</p>
{completeness_banner}

<h2>OEE by machine and shift</h2>
<table><tr><th>Machine</th><th>Shift</th><th>Availability</th><th>Performance</th><th>Quality</th><th>OEE</th><th>Good</th><th>Reject</th></tr>
{''.join(rows)}
</table>

<h2>Downtime Pareto by reason</h2>
<table><tr><th>Machine</th><th>Shift</th><th>Reason</th><th>Duration</th></tr>
{''.join(pareto_rows)}
</table>

<h2>Alarm summary</h2>
<table><tr><th>Machine</th><th>Alarm code</th><th>Count</th><th>Total duration</th></tr>
{''.join(alarm_rows)}
</table>

<h2>Ingest message ledger (known-total reconciliation)</h2>
<table><tr><th>Category</th><th>Count</th></tr>
{''.join(ingest_rows)}
</table>

<h2>Formulas</h2>
<ul>
{''.join(f'<li><b>{_esc(k)}</b>: {_esc(v)}</li>' for k, v in data['formulas'].items())}
</ul>
</body></html>
"""


def publish_report(db_path: str, out_path: str) -> Path:
    data = build_report_data(db_path)
    html = render_html(data)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(out.parent), prefix=".report-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(html)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, out)  # atomic on the same filesystem, for this file alone
    except BaseException:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise
    # sidecar JSON with the same data, for machine consumption / tests.
    # NOT part of one report-set transaction with the HTML above (see the
    # module docstring): if this second write/replace fails, the directory
    # is left with the new HTML and the old JSON. A reader that needs a
    # verified matching pair should compare data["generated_at"] from the
    # JSON against the "Generated at ..." line in the HTML.
    json_out = out.with_suffix(".json")
    fd2, tmp2 = tempfile.mkstemp(dir=str(out.parent), prefix=".report-json-", suffix=".tmp")
    try:
        with os.fdopen(fd2, "w") as f:
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp2, json_out)
    except BaseException:
        if os.path.exists(tmp2):
            os.remove(tmp2)
        raise
    return out


def main() -> None:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()
    out = publish_report(args.db, args.out)
    print(f"published {out}")


if __name__ == "__main__":
    main()
