"""
Generates the shift report (OEE breakdown, downtime Pareto by reason, alarm
summary) from DuckDB, and publishes it atomically.

Coherent reads: this module assumes ingestion for the reported window has
finished (a "shift report" runs after the shift closes) - it does not claim
to give a consistent snapshot against a concurrently-writing ingester. All
of the report's queries run against one DuckDB connection opened read-only,
so nothing else can be writing through that connection while the report is
built.

Completeness (observed-coverage rule, NOT state tiling or count magnitude):
  A machine/shift's telemetry "covers" the window only when there is actual
  evidence a message could have arrived throughout it. This is judged from
  the timestamps of every message this pipeline can receive for that
  machine - STATE, COUNT, ALARM, and the periodic HEARTBEAT (events.py,
  schedule.HEARTBEAT_INTERVAL_S) added specifically to keep evidence
  flowing through DOWN/IDLE stretches where COUNT stops entirely and STATE
  only fires on a transition. A window is complete only if ALL of:
    (a) there is an observation at or near the window start (within G);
    (b) there is an observation within G of the window end - a terminal
        observation proving the machine was still reporting through the
        close of the window, not just at some point inside it;
    (c) no gap between two consecutive observations inside the window
        exceeds G.
  G is COVERAGE_GAP_SECONDS below - the same 600s/10-minute "how long is
  too long without hearing from a machine" threshold already used for the
  ingester's own clock-gap monitoring signal (ingester.CLOCK_GAP_THRESHOLD_
  SECONDS), reused here rather than duplicated. See _coverage() for the
  exact check and schedule.HEARTBEAT_INTERVAL_S for why 5-minute heartbeats
  make G achievable for a genuinely-reporting machine.

  This replaces two earlier, broken proxies for coverage, both now
  deliberately excluded:
    - STATE "tiling": `_state_seconds()` extrapolates a STATE event's
      segment to the next STATE event, or to the shift's end if there is
      no next one (lead()/COALESCE in the SQL) - so a single RUN report and
      nothing else "tiles" the entire window on its own, with zero
      corroborating evidence anything was heard from after that one
      message. Still used below to size run/down/idle seconds for display,
      but never to decide `complete`.
    - COUNT magnitude/fraction thresholds: a large count total is evidence
      of production, not of coverage - a machine can produce a lot in a
      burst and then go silent, or produce genuinely little while staying
      fully covered (a slow but fully-instrumented shift). Coverage is
      about observed messages over time, never about how many units they
      report.
  A machine/shift failing the coverage rule is marked incomplete
  (`complete: False`) and its availability/performance/quality/OEE are
  withheld (`None`) rather than computed and reported as an ordinary
  (mathematically valid but misleading) zero - low genuine production on a
  fully-covered window is never treated as lost telemetry.
  `data["completeness"]["all_complete"]` is the whole-report rollup;
  render_html() renders incomplete cells distinctly and banners the report
  with the specific reason. See also README.md's "Completeness" section.

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

from ingester import CLOCK_GAP_THRESHOLD_SECONDS
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

# G: the maximum gap (seconds) between observed messages for a machine that
# still counts as "covered". Deliberately the same threshold the ingester
# already uses for its own clock-gap monitoring signal (documented in
# README.md's "Ingest validation policy" section) rather than a second,
# independently-tunable number - one definition of "too long without
# hearing from a machine", used both places.
COVERAGE_GAP_SECONDS = CLOCK_GAP_THRESHOLD_SECONDS

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
            # All of this machine's observed message timestamps (STATE,
            # COUNT, ALARM, HEARTBEAT) across the whole day, fetched once
            # and reused for every shift's coverage check below - coverage
            # is about when messages arrived, not what any one of them said.
            observations = _observed_timestamps(con, machine)
            for shift_name, s_start_tz, s_end_tz in SHIFTS:
                # DuckDB TIMESTAMP is naive (UTC by construction throughout
                # this pipeline); strip tzinfo so comparisons against rows
                # read back from the DB are well-defined.
                s_start, s_end = s_start_tz.replace(tzinfo=None), s_end_tz.replace(tzinfo=None)
                planned = (s_end - s_start).total_seconds()
                run_s = _state_seconds(con, machine, shift_name, s_start, s_end, "RUN")
                down_s = _state_seconds(con, machine, shift_name, s_start, s_end, "DOWN")
                idle_s = _state_seconds(con, machine, shift_name, s_start, s_end, "IDLE")

                g, r = _counts(con, machine, s_start, s_end)
                total = g + r

                complete, cov = _coverage(observations, s_start, s_end, COVERAGE_GAP_SECONDS)
                # Liveness (HEARTBEAT) proves the machine was reporting, not what state it was in.
                # State evidence is required too: a STATE event at or before the window start
                # (within the coverage gap) or inside the window, and some STATE time in the window.
                # A known state must be carried into the window from at or before its start, and the
                # STATE segments must tile the whole window: an unknown prefix withholds the window.
                state_known = (_state_evidence(con, machine, s_start, s_end, COVERAGE_GAP_SECONDS)
                               and abs((run_s + down_s + idle_s) - planned) < 1.0)
                cov["state_ok"] = state_known
                complete = complete and state_known
                completeness_reason = None
                if not complete:
                    reasons = []
                    if not state_known:
                        reasons.append(
                            f"unknown machine state for {machine} in part of the {shift_name} window - no STATE "
                            "event at or before the window start, or STATE segments do not cover the whole "
                            "window; heartbeats prove reporting, not machine state."
                        )
                    if not cov["start_ok"]:
                        reasons.append(
                            f"no observed telemetry (STATE/COUNT/ALARM/HEARTBEAT) within "
                            f"{COVERAGE_GAP_SECONDS:.0f}s of the {shift_name} window start for "
                            f"{machine} - coverage at the start of the window is unproven."
                        )
                    if not cov["end_ok"]:
                        reasons.append(
                            f"no observed telemetry within {COVERAGE_GAP_SECONDS:.0f}s of the "
                            f"{shift_name} window end for {machine} - no terminal observation "
                            "proves the machine was still reporting through the close of the "
                            "window (a single early message extrapolated forward is not evidence)."
                        )
                    if not cov["gaps_ok"]:
                        gap_a, gap_b = cov["max_internal_gap_at"]
                        reasons.append(
                            f"an inter-message gap of {cov['max_internal_gap_seconds']:.0f}s "
                            f"(from {gap_a.isoformat()} to {gap_b.isoformat()}) inside the window "
                            f"exceeds the {COVERAGE_GAP_SECONDS:.0f}s coverage gap - telemetry "
                            "coverage is broken here, regardless of what state was last reported."
                        )
                    completeness_reason = " ".join(reasons)
                    incomplete.append({
                        "machine": machine,
                        "shift": shift_name,
                        "reason": completeness_reason,
                        "start_ok": cov["start_ok"],
                        "end_ok": cov["end_ok"],
                        "gaps_ok": cov["gaps_ok"],
                        "max_internal_gap_seconds": cov["max_internal_gap_seconds"],
                        "observation_count": cov["n_observations"],
                    })
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


def _observed_timestamps(con, machine) -> list:
    """Every distinct timestamp at which this machine produced ANY message
    - STATE, COUNT, ALARM or HEARTBEAT - sorted ascending. This is the raw
    evidence _coverage() judges a shift window's completeness from; it
    deliberately does not care what any of these messages said, only that
    one arrived at that moment."""
    rows = con.execute(
        """
        SELECT ts FROM state_events WHERE machine = ?
        UNION
        SELECT ts FROM count_ticks WHERE machine = ?
        UNION
        SELECT ts FROM alarm_events WHERE machine = ?
        UNION
        SELECT ts FROM heartbeats WHERE machine = ?
        ORDER BY ts
        """,
        [machine, machine, machine, machine],
    ).fetchall()
    return [r[0] for r in rows]


def _state_evidence(con, machine, s_start, s_end, gap_s: float) -> bool:
    """True when a STATE event exists at or before s_start, i.e. the machine's state is known
    from the first second of the window (carried state). A first STATE midway leaves an
    unknown prefix and returns False."""
    row = con.execute(
        "SELECT count(*) FROM state_events WHERE machine = ? AND ts <= ?",
        [machine, s_start],
    ).fetchone()
    return bool(row and row[0] > 0)


def _coverage(observations: list, s_start, s_end, gap_s: float) -> tuple[bool, dict]:
    """Observed-coverage check for one shift window (see module docstring):
    (a) an observation at or near s_start, (b) an observation within gap_s
    of s_end, (c) no gap between consecutive observations inside the window
    wider than gap_s. `observations` may include timestamps from outside
    [s_start, s_end] (e.g. the previous/next shift's last/first message) -
    that's deliberate: a message at 07:58 is legitimate "near the start"
    evidence for an 08:00 shift start, not just messages strictly inside
    the window count."""
    before_start = [ts for ts in observations if ts <= s_start]
    at_or_after_start = [ts for ts in observations if ts >= s_start]
    start_ok = (
        (bool(at_or_after_start) and (at_or_after_start[0] - s_start).total_seconds() <= gap_s)
        or (bool(before_start) and (s_start - before_start[-1]).total_seconds() <= gap_s)
    )

    at_or_before_end = [ts for ts in observations if ts <= s_end]
    after_end = [ts for ts in observations if ts >= s_end]
    end_ok = (
        (bool(at_or_before_end) and (s_end - at_or_before_end[-1]).total_seconds() <= gap_s)
        or (bool(after_end) and (after_end[0] - s_end).total_seconds() <= gap_s)
    )

    window_obs = [ts for ts in observations if s_start <= ts <= s_end]
    # Gaps are measured across the whole evidence chain, including the boundary
    # observations that satisfied (a) and (b): 07:59:59 and 16:00:01 alone must not
    # pass as a gap-free 08:00-16:00 window.
    chain = ([before_start[-1]] if before_start else []) + window_obs + ([after_end[0]] if after_end else [])
    chain = sorted(set(chain))
    max_gap = 0.0
    max_gap_at = (s_start, s_start)
    for a, b in zip(chain, chain[1:]):
        d = (b - a).total_seconds()
        if d > max_gap:
            max_gap, max_gap_at = d, (a, b)
    gaps_ok = max_gap <= gap_s

    ok = start_ok and end_ok and gaps_ok
    return ok, {
        "start_ok": start_ok,
        "end_ok": end_ok,
        "gaps_ok": gaps_ok,
        "max_internal_gap_seconds": max_gap,
        "max_internal_gap_at": max_gap_at,
        "n_observations": len(window_obs),
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
    # Pairs each RAISE with "the next event for this (machine, code)" by
    # event ts (ORDER BY ts below), which is safe by construction: the
    # ingester (ingester.py) enforces, per (machine, alarm_code), both
    # strictly alternating RAISE/CLEAR phases AND non-decreasing event ts
    # (alarm_out_of_order_rejected covers a phase repeat and a ts
    # regression alike). A ts-sorted alternating, ts-monotonic sequence is
    # already in RAISE/CLEAR pairing order, so this is pairing by
    # timestamp identity, not receipt order - "the next event by ts" is
    # always that RAISE's actual CLEAR, never a fabricated one from a
    # different arrival order.
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
        d = summary.setdefault(key, {"count": 0, "duration_seconds": 0.0, "open_count": 0})
        d["count"] += 1
        if cleared is not None:
            d["duration_seconds"] += (cleared - raised).total_seconds()
        else:
            # A RAISE with no following CLEAR yet (either genuinely still
            # open, or its CLEAR was rejected - see ingester.py). Counted
            # explicitly rather than silently folded into an
            # indistinguishable "0 extra seconds" of duration.
            d["open_count"] += 1
    return [
        {
            "machine": m, "alarm_code": c,
            "count": v["count"], "duration_seconds": v["duration_seconds"],
            "open_count": v["open_count"],
        }
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
        f"<td>{_esc(a['count'])}</td><td>{a['duration_seconds']:.0f}s</td>"
        f"<td>{_esc(a['open_count'])}</td></tr>"
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
            "machine/shift rows lack proven observed-telemetry coverage (an "
            "observation near the window start, one near the window end, and "
            "no inter-message gap wider than the coverage threshold - see "
            "README's \"Completeness\" section); their "
            "availability/performance/quality/OEE are withheld, not reported as "
            f"zero. Coverage is judged from when messages arrived, never from "
            f"how much they reported:</p><ul>{items}</ul>"
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
<table><tr><th>Machine</th><th>Alarm code</th><th>Count</th><th>Total duration</th><th>Open (unpaired)</th></tr>
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
