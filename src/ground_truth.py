"""
Planted ground truth, computed directly from schedule.py - independently of
the MQTT stream, the ingester and DuckDB. This is the oracle that
tests/test_reconciliation.py checks the generated report against.

OEE formulas used throughout this demo (documented here and in README.md):

  Planned Production Time = shift duration (8h); no separate break time is
    modelled in this demo.
  Run Time                = sum of RUN-state seconds for the machine in the shift.
  Downtime                = Planned Production Time - Run Time
                             (both DOWN and IDLE segments count as downtime;
                             the demo does not model a distinct "minor stop"
                             category separate from Availability loss).
  Availability             = Run Time / Planned Production Time
  Total Count               = Good Count + Reject Count (production ticks in RUN)
  Performance               = (Total Count * Ideal Cycle Time) / Run Time
  Quality                   = Good Count / Total Count
  OEE                       = Availability * Performance * Quality

All figures are computed on synthetic, planted data for demonstration only.
No benchmark or real-world performance claim is made anywhere in this repo.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime

from events import tick_times
from schedule import (
    ALARMS,
    IDEAL_CYCLE_S,
    MACHINES,
    REJECT_EVERY_N,
    SHIFTS,
    STATE_SEGMENTS,
)


def _shift_for(ts: datetime) -> str:
    for name, start, end in SHIFTS:
        if start <= ts < end:
            return name
    raise ValueError(f"timestamp {ts} falls outside all defined shifts")


def compute_ground_truth() -> dict:
    shift_bounds = {name: (start, end) for name, start, end in SHIFTS}

    # machine -> shift -> accumulators
    run_seconds = defaultdict(lambda: defaultdict(float))
    down_seconds = defaultdict(lambda: defaultdict(float))
    idle_seconds = defaultdict(lambda: defaultdict(float))
    downtime_pareto = defaultdict(lambda: defaultdict(float))  # (machine,shift) -> reason -> seconds
    down_event_count = defaultdict(lambda: defaultdict(int))  # (machine,shift) -> reason -> count

    for machine, state, start, end, reason in STATE_SEGMENTS:
        s_shift = _shift_for(start)
        # segments are authored to never cross a shift boundary; assert it.
        if end in (b[1] for b in shift_bounds.values()):
            e_shift = s_shift  # ends exactly on a shift boundary -> belongs to s_shift
        else:
            e_shift = _shift_for(end)
        if s_shift != e_shift:
            raise AssertionError(
                f"segment {machine} {state} {start}-{end} crosses a shift boundary"
            )
        duration = (end - start).total_seconds()
        if state == "RUN":
            run_seconds[machine][s_shift] += duration
        elif state == "DOWN":
            down_seconds[machine][s_shift] += duration
            downtime_pareto[(machine, s_shift)][reason] += duration
            down_event_count[(machine, s_shift)][reason] += 1
        elif state == "IDLE":
            idle_seconds[machine][s_shift] += duration
        else:
            raise AssertionError(f"unknown state {state}")

    # counts, via the same deterministic tick generator the simulator uses
    good = defaultdict(lambda: defaultdict(int))
    reject = defaultdict(lambda: defaultdict(int))
    reject_counters = {m: 0 for m in MACHINES}
    for machine, state, start, end, _reason in STATE_SEGMENTS:
        if state != "RUN":
            continue
        shift = _shift_for(start)
        cycle = IDEAL_CYCLE_S[machine]
        n_reject = REJECT_EVERY_N[machine]
        for _ts in tick_times(start, end, cycle):
            reject_counters[machine] += 1
            if reject_counters[machine] % n_reject == 0:
                reject[machine][shift] += 1
            else:
                good[machine][shift] += 1

    machines_out = {}
    for machine in MACHINES:
        shifts_out = {}
        for shift_name, s_start, s_end in SHIFTS:
            planned = (s_end - s_start).total_seconds()
            run_s = run_seconds[machine][shift_name]
            down_s = down_seconds[machine][shift_name]
            idle_s = idle_seconds[machine][shift_name]
            assert abs((run_s + down_s + idle_s) - planned) < 1e-6, (
                machine, shift_name, run_s, down_s, idle_s, planned,
            )
            g = good[machine][shift_name]
            r = reject[machine][shift_name]
            total = g + r
            availability = run_s / planned
            performance = (total * IDEAL_CYCLE_S[machine]) / run_s if run_s > 0 else 0.0
            quality = (g / total) if total > 0 else 0.0
            oee = availability * performance * quality
            shifts_out[shift_name] = {
                "planned_production_seconds": planned,
                "run_seconds": run_s,
                "down_seconds": down_s,
                "idle_seconds": idle_s,
                "downtime_seconds": down_s + idle_s,
                "good_count": g,
                "reject_count": r,
                "total_count": total,
                "availability": availability,
                "performance": performance,
                "quality": quality,
                "oee": oee,
                "downtime_pareto": dict(downtime_pareto[(machine, shift_name)]),
                "downtime_event_count": dict(down_event_count[(machine, shift_name)]),
            }
        machines_out[machine] = shifts_out

    # alarm summary: count + total duration by (machine, alarm_code, shift)
    alarm_summary = defaultdict(lambda: {"count": 0, "duration_seconds": 0.0})
    for machine, code, raised, cleared in ALARMS:
        shift = _shift_for(raised)
        key = (machine, shift, code)
        alarm_summary[key]["count"] += 1
        alarm_summary[key]["duration_seconds"] += (cleared - raised).total_seconds()

    alarms_out = [
        {
            "machine": m,
            "shift": s,
            "alarm_code": c,
            "count": v["count"],
            "duration_seconds": v["duration_seconds"],
        }
        for (m, s, c), v in sorted(alarm_summary.items())
    ]

    total_state_events = len(STATE_SEGMENTS)
    total_alarm_events = len(ALARMS) * 2  # RAISE + CLEAR
    total_count_events = sum(
        len(tick_times(start, end, IDEAL_CYCLE_S[machine], ))
        for machine, state, start, end, _ in STATE_SEGMENTS
        if state == "RUN"
    )

    return {
        "machines": machines_out,
        "alarms": alarms_out,
        "message_totals": {
            "state_events": total_state_events,
            "count_events": total_count_events,
            "alarm_events": total_alarm_events,
            "total": total_state_events + total_count_events + total_alarm_events,
        },
    }


def main() -> None:
    gt = compute_ground_truth()
    print(json.dumps(gt, indent=2, default=str))


if __name__ == "__main__":
    main()
