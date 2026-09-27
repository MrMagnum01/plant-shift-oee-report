"""
Deterministic, hand-authored schedule for a fictional generic packaging line.

Line A: FILLER -> CAPPER -> LABELLER (a made-up three-station line; not any
real plant's process). All tag names, reason codes and timings below are
synthetic and were authored for this demo, not derived from any employer's
data or process.

The schedule fully determines ground truth: every state segment, every
count-tick, and every alarm is listed explicitly. The simulator (simulate.py)
replays this schedule over MQTT; ground_truth.py recomputes OEE straight from
this same schedule (not from what got ingested), giving an independent
oracle that the report is reconciled against.

Synthetic date: 2024-01-01 (arbitrary, not a real production day).
Three 8h shifts (UTC): SHIFT_1 00:00-08:00, SHIFT_2 08:00-16:00, SHIFT_3 16:00-24:00.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

BASE_DAY = datetime(2024, 1, 1, tzinfo=timezone.utc)

SHIFTS = [
    ("SHIFT_1", BASE_DAY, BASE_DAY + timedelta(hours=8)),
    ("SHIFT_2", BASE_DAY + timedelta(hours=8), BASE_DAY + timedelta(hours=16)),
    ("SHIFT_3", BASE_DAY + timedelta(hours=16), BASE_DAY + timedelta(hours=24)),
]

MACHINES = ["LINE_A.FILLER", "LINE_A.CAPPER", "LINE_A.LABELLER"]

# Every machine also publishes a periodic HEARTBEAT (events.py) independent
# of its state - it exists purely to prove "this machine was still
# reporting" during long DOWN/IDLE stretches, where COUNT ticks stop
# entirely and STATE only emits on a transition. 5 minutes is comfortably
# below every DOWN/IDLE segment length in STATE_SEGMENTS (shortest is 10
# minutes) so a genuinely reporting machine always has multiple heartbeats
# inside any such segment, and comfortably below report.py's coverage gap
# G (ingester.CLOCK_GAP_THRESHOLD_SECONDS, 600s / 10 minutes - the same
# "how long is too long without hearing from a machine" threshold already
# used for the ingester's own clock-gap monitoring signal) so losing a
# single heartbeat still leaves coverage intact.
HEARTBEAT_INTERVAL_S = 300.0

# Ideal cycle time in seconds/unit for each machine (made-up nameplate figure).
# Chosen coarse enough that a full synthetic day is a few thousand MQTT
# messages, not hundreds of thousands - this is a demo, not a load test.
IDEAL_CYCLE_S = {
    "LINE_A.FILLER": 30.0,
    "LINE_A.CAPPER": 27.0,
    "LINE_A.LABELLER": 22.0,
}

DOWN_REASONS = {
    "LINE_A.FILLER": ["JAM", "CHANGEOVER", "NO_MATERIAL"],
    "LINE_A.CAPPER": ["JAM", "TOOL_WEAR", "NO_MATERIAL"],
    "LINE_A.LABELLER": ["JAM", "LABEL_OUT", "CHANGEOVER"],
}


def _t(hh: int, mm: int = 0, ss: int = 0) -> datetime:
    return BASE_DAY + timedelta(hours=hh, minutes=mm, seconds=ss)


# ---------------------------------------------------------------------------
# STATE SEGMENTS: (machine, state, start, end, reason_code or None)
# Fully covers 00:00-24:00 for every machine, no gaps, no overlaps.
# state in {RUN, IDLE, DOWN}
# ---------------------------------------------------------------------------
STATE_SEGMENTS = [
    # --- LINE_A.FILLER ---
    ("LINE_A.FILLER", "RUN", _t(0, 0), _t(2, 30), None),
    ("LINE_A.FILLER", "DOWN", _t(2, 30), _t(2, 45), "JAM"),
    ("LINE_A.FILLER", "RUN", _t(2, 45), _t(5, 0), None),
    ("LINE_A.FILLER", "IDLE", _t(5, 0), _t(5, 10), None),
    ("LINE_A.FILLER", "RUN", _t(5, 10), _t(8, 0), None),
    ("LINE_A.FILLER", "RUN", _t(8, 0), _t(10, 0), None),
    ("LINE_A.FILLER", "DOWN", _t(10, 0), _t(10, 30), "CHANGEOVER"),
    ("LINE_A.FILLER", "RUN", _t(10, 30), _t(13, 45), None),
    ("LINE_A.FILLER", "DOWN", _t(13, 45), _t(13, 55), "NO_MATERIAL"),
    ("LINE_A.FILLER", "RUN", _t(13, 55), _t(16, 0), None),
    ("LINE_A.FILLER", "RUN", _t(16, 0), _t(19, 0), None),
    ("LINE_A.FILLER", "IDLE", _t(19, 0), _t(19, 20), None),
    ("LINE_A.FILLER", "RUN", _t(19, 20), _t(22, 0), None),
    ("LINE_A.FILLER", "DOWN", _t(22, 0), _t(22, 15), "JAM"),
    ("LINE_A.FILLER", "RUN", _t(22, 15), _t(24, 0), None),
    # --- LINE_A.CAPPER ---
    ("LINE_A.CAPPER", "RUN", _t(0, 0), _t(1, 45), None),
    ("LINE_A.CAPPER", "DOWN", _t(1, 45), _t(2, 5), "TOOL_WEAR"),
    ("LINE_A.CAPPER", "RUN", _t(2, 5), _t(6, 0), None),
    ("LINE_A.CAPPER", "IDLE", _t(6, 0), _t(6, 15), None),
    ("LINE_A.CAPPER", "RUN", _t(6, 15), _t(8, 0), None),
    ("LINE_A.CAPPER", "RUN", _t(8, 0), _t(11, 30), None),
    ("LINE_A.CAPPER", "DOWN", _t(11, 30), _t(11, 40), "JAM"),
    ("LINE_A.CAPPER", "RUN", _t(11, 40), _t(14, 0), None),
    ("LINE_A.CAPPER", "DOWN", _t(14, 0), _t(14, 25), "NO_MATERIAL"),
    ("LINE_A.CAPPER", "RUN", _t(14, 25), _t(16, 0), None),
    ("LINE_A.CAPPER", "RUN", _t(16, 0), _t(18, 30), None),
    ("LINE_A.CAPPER", "DOWN", _t(18, 30), _t(18, 45), "TOOL_WEAR"),
    ("LINE_A.CAPPER", "RUN", _t(18, 45), _t(21, 50), None),
    ("LINE_A.CAPPER", "IDLE", _t(21, 50), _t(22, 5), None),
    ("LINE_A.CAPPER", "RUN", _t(22, 5), _t(24, 0), None),
    # --- LINE_A.LABELLER ---
    ("LINE_A.LABELLER", "RUN", _t(0, 0), _t(3, 0), None),
    ("LINE_A.LABELLER", "DOWN", _t(3, 0), _t(3, 20), "LABEL_OUT"),
    ("LINE_A.LABELLER", "RUN", _t(3, 20), _t(8, 0), None),
    ("LINE_A.LABELLER", "RUN", _t(8, 0), _t(9, 30), None),
    ("LINE_A.LABELLER", "DOWN", _t(9, 30), _t(9, 45), "CHANGEOVER"),
    ("LINE_A.LABELLER", "RUN", _t(9, 45), _t(12, 50), None),
    ("LINE_A.LABELLER", "IDLE", _t(12, 50), _t(13, 0), None),
    ("LINE_A.LABELLER", "RUN", _t(13, 0), _t(16, 0), None),
    ("LINE_A.LABELLER", "RUN", _t(16, 0), _t(20, 0), None),
    ("LINE_A.LABELLER", "DOWN", _t(20, 0), _t(20, 30), "LABEL_OUT"),
    ("LINE_A.LABELLER", "RUN", _t(20, 30), _t(23, 15), None),
    ("LINE_A.LABELLER", "DOWN", _t(23, 15), _t(23, 25), "JAM"),
    ("LINE_A.LABELLER", "RUN", _t(23, 25), _t(24, 0), None),
]

# ---------------------------------------------------------------------------
# ALARMS: (machine, alarm_code, raised_at, cleared_at)
# Independent of state segments (an alarm may occur during RUN, e.g. a
# warning that doesn't stop the machine).
# ---------------------------------------------------------------------------
ALARMS = [
    ("LINE_A.FILLER", "TEMP_HIGH", _t(1, 5), _t(1, 12)),
    ("LINE_A.FILLER", "JAM", _t(2, 30), _t(2, 45)),
    ("LINE_A.FILLER", "CHANGEOVER", _t(10, 0), _t(10, 30)),
    ("LINE_A.FILLER", "NO_MATERIAL", _t(13, 45), _t(13, 55)),
    ("LINE_A.FILLER", "JAM", _t(22, 0), _t(22, 15)),
    ("LINE_A.FILLER", "VIBRATION", _t(20, 40), _t(20, 44)),
    ("LINE_A.CAPPER", "TOOL_WEAR", _t(1, 45), _t(2, 5)),
    ("LINE_A.CAPPER", "JAM", _t(11, 30), _t(11, 40)),
    ("LINE_A.CAPPER", "NO_MATERIAL", _t(14, 0), _t(14, 25)),
    ("LINE_A.CAPPER", "TOOL_WEAR", _t(18, 30), _t(18, 45)),
    ("LINE_A.CAPPER", "PRESSURE_LOW", _t(9, 10), _t(9, 14)),
    ("LINE_A.LABELLER", "LABEL_OUT", _t(3, 0), _t(3, 20)),
    ("LINE_A.LABELLER", "CHANGEOVER", _t(9, 30), _t(9, 45)),
    ("LINE_A.LABELLER", "LABEL_OUT", _t(20, 0), _t(20, 30)),
    ("LINE_A.LABELLER", "JAM", _t(23, 15), _t(23, 25)),
    ("LINE_A.LABELLER", "MISALIGN", _t(5, 40), _t(5, 43)),
]

# ---------------------------------------------------------------------------
# REJECT PATTERN: every Nth good unit on a machine is instead a reject.
# Deterministic, no RNG. N differs per machine so the demo isn't uniform.
# ---------------------------------------------------------------------------
REJECT_EVERY_N = {
    "LINE_A.FILLER": 23,
    "LINE_A.CAPPER": 31,
    "LINE_A.LABELLER": 17,
}
