"""
Turns the hand-authored schedule (schedule.py) into a single, globally
ordered, sequence-numbered list of MQTT-shaped events. This is the exact
message stream the simulator publishes. seq is assigned in emission order
and is per-connection global (not per-tag) - simplest possible contract for
the ingester's duplicate/out-of-order checks.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator

from schedule import (
    ALARMS,
    IDEAL_CYCLE_S,
    REJECT_EVERY_N,
    STATE_SEGMENTS,
)


def tick_times(start: datetime, end: datetime, cycle_s: float) -> list[datetime]:
    """Deterministic production ticks inside the half-open interval
    [start, end): one every cycle_s seconds, starting at start + cycle_s. A
    partial final cycle produces no tick (no fractional units), and a tick
    that would land exactly on `end` is excluded - it belongs to whatever
    segment/shift starts there, consistent with every other half-open
    [start, end) window this pipeline uses (state segments, shift windows)."""
    duration = (end - start).total_seconds()
    n = int(duration // cycle_s)
    if n * cycle_s >= duration:
        n -= 1
    return [start + timedelta(seconds=cycle_s * (k + 1)) for k in range(n)]


def raw_events() -> list[dict]:
    """All events, unsorted, no seq yet. Each event is a dict with at least
    ts (datetime), tag, type."""
    events: list[dict] = []

    for machine, state, start, end, reason in STATE_SEGMENTS:
        events.append(
            {
                "ts": start,
                "tag": machine,
                "type": "STATE",
                "state": state,
                "reason_code": reason,
            }
        )

    reject_counters = {m: 0 for m in IDEAL_CYCLE_S}
    for machine, state, start, end, _reason in STATE_SEGMENTS:
        if state != "RUN":
            continue
        cycle = IDEAL_CYCLE_S[machine]
        n_reject = REJECT_EVERY_N[machine]
        for ts in tick_times(start, end, cycle):
            reject_counters[machine] += 1
            is_reject = reject_counters[machine] % n_reject == 0
            events.append(
                {
                    "ts": ts,
                    "tag": machine,
                    "type": "COUNT",
                    "good_delta": 0 if is_reject else 1,
                    "reject_delta": 1 if is_reject else 0,
                }
            )

    for machine, alarm_code, raised, cleared in ALARMS:
        events.append(
            {
                "ts": raised,
                "tag": machine,
                "type": "ALARM",
                "alarm_code": alarm_code,
                "phase": "RAISE",
            }
        )
        events.append(
            {
                "ts": cleared,
                "tag": machine,
                "type": "ALARM",
                "alarm_code": alarm_code,
                "phase": "CLEAR",
            }
        )

    return events


def ordered_events() -> list[dict]:
    """Stable global chronological order (ties broken by tag then type then
    insertion order), with a global seq assigned."""
    events = raw_events()
    events.sort(key=lambda e: (e["ts"], e["tag"], e["type"]))
    for i, e in enumerate(events):
        e["seq"] = i
    return events
