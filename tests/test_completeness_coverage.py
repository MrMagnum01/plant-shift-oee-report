"""
Wires in Astra's second completeness recheck
(~/vault/40-sessions/2026-09-27-astra-plant-79a2-recheck.md and its
companion -recheck-probes.py / -recheck-results.json) as regression tests
asserting the corrected observed-coverage rule, not the state-tiling /
count-magnitude proxies that were originally observed to fabricate
completeness:

1. A single RUN, DOWN or IDLE STATE event with no later telemetry stays
   incomplete on every shift (test_rereview_fixes.py only covered RUN;
   DOWN and IDLE were the still-open false positives - the recheck found
   both reported `complete: true, oee: 0.0`).
2. A COUNT delta of any magnitude does not establish coverage by itself -
   an inflated single COUNT message with no terminal observation stays
   incomplete exactly like the case with no COUNT at all (the recheck
   found it reported `complete: true, oee: 104.1667`).
3. The fully observed synthetic fixture (the real day, including its
   periodic per-machine HEARTBEATs - schedule.HEARTBEAT_INTERVAL_S) keeps
   its exact measured reconciliation against ground_truth.py - the
   coverage rule must not introduce a false negative on real dense
   telemetry.
4. A gap between two observations inside a window that exceeds G breaks
   coverage even when both the window start and window end individually
   have a nearby observation - this is why condition (c) (no internal gap)
   is required, not just the two boundary checks, and why the simulator
   needed a periodic heartbeat in the first place.

Frozen scope: only the completeness/coverage rule (report.py:116-127
originally). No other product behaviour is touched here.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from events import ordered_events  # noqa: E402
from ground_truth import compute_ground_truth  # noqa: E402
from ingester import Ingester  # noqa: E402
from report import build_report_data  # noqa: E402


def _msg(seq=1, tag="LINE_A.FILLER", type="COUNT", ts="2024-01-01T00:00:00Z", **kw):
    d = dict(seq=seq, tag=tag, type=type, ts=ts)
    d.update(kw)
    return d


def _machine_db(tmp_path, name, messages) -> str:
    db_path = str(tmp_path / f"{name}.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    for m in messages:
        cat = ing.handle_raw(json.dumps(m))
        assert cat == "accepted", (m, cat)
    ing.commit()
    con.close()
    return db_path


# --------------------------------------------------------------------------
# 1. A single RUN, DOWN or IDLE state with no later telemetry stays
#    incomplete on every shift.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("state,reason_code", [
    ("RUN", None), ("DOWN", "FAULT"), ("IDLE", None),
])
def test_single_state_no_later_telemetry_stays_incomplete(tmp_path, state, reason_code):
    db_path = _machine_db(tmp_path, f"single-{state}", [
        _msg(type="STATE", state=state, reason_code=reason_code),
    ])
    data = build_report_data(db_path)
    for shift_name in ("SHIFT_1", "SHIFT_2", "SHIFT_3"):
        s = data["machines"]["LINE_A.FILLER"][shift_name]
        assert s["complete"] is False, (state, shift_name, s)
        assert s["oee"] is None
        assert s["availability"] is None
        assert s["performance"] is None
        assert s["quality"] is None
    assert data["completeness"]["all_complete"] is False


# --------------------------------------------------------------------------
# 2. Increasing count magnitude never establishes coverage.
# --------------------------------------------------------------------------

def test_inflated_count_does_not_establish_coverage(tmp_path):
    db_path = _machine_db(tmp_path, "inflated", [
        _msg(seq=1, type="STATE", state="RUN", reason_code=None),
        _msg(seq=2, type="COUNT", good_delta=100000, reject_delta=0),
    ])
    data = build_report_data(db_path)
    shift1 = data["machines"]["LINE_A.FILLER"]["SHIFT_1"]
    # Same telemetry shape as test 1's single-RUN case plus one huge COUNT
    # at the same instant - still only one observed moment in time, so
    # still no terminal observation proving the window was covered through
    # to its end. Previously this reported complete:true, oee:104.1667.
    assert shift1["run_seconds"] == 28800.0
    assert shift1["complete"] is False
    assert shift1["oee"] is None


# --------------------------------------------------------------------------
# 3. The fully observed synthetic fixture keeps its exact measured
#    reconciliation - no false negative from the coverage rule.
# --------------------------------------------------------------------------

def test_fully_observed_fixture_keeps_exact_reconciliation(tmp_path):
    events = ordered_events()
    db_path = str(tmp_path / "full_day.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    for e in events:
        d = dict(e)
        ts = d.pop("ts")
        d["ts"] = ts.isoformat()
        cat = ing.handle_raw(json.dumps(d))
        assert cat == "accepted", d
    ing.commit()
    con.close()

    data = build_report_data(db_path)
    gt = compute_ground_truth()
    assert data["completeness"]["all_complete"] is True

    for machine, shifts in gt["machines"].items():
        for shift_name, gtv in shifts.items():
            rv = data["machines"][machine][shift_name]
            assert rv["complete"] is True, (machine, shift_name, rv["completeness_reason"])
            for key in ("run_seconds", "down_seconds", "idle_seconds",
                        "good_count", "reject_count", "total_count"):
                assert rv[key] == gtv[key], (machine, shift_name, key)
            for key in ("availability", "performance", "quality", "oee"):
                assert abs(rv[key] - gtv[key]) < 1e-9, (machine, shift_name, key)


# --------------------------------------------------------------------------
# 4. An internal gap wider than G breaks coverage even when the window
#    start and end individually have a nearby observation.
# --------------------------------------------------------------------------

def test_internal_gap_past_g_breaks_coverage_despite_boundary_observations(tmp_path):
    db_path = _machine_db(tmp_path, "internal-gap", [
        _msg(seq=1, type="STATE", state="RUN", reason_code=None,
             ts="2024-01-01T00:00:00Z"),
        _msg(seq=2, type="COUNT", good_delta=1, reject_delta=0,
             ts="2024-01-01T07:55:00Z"),
    ])
    data = build_report_data(db_path)
    shift1 = data["machines"]["LINE_A.FILLER"]["SHIFT_1"]
    # Both boundary checks individually pass (an observation at t=0, one
    # 5 minutes before the shift ends) but nothing was heard for almost 8
    # hours in between - exactly the gap a periodic heartbeat exists to
    # rule out.
    assert shift1["complete"] is False
    assert shift1["oee"] is None
    assert "gap" in shift1["completeness_reason"]
