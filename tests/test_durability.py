"""
Wires in the cases from Astra's HOLD review
(~/vault/40-sessions/2026-09-27-astra-plant-shift-oee-review.md and its
companion -probes.py / -extra-probes.py / -results.json) as regression
tests asserting the corrected behaviour, not the broken behaviour that was
originally observed:

1. accepted-log write and fact write are one transaction, including at
   batch-commit boundaries (no MUST-FIX-1 split after a crash).
2. a restart replay does not double-count (no MUST-FIX-2).
3. malformed input (list/object values for state/phase/seq, out-of-range
   COUNT deltas) is a categorised rejection, never an exception out of the
   callback (no MUST-FIX-3).
4. missing telemetry is never reported as an ordinary zero OEE - it is
   marked incomplete and derived metrics are withheld (no MUST-FIX-4).
5. every input-derived string in the report HTML is escaped, tested with
   Astra's exact XSS payload (no MUST-FIX-5).
6. alarm phases must alternate RAISE/CLEAR; an out-of-order phase is
   rejected, not silently treated as an implicit clear (item 6, narrowed
   by implementing enforcement).
7. report publish is atomic per file, not as an (html, json) set - this
   test pins that narrowed contract exactly (item 7).
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from ingester import COMMIT_BATCH_SIZE, Ingester
from report import build_report_data, publish_report, render_html

SRC = Path(__file__).resolve().parent.parent / "src"


def msg(seq=1, **kw):
    base = dict(seq=seq, tag="LINE_A.FILLER", type="COUNT", ts="2024-01-01T01:00:00Z",
                good_delta=1, reject_delta=0)
    base.update(kw)
    return base


# --------------------------------------------------------------------------
# 1. Log write + fact write are one transaction, including at batch
#    boundaries (MUST-FIX-1).
# --------------------------------------------------------------------------

_CRASH_SCRIPT = """
import sys, os, json, duckdb
sys.path.insert(0, {src!r})
import ingester
ingester.COMMIT_BATCH_SIZE = {batch_size}
from ingester import Ingester

con = duckdb.connect(sys.argv[1])
ing = Ingester(con)
n = {n_messages}
for i in range(1, n + 1):
    m = dict(seq=i, tag="LINE_A.FILLER", type="COUNT",
              ts="2024-01-01T01:{{:02d}}:00Z".format(i % 60),
              good_delta=1, reject_delta=0)
    ing.handle_raw(json.dumps(m))
os._exit(0)  # abrupt termination, no commit() call - simulates a hard crash
"""


def _run_crash_scenario(tmp_path, n_messages: int, batch_size: int) -> tuple[int, int]:
    db_path = str(tmp_path / "crash.duckdb")
    script = _CRASH_SCRIPT.format(src=str(SRC), batch_size=batch_size, n_messages=n_messages)
    subprocess.run([sys.executable, "-c", script, db_path], check=True, timeout=30)
    con = duckdb.connect(db_path)
    try:
        log_count = con.execute(
            "SELECT count(*) FROM ingest_log WHERE category = 'accepted'"
        ).fetchone()[0]
        fact_count = con.execute("SELECT count(*) FROM count_ticks").fetchone()[0]
        return log_count, fact_count
    finally:
        con.close()


def test_crash_before_first_commit_loses_nothing_and_splits_nothing(tmp_path):
    # Two messages accepted, never committed, then the process is killed.
    # Both must be rolled back together: 0 logs, 0 facts - not 1 log / 0
    # facts (the originally observed MUST-FIX-1 bug).
    log_count, fact_count = _run_crash_scenario(tmp_path, n_messages=2, batch_size=500)
    assert (log_count, fact_count) == (0, 0)


def test_crash_at_batch_boundary_commits_log_and_fact_together(tmp_path):
    # With a small batch size, an internal auto-commit fires mid-run; then
    # more messages are accepted and the process is killed before the next
    # commit. The committed batch's logs and facts must match exactly -
    # never a log row without its fact row.
    batch_size = 5
    n_messages = 2 * batch_size + 2  # two full auto-committed batches, plus an uncommitted tail
    log_count, fact_count = _run_crash_scenario(tmp_path, n_messages=n_messages, batch_size=batch_size)
    assert log_count == fact_count == 2 * batch_size


# --------------------------------------------------------------------------
# 2. Restart replay does not double-count (MUST-FIX-2).
# --------------------------------------------------------------------------

def test_restart_replay_does_not_double_count(tmp_path):
    db_path = str(tmp_path / "restart.duckdb")
    m = msg(seq=1)

    con1 = duckdb.connect(db_path)
    ing1 = Ingester(con1)
    assert ing1.handle_raw(json.dumps(m)) == "accepted"
    ing1.commit()
    con1.close()

    # Reopen with a fresh Ingester (as a restarted process would) and
    # replay the exact same already-committed message.
    con2 = duckdb.connect(db_path)
    ing2 = Ingester(con2)
    result = ing2.handle_raw(json.dumps(m))
    ing2.commit()

    assert result == "duplicate"
    total = con2.execute("SELECT sum(good_delta) FROM count_ticks").fetchone()[0]
    assert total == 1  # not 2
    con2.close()


def test_restart_replay_restores_late_watermark_and_alarm_state(tmp_path):
    # A restarted Ingester must also reconstruct per-tag watermarks (so a
    # genuinely-late message after restart is still classified correctly)
    # and per-alarm phase state (see section 6 below).
    db_path = str(tmp_path / "restart2.duckdb")
    con1 = duckdb.connect(db_path)
    ing1 = Ingester(con1)
    ing1.handle_raw(json.dumps(msg(seq=1, ts="2024-01-01T01:00:00Z")))
    ing1.handle_raw(json.dumps(dict(
        seq=2, tag="LINE_A.FILLER", type="ALARM", alarm_code="JAM",
        phase="RAISE", ts="2024-01-01T01:00:05Z",
    )))
    ing1.commit()
    con1.close()

    con2 = duckdb.connect(db_path)
    ing2 = Ingester(con2)
    # A second RAISE for the same (machine, code) must still be rejected
    # after restart - the open-alarm state was reconstructed, not reset.
    result = ing2.handle_raw(json.dumps(dict(
        seq=3, tag="LINE_A.FILLER", type="ALARM", alarm_code="JAM",
        phase="RAISE", ts="2024-01-01T01:05:00Z",
    )))
    assert result == "alarm_out_of_order_rejected"
    con2.close()


# --------------------------------------------------------------------------
# 3. Malformed input is a categorised rejection, never an exception
#    (MUST-FIX-3). Exact cases from Astra's -probes.py.
# --------------------------------------------------------------------------

@pytest.fixture
def ing():
    return Ingester(duckdb.connect(":memory:"))


@pytest.mark.parametrize("patch", [
    {"type": "STATE", "state": []},
    {"seq": {}},
    {"good_delta": 2**40},
    {"type": "ALARM", "alarm_code": "X", "phase": []},
])
def test_malformed_input_never_raises_and_is_bad_payload(ing, patch):
    m = msg()
    m.update(patch)
    result = ing.handle_raw(json.dumps(m))  # must not raise
    assert result == "bad_payload"
    row = ing.con.execute("SELECT category FROM ingest_log").fetchone()
    assert row[0] == "bad_payload"


def test_malformed_seq_does_not_poison_the_next_valid_message(ing):
    bad = msg()
    bad["seq"] = {}
    assert ing.handle_raw(json.dumps(bad)) == "bad_payload"
    good = msg(seq=1)
    assert ing.handle_raw(json.dumps(good)) == "accepted"


# --------------------------------------------------------------------------
# 4. Missing telemetry is never reported as an ordinary zero OEE
#    (MUST-FIX-4).
# --------------------------------------------------------------------------

def test_empty_db_marks_every_machine_shift_incomplete(tmp_path):
    db_path = str(tmp_path / "empty.duckdb")
    Ingester(duckdb.connect(db_path))  # just initialises the schema
    data = build_report_data(db_path)

    assert "completeness" in data
    assert data["completeness"]["all_complete"] is False
    assert len(data["completeness"]["incomplete"]) == 9  # 3 machines x 3 shifts

    for machine, shifts in data["machines"].items():
        for shift, v in shifts.items():
            assert v["complete"] is False
            assert v["availability"] is None
            assert v["performance"] is None
            assert v["quality"] is None
            assert v["oee"] is None

    html = render_html(data)
    assert "INCOMPLETE DATA" in html
    assert "incomplete" in html


def test_full_shift_coverage_is_marked_complete(tmp_path):
    # A machine/shift with a STATE observation at the window start and
    # dense COUNT telemetry (30s cadence, comfortably under the coverage
    # gap G) reaching all the way to the window end must NOT be marked
    # incomplete (no false positives from the observed-coverage check -
    # see report.py's _coverage() and tests/test_completeness_coverage.py
    # for the false-positive-the-other-way cases this is paired with).
    from schedule import IDEAL_CYCLE_S

    db_path = str(tmp_path / "full_shift.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    seq = 1
    ing.handle_raw(json.dumps(dict(
        seq=seq, tag="LINE_A.FILLER", type="STATE", state="RUN",
        reason_code=None, ts=t0.isoformat(),
    )))
    seq += 1
    cycle = IDEAL_CYCLE_S["LINE_A.FILLER"]
    tick = t0
    for _ in range(960):  # 8h / 30s ideal cycle - full corroborating coverage
        tick = tick + timedelta(seconds=cycle)
        ing.handle_raw(json.dumps(dict(
            seq=seq, tag="LINE_A.FILLER", type="COUNT",
            good_delta=1, reject_delta=0, ts=tick.isoformat(),
        )))
        seq += 1
    ing.commit()
    con.close()
    data = build_report_data(db_path)
    shift1 = data["machines"]["LINE_A.FILLER"]["SHIFT_1"]
    assert shift1["complete"] is True
    assert shift1["availability"] == 1.0


# --------------------------------------------------------------------------
# 5. Every input-derived string in the report HTML is escaped
#    (MUST-FIX-5). Astra's exact XSS payload.
# --------------------------------------------------------------------------

XSS_PAYLOAD = "<img src=x onerror=alert(1)>"


def test_xss_payload_is_escaped_in_report_html(tmp_path):
    db_path = str(tmp_path / "xss.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    ing.handle_raw(json.dumps(dict(
        seq=1, tag="LINE_A.FILLER", type="STATE", state="DOWN",
        reason_code=XSS_PAYLOAD, ts="2024-01-01T01:00:00Z",
    )))
    ing.commit()
    con.close()

    data = build_report_data(db_path)
    html = render_html(data)
    assert XSS_PAYLOAD not in html
    assert "&lt;img src=x onerror=alert(1)&gt;" in html


# --------------------------------------------------------------------------
# 6. Alarm sequencing: alternating well-formed pairs enforced (item 6).
#    Reproduces the "repeated raises" case from -extra-probes.py.
# --------------------------------------------------------------------------

def test_repeated_raise_is_rejected_not_treated_as_implicit_clear(tmp_path):
    db_path = str(tmp_path / "alarms.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    results = []
    for seq, phase, t in [(1, "RAISE", "01:00"), (2, "RAISE", "01:10"), (3, "CLEAR", "01:20")]:
        results.append(ing.handle_raw(json.dumps(dict(
            seq=seq, tag="LINE_A.FILLER", type="ALARM", alarm_code="X",
            phase=phase, ts=f"2024-01-01T{t}:00Z",
        ))))
    ing.commit()
    con.close()

    assert results == ["accepted", "alarm_out_of_order_rejected", "accepted"]

    data = build_report_data(db_path)
    alarms = {(a["machine"], a["alarm_code"]): a for a in data["alarms"]}
    a = alarms[("LINE_A.FILLER", "X")]
    assert a["count"] == 1
    assert a["duration_seconds"] == 1200.0  # 01:00 -> 01:20, not a fabricated 01:00->01:10


def test_clear_without_open_raise_is_rejected(tmp_path):
    con = duckdb.connect(str(tmp_path / "alarms2.duckdb"))
    ing = Ingester(con)
    result = ing.handle_raw(json.dumps(dict(
        seq=1, tag="LINE_A.FILLER", type="ALARM", alarm_code="X",
        phase="CLEAR", ts="2024-01-01T01:00:00Z",
    )))
    assert result == "alarm_out_of_order_rejected"
    con.close()


# --------------------------------------------------------------------------
# 7. Publish is atomic per file, not as an (html, json) set - the narrowed
#    contract (item 7). Reproduces the injected-sidecar-failure case from
#    -extra-probes.py and pins the documented, accepted outcome.
# --------------------------------------------------------------------------

def test_publish_is_per_file_atomic_not_report_set_atomic(tmp_path, monkeypatch):
    import report as report_mod

    db_path = str(tmp_path / "pub.duckdb")
    Ingester(duckdb.connect(db_path))
    out = tmp_path / "report.html"
    publish_report(db_path, str(out))
    old_html = out.read_bytes()
    old_json = out.with_suffix(".json").read_bytes()

    real_replace = report_mod.os.replace

    def fail_on_json(src, dst):
        if str(dst).endswith(".json"):
            raise OSError("injected sidecar failure")
        return real_replace(src, dst)

    monkeypatch.setattr(report_mod.os, "replace", fail_on_json)
    with pytest.raises(OSError):
        publish_report(db_path, str(out))

    # Documented, narrowed contract: the HTML (written/renamed first)
    # succeeded and changed; the JSON (second) failed and was left as-is.
    # This is the accepted per-file-atomicity behaviour, not a bug - a
    # reader wanting a verified matching pair must cross-check
    # generated_at between the two files.
    assert out.read_bytes() != old_html
    assert out.with_suffix(".json").read_bytes() == old_json
