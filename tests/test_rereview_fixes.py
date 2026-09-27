"""
Wires in the five clauses left open by Astra's rereview
(~/vault/40-sessions/2026-09-27-astra-plant-shift-oee-rereview.md and its
companion -rereview-probes.py / -rereview-results.json) as regression tests
asserting the corrected behaviour, not the fabricated/broken behaviour
originally observed. Frozen scope: only these five items.

1. Write failure poisons dedup state (Original1 MUST-FIX): a DB write
   failure after in-memory dedup state was updated must not leave seen_seq
   ahead of what's durable - a retry must be accepted, not lost forever as
   a phantom duplicate.
2. Same seq, different content (Original2 MUST-FIX): a replay of an
   already-ingested seq with different content is a seq_conflict, not a
   duplicate.
3. Completeness is fabricated (Original4 MUST-FIX): one RUN STATE event
   and no COUNT telemetry must not be reported complete - the coverage
   rule requires STATE tiling AND corroborating COUNT telemetry (see
   report.py's module docstring and README.md's "Completeness" section).
4. Alarm pairing by timestamp (Original6 MUST-FIX): RAISE 01:10,
   CLEAR 01:05, RAISE 01:11 must be classified as out-of-order/rejected,
   not silently paired by receipt/sort order into a fabricated duration.
5. Container ownership (Original9 NARROW): start_broker() must never
   remove a container from a different run just because the default name
   collided.
"""
from __future__ import annotations

import json

import duckdb
import pytest

from ingester import Ingester
from report import build_report_data


def msg(seq=1, ts="2024-01-01T00:00:00Z", **kw):
    base = dict(seq=seq, tag="LINE_A.FILLER", type="COUNT", ts=ts, good_delta=1, reject_delta=0)
    base.update(kw)
    return base


# --------------------------------------------------------------------------
# 1. Write failure poisons dedup state.
# --------------------------------------------------------------------------

class _FaultOnce:
    """Wraps a real DuckDB connection and raises once, on the first
    statement whose SQL starts with `trigger_prefix`, then behaves
    normally for every call after (including the ROLLBACK issued by
    Ingester._recover_from_write_failure)."""

    def __init__(self, con, trigger_prefix: str):
        self._con = con
        self._trigger_prefix = trigger_prefix
        self._armed = True

    def execute(self, sql, *args, **kwargs):
        if self._armed and sql.startswith(self._trigger_prefix):
            self._armed = False
            raise RuntimeError("injected fact write failure")
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


def test_write_failure_rolls_back_and_retry_is_accepted(tmp_path):
    db_path = str(tmp_path / "rollback.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)

    real_con = ing.con
    ing.con = _FaultOnce(real_con, "INSERT INTO count_ticks")

    m = msg(seq=1)
    with pytest.raises(RuntimeError, match="injected fact write failure"):
        ing.handle_raw(json.dumps(m))

    # No poisoned state left behind: seq 1 was never durably committed, so
    # it must not still be "seen" in memory.
    assert ing.seen_seq == set()
    assert ing._in_txn is False
    assert real_con.execute("SELECT count(*) FROM ingest_log").fetchone()[0] == 0
    assert real_con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 0

    # A retry of the exact same message must now be accepted, not
    # perpetually rejected as a duplicate of a fact that was never
    # actually written.
    result = ing.handle_raw(json.dumps(m))
    assert result == "accepted"
    ing.commit()
    assert real_con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 1
    con.close()


def test_write_failure_mid_batch_rolls_back_batch_predecessors_too(tmp_path):
    # A failure doesn't just roll back the failing message - it rolls back
    # the whole open (uncommitted) transaction, same as a hard crash, and
    # every earlier-in-the-batch message must also be retriable afterwards.
    db_path = str(tmp_path / "rollback2.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)

    assert ing.handle_raw(json.dumps(msg(seq=1, ts="2024-01-01T00:00:00Z"))) == "accepted"
    assert ing.handle_raw(json.dumps(msg(seq=2, ts="2024-01-01T00:00:05Z"))) == "accepted"
    assert {1, 2} <= ing.seen_seq

    real_con = ing.con
    ing.con = _FaultOnce(real_con, "INSERT INTO count_ticks")
    with pytest.raises(RuntimeError):
        ing.handle_raw(json.dumps(msg(seq=3, ts="2024-01-01T00:00:10Z")))

    # seq 1 and 2 were never committed (still inside the aborted batch
    # transaction) - they must be retriable too, not just seq 3.
    assert ing.seen_seq == set()
    assert real_con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 0

    for seq, ts in [(1, "2024-01-01T00:00:00Z"), (2, "2024-01-01T00:00:05Z"), (3, "2024-01-01T00:00:10Z")]:
        assert ing.handle_raw(json.dumps(msg(seq=seq, ts=ts))) == "accepted"
    ing.commit()
    assert real_con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 3
    con.close()


# --------------------------------------------------------------------------
# 2. Same seq, different content is a conflict, not a duplicate.
# --------------------------------------------------------------------------

def test_same_seq_different_content_is_seq_conflict_not_duplicate(tmp_path):
    db_path = str(tmp_path / "conflict.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)

    assert ing.handle_raw(json.dumps(msg(seq=1, good_delta=1))) == "accepted"
    ing.commit()

    result = ing.handle_raw(json.dumps(msg(seq=1, good_delta=100)))
    assert result == "seq_conflict"

    # The original fact row is untouched - not overwritten, not duplicated.
    rows = con.execute("SELECT good_delta FROM count_ticks WHERE seq = 1").fetchall()
    assert rows == [(1,)]

    # Evidence of the conflicting payload is retained (never silently
    # dropped), tagged with its own category.
    cats = dict(con.execute("SELECT category, count(*) FROM ingest_log GROUP BY 1").fetchall())
    assert cats == {"accepted": 1, "seq_conflict": 1}
    conflict_row = con.execute(
        "SELECT raw_payload FROM ingest_log WHERE category = 'seq_conflict'"
    ).fetchone()
    assert json.loads(conflict_row[0])["good_delta"] == 100
    con.close()


def test_same_seq_same_content_is_still_a_duplicate(tmp_path):
    # Exact retries must not regress to seq_conflict.
    db_path = str(tmp_path / "dup.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    m = json.dumps(msg(seq=1, good_delta=1))
    assert ing.handle_raw(m) == "accepted"
    assert ing.handle_raw(m) == "duplicate"
    con.close()


def test_seq_conflict_survives_restart(tmp_path):
    # The content hash used to distinguish duplicate from seq_conflict must
    # itself be durable/reconstructed on restart, not just in-memory.
    db_path = str(tmp_path / "conflict_restart.duckdb")
    con1 = duckdb.connect(db_path)
    ing1 = Ingester(con1)
    assert ing1.handle_raw(json.dumps(msg(seq=1, good_delta=1))) == "accepted"
    ing1.commit()
    con1.close()

    con2 = duckdb.connect(db_path)
    ing2 = Ingester(con2)
    assert ing2.handle_raw(json.dumps(msg(seq=1, good_delta=1))) == "duplicate"
    assert ing2.handle_raw(json.dumps(msg(seq=1, good_delta=100))) == "seq_conflict"
    con2.close()


# --------------------------------------------------------------------------
# 3. Completeness must come from observed telemetry coverage, not
#    STATE-only extrapolation. (Superseded/tightened further by Astra's
#    second recheck - see tests/test_completeness_coverage.py, which also
#    covers DOWN/IDLE and count-magnitude false positives this original
#    probe did not.)
# --------------------------------------------------------------------------

def test_one_state_no_counts_is_incomplete_with_oee_withheld(tmp_path):
    db_path = str(tmp_path / "coverage.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    ing.handle_raw(json.dumps(dict(
        seq=1, tag="LINE_A.FILLER", type="STATE", state="RUN",
        reason_code=None, ts="2024-01-01T00:00:00Z",
    )))
    ing.commit()
    con.close()

    data = build_report_data(db_path)
    for shift_name in ("SHIFT_1", "SHIFT_2", "SHIFT_3"):
        s = data["machines"]["LINE_A.FILLER"][shift_name]
        # STATE tiling alone (extrapolated via lead()/COALESCE from one
        # event) reports a numerically "full" run_seconds - that must NOT
        # be enough on its own.
        assert s["run_seconds"] == 28800.0
        assert s["complete"] is False
        assert s["oee"] is None
        assert s["availability"] is None
        assert "observed telemetry" in s["completeness_reason"]

    assert data["completeness"]["all_complete"] is False


# --------------------------------------------------------------------------
# 4. Alarm pairing by event timestamp, not receipt order.
# --------------------------------------------------------------------------

def test_alarm_ts_regression_is_rejected_not_fabricated_into_a_pair(tmp_path):
    db_path = str(tmp_path / "alarm_ts.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    results = []
    for seq, ts, phase in [
        (1, "2024-01-01T01:10:00Z", "RAISE"),
        (2, "2024-01-01T01:05:00Z", "CLEAR"),  # precedes its RAISE's ts
        (3, "2024-01-01T01:11:00Z", "RAISE"),
    ]:
        results.append(ing.handle_raw(json.dumps(dict(
            seq=seq, tag="LINE_A.FILLER", type="ALARM", alarm_code="SYNTHETIC",
            phase=phase, ts=ts,
        ))))
    ing.commit()
    con.close()

    # Both problematic events are classified out-of-order/rejected - never
    # silently accepted and paired into a fabricated 60-second duration.
    assert results == ["accepted", "alarm_out_of_order_rejected", "alarm_out_of_order_rejected"]

    data = build_report_data(db_path)
    alarms = {(a["machine"], a["alarm_code"]): a for a in data["alarms"]}
    a = alarms[("LINE_A.FILLER", "SYNTHETIC")]
    assert a["count"] == 1          # only the RAISE@01:10 was ever accepted
    assert a["duration_seconds"] == 0.0   # no fabricated CLEAR pairing
    assert a["open_count"] == 1     # explicitly flagged as open/unpaired


def test_well_ordered_alarm_pair_still_computes_correct_duration(tmp_path):
    # No regression: a normal, chronologically consistent RAISE/CLEAR pair
    # must still pair and compute duration correctly.
    db_path = str(tmp_path / "alarm_ok.duckdb")
    con = duckdb.connect(db_path)
    ing = Ingester(con)
    ing.handle_raw(json.dumps(dict(
        seq=1, tag="LINE_A.FILLER", type="ALARM", alarm_code="X",
        phase="RAISE", ts="2024-01-01T01:00:00Z",
    )))
    ing.handle_raw(json.dumps(dict(
        seq=2, tag="LINE_A.FILLER", type="ALARM", alarm_code="X",
        phase="CLEAR", ts="2024-01-01T01:05:00Z",
    )))
    ing.commit()
    con.close()
    data = build_report_data(db_path)
    a = {(x["machine"], x["alarm_code"]): x for x in data["alarms"]}[("LINE_A.FILLER", "X")]
    assert a["count"] == 1
    assert a["duration_seconds"] == 300.0
    assert a["open_count"] == 0


# --------------------------------------------------------------------------
# 5. Container ownership: start_broker() must never remove a container
#    from a different run just because the default name collided. (The
#    same-run replace-own-leftover path is already covered, unchanged, by
#    tests/test_broker_ownership.py::
#    test_start_broker_cleans_up_its_own_stale_leftover - that test calls
#    start_broker() twice from the same test process, i.e. the same
#    broker.RUN_ID, which is exactly the "own run" case.)
# --------------------------------------------------------------------------

def test_start_broker_refuses_a_same_name_container_from_a_different_run():
    import subprocess

    import broker

    name = "plant-oee-demo-mosquitto-other-run-test"
    subprocess.run(["podman", "rm", "-f", name], capture_output=True)
    # Simulate a container created by a DIFFERENT run of this same demo:
    # project-owned, but a run-id that is not this test process's
    # broker.RUN_ID.
    subprocess.run(
        [
            "podman", "run", "-d", "--rm", "--name", name,
            "--label", f"{broker.OWNER_LABEL_KEY}={broker.OWNER_LABEL_VALUE}",
            "--label", f"{broker.RUN_ID_LABEL_KEY}=not-this-run-{broker.RUN_ID}",
            broker.IMAGE, "sh", "-c", "sleep 300",
        ],
        check=True, capture_output=True,
    )
    try:
        with pytest.raises(RuntimeError, match="DIFFERENT run"):
            broker.start_broker(name=name)
        # Never force-removed the other run's still-active container.
        r = subprocess.run(
            ["podman", "inspect", "--format", "{{.State.Running}}", name],
            capture_output=True, text=True,
        )
        assert r.returncode == 0
        assert r.stdout.strip() == "true"
    finally:
        subprocess.run(["podman", "rm", "-f", name], capture_output=True)
