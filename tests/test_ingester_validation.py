"""
Ingester validation, unit-level (no MQTT broker): every payload class is
categorised, nothing is silently dropped, and each accepted/quarantined
message is counted so category totals always reconcile to the number of
calls made.
"""
import json

import duckdb
import pytest

from ingester import (
    CLOCK_GAP_THRESHOLD_SECONDS,
    LATE_GRACE_SECONDS,
    Ingester,
)


@pytest.fixture
def ing():
    con = duckdb.connect(":memory:")
    return Ingester(con=con)


def msg(tag="LINE_A.FILLER", type_="STATE", seq=1, ts="2024-01-01T00:00:00+00:00", **kw):
    base = {"seq": seq, "ts": ts, "tag": tag, "type": type_}
    if type_ == "STATE":
        base.update(state=kw.pop("state", "RUN"), reason_code=kw.pop("reason_code", None))
    elif type_ == "COUNT":
        base.update(good_delta=kw.pop("good_delta", 1), reject_delta=kw.pop("reject_delta", 0))
    elif type_ == "ALARM":
        base.update(alarm_code=kw.pop("alarm_code", "JAM"), phase=kw.pop("phase", "RAISE"))
    base.update(kw)
    return json.dumps(base)


def _total_logged(ing: Ingester) -> int:
    return ing.con.execute("SELECT count(*) FROM ingest_log").fetchone()[0]


def test_accepted_state_and_count(ing):
    assert ing.handle_raw(msg(seq=1, type_="STATE", state="RUN")) == "accepted"
    assert ing.handle_raw(msg(seq=2, type_="COUNT", good_delta=1, reject_delta=0,
                               ts="2024-01-01T00:00:05+00:00")) == "accepted"
    assert ing.con.execute("SELECT count(*) FROM state_events").fetchone()[0] == 1
    assert ing.con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 1
    assert _total_logged(ing) == 2


def test_invalid_json_is_bad_payload_not_dropped(ing):
    assert ing.handle_raw("{not json") == "bad_payload"
    assert _total_logged(ing) == 1
    row = ing.con.execute("SELECT category, raw_payload FROM ingest_log").fetchone()
    assert row[0] == "bad_payload"
    assert row[1] == "{not json"  # raw payload preserved verbatim


@pytest.mark.parametrize("bad", [
    lambda: json.dumps({"seq": "1", "ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "STATE", "state": "RUN", "reason_code": None}),  # seq wrong type
    lambda: json.dumps({"seq": 1, "ts": "not-a-timestamp", "tag": "LINE_A.FILLER", "type": "STATE", "state": "RUN", "reason_code": None}),  # bad ts
    lambda: json.dumps({"seq": 1, "ts": "2024-01-01T00:00:00", "tag": "LINE_A.FILLER", "type": "STATE", "state": "RUN", "reason_code": None}),  # ts missing tz
    lambda: json.dumps({"seq": 1, "ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "STATE", "state": "SPINNING", "reason_code": None}),  # bad enum
    lambda: json.dumps({"seq": 1, "ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "STATE", "state": "DOWN", "reason_code": None}),  # DOWN needs reason
    lambda: json.dumps({"seq": 1, "ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "STATE", "state": "RUN", "reason_code": "JAM"}),  # RUN must not have reason
    lambda: json.dumps({"seq": 1, "ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "COUNT", "good_delta": -1, "reject_delta": 0}),  # negative
    lambda: json.dumps({"seq": 1, "ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "COUNT", "good_delta": 0, "reject_delta": 0}),  # both zero
    lambda: json.dumps({"seq": 1, "ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "ALARM", "alarm_code": "JAM", "phase": "MAYBE"}),  # bad phase
    lambda: json.dumps({"ts": "2024-01-01T00:00:00+00:00", "tag": "LINE_A.FILLER", "type": "STATE", "state": "RUN", "reason_code": None}),  # missing seq
])
def test_grammar_violations_are_bad_payload(ing, bad):
    assert ing.handle_raw(bad()) == "bad_payload"
    assert _total_logged(ing) == 1


def test_unknown_tag_is_quarantined(ing):
    result = ing.handle_raw(msg(tag="LINE_A.SEALER", seq=1))
    assert result == "unknown_tag"
    assert ing.con.execute("SELECT count(*) FROM state_events").fetchone()[0] == 0
    row = ing.con.execute("SELECT category, raw_payload FROM ingest_log").fetchone()
    assert row[0] == "unknown_tag"
    assert "LINE_A.SEALER" in row[1]  # raw payload preserved


def test_duplicate_seq_is_categorised_and_not_reapplied(ing):
    m = msg(seq=7, type_="COUNT", good_delta=1, reject_delta=0)
    assert ing.handle_raw(m) == "accepted"
    assert ing.handle_raw(m) == "duplicate"
    assert ing.con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 1
    assert _total_logged(ing) == 2
    cats = dict(ing.con.execute("SELECT category, count(*) FROM ingest_log GROUP BY 1").fetchall())
    assert cats == {"accepted": 1, "duplicate": 1}


def test_late_within_grace_is_accepted_late(ing):
    assert ing.handle_raw(msg(seq=1, type_="COUNT", ts="2024-01-01T00:10:00+00:00")) == "accepted"
    from datetime import datetime, timedelta, timezone
    max_ts = datetime(2024, 1, 1, 0, 10, 0, tzinfo=timezone.utc)
    late = (max_ts - timedelta(seconds=LATE_GRACE_SECONDS / 2)).isoformat()
    result = ing.handle_raw(msg(seq=2, type_="COUNT", ts=late))
    assert result == "accepted_late"
    row = ing.con.execute("SELECT is_late FROM count_ticks WHERE seq = 2").fetchone()
    assert row[0] is True
    assert ing.con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 2


def test_late_beyond_grace_is_out_of_order_rejected(ing):
    from datetime import datetime, timedelta, timezone

    max_ts = datetime(2024, 1, 1, 0, 10, 0, tzinfo=timezone.utc)
    assert ing.handle_raw(msg(seq=1, type_="COUNT", ts=max_ts.isoformat())) == "accepted"
    too_late = (max_ts - timedelta(seconds=LATE_GRACE_SECONDS * 2)).isoformat()
    result = ing.handle_raw(msg(seq=2, type_="COUNT", ts=too_late))
    assert result == "out_of_order_rejected"
    # not written to the fact table, but logged (never silently dropped)
    assert ing.con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 1
    row = ing.con.execute("SELECT category, raw_payload FROM ingest_log WHERE seq = 2").fetchone()
    assert row[0] == "out_of_order_rejected"
    assert json.loads(row[1])["seq"] == 2


def test_clock_gap_is_recorded(ing):
    from datetime import datetime, timedelta, timezone

    t0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=CLOCK_GAP_THRESHOLD_SECONDS * 2)
    assert ing.handle_raw(msg(seq=1, type_="COUNT", ts=t0.isoformat())) == "accepted"
    assert ing.handle_raw(msg(seq=2, type_="COUNT", ts=t1.isoformat())) == "accepted"
    gaps = ing.con.execute("SELECT machine, gap_seconds FROM clock_gaps").fetchall()
    assert len(gaps) == 1
    assert gaps[0][0] == "LINE_A.FILLER"
    assert abs(gaps[0][1] - CLOCK_GAP_THRESHOLD_SECONDS * 2) < 1e-6


def test_known_total_reconciliation(ing):
    n = 0
    for i in range(1, 6):
        ing.handle_raw(msg(seq=i, type_="COUNT", ts=f"2024-01-01T00:{i:02d}:00+00:00"))
        n += 1
    ing.handle_raw(msg(seq=999, tag="LINE_A.SEALER"))
    n += 1
    ing.handle_raw("not json at all")
    n += 1
    assert _total_logged(ing) == n
    total_by_category = sum(v for v in ing.stats.values())
    assert total_by_category == n
