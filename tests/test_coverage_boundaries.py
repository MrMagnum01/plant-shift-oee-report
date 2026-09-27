"""Astra recheck bf51d98 (plant 36afe6d): boundary observations must not evade the gap test,
and heartbeat liveness must not replace STATE evidence."""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from ingester import Ingester  # noqa: E402
from report import _coverage, build_report_data  # noqa: E402

S = datetime(2024, 1, 1, 8)
E = S + timedelta(hours=8)


def test_outside_boundaries_alone_are_not_coverage():
    ok, cov = _coverage([S - timedelta(seconds=1), E + timedelta(seconds=1)], S, E, 600)
    assert not ok and not cov["gaps_ok"]


def test_one_inside_observation_leaves_large_gaps():
    ok, cov = _coverage([S - timedelta(seconds=1), S + timedelta(hours=4), E + timedelta(seconds=1)], S, E, 600)
    assert not ok and not cov["gaps_ok"]


def test_dense_observations_are_covered():
    ok, _ = _coverage([S + timedelta(minutes=m) for m in range(0, 481, 5)], S, E, 600)
    assert ok


def _db(tmp_path, name, msgs):
    p = tmp_path / f"{name}.db"
    c = duckdb.connect(str(p))
    i = Ingester(c)
    for m in msgs:
        assert i.handle_raw(json.dumps(m)) == "accepted"
    i.commit()
    c.close()
    return build_report_data(str(p))


def test_heartbeats_without_state_are_incomplete(tmp_path):
    msgs = [dict(seq=n + 1, tag="LINE_A.FILLER", type="HEARTBEAT",
                 ts=(datetime(2024, 1, 1) + timedelta(minutes=5 * n)).isoformat() + "Z") for n in range(97)]
    shift = _db(tmp_path, "hb", msgs)["machines"]["LINE_A.FILLER"]["SHIFT_1"]
    assert shift["complete"] is False and shift["oee"] is None


def test_boundary_only_db_is_incomplete(tmp_path):
    msgs = [dict(seq=1, tag="LINE_A.FILLER", type="STATE", state="DOWN", reason_code="FAULT", ts="2024-01-01T07:59:59Z"),
            dict(seq=2, tag="LINE_A.FILLER", type="HEARTBEAT", ts="2024-01-01T16:00:01Z")]
    shift = _db(tmp_path, "edge", msgs)["machines"]["LINE_A.FILLER"]["SHIFT_2"]
    assert shift["complete"] is False and shift["oee"] is None
