"""
Full pipeline: broker -> simulator -> ingester -> DuckDB -> report.
Reconciles the generated report to the planted ground truth (computed
independently from schedule.py, never touching the DB) exactly.
"""
import json
import threading

from events import ordered_events
from ground_truth import compute_ground_truth
from ingester import run_mqtt_ingester
from report import build_report_data, publish_report
from simulate import publish_events


def _run_full_pipeline(broker, db_path) -> dict:
    events = ordered_events()
    result = {}

    def ingest():
        result["stats"] = run_mqtt_ingester(
            broker.host, broker.port, db_path, idle_timeout=10, expected_count=len(events)
        )

    t = threading.Thread(target=ingest)
    t.start()
    import time

    time.sleep(1.0)
    published = publish_events(events, broker.host, broker.port)
    t.join(timeout=180)
    assert not t.is_alive(), "ingester did not finish within timeout"
    assert published == len(events)
    return result["stats"]


def test_full_reconciliation_exact(mqtt_broker, tmp_db):
    stats = _run_full_pipeline(mqtt_broker, tmp_db)
    gt = compute_ground_truth()

    # Every published message was accepted; none silently dropped or miscategorized.
    assert stats == {"accepted": gt["message_totals"]["total"]}

    data = build_report_data(tmp_db)

    for machine, shifts in gt["machines"].items():
        for shift_name, gtv in shifts.items():
            rv = data["machines"][machine][shift_name]
            for key in (
                "run_seconds", "down_seconds", "idle_seconds",
                "good_count", "reject_count", "total_count",
            ):
                assert rv[key] == gtv[key], (machine, shift_name, key, gtv[key], rv[key])
            for key in ("availability", "performance", "quality", "oee"):
                assert abs(rv[key] - gtv[key]) < 1e-9, (machine, shift_name, key, gtv[key], rv[key])
            assert rv["downtime_pareto"] == gtv["downtime_pareto"], (machine, shift_name)

    # Alarm summary: report aggregates across the whole day per (machine, code);
    # ground truth is per (machine, shift, code) - roll it up the same way.
    gt_alarm_agg: dict[tuple, dict] = {}
    for a in gt["alarms"]:
        key = (a["machine"], a["alarm_code"])
        d = gt_alarm_agg.setdefault(key, {"count": 0, "duration_seconds": 0.0})
        d["count"] += a["count"]
        d["duration_seconds"] += a["duration_seconds"]

    rep_alarm_map = {(a["machine"], a["alarm_code"]): a for a in data["alarms"]}
    assert set(rep_alarm_map) == set(gt_alarm_agg)
    for key, gtv in gt_alarm_agg.items():
        rv = rep_alarm_map[key]
        assert rv["count"] == gtv["count"], key
        assert abs(rv["duration_seconds"] - gtv["duration_seconds"]) < 1e-9, key


def test_report_publish_is_atomic_and_readable(mqtt_broker, tmp_db, tmp_path):
    _run_full_pipeline(mqtt_broker, tmp_db)
    out_path = tmp_path / "report.html"
    published = publish_report(tmp_db, str(out_path))
    assert published.exists()
    html = published.read_text()
    assert "Line A" in html
    assert "Synthetic portfolio demonstration" in html

    json_path = out_path.with_suffix(".json")
    data = json.loads(json_path.read_text())
    assert data["role"].startswith("Synthetic portfolio demonstration")
    assert "no benchmark" in data["method_note"].lower() or "no benchmark" in data["method_note"]
