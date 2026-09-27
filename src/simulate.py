"""
Deterministic simulator: publishes the canonical event stream (events.py,
derived from schedule.py) over MQTT as the plant's machines would.

Payload grammar (all fields required unless noted):
  {"seq": <int>, "ts": "<ISO8601, timezone-aware>", "tag": "<machine>", "type": "STATE|COUNT|ALARM|HEARTBEAT", ...}
  STATE: state in {RUN,IDLE,DOWN}, reason_code (string if DOWN, else null)
  COUNT: good_delta (int>=0), reject_delta (int>=0)
  ALARM: alarm_code (non-empty string), phase in {RAISE,CLEAR}
  HEARTBEAT: no extra fields - a periodic "still reporting" pulse per
    machine (schedule.HEARTBEAT_INTERVAL_S), used by report.py to
    establish telemetry coverage during DOWN/IDLE stretches with no COUNT
    ticks.

Topic: demo/plant/<tag>/<type-lowercased>
"""
from __future__ import annotations

import json
from datetime import datetime

import paho.mqtt.client as mqtt

from events import ordered_events


def _payload(event: dict) -> str:
    e = dict(event)
    ts: datetime = e.pop("ts")
    e["ts"] = ts.isoformat()
    return json.dumps(e)


def _topic(event: dict) -> str:
    return f"demo/plant/{event['tag']}/{event['type'].lower()}"


def publish_events(events: list[dict], host: str, port: int,
                    client_id: str = "plant-oee-simulator", qos: int = 1) -> int:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True)
    client.connect(host, port, keepalive=30)
    client.loop_start()
    try:
        for e in events:
            info = client.publish(_topic(e), _payload(e), qos=qos)
            info.wait_for_publish(timeout=10)
    finally:
        client.loop_stop()
        client.disconnect()
    return len(events)


def main() -> None:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    args = p.parse_args()

    events = ordered_events()
    n = publish_events(events, args.host, args.port)
    print(f"published {n} events")


if __name__ == "__main__":
    main()
