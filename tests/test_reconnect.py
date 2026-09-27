"""
Broker disconnect/reconnect: documented policy is "no loss" via a
persistent MQTT session (fixed client_id, clean_session=False, QoS 1
subscription) - the broker queues messages published while the ingester is
disconnected and redelivers them on reconnect. This test proves that
property holds for a disconnect that stays within the broker's queue
capacity, and that the resulting DuckDB state exactly matches the ground
truth for the messages published (no loss, no duplication).
"""
import time

from ingester import MqttIngestSession
from simulate import publish_events


def _mk_events(n: int, start_seq: int = 0):
    from datetime import datetime, timedelta, timezone

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    out = []
    for i in range(n):
        out.append({
            "ts": base + timedelta(seconds=start_seq + i),
            "tag": "LINE_A.FILLER",
            "type": "COUNT",
            "good_delta": 1,
            "reject_delta": 0,
            "seq": start_seq + i,
        })
    return out


def test_disconnect_then_reconnect_no_loss(mqtt_broker, tmp_db):
    session = MqttIngestSession(mqtt_broker.host, mqtt_broker.port, tmp_db)
    session.connect()
    time.sleep(0.5)

    batch1 = _mk_events(20, start_seq=0)
    publish_events(batch1, mqtt_broker.host, mqtt_broker.port)
    session.run_until_idle(idle_timeout=3)
    assert session.total_received == 20

    # Simulate an outage: the ingester disconnects (client_id/session persist
    # at the broker) while more data is published.
    session.disconnect()
    batch2 = _mk_events(15, start_seq=20)
    publish_events(batch2, mqtt_broker.host, mqtt_broker.port)

    # Reconnect with the same persistent session - broker redelivers the
    # queued messages published during the outage.
    session.connect()
    session.run_until_idle(idle_timeout=6, expected_total=35)

    try:
        assert session.total_received == 35, (
            f"expected no loss across reconnect, got {session.total_received}/35 "
            "- if this ever fails, the policy is: loss must be reported via "
            "the ingest_log total falling short of the publisher's count, "
            "never silently absorbed"
        )
        stats = session.ing.stats
        assert stats.get("accepted", 0) == 35
        assert session.con.execute("SELECT count(*) FROM count_ticks").fetchone()[0] == 35
        assert session.con.execute(
            "SELECT sum(good_delta) FROM count_ticks"
        ).fetchone()[0] == 35
    finally:
        session.disconnect()
        session.close()
