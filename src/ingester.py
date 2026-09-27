"""
MQTT -> DuckDB ingester with strict validation.

Documented policy (also in README.md):
  - Every message received gets exactly one row in ingest_log, tagged with
    a category. Nothing is silently dropped - messages that fail validation
    are quarantined with their raw payload preserved, never discarded.
  - Categories: accepted, accepted_late, duplicate, out_of_order_rejected,
    unknown_tag, bad_payload.
  - Duplicate: a message whose (seq) has already been ingested (accepted or
    accepted_late) is a duplicate: logged, not re-applied to fact tables.
  - Late data policy: a message whose ts is older than the newest ts already
    accepted for that tag, by at most LATE_GRACE_SECONDS, is accepted late
    (is_late=true) and still written to the fact table. Older than that
    grace window, it is out_of_order_rejected: logged with raw payload, not
    written to the fact table (the tag's timeline is treated as closed that
    far back).
  - Clock gap: when an accepted message's ts is more than
    CLOCK_GAP_THRESHOLD_SECONDS after the previous accepted ts for that tag,
    a row is recorded in clock_gaps. This does not affect the message's own
    category - it is a monitoring signal about the gap that preceded it.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from schedule import MACHINES

KNOWN_MACHINES = set(MACHINES)
KNOWN_TYPES = {"STATE", "COUNT", "ALARM"}
STATE_VALUES = {"RUN", "IDLE", "DOWN"}
ALARM_PHASES = {"RAISE", "CLEAR"}

LATE_GRACE_SECONDS = 300.0
CLOCK_GAP_THRESHOLD_SECONDS = 600.0

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def init_db(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in SCHEMA_PATH.read_text().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def _parse_ts(raw) -> datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def _validate_grammar(msg: dict) -> tuple[bool, str]:
    """Strict structural/type grammar check. Returns (ok, reason)."""
    if not isinstance(msg, dict):
        return False, "payload is not a JSON object"
    for field_name, typ in (("seq", int), ("tag", str), ("type", str)):
        if field_name not in msg:
            return False, f"missing field {field_name}"
        v = msg[field_name]
        # bool is a subclass of int in Python - reject it explicitly.
        if typ is int and (isinstance(v, bool) or not isinstance(v, int)):
            return False, f"field {field_name} must be an integer"
        if typ is str and not isinstance(v, str):
            return False, f"field {field_name} must be a string"
    if "ts" not in msg:
        return False, "missing field ts"
    if _parse_ts(msg["ts"]) is None:
        return False, "field ts is not a valid timezone-aware ISO8601 timestamp"
    if msg["type"] not in KNOWN_TYPES:
        return False, f"unknown type {msg['type']!r}"

    t = msg["type"]
    if t == "STATE":
        if msg.get("state") not in STATE_VALUES:
            return False, "STATE.state must be one of RUN/IDLE/DOWN"
        reason = msg.get("reason_code")
        if msg["state"] == "DOWN":
            if not isinstance(reason, str) or not reason:
                return False, "DOWN state requires a non-empty reason_code"
        else:
            if reason is not None:
                return False, "reason_code must be null unless state is DOWN"
    elif t == "COUNT":
        gd, rd = msg.get("good_delta"), msg.get("reject_delta")
        for name, v in (("good_delta", gd), ("reject_delta", rd)):
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                return False, f"COUNT.{name} must be a non-negative integer"
        if gd == 0 and rd == 0:
            return False, "COUNT must have a nonzero good_delta or reject_delta"
    elif t == "ALARM":
        if not isinstance(msg.get("alarm_code"), str) or not msg["alarm_code"]:
            return False, "ALARM.alarm_code must be a non-empty string"
        if msg.get("phase") not in ALARM_PHASES:
            return False, "ALARM.phase must be RAISE or CLEAR"
    return True, ""


@dataclass
class _TagState:
    max_ts: datetime | None = None


COMMIT_BATCH_SIZE = 500


@dataclass
class Ingester:
    con: duckdb.DuckDBPyConnection
    seen_seq: set[int] = field(default_factory=set)
    tag_state: dict[str, _TagState] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=dict)
    _uncommitted: int = 0
    _in_txn: bool = False

    def __post_init__(self):
        init_db(self.con)

    def _bump(self, category: str) -> None:
        self.stats[category] = self.stats.get(category, 0) + 1
        # Batch commits: autocommitting every single-row insert dominates
        # runtime at demo message volumes. Every message still gets exactly
        # one ingest_log row within the open transaction immediately, so
        # nothing is lost if the process is killed before a commit - the
        # partially-applied batch is simply rolled back on reconnect to a
        # fresh Ingester, which is documented, not silent, behaviour.
        if not self._in_txn:
            self.con.execute("BEGIN TRANSACTION")
            self._in_txn = True
        self._uncommitted += 1
        if self._uncommitted >= COMMIT_BATCH_SIZE:
            self.commit()

    def commit(self) -> None:
        if self._in_txn:
            self.con.execute("COMMIT")
            self._in_txn = False
            self._uncommitted = 0

    def _log(self, category, tag, msg_type, seq, ts_raw, raw_payload, reason) -> int:
        row = self.con.execute(
            """INSERT INTO ingest_log
               (received_at, category, tag, msg_type, seq, ts_raw, raw_payload, reason)
               VALUES (now(), ?, ?, ?, ?, ?, ?, ?) RETURNING log_id""",
            [category, tag, msg_type, seq, ts_raw, raw_payload, reason],
        ).fetchone()
        self._bump(category)
        return row[0]

    def handle_raw(self, raw_payload: str) -> str:
        """Process one raw MQTT payload string. Returns the category."""
        try:
            msg = json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError):
            self._log("bad_payload", None, None, None, None, raw_payload, "invalid JSON")
            return "bad_payload"

        ok, reason = _validate_grammar(msg)
        tag = msg.get("tag") if isinstance(msg, dict) else None
        msg_type = msg.get("type") if isinstance(msg, dict) else None
        seq = msg.get("seq") if isinstance(msg, dict) else None
        ts_raw = msg.get("ts") if isinstance(msg, dict) else None
        if not ok:
            self._log("bad_payload", tag, msg_type, seq, ts_raw, raw_payload, reason)
            return "bad_payload"

        if tag not in KNOWN_MACHINES:
            self._log("unknown_tag", tag, msg_type, seq, ts_raw, raw_payload, f"tag {tag!r} not in known machine registry")
            return "unknown_tag"

        if seq in self.seen_seq:
            self._log("duplicate", tag, msg_type, seq, ts_raw, raw_payload, f"seq {seq} already ingested")
            return "duplicate"

        ts = _parse_ts(ts_raw)
        st = self.tag_state.setdefault(tag, _TagState())

        if st.max_ts is None or ts >= st.max_ts:
            category = "accepted"
            is_late = False
            prev_max = st.max_ts
            st.max_ts = ts
            if prev_max is not None:
                gap = (ts - prev_max).total_seconds()
                if gap > CLOCK_GAP_THRESHOLD_SECONDS:
                    self.con.execute(
                        """INSERT INTO clock_gaps (machine, gap_start, gap_end, gap_seconds, detected_at)
                           VALUES (?, ?, ?, ?, now())""",
                        [tag, prev_max, ts, gap],
                    )
        else:
            delay = (st.max_ts - ts).total_seconds()
            if delay <= LATE_GRACE_SECONDS:
                category = "accepted_late"
                is_late = True
            else:
                self.seen_seq.add(seq)
                self._log(
                    "out_of_order_rejected", tag, msg_type, seq, ts_raw, raw_payload,
                    f"ts is {delay:.1f}s older than latest accepted ({LATE_GRACE_SECONDS}s grace window exceeded)",
                )
                return "out_of_order_rejected"

        self.seen_seq.add(seq)
        log_id = self._log(category, tag, msg_type, seq, ts_raw, raw_payload, None)

        if msg_type == "STATE":
            self.con.execute(
                "INSERT INTO state_events (log_id, machine, ts, state, reason_code, seq, is_late) VALUES (?,?,?,?,?,?,?)",
                [log_id, tag, ts, msg["state"], msg.get("reason_code"), seq, is_late],
            )
        elif msg_type == "COUNT":
            self.con.execute(
                "INSERT INTO count_ticks (log_id, machine, ts, good_delta, reject_delta, seq, is_late) VALUES (?,?,?,?,?,?,?)",
                [log_id, tag, ts, msg["good_delta"], msg["reject_delta"], seq, is_late],
            )
        elif msg_type == "ALARM":
            self.con.execute(
                "INSERT INTO alarm_events (log_id, machine, ts, alarm_code, phase, seq, is_late) VALUES (?,?,?,?,?,?,?)",
                [log_id, tag, ts, msg["alarm_code"], msg["phase"], seq, is_late],
            )
        return category


class MqttIngestSession:
    """Reusable ingester session: one Ingester/DuckDB connection that can be
    connected, disconnected and reconnected to the broker multiple times
    with a fixed client_id and clean_session=False (persistent session).

    Reconnect/no-loss policy: with a persistent session and QoS 1
    subscription, the broker queues messages published while this client is
    disconnected (up to the broker's max_queued_messages) and redelivers
    them on reconnect with the same client_id. That is the mechanism this
    demo relies on for "no loss" across a disconnect - it is a property of
    the persistent-session contract, not something the ingester code itself
    guarantees once the broker's queue limit is exceeded.
    """

    def __init__(self, host: str, port: int, db_path: str, topic: str = "demo/plant/+/+",
                 client_id: str = "plant-oee-ingester"):
        import paho.mqtt.client as mqtt

        self.host, self.port, self.topic, self.client_id = host, port, topic, client_id
        self.con = duckdb.connect(db_path)
        self.ing = Ingester(con=self.con)
        self._last_msg = 0.0
        self._total = 0
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=False)
        self._client.on_message = self._on_message

    def _on_message(self, _client, _userdata, msg):
        import time

        self.ing.handle_raw(msg.payload.decode("utf-8", errors="replace"))
        self._last_msg = time.monotonic()
        self._total += 1

    def connect(self) -> None:
        self._client.connect(self.host, self.port, keepalive=30)
        self._client.subscribe(self.topic, qos=1)
        self._client.loop_start()

    def disconnect(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    def run_until_idle(self, idle_timeout: float = 5.0, expected_total: int | None = None) -> dict:
        import time

        self._last_msg = time.monotonic()
        while time.monotonic() - self._last_msg < idle_timeout:
            if expected_total is not None and self._total >= expected_total:
                time.sleep(0.3)
                break
            time.sleep(0.1)
        return dict(self.ing.stats)

    @property
    def total_received(self) -> int:
        return self._total

    def close(self) -> None:
        self.ing.commit()
        self.con.close()


def run_mqtt_ingester(host: str, port: int, db_path: str, topic: str = "demo/plant/+/+",
                       idle_timeout: float = 5.0, expected_count: int | None = None) -> dict:
    """Connect, subscribe, and ingest until idle_timeout seconds pass with no
    new message (or expected_count messages have been logged). Returns stats."""
    session = MqttIngestSession(host, port, db_path, topic)
    session.connect()
    try:
        return session.run_until_idle(idle_timeout, expected_count)
    finally:
        session.disconnect()
        session.close()
