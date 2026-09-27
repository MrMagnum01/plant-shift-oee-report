"""
MQTT -> DuckDB ingester with strict validation.

Documented policy (also in README.md):
  - Every message received gets exactly one row in ingest_log, tagged with
    a category. Nothing is silently dropped - messages that fail validation
    are quarantined with their raw payload preserved, never discarded.
  - Categories: accepted, accepted_late, duplicate, out_of_order_rejected,
    alarm_out_of_order_rejected, unknown_tag, bad_payload.
  - Duplicate: a message whose (seq) has already been ingested (accepted,
    accepted_late, out_of_order_rejected or alarm_out_of_order_rejected) is
    a duplicate: logged, not re-applied to fact tables.
  - Late data policy: a message whose ts is older than the newest ts already
    accepted for that tag, by at most LATE_GRACE_SECONDS, is accepted late
    (is_late=true) and still written to the fact table. Older than that
    grace window, it is out_of_order_rejected: logged with raw payload, not
    written to the fact table (the tag's timeline is treated as closed that
    far back).
  - Alarm sequencing: for a given (machine, alarm_code), phases must
    strictly alternate RAISE, CLEAR, RAISE, CLEAR, ... An ALARM message
    whose phase does not follow the last accepted phase for that
    (machine, alarm_code) - e.g. two RAISEs in a row, or a CLEAR with no
    open RAISE - is classified alarm_out_of_order_rejected: logged with raw
    payload, not written to alarm_events. This guarantees report.py's
    alarm summary (which pairs each RAISE with the next event as its CLEAR)
    is always reading well-formed alternating pairs.
  - Clock gap: when an accepted message's ts is more than
    CLOCK_GAP_THRESHOLD_SECONDS after the previous accepted ts for that tag,
    a row is recorded in clock_gaps. This does not affect the message's own
    category - it is a monitoring signal about the gap that preceded it.

Durability and crash recovery:
  - The accepted-log row (ingest_log) and its fact row (state_events /
    count_ticks / alarm_events), and the clock-gap row when one applies,
    are always written inside the same open DuckDB transaction and are
    committed together - never split across a commit boundary, including
    at COMMIT_BATCH_SIZE batch boundaries. A message is either fully
    durable (log + fact + any clock gap) or not durable at all.
  - A hard process crash (e.g. os._exit, SIGKILL) before a batch's COMMIT
    loses only the uncommitted tail of the batch, cleanly (DuckDB rolls
    back the open transaction on reopen) - it never leaves a log row
    without its fact row or vice versa. This is tested for an abrupt
    same-process termination mid-batch; it is not a guarantee about the
    MQTT broker's own durability, which is documented separately in
    MqttIngestSession.
  - Restart replay safety: on construction, Ingester reconstructs seen
    sequence numbers and per-tag watermarks (max accepted ts) directly from
    the DuckDB tables already committed, rather than starting from empty
    in-memory state. Replaying the same already-committed messages after a
    restart is therefore recognised as duplicates/late, not double-counted.
    This is specific to this demo's single synthetic stream's sequence
    numbering (see events.py) - it does not by itself make sequence numbers
    safe to reuse across multiple independent publishers or days.
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

# schema.sql: good_delta/reject_delta are INTEGER (32-bit signed); seq is
# BIGINT (64-bit signed). Reject out-of-range values at the grammar stage
# so they are classified as bad_payload instead of raising a DB conversion
# error at insert time.
MAX_COUNT_DELTA = 2_147_483_647
SEQ_MIN = -(2**63)
SEQ_MAX = 2**63 - 1

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

SEEN_SEQ_CATEGORIES = (
    "accepted",
    "accepted_late",
    "out_of_order_rejected",
    "alarm_out_of_order_rejected",
)


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


def _safe_log_fields(msg) -> tuple[str | None, str | None, int | None, str | None]:
    """Extract (tag, msg_type, seq, ts_raw) for logging, coercing anything
    that isn't the expected scalar type to None. ingest_log's columns are
    typed (seq BIGINT, tag/msg_type/ts_raw TEXT); a malformed payload can
    put a list/dict in any of these fields, and binding that straight to a
    typed column raises a DB conversion error instead of being classified
    as a rejection. The raw payload (logged verbatim separately) is never
    lost - only these four convenience columns are sanitised."""
    if not isinstance(msg, dict):
        return None, None, None, None
    tag = msg.get("tag")
    tag = tag if isinstance(tag, str) else None
    msg_type = msg.get("type")
    msg_type = msg_type if isinstance(msg_type, str) else None
    seq = msg.get("seq")
    seq = seq if (isinstance(seq, int) and not isinstance(seq, bool) and SEQ_MIN <= seq <= SEQ_MAX) else None
    ts_raw = msg.get("ts")
    ts_raw = ts_raw if isinstance(ts_raw, str) else None
    return tag, msg_type, seq, ts_raw


def _validate_grammar(msg: dict) -> tuple[bool, str]:
    """Strict structural/type grammar check. Returns (ok, reason). Never
    raises - every field is isinstance-checked before it is used in a
    membership test, comparison or range check, so a malformed value
    (list/dict/etc. where a scalar is expected) is always classified here,
    never left to raise later (e.g. unhashable-type from `x in some_set`,
    or a DB conversion error from an out-of-range value)."""
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
    if not (SEQ_MIN <= msg["seq"] <= SEQ_MAX):
        return False, "field seq is out of BIGINT range"
    if "ts" not in msg:
        return False, "missing field ts"
    if _parse_ts(msg["ts"]) is None:
        return False, "field ts is not a valid timezone-aware ISO8601 timestamp"
    if msg["type"] not in KNOWN_TYPES:
        return False, f"unknown type {msg['type']!r}"

    t = msg["type"]
    if t == "STATE":
        state = msg.get("state")
        if not isinstance(state, str) or state not in STATE_VALUES:
            return False, "STATE.state must be one of RUN/IDLE/DOWN"
        reason = msg.get("reason_code")
        if state == "DOWN":
            if not isinstance(reason, str) or not reason:
                return False, "DOWN state requires a non-empty reason_code"
        else:
            if reason is not None:
                return False, "reason_code must be null unless state is DOWN"
    elif t == "COUNT":
        gd, rd = msg.get("good_delta"), msg.get("reject_delta")
        for name, v in (("good_delta", gd), ("reject_delta", rd)):
            if isinstance(v, bool) or not isinstance(v, int) or v < 0 or v > MAX_COUNT_DELTA:
                return False, f"COUNT.{name} must be an integer in [0, {MAX_COUNT_DELTA}]"
        if gd == 0 and rd == 0:
            return False, "COUNT must have a nonzero good_delta or reject_delta"
    elif t == "ALARM":
        if not isinstance(msg.get("alarm_code"), str) or not msg["alarm_code"]:
            return False, "ALARM.alarm_code must be a non-empty string"
        phase = msg.get("phase")
        if not isinstance(phase, str) or phase not in ALARM_PHASES:
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
    alarm_state: dict[tuple[str, str], str] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=dict)
    _uncommitted: int = 0
    _in_txn: bool = False

    def __post_init__(self):
        init_db(self.con)
        self._restore_state()

    def _restore_state(self) -> None:
        """Reconstruct in-memory dedup/watermark/alarm-phase state from what
        is already durably committed in DuckDB, so a fresh Ingester opened
        against an existing DB (e.g. after a restart) does not replay
        already-accepted messages as new accepted ones."""
        seq_rows = self.con.execute(
            "SELECT DISTINCT seq FROM ingest_log WHERE category IN "
            "('accepted','accepted_late','out_of_order_rejected','alarm_out_of_order_rejected') "
            "AND seq IS NOT NULL"
        ).fetchall()
        self.seen_seq = {r[0] for r in seq_rows}

        watermark_rows = self.con.execute(
            """
            SELECT machine, max(ts) FROM (
                SELECT machine, ts FROM state_events
                UNION ALL SELECT machine, ts FROM count_ticks
                UNION ALL SELECT machine, ts FROM alarm_events
            ) GROUP BY machine
            """
        ).fetchall()
        self.tag_state = {}
        for machine, ts in watermark_rows:
            if ts is not None:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                self.tag_state[machine] = _TagState(max_ts=ts)

        alarm_rows = self.con.execute(
            """
            SELECT machine, alarm_code, phase FROM (
                SELECT machine, alarm_code, phase,
                       row_number() OVER (
                           PARTITION BY machine, alarm_code ORDER BY ts DESC, seq DESC
                       ) AS rn
                FROM alarm_events
            ) WHERE rn = 1
            """
        ).fetchall()
        self.alarm_state = {(machine, code): phase for machine, code, phase in alarm_rows}

    def _begin_if_needed(self) -> None:
        if not self._in_txn:
            self.con.execute("BEGIN TRANSACTION")
            self._in_txn = True

    def commit(self) -> None:
        if self._in_txn:
            self.con.execute("COMMIT")
            self._in_txn = False
            self._uncommitted = 0

    def _log(self, category, tag, msg_type, seq, ts_raw, raw_payload, reason) -> int:
        # Begin the transaction (if one isn't already open) *before* this
        # insert, so the log row and any fact row written afterwards for
        # the same message are always part of the same transaction and can
        # never be split by an autocommit of the log row alone.
        self._begin_if_needed()
        row = self.con.execute(
            """INSERT INTO ingest_log
               (received_at, category, tag, msg_type, seq, ts_raw, raw_payload, reason)
               VALUES (now(), ?, ?, ?, ?, ?, ?, ?) RETURNING log_id""",
            [category, tag, msg_type, seq, ts_raw, raw_payload, reason],
        ).fetchone()
        return row[0]

    def _record(self, category: str) -> None:
        """Call exactly once per handled message, after all of its writes
        (log row and any fact/clock-gap row) are issued. Only here - never
        between a log write and its fact write - do we count the message
        and consider committing the batch, so a batch-size commit can never
        land between a log row and its fact row."""
        self.stats[category] = self.stats.get(category, 0) + 1
        self._uncommitted += 1
        if self._uncommitted >= COMMIT_BATCH_SIZE:
            self.commit()

    def handle_raw(self, raw_payload: str) -> str:
        """Process one raw MQTT payload string. Returns the category. Never
        raises for malformed input - every failure mode is classified into
        a category and logged with its raw payload."""
        try:
            msg = json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError):
            self._log("bad_payload", None, None, None, None, raw_payload, "invalid JSON")
            self._record("bad_payload")
            return "bad_payload"

        ok, reason = _validate_grammar(msg)
        tag, msg_type, seq, ts_raw = _safe_log_fields(msg)
        if not ok:
            self._log("bad_payload", tag, msg_type, seq, ts_raw, raw_payload, reason)
            self._record("bad_payload")
            return "bad_payload"

        if tag not in KNOWN_MACHINES:
            self._log("unknown_tag", tag, msg_type, seq, ts_raw, raw_payload, f"tag {tag!r} not in known machine registry")
            self._record("unknown_tag")
            return "unknown_tag"

        if seq in self.seen_seq:
            self._log("duplicate", tag, msg_type, seq, ts_raw, raw_payload, f"seq {seq} already ingested")
            self._record("duplicate")
            return "duplicate"

        ts = _parse_ts(ts_raw)
        st = self.tag_state.setdefault(tag, _TagState())

        if st.max_ts is None or ts >= st.max_ts:
            category = "accepted"
            is_late = False
            prev_max = st.max_ts
            st.max_ts = ts
            gap_row = None
            if prev_max is not None:
                gap = (ts - prev_max).total_seconds()
                if gap > CLOCK_GAP_THRESHOLD_SECONDS:
                    gap_row = (tag, prev_max, ts, gap)
        else:
            delay = (st.max_ts - ts).total_seconds()
            if delay <= LATE_GRACE_SECONDS:
                category = "accepted_late"
                is_late = True
                gap_row = None
            else:
                self.seen_seq.add(seq)
                self._log(
                    "out_of_order_rejected", tag, msg_type, seq, ts_raw, raw_payload,
                    f"ts is {delay:.1f}s older than latest accepted ({LATE_GRACE_SECONDS}s grace window exceeded)",
                )
                self._record("out_of_order_rejected")
                return "out_of_order_rejected"

        if msg_type == "ALARM":
            key = (tag, msg["alarm_code"])
            last_phase = self.alarm_state.get(key)
            phase = msg["phase"]
            expected_next = "RAISE" if last_phase in (None, "CLEAR") else "CLEAR"
            if phase != expected_next:
                self.seen_seq.add(seq)
                self._log(
                    "alarm_out_of_order_rejected", tag, msg_type, seq, ts_raw, raw_payload,
                    f"alarm {tag}/{msg['alarm_code']} phase {phase} out of order "
                    f"(last accepted phase was {last_phase!r}, expected {expected_next!r})",
                )
                self._record("alarm_out_of_order_rejected")
                return "alarm_out_of_order_rejected"

        self.seen_seq.add(seq)
        log_id = self._log(category, tag, msg_type, seq, ts_raw, raw_payload, None)

        if gap_row is not None:
            self._begin_if_needed()
            self.con.execute(
                """INSERT INTO clock_gaps (machine, gap_start, gap_end, gap_seconds, detected_at)
                   VALUES (?, ?, ?, ?, now())""",
                list(gap_row),
            )

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
            self.alarm_state[(tag, msg["alarm_code"])] = msg["phase"]
        self._record(category)
        return category


class MqttIngestSession:
    """Reusable ingester session: one Ingester/DuckDB connection that can be
    connected, disconnected and reconnected to the broker multiple times
    with a fixed client_id and clean_session=False (persistent session).

    Reconnect/no-loss policy: with a persistent session and QoS 1
    subscription, the broker queues messages published while this client is
    disconnected (up to the broker's max_queued_messages) and redelivers
    them on reconnect with the same client_id. That is the mechanism this
    demo relies on for "no loss" across a *disconnect* - it is a property of
    the persistent-session contract, not something the ingester code itself
    guarantees once the broker's queue limit is exceeded.

    This is distinct from process-crash recovery: a disconnect/reconnect
    keeps the same Ingester (in-memory state intact) and is covered by
    tests/test_reconnect.py. A hard process crash tears down the Ingester;
    recovery there relies on Ingester's restart replay safety (durable
    commits + state reconstructed from DuckDB on the next construction),
    not on the broker's persistent session.
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


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="Run the MQTT->DuckDB ingester until idle.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--db", required=True, help="DuckDB file path (parent directory is created if missing)")
    p.add_argument("--topic", default="demo/plant/+/+")
    p.add_argument("--idle-timeout", type=float, default=15.0)
    p.add_argument("--expected-count", type=int, default=None)
    args = p.parse_args()

    Path(args.db).resolve().parent.mkdir(parents=True, exist_ok=True)
    stats = run_mqtt_ingester(
        args.host, args.port, args.db, topic=args.topic,
        idle_timeout=args.idle_timeout, expected_count=args.expected_count,
    )
    print(f"ingest stats: {stats}")


if __name__ == "__main__":
    main()
