-- DuckDB schema (MIT-licensed DuckDB, used as the time-series store).
CREATE SEQUENCE IF NOT EXISTS log_id_seq START 1;

CREATE TABLE IF NOT EXISTS ingest_log (
    log_id        BIGINT PRIMARY KEY DEFAULT nextval('log_id_seq'),
    received_at   TIMESTAMP NOT NULL,
    category      TEXT NOT NULL,   -- accepted | accepted_late | duplicate |
                                    -- out_of_order_rejected | unknown_tag | bad_payload
    tag           TEXT,
    msg_type      TEXT,
    seq           BIGINT,
    ts_raw        TEXT,
    raw_payload   TEXT NOT NULL,
    reason        TEXT,
    content_hash  TEXT  -- sha256 of the canonicalised message - identity for
                          -- duplicate vs seq_conflict classification
);

CREATE TABLE IF NOT EXISTS state_events (
    log_id      BIGINT PRIMARY KEY,
    machine     TEXT NOT NULL,
    ts          TIMESTAMP NOT NULL,
    state       TEXT NOT NULL,
    reason_code TEXT,
    seq         BIGINT NOT NULL,
    is_late     BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS count_ticks (
    log_id        BIGINT PRIMARY KEY,
    machine       TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL,
    good_delta    INTEGER NOT NULL,
    reject_delta  INTEGER NOT NULL,
    seq           BIGINT NOT NULL,
    is_late       BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS alarm_events (
    log_id      BIGINT PRIMARY KEY,
    machine     TEXT NOT NULL,
    ts          TIMESTAMP NOT NULL,
    alarm_code  TEXT NOT NULL,
    phase       TEXT NOT NULL,
    seq         BIGINT NOT NULL,
    is_late     BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS clock_gaps (
    id            BIGINT PRIMARY KEY DEFAULT nextval('log_id_seq'),
    machine       TEXT NOT NULL,
    gap_start     TIMESTAMP NOT NULL,
    gap_end       TIMESTAMP NOT NULL,
    gap_seconds   DOUBLE NOT NULL,
    detected_at   TIMESTAMP NOT NULL
);
