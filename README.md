# Plant shift OEE report (synthetic demo)

**Role:** Synthetic portfolio demonstration, implemented with AI coding agents and independently reviewed by a separate AI reviewer. No client data or client work.

An MQTT -> DuckDB -> HTML pipeline that turns machine state/count/alarm
telemetry from a **fictional, generic** packaging line into a shift OEE
(Overall Equipment Effectiveness) report. Everything - the line, the tags,
the schedule, the numbers - is synthetic and hand-authored for this repo.
Nothing here is derived from, or resembles, any specific employer's process,
schema or site.

## What's in the box

- **`src/schedule.py`** - the one hand-authored source of truth: a fictional
  three-station line ("Line A: filler -> capper -> labeller"), 24h of
  deterministic state segments (RUN/IDLE/DOWN + reason codes) and alarms for
  three 8h shifts, on a made-up synthetic date (2024-01-01).
- **`src/events.py`** - turns the schedule into a single, globally
  sequence-numbered MQTT event stream (state changes, production ticks,
  alarms).
- **`src/simulate.py`** - publishes that event stream over MQTT.
- **`src/ingester.py`** - subscribes, validates strictly, and writes to
  DuckDB. See "Ingest validation policy" below.
- **`src/report.py`** - reads DuckDB and publishes the shift report
  (`report.html` + `report.json`) atomically.
- **`src/ground_truth.py`** - computes the exact planted OEE/downtime/alarm
  figures straight from `schedule.py`, independently of the MQTT/DuckDB
  pipeline. This is the oracle `tests/test_reconciliation.py` checks the
  report against.
- **`src/broker.py`** - starts/stops the Mosquitto broker in a podman
  container bound to `127.0.0.1` on a random port.

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# start the broker (podman, 127.0.0.1, random port)
python3 src/broker.py            # prints "broker up on 127.0.0.1:<port>"

# in one terminal: ingest
python3 -c "from src.ingester import run_mqtt_ingester; \
  print(run_mqtt_ingester('127.0.0.1', <port>, 'out/plant.duckdb', idle_timeout=15))"

# in another: simulate a full synthetic day (3 shifts, ~9.4k messages)
python3 src/simulate.py --port <port>

# generate the report
python3 src/report.py --db out/plant.duckdb --out out/report.html

python3 src/broker.py stop
```

Or just run the test suite, which drives the whole pipeline end to end:

```bash
python3 -m pytest
```

## OEE formulas (documented, not implied)

- Planned Production Time = shift duration (8h). This demo does not model a
  separate scheduled-break allowance; the whole shift is planned time.
- Run Time = seconds the machine's state was RUN within the shift.
- Downtime = Planned Production Time - Run Time (both DOWN and IDLE count as
  downtime; there is no separate "minor stop" category distinct from
  Availability loss in this demo).
- **Availability** = Run Time / Planned Production Time
- **Performance** = (Good Count + Reject Count) x Ideal Cycle Time / Run Time
- **Quality** = Good Count / (Good Count + Reject Count)
- **OEE** = Availability x Performance x Quality

**OEE here is computed on synthetic, planted data using the formulas above,
to demonstrate the pipeline's mechanics. It is not a benchmark, and no claim
is made about any real production line's performance.**

## Ingest validation policy

Every MQTT message the ingester receives gets exactly one row in
`ingest_log`, tagged with a category. Nothing is silently dropped -
quarantined messages keep their raw payload so they can be inspected later.

| Category | Meaning | Written to fact table? |
|---|---|---|
| `accepted` | Passed grammar/type/tag checks, in chronological order for its tag | yes |
| `accepted_late` | Valid, but older than the tag's latest accepted timestamp by no more than the late-grace window (5 min) | yes, flagged `is_late` |
| `duplicate` | Same `seq` already ingested | no |
| `out_of_order_rejected` | Valid, but older than the tag's latest accepted timestamp by more than the grace window | no, quarantined with raw payload |
| `unknown_tag` | `tag` is not one of the three known machines | no, quarantined |
| `bad_payload` | Invalid JSON, missing/mistyped field, or an invalid enum value (state/phase/etc.) | no, quarantined |

A **clock gap** is recorded in `clock_gaps` (separately from the category
above) whenever an accepted message's timestamp is more than 10 minutes
after the previous accepted timestamp for that tag - a monitoring signal
about the gap, not a rejection of the message that closed it.

**Known-total reconciliation**: `sum(count(*) group by category from
ingest_log)` always equals the number of messages the ingester received -
verified in `tests/test_ingester_validation.py::test_known_total_reconciliation`
and, at full pipeline scale, in `tests/test_reconciliation.py`.

## Reconnect / no-loss policy

The ingester uses a fixed MQTT client ID with `clean_session=False` and QoS 1
subscriptions. Mosquitto queues messages published while the ingester is
disconnected (up to the broker's configured queue depth,
`max_queued_messages 200000` in `scripts/mosquitto.conf`) and redelivers them
on reconnect under the same client ID. `tests/test_reconnect.py` disconnects
the ingester mid-stream, publishes more data, reconnects, and asserts every
message arrived exactly once. If a real deployment's outage ever exceeded the
broker's queue depth, the documented behaviour is: report the loss (the
`ingest_log` total will fall short of the publisher's count) rather than
silently absorb it - this demo does not claim unlimited-duration lossless
buffering.

## Atomic publish

`report.py` writes the HTML and JSON report to temp files in the destination
directory and moves them into place with `os.replace` (atomic rename on the
same filesystem), so a reader of the published path always sees either the
previous complete report or the new one, never a partial write.

Report generation itself reads DuckDB through one read-only connection
wrapped in a single transaction, after ingestion for the reported window has
finished - this is a "generate after the shift closes" report, not a
live dashboard; it does not claim a coherent snapshot against a
concurrently-writing ingester.

## Tests

- `tests/test_ingester_validation.py` - every validation category, unit
  level, no broker needed.
- `tests/test_reconciliation.py` - full broker -> simulate -> ingest ->
  report pipeline, reconciled exactly to the planted ground truth (state
  seconds, good/reject counts, availability/performance/quality/OEE to
  1e-9, downtime Pareto, alarm summary), plus an atomic-publish check.
- `tests/test_reconnect.py` - disconnect/reconnect with no message loss.

Run everything: `python3 -m pytest -v` (takes roughly 2-3 minutes; the full
synthetic day is ~9,400 MQTT messages).

## Self-check performed before delivery

- Clean checkout: fresh clone, fresh venv, `pip install -r requirements.txt`,
  full test suite, full manual pipeline run.
- `gitleaks` run over the working tree and full git history.
- Grepped for owner/host/employer/industry/site strings - none present;
  the line, machines, tags and reason codes are all invented for this demo.
- Podman: only this repo's own `plant-oee-demo-mosquitto` container is
  created and removed; `podman ps -a` is checked before and after.

## What this demo does not claim

- No real plant, employer schema, or production process is represented.
- No industry benchmark or comparative performance claim.
- No live dashboard / concurrent-write coherent-read guarantee - the report
  is a post-shift batch report.
- No unbounded-duration lossless buffering across a broker outage - loss
  beyond the broker's queue depth is reported, not silently absorbed.
