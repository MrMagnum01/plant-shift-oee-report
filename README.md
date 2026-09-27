# Plant shift OEE report (synthetic demo)

**Role:** Synthetic portfolio demonstration, implemented with AI coding agents and independently reviewed by a separate AI reviewer. No client data or client work.

An MQTT -> DuckDB -> HTML pipeline that turns machine state/count/alarm
telemetry from a **fictional, generic** packaging line into a shift OEE
(Overall Equipment Effectiveness) report. Everything - the line, the tags,
the schedule, the numbers - is synthetic and hand-authored for this repo.
It was not derived from any specific employer's process, schema or site; the
line, tag names, shifts and formulas are generic and generic-looking by
construction, but this is not a claim that no real plant's setup happens to
resemble them - that provenance cannot be certified.

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
  report against. It is schedule-based, not a fully independent
  reimplementation: it calls the same `events.tick_times()` helper the
  actual event stream is built from to derive expected tick timestamps, so
  reconciliation does not independently verify that one helper - it does
  independently verify everything downstream of it (MQTT transport,
  ingester, DuckDB, report aggregation).
- **`src/broker.py`** - starts/stops the Mosquitto broker in a podman
  container bound to `127.0.0.1` on a random port.

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# start the broker (podman, 127.0.0.1, random port)
python3 src/broker.py            # prints "broker up on 127.0.0.1:<port>"

# in one terminal: ingest (creates the out/ directory if it doesn't exist)
python3 src/ingester.py --host 127.0.0.1 --port <port> --db out/plant.duckdb --idle-timeout 15

# in another: simulate a full synthetic day (3 shifts, ~9.4k messages)
python3 src/simulate.py --port <port>

# generate the report
python3 src/report.py --db out/plant.duckdb --out out/report.html

python3 src/broker.py stop
```

Every `python3 src/<module>.py` invocation above works from a clean checkout
with no `PYTHONPATH` set - running a script adds its own directory (`src/`)
to `sys.path`, which is how `src/broker.py`, `src/simulate.py` and
`src/report.py` already resolved their sibling imports; `src/ingester.py`
now has the same `argparse` CLI entry point (see `Ingester`'s `main()`),
tested exactly as written above in `tests/test_quickstart.py`.

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
| `alarm_out_of_order_rejected` | ALARM message whose phase doesn't follow the last accepted phase for that (machine, alarm_code) - e.g. two RAISEs in a row, or a CLEAR with no open RAISE | no, quarantined with raw payload |
| `unknown_tag` | `tag` is not one of the three known machines | no, quarantined |
| `bad_payload` | Invalid JSON, missing/mistyped field, an invalid enum value (state/phase/etc.), or a scalar field (seq/state/phase/good_delta/...) that is the wrong JSON type (list/object) or out of range | no, quarantined |

**Alarm sequencing**: for a given `(machine, alarm_code)`, phases must
strictly alternate RAISE, CLEAR, RAISE, CLEAR, ... The ingester enforces
this and rejects (`alarm_out_of_order_rejected`) anything that breaks the
alternation, rather than silently treating the next RAISE as an implicit
CLEAR of the previous one. `report.py`'s alarm summary relies on this: it
pairs each RAISE with "the next event for that (machine, code)" and is only
correct because the ingester guarantees that pairing is always well-formed.

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
buffering. This is a same-process disconnect/reconnect (the `Ingester`
object and its in-memory state stay alive); it is not process-crash
recovery, which is covered separately below.

## Durability and crash/restart recovery

Within a batch, the accepted-log row (`ingest_log`) and its fact row
(`state_events`/`count_ticks`/`alarm_events`), and any `clock_gaps` row, are
always written and committed together in one DuckDB transaction - never
split across a batch-commit boundary. A hard process crash before that
commit loses only the still-open (uncommitted) tail of the batch, and loses
it cleanly: never a log row with no matching fact row, or vice versa.
Verified in `tests/test_durability.py` by killing a subprocess with
`os._exit()` mid-batch and inspecting the reopened DB.

On construction, `Ingester` reconstructs its dedup set (seen `seq` values),
per-tag watermarks (max accepted timestamp) and per-alarm phase state
directly from what is already committed in DuckDB, instead of starting from
empty in-memory state. Replaying already-committed messages against a
reopened DB is therefore recognised as duplicate/late, not double-counted.
Verified in `tests/test_durability.py` by committing one message, reopening
the DB with a fresh `Ingester`, and replaying the same message. This is
specific to this demo's single synthetic stream's sequence numbering (see
`events.py`) - it is not a claim that sequence numbers are safe to reuse
across multiple independent publishers or days.

## Atomic publish (per file, not per report-set)

`report.py` writes the HTML and the JSON report **each** to its own temp
file in the destination directory and moves **that file** into place with
`os.replace` (atomic rename on the same filesystem) - so a reader of either
published path *alone* always sees either the previous complete file or the
new complete one, never a partial write.

This is **not** a report-set transaction: the `(html, json)` pair is not
atomic together. If the process fails between the two renames, the
directory can be left with the new HTML and the old JSON (or vice versa). A
reader that needs a verified matching pair should compare the
`generated_at` timestamp embedded in both files (the JSON's
`data["generated_at"]` and the HTML's "Generated at ..." line) and treat a
mismatch as "no consistent pair yet" rather than assume the two always
agree.

Report generation itself reads DuckDB through one read-only connection
wrapped in a single transaction, after ingestion for the reported window has
finished - this is a "generate after the shift closes" report, not a
live dashboard; it does not claim a coherent snapshot against a
concurrently-writing ingester.

## Completeness

Each machine/shift's `availability`/`performance`/`quality`/`oee` are only
computed when its STATE telemetry fully covers the shift window (RUN + DOWN
+ IDLE seconds equal the planned production time, within a small
tolerance). If telemetry is missing or partial for a machine/shift - e.g. an
empty database, or a machine that never reported - those fields are
withheld (`null` in the JSON) rather than computed from a hole in the
timeline and reported as an ordinary-looking zero. `data["completeness"]`
gives the whole-report rollup (`all_complete` plus a list of the specific
incomplete machine/shift rows with a reason), and the rendered HTML banners
the report and marks each incomplete cell "incomplete" instead of a number.

## Tests

- `tests/test_ingester_validation.py` - every validation category, unit
  level, no broker needed, including malformed-input classification
  (list/object values for state/phase/seq, out-of-range COUNT deltas) and
  alarm sequencing.
- `tests/test_reconciliation.py` - full broker -> simulate -> ingest ->
  report pipeline, reconciled exactly to the planted ground truth (state
  seconds, good/reject counts, availability/performance/quality/OEE to
  1e-9, downtime Pareto, alarm summary), plus a per-file atomic-publish
  check.
- `tests/test_reconnect.py` - disconnect/reconnect with no message loss.
- `tests/test_durability.py` - batch-boundary crash durability, restart
  replay safety, XSS-payload escaping in the rendered report, and
  completeness withholding on an empty/partial database.
- `tests/test_quickstart.py` - runs this README's own Quickstart commands
  (verbatim, parsed from this file) against a real broker from a clean
  subprocess, to catch the class of bug where the README and the code
  drift apart.

Run everything: `python3 -m pytest -v` (takes roughly 2-3 minutes; the full
synthetic day is ~9,400 MQTT messages).

## Self-check performed before delivery

- Clean checkout: fresh clone, fresh venv, `pip install -r requirements.txt`,
  full test suite, full manual pipeline run including this README's own
  Quickstart commands verbatim.
- `gitleaks` run over the working tree and full git history.
- Grepped for owner/host/employer/industry/site strings - none present;
  the line, machines, tags and reason codes are all invented for this demo.
- Podman: this repo's own container(s) are labelled with an ownership tag
  and only a container carrying that label is ever force-removed; a
  same-name container without it is left untouched and `start_broker()`
  raises instead. `podman ps -a` is checked before and after - no leftover
  containers from this repo, and no other container on the host is
  touched.

## What this demo does not claim

- No real plant, employer schema, or production process is represented,
  and no claim is made that no real plant's setup happens to resemble this
  synthetic one.
- No industry benchmark or comparative performance claim.
- No live dashboard / concurrent-write coherent-read guarantee - the report
  is a post-shift batch report.
- No unbounded-duration lossless buffering across a broker outage - loss
  beyond the broker's queue depth is reported, not silently absorbed.
- No report-set (HTML+JSON pair) publish transaction - only per-file
  atomicity (see "Atomic publish" above).
- The ground-truth oracle is schedule-based and shares its tick-generation
  helper with the event stream it's checked against - it is not a fully
  independent reimplementation (see "What's in the box" above).
