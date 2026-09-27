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
  alarms, and a periodic per-machine heartbeat - see "Completeness" below).
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

# in another: simulate a full synthetic day (3 shifts, ~10.3k messages)
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
| `duplicate` | Same `seq` already ingested, with the same content (see below) | no |
| `seq_conflict` | Same `seq` already ingested, but with **different** content (content-hash mismatch) - a conflict, not a duplicate | no, quarantined with raw payload; the original fact row is left untouched |
| `out_of_order_rejected` | Valid, but older than the tag's latest accepted timestamp by more than the grace window | no, quarantined with raw payload |
| `alarm_out_of_order_rejected` | ALARM message whose phase doesn't follow the last accepted phase for that (machine, alarm_code), OR whose ts precedes the last accepted event's ts for that pair - e.g. two RAISEs in a row, a CLEAR with no open RAISE, or a CLEAR timestamped before its RAISE | no, quarantined with raw payload |
| `unknown_tag` | `tag` is not one of the three known machines | no, quarantined |
| `bad_payload` | Invalid JSON, missing/mistyped field, an invalid enum value (state/phase/etc.), or a scalar field (seq/state/phase/good_delta/...) that is the wrong JSON type (list/object) or out of range | no, quarantined |

`type` is one of `STATE`, `COUNT`, `ALARM`, `HEARTBEAT` - all four go
through the same categories and validation above. `HEARTBEAT` carries no
fields beyond `seq`/`ts`/`tag`/`type`; it exists only to prove the machine
was still reporting for `report.py`'s completeness check (see
"Completeness" below), not to convey any state.

**Duplicate vs seq_conflict**: durable event identity is bound to
`(seq, content)`, not `seq` alone. Every accepted/accepted_late/rejected
message has a sha256 content hash stored against its `seq`
(`ingest_log.content_hash`, reconstructed on restart the same way
`seen_seq` is). A later message under an already-seen `seq` is a
`duplicate` only if its content hash matches; different content under the
same `seq` is a `seq_conflict` - logged as evidence, never re-applied, and
the original fact row is never overwritten.

**Alarm sequencing**: for a given `(machine, alarm_code)`, phases must
strictly alternate RAISE, CLEAR, RAISE, CLEAR, ... **and** each event's ts
must not precede the ts of the last accepted event for that same pair - the
ingester enforces both together, so RAISE/CLEAR identity is established at
ingest time by event timestamp, not by receipt/arrival order. It rejects
(`alarm_out_of_order_rejected`) anything that breaks either check, rather
than silently treating the next RAISE as an implicit CLEAR of the previous
one, or pairing a CLEAR with a RAISE it doesn't chronologically belong to.
`report.py`'s alarm summary relies on this: it pairs each RAISE with "the
next event for that (machine, code)" by ts, and is only correct because the
ingester guarantees every ts-ordered, phase-alternating sequence it admits
is already in true RAISE/CLEAR pairing order. A RAISE with no following
CLEAR (either genuinely still open, or its CLEAR was rejected) is reported
as an explicit `open_count`, not folded into an indistinguishable
zero-duration pair.

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

**Write-failure recovery**: a message's in-memory dedup/watermark/alarm
state is updated together with its durable writes, but if any of those
writes raises (e.g. the fact `INSERT` itself fails) *after* the in-memory
update, `handle_raw` rolls back the open DuckDB transaction and rebuilds
**every** piece of in-memory state (`seen_seq`, `seq_hash`, per-tag
watermarks, per-alarm phase state, `stats`) from what DuckDB actually has
committed, then re-raises. This discards the same uncommitted batch tail a
hard crash would discard - never a state that is ahead of the database. A
retry of the same message after such a failure is therefore evaluated
against reality and is accepted, not permanently lost as a phantom
duplicate. Verified in `tests/test_rereview_fixes.py` by injecting a
failure into the `count_ticks` INSERT (directly, and mid-batch after other
messages were already accepted-but-uncommitted) and asserting a retry -
and the batch predecessors - are accepted.

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
computed when the machine's telemetry is proven to have **covered** the
shift window - never inferred from state tiling, and never inferred from
how much a machine reported.

Every machine publishes four message types: `STATE`, `COUNT`, `ALARM`, and
a periodic `HEARTBEAT` (`schedule.HEARTBEAT_INTERVAL_S`, 5 minutes) that
carries no payload beyond identifying the machine - its only job is to
prove the machine was still reporting during DOWN/IDLE stretches, where
`COUNT` stops entirely and `STATE` only fires on a transition. Coverage for
a shift window is the timestamps of these messages, nothing else. A window
is complete only if **all** of:

1. **Start coverage**: there is an observation at or near the window start.
2. **End coverage**: there is an observation within **G** of the window
   end - a *terminal* observation proving the machine was still reporting
   through the close of the window, not just at some point inside it.
3. **No internal gap**: no gap between two consecutive observations inside
   the window exceeds **G**.

**G = 600 seconds (10 minutes)** - `report.COVERAGE_GAP_SECONDS`, which is
literally the same constant as the ingester's own clock-gap monitoring
threshold (`ingester.CLOCK_GAP_THRESHOLD_SECONDS`, see "Ingest validation
policy" above), reused rather than duplicated: one definition of "too long
without hearing from a machine". The 5-minute heartbeat cadence is
comfortably below G (a single missed heartbeat still leaves a gap under G)
and comfortably below every DOWN/IDLE segment length in the schedule
(shortest is 10 minutes), so a genuinely-reporting machine always clears
all three checks.

This deliberately replaces two earlier, broken proxies for coverage that
this demo shipped with and Astra's review caught:

- **STATE "tiling" alone**: `_state_seconds()` extends a STATE event's
  segment to the next STATE event, or to the shift's end if there is no
  next one (`lead()`/`COALESCE` in the SQL) - so a single STATE report
  (RUN, DOWN, *or* IDLE) and nothing else "tiles" the entire window on its
  own, reporting a numerically "full" duration with zero evidence anything
  was heard from after that one message. `run_seconds`/`down_seconds`/
  `idle_seconds` are still computed this way for display, but this number
  is no longer used to decide `complete` - only observed messages are.
- **COUNT magnitude/fraction thresholds**: an earlier version required
  counted units to reach some fraction of an ideal-cycle estimate. That
  measures production, not coverage, and cuts both ways: a burst of counts
  followed by silence would have passed it, and a fully-covered but
  genuinely slow/low-output shift would have failed it. Neither is a
  telemetry-coverage claim. Count magnitude establishes nothing about
  coverage now, in either direction - low genuine production on a
  fully-observed window is reported as low production, not withheld as
  "incomplete".

If coverage fails - e.g. an empty database, a machine that never reported,
or a single STATE/COUNT message with no later telemetry proving the window
was covered through to its end - the derived fields are withheld (`null`
in the JSON) rather than computed from an unproven claim and reported as an
ordinary-looking zero. `data["completeness"]` gives the whole-report
rollup (`all_complete` plus a list of the specific incomplete machine/shift
rows, each with `start_ok`/`end_ok`/`gaps_ok`, `max_internal_gap_seconds`,
`observation_count`, and a reason naming exactly which check(s) failed),
and the rendered HTML banners the report and marks each incomplete cell
"incomplete" instead of a number. See `report.py`'s module docstring and
`_coverage()` for the exact check.

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
- `tests/test_broker_ownership.py` - `start_broker`/`stop_broker` refuse to
  touch a same-name container they don't own, and clean up only their own
  same-run leftover.
- `tests/test_rereview_fixes.py` - the five items from Astra's rereview
  (write-failure dedup-state rollback and retriable replay, seq_conflict
  vs duplicate classification including across a restart, one STATE
  message with no later telemetry staying incomplete under the
  observed-coverage rule, alarm ts-regression rejection with a
  well-ordered-pair regression check, and start_broker refusing a
  same-name container from a different run).
- `tests/test_completeness_coverage.py` - the observed-coverage completeness
  rule from Astra's second recheck (state tiling and count magnitude are
  not coverage): a single RUN/DOWN/IDLE state with no later telemetry stays
  incomplete on every shift; an inflated COUNT delta with no terminal
  observation does not establish coverage; the fully-observed synthetic
  fixture (with heartbeats) stays complete with its exact measured
  reconciliation; and a gap inside a window wider than G breaks coverage
  even with earlier and later messages present.

Run everything: `python3 -m pytest -v` (takes roughly 5-6 minutes; the full
synthetic day is ~10,275 MQTT messages, including periodic per-machine heartbeats).

## Container ownership and run isolation

`src/broker.py` labels every container it creates with two labels: a
project-ownership label (`com.plant-oee-demo.owner`) and a per-run id
label (`com.plant-oee-demo.run-id`, one fresh UUID generated per Python
process/import - `broker.RUN_ID`). `start_broker()`'s handling of a
same-name collision:

- **Not project-labelled** (belongs to something else on the host): left
  untouched, `RuntimeError` raised. Unchanged from before.
- **Project-labelled, same run-id** (this exact process called
  `start_broker()` again without stopping first - e.g. crash recovery
  within one run): treated as this run's own stale leftover and replaced.
- **Project-labelled, different run-id** (a different, possibly still
  active, run of this same demo happened to use the same name): **refused**
  with a `RuntimeError`, never force-removed. This is the fix for the
  narrowed finding: a fixed default name previously let a second run's
  `start_broker()` silently kill a first run's still-active container.

**Narrowed claim, not "never touches any other container of this demo
under any circumstances"**: two runs that both explicitly pass the *same*
non-default `name` are still, by design, refused from colliding (the
run-id check above) - they will never race to remove each other, but the
second one to start will fail loudly rather than get its own broker. True
concurrency isolation requires distinct names, which is why
`broker.unique_name()` (a name suffixed with a fresh UUID) exists and is
what `tests/conftest.py`'s `mqtt_broker` fixture always uses - every test
run gets its own name and never depends on the collision-handling logic
above at all. The CLI (`python3 src/broker.py` / `... stop`) still uses the
fixed default `CONTAINER_NAME` across its two separate invocations by
design (so the `stop` subcommand, run as a second process with no state
passed to it, can find what `start_broker()` created) - this is a
single-instance-at-a-time contract for manual CLI use, not a claim that
concurrent manual CLI runs are supported. Verified in
`tests/test_broker_ownership.py` (unowned and same-run-leftover cases) and
`tests/test_rereview_fixes.py::test_start_broker_refuses_a_same_name_container_from_a_different_run`.

## Self-check performed before delivery

- Clean checkout: fresh clone, fresh venv, `pip install -r requirements.txt`,
  full test suite, full manual pipeline run including this README's own
  Quickstart commands verbatim.
- `gitleaks` run over the working tree and full git history.
- Grepped for owner/host/employer/industry/site strings - none present;
  the line, machines, tags and reason codes are all invented for this demo.
- Podman: this repo's own container(s) are labelled with an ownership tag
  and a per-run id tag (see "Container ownership and run isolation"
  below); only a container carrying both this project's ownership label
  AND this exact run's id is ever force-removed as a "stale leftover" -
  same-name, different-owner is refused, and same-name/same-owner but a
  *different* run's id is also refused (not silently killed). `podman ps
  -a` is checked before and after - no leftover containers from this repo,
  and no other container on the host is touched.

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
- No concurrent-manual-CLI-runs support: `python3 src/broker.py` /
  `... stop` use a fixed default container name across their two separate
  invocations by design; two concurrent runs that both rely on that
  default will correctly refuse to collide (see "Container ownership and
  run isolation" above) rather than race, but only one of them gets a
  broker. Use `broker.unique_name()` (as the test suite does) for genuine
  concurrent isolation.
