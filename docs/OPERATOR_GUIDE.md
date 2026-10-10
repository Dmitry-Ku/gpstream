# gpstream — Operator Guide

This guide covers the day-to-day life of a **gpstream** job: the command
workflow an operator runs to move data from a **PROD** Greenplum 6 source
cluster to a **PREPROD** receiver, the flags you actually tune, and how to
watch, resume, cancel, and roll a run back.

It is **not** the flag reference or the configuration reference — see
[CLI_REFERENCE.md](CLI_REFERENCE.md) and [CONFIGURATION.md](CONFIGURATION.md).
This is the *operating* procedure.

---

## 1. What a gpstream job is

gpstream is an open-source Go CLI (module `github.com/gpstream/gpstream`) that
copies data **logically** between two Greenplum 6 clusters over a direct,
high-throughput network. It uses `COPY (binary)` streaming with **no
intermediate storage**: the data path is

```
source segment -> source agent -> [network stream] -> dest agent -> dest segment
```

Nothing is written to local disk in the middle. Only small **service** files
are touched (metadata, journal, checksums, status, checkpoints). gpstream is not
`gpbackup`/`gprestore`/`gpcopy`; it streams directly cluster-to-cluster with
back-pressure and in-memory buffers only.

Because of that, an operator's main job is to keep the **run** healthy, not to
manage a staging directory. If a network or disk problem hits, it shows up as
throughput / rate-limit / error-rate signals, not as a growing temp folder.

---

## 2. The golden path

The safe sequence for any new copy is:

```bash
# 0. Point at the two clusters (flags or the config file; no GPSTREAM_SOURCE/DEST env).
SRC="host=prod-coordinator   port=5432 user=gpstream dbname=postgres"
DST="host=preprod-coordinator  port=5432 user=gpstream dbname=postgres"

# 1. Check the two clusters are reachable and healthy BEFORE you touch anything.
gpstream dry-run --source "$SRC" --dest "$DST"

# 2. See exactly what would happen, with no side effects.
gpstream dry-run --source "$SRC" --dest "$DST" \
  --dbs app --schemas app.public --tables app.public.orders,app.public.customers

# 3. Produce a deterministic plan (hash-pinned) you can inspect / diff.
#    The plan is printed (not persisted — dry-run holds no state).
gpstream dry-run --source "$SRC" --dest "$DST" \
  --dbs app

# 4. Run the copy.
gpstream run --source "$SRC" --dest "$DST" \
  --dbs app --operation-id my-copy

# 5. Watch progress and final outcome.
gpstream status --operation-id my-copy
```

`dry-run` exists so you never surprise a production source. The plan
is **deterministic** (it carries a hash), so the same inputs produce the same
plan — diff two plans to review a change before you run it.

---

## 3. Selecting what to copy

Selection is the first decision on any run:

| Goal | Flag |
|------|------|
| Copy specific DBs | `--dbs app,billing` (comma-separated) |
| Copy specific schemas | `--schemas app.public` (comma-separated) |
| Copy specific tables | `--tables app.public.orders,app.public.customers` (comma-separated) |
| Exclude a set | `--exclude app.public.tmp_*` (comma-separated) |

Scope the **metadata** and **data** independently:

| Goal | Flag |
|------|------|
| Copy schema/DDL only | `--metadata-only` |
| Copy row data only | `--data-only` |

`--metadata-only` and `--data-only` are **mutually exclusive** — set at most one.
A typical pre-migration is `--metadata-only` (get the objects over first), then a
follow-up `--data-only` for the rows.

---

## 4. Conflict handling (the single most important safety control)

The destination may already contain objects. **By default gpstream refuses to
touch them: a conflict stops the run** (FR-10 §8). Nothing is clobbered and
nothing is silently skipped — the copy halts with a conflict error so the
operator decides deliberately.

| `--resolve-conflicts` | Default | Meaning |
|----------------------|---------|---------|
| `stop` | **default** | A conflict **stops the run**. Nothing overwritten, nothing skipped. |
| `skip` | — | Skip conflicting objects and continue; they are reported in `conflicts.jsonl`. |
| `overwrite` | — | Replace the destination object (staging + atomic swap). Always rewrites — an existing destination is re-copied even when its data already matches the source; there is no pre-copy content comparison (it would read the whole table twice). Dependent foreign keys on other destination tables are dropped before the swap and re-applied from the source after the load (F-11). |
| `truncate` | — | Truncate then reload (in-place). |

`--overwrite` and `--truncate` are shorthand opt-ins that set the corresponding
policy. The **default is `stop`**, so a bare `gpstream run` **stops on the
first conflict** rather than silently skipping or overwriting.

Also, `--map-db src=dst`, `--map-schema src=dst`, and `--map-table src=dst`
(all `src=dst` form) remap names into the destination so a run never lands on
a live name it should not touch.

> Rule of thumb: in production, run strict (leave the default `stop`), let the
> run stop on the first conflict, review `conflicts.jsonl`, and only then
> re-run with an explicit `--resolve-conflicts` policy.

---

## 5. Verification

After each unit is streamed and landed, gpstream can verify it:

| Mode | Flag | Behaviour |
|------|------|-----------|
| `on` | `--verify on` **default** | After each unit lands, the content digest is compared on the source and on the destination; a mismatch sets the unit to `failed` (the staging table is dropped). |
| `off` | `--verify off` | Disables the swap gate; the COPY row count is the landed-data check. |

The default is **`on`**. The content digest is computed **inline on the data
path** on both sides (source digest rides the slice trailers; the destination
mini tees the stream into the utility COPY), so **no table is re-read** to
verify — a default run still reads each source table exactly once. Each
verified unit records a checksum and the journal persists it, so a resumed run
knows what was already verified; `--verify off` only disables the swap gate.

Consistency (F-02): the source agent always reads under one run-wide
REPEATABLE READ snapshot per database, so every table is copied from the same
point in time.

---

## 6. Performance knobs you actually turn

| Flag | Default | What it does |
|------|---------|--------------|
| `--rate-limit` | `0` (none) | Baseline for the ETA + adaptive load diagnostics in Gbps (accepts `bps`/`mbit`/`gbit` units). Does not cap the flow — backpressure paces it; enforced ceiling is backlog (PERFORMANCE.md). |
| `--workers` | `2` | Concurrent workers per host-pair. |
| `--compress` | `zstd` | `none` / `zlib` / `zstd`. |
| `--compress zstd:9` | level 3 | zstd level **1–19** (validated). |

Default compression is **`zstd` at level 3**. A higher level compresses more
but uses more CPU. **Auto-degrading the level under CPU saturation is not
implemented yet** (backlog) — the level is static for a run. The
`gpstream_compression_level` / `gpstream_compression_mode` gauges exist and
will report the level once the degrade path is wired in (see §8).

**Transport (two-agent, ADR-002):** the data plane lives in the agents: a
source agent (source cluster) reads the table and pushes a **compressed**
stream over a real, optionally TLS-encrypted socket to a dest agent
(destination cluster); the controller orchestrates (plan, journal, verify,
swap, indexes) and holds **no data connections**. `gpstream_bytes_total` is
the on-wire agent-to-agent volume, compression genuinely reduces
inter-cluster bandwidth, and the
controller is a control-plane trust root only. It requires
the agent binaries resolve by convention (packaged install: all four in `/usr/bin`; override via `GPSTREAM_SOURCE_AGENT_BINARY` / `GPSTREAM_DEST_AGENT_BINARY` for dev builds) —
hosts). See `docs/adr/002-single-controller-data-path.md`.

---

## 7. Back-pressure and adaptive rate-limiting

The stream uses **back-pressure**: the source agent blocks when the destination
cannot keep up, so gpstream never buffers the whole table in RAM — only small
in-memory buffers. There is no intermediate disk.

The adaptive limiter (F-14) is built when a rate-limit is set. It samples
the **controller host's** load (CPU iowait, disk await/utilisation, TCP
retransmits) every second and moves the current rate by AIMD:

- **reduces** the rate by a step (~25%, floor ~1 Gbps) on sustained
  overload;
- **recovers** toward the baseline when load subsides (~10% per healthy
  interval).

Honest status: until the pacer is wired into the data path (backlog), the
limiter is **diagnostic only** — it does not cap the flow, and the metrics
come from the controller host (the copy load sits on the agent hosts). The
destination receive-queue signal (reason `dest-receive-queue`, the
`gpstream_receive_write_latency_ms` gauge) is not populated in the two-agent
path yet. A rate change is a diagnostic message (it carries the triggering
metric values) in `events.jsonl` (`rate_limit_shift`) and the metrics
endpoint, so "how was load trending?" has an answer. Typical trigger
thresholds: iowait > 30%, disk utilisation > 90%, TCP retransmits > 100/s.

---

## 8. Watching a run

Run the metrics server alongside a long job:

```bash
gpstream run --source "$SRC" --dest "$DST" --dbs app \
  --operation-id my-copy --metrics-addr :9090
```

There is **no separate `metrics` subcommand** — the Prometheus textformat
endpoint (`/metrics`) is served *during* `run` via the address you pass to
`--metrics-addr`. The exposed series include:

| Metric | Type | Meaning |
|--------|------|---------|
| `gpstream_bytes_total` | counter | Bytes written to the data transport (compressed; two-agent: the on-wire agent-to-agent socket volume). |
| `gpstream_bytes_decoded_total` | counter | Logical bytes after decompression. |
| `gpstream_errors_total` | counter | Transfer errors. |
| `gpstream_frame_errors_total` | counter | CRC / framing errors. |
| `gpstream_rate_limit_gbits` | gauge | Current rate limit (Gbps). |
| `gpstream_throughput_bytes` | gauge | Recent throughput (bytes/sec, ~60s window). |
| `gpstream_compression_level` | gauge | Current zstd level. |
| `gpstream_compression_mode` | gauge | Current mode (`none`/`zlib`/`zstd`). |

`gpstream status --operation-id <id>` shows the live operation state (per-unit
state, rows, ETA). The final summary is printed on completion and the durable
record is the journal under `<state-dir>/<operation-id>/`.

**Segment balance (F-15, live).** On a Greenplum destination, after each
table is swapped to its final name the run measures the per-segment fill of
the copied data (`SELECT gp_segment_id, count(*) ... GROUP BY`) and prints a
final-report section:

```
segment balance (F-15, live, per destination segment):
  balance public.orders                          max=5082 avg=5050.5 max/avg=1.01 (5082 5019)
  balance public.hotkey                          max=9000 avg=3333.3 max/avg=2.70 (9000 2000 2000)
  WARNING segment balance: public.hotkey max/avg = 2.70 (> threshold 1.50, max=9000 avg=3333.3 over 3 segments)
```

The counts are the per-segment row counts in `gp_segment_id` order; `max/avg`
is the fill-balance ratio (1.0 = perfectly even). A table whose `max/avg`
exceeds the fixed F-15 segment-fill gate (`1.5`,
`dest.DefaultBalanceThreshold`) emits a `WARNING` line and a `warning: true`
`segment_balance` event (spec F-15: "warning on threshold exceeded"). The measurement is non-fatal and Greenplum-only: a plain
PostgreSQL destination has no segments and the section is omitted.

### The diagnostic event log (`events.jsonl`)

Every run appends a per-operation diagnostic stream to
`<state-dir>/<operation-id>/events.jsonl` (next to `journal.jsonl` and
`conflicts.jsonl`) — one JSON object per line: `{"ts", "kind", ...fields}`.
It is the operator's audit trail for the run's self-diagnosis (spec F-04/F-14):

| `kind` | When | Key fields |
|--------|------|-----------|
| `rate_limit_shift` | every adaptive rate-limit change (F-14) | `from_gbps`, `to_gbps`, `reason` (cpu %, disk latency, queue) |
| `compression_off` | a small table is copied with compression off (F-12 auto-disable) | `table`, `bytes` (estimated size), `threshold` (`--small-table-threshold`), `reason` |
| `conflict` | a destination collision is hit (F-04) | `schema`, `table`, `action` = `stop` (default) / `skip` / `overwrite` / `truncate` |
| `unit_failed` | a unit fails or panics (retried by resume/re-run) | `schema`, `table`, `attempts`, `on_unit_failure` (`continue`/`abort`, spec F-06), `error` |
| `unit_reread` | a resume re-reads a table under the fresh snapshot (T5) | `schema`, `table`, `reason`, `epoch` |
| `resume_swap_completed` | a re-run finished the swap of a unit a crashed run left `completed` (data verified in staging, rename never ran) — the destination now has the table (FR-06/FR-07) | `schema`, `table`, `rows` |
| `resume_swap_recorded` | a re-run found the unit's rename already landed but the `swapped` journal write lost (staging gone, final present) — recorded `swapped` without a swap | `schema`, `table`, `reason` |
| `resume_swap_data_lost` | a re-run found a `completed` unit whose staging and final tables are both gone (verified data lost, e.g. an operator cleanup) — the unit is marked `failed` for a re-copy | `schema`, `table`, `reason` |
| `resume_swap_failed` | a re-run's recovered swap itself failed (the verified staging data is kept; the unit is marked `failed` and retried on the next resume) | `schema`, `table`, `error` |
| `sequence_synced` | a table's sequence was recreated on the destination and resynchronised to the source last_value (F-11 post-load) | `table`, `sequence`, `last_value` |
| `sequence_sync_failed` | a sequence recreation/resync step failed (non-fatal; the data is in place) | `table`, `sequence`, `reason`, `error` |
| `segment_balance` | the live segment-fill balance of a copied table was measured on a Greenplum destination (F-15, after the swap) | `table`, `segments`, `max`, `avg`, `max_avg`, `threshold` (fixed 1.5 gate), `warning` (true when `max_avg` exceeds the gate) |
| `collation_diff` | a collation difference was detected in the plan (F-16, at run start) | `level` = `db` (`db`, `lc_collate`, `lc_ctype`) or `column` (`table`, `column`, `source`, `destination`) |

It is a diagnostic log, not the durable record (that is the journal), so a
write failure never fails the copy. With no `--state-dir` journal mirror the
log is a no-op. Consume it with `jq -c . events.jsonl` or any JSONL tooling.

### The action audit log (`audit.log`, F-13)

In parallel, every operator action on the operation is appended to
`<state-dir>/<operation-id>/audit.log` (0600) — one JSON object per line:
`{"ts", "action", ...fields}` (spec F-13 "audit log of actions"):

| `action` | When | Key fields |
|----------|------|-----------|
| `run` | `gpstream run` starts (before any copy) | `operation-id`, `units`, `copy-mode`, `on-unit-failure`, `compression` |
| `run-finished` | `gpstream run` ends | `operation-id`, `units`, `rows`, `skipped`, `failed`, `error` (if any) |
| `resume` | `gpstream resume` | `operation-id`, `requeued`, `attempt` (or `error`) |
| `cleanup` | `gpstream cleanup` — logged **before** the state dir is removed | `operation-id`, `mode` (`state`/`drop`), `units`, `dropped` (objects removed with `--mode drop`) |
| `cleanup_drop` | each destination object removed by `cleanup --mode drop` (F-02) | `operation-id`, `kind` (`staging`/`table`/`schema`), `schema`, `object`, `reason` |
| `cleanup_drop_failed` | a destination object that could NOT be dropped (reported, run continues) | `operation-id`, `kind`, `schema`, `object`, `error` |

`events.jsonl` is what the run did; `audit.log` is what the operator did.
Both are best-effort (a write failure never fails the action).

---

## 9. Resuming, cancelling, rolling back

| Command | When to use |
|---------|-------------|
| `gpstream resume --operation-id <id>` | A run stopped or crashed; resume only the incomplete units. A unit a crashed run left `completed` (data verified in the staging table, but the atomic rename never ran) is **finished** on the next re-run — the staging table is swapped to its final name (the re-run reports it as a completed unit, not a skip). |
| `kill <pid>` (no `cancel` subcommand) | Stop an in-flight run; `resume` re-queues the incomplete units. |
| `gpstream cleanup --operation-id <id> --yes` | Clear the operation's state after it is done. |
| `gpstream cleanup --operation-id <id> --mode drop --dest <conn> --yes` | Also discard the destination objects the run created. |

Resume is bounded by `--auto-resume N`. **`N = 0` means no auto-resume**
(the default). On resume, gpstream replays the journal and restarts every unit
not in a terminal state; units already in a terminal state are skipped. A worker
dying **does not abort the whole job** — the manager reassigns the dead
worker's units to a live one.

What happens to the REST of the job when a unit fails is `--on-unit-failure`
(spec F-06): the default `continue` records the failed unit (journal +
`events.jsonl`) and keeps copying everything else, so the run ends with a full
summary and `gpstream resume --operation-id <id>` re-queues the failed units.
`--on-unit-failure=abort` stops the run at the first failure instead (in-flight
units finish; no new unit starts) — use it when a failed unit usually means
the environment is broken and copying on would only make it worse.

---

## 10. The job state machine

Every unit moves through a small state machine; understanding it tells you
whether a finished run actually **succeeded**:

| State | Meaning |
|-------|---------|
| `pending` | Not yet started. |
| `copying` | In progress. |
| `verifying` | Awaiting checksum verification. |
| `completed` | Data + checksum verified. |
| **`swapped`** | Completed unit atomically renamed to its final name — **terminal success**. |
| **`failed`** | Unit exhausted its retries — **terminal failure**. |
| **`skipped`** | Unit deliberately not copied (a `--resolve-conflicts skip`, or an
  — **terminal,
  nothing to do**. |

There is **no `done` state**. A job's terminal outcomes are `swapped` (success),
`failed` (failure), and `skipped` (deliberately not copied). This matters for
`resume`: a unit already `swapped`, `failed`, or `skipped` will not be
re-copied — `status` counts all three as terminal.

---

## 11. Indexes

Indexes are handled separately from the data copy:

| Flag | Meaning |
|------|---------|
| `--index-workers N` | Number of background index-build workers (F-17, on by default). |
| `--no-indexes` | Skip index creation entirely for this run (master switch: `--index-workers` / `--index-priority` are then ignored, with an advisory naming them). |
| `--index-priority size\|fifo` | Index build order (`size` = largest first, the default; `fifo` = first-ready-first-built). |

Indexes are rebuilt in the **background** as units land (F-17) by default, so
index builds do not contend with the bulk copy. There is **no separate
`indexes` subcommand** — index work is part of `run`.

When a table is renamed (`--map-table`), its index names are rewritten to the
destination table's naming (index names are unique per schema, so the source
names may already be taken by another table on the destination):
`t_m1_pkey` on `t_m1` → `t_m1_demo_pkey` on `t_m1_demo`. An index name with no
`<table>_` prefix gets the destination table name prefixed (`idx_c3` →
`t_m1_demo_idx_c3`).

**Primary keys** are restored in the post-swap constraint pass: the pool
builds only the *secondary* indexes, and the PK's unique index is created
atomically by `ALTER TABLE ... ADD CONSTRAINT <name> PRIMARY KEY` (PostgreSQL
does not adopt a pre-existing standalone unique index there, so the pool must
not build it first). PK constraints are applied **before** foreign keys, so
an FK that references a migrated table's key column finds its unique index.
On a rename the PK constraint keeps the destination table's naming, too
(`t_m1_pkey` → `t_m1_demo_pkey`).

---

## 12. Honest caveats (live stand)

gpstream's unit tests exercise the **fake** SQL/data path. Criteria that need a
**real** Greenplum cluster are **not verified without a live stand**:

- `kill -9` crash recovery,
- `tc netem` network loss / latency,
- disk-fill / full-disk behaviour,
- a genuine 100 Gbps channel,
- a real 100 TB–1 PB copy.

Treat this guide's "safe by default" guarantees as true for the logic that is
unit-tested; for the physical-scale and fault-injection claims, verify on a
representative live cluster before you trust them in production.
