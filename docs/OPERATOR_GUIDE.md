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
# 1. Check the two clusters are reachable and healthy BEFORE you touch anything.
gpstream doctor --source "$GPSTREAM_SOURCE" --dest "$GPSTREAM_DEST"

# 2. See exactly what would happen, with no side effects.
gpstream dry-run --source "$GPSTREAM_SOURCE" --dest "$GPSTREAM_DEST" \
  --db app --schema public --table orders,customers

# 3. Produce a deterministic plan (hash-pinned) you can inspect / diff.
gpstream dry-run --source "$GPSTREAM_SOURCE" --dest "$GPSTREAM_DEST" \
  --db app --plan-file plan.yaml

# 4. Run the copy.
gpstream run --source "$GPSTREAM_SOURCE" --dest "$GPSTREAM_DEST" \
  --plan-file plan.yaml

# 5. Watch progress and final outcome.
gpstream status --job <jobid>
gpstream report --job <jobid> --report-json run.json
```

`dry-run` and `plan` exist so you never surprise a production source. The plan
is **deterministic** (it carries a hash), so the same inputs produce the same
plan — diff two plans to review a change before you run it.

---

## 3. Selecting what to copy

Selection is the first decision on any run:

| Goal | Flag |
|------|------|
| Copy a whole database | `--full` |
| Copy specific DBs | `--db app --db billing` (repeatable) |
| Copy specific schemas | `--schema app.public` (repeatable) |
| Copy specific tables | `--table app.public.orders` (repeatable) |
| Exclude a set | `--exclude app.public.tmp_*` (repeatable) |

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

| Setting | Default | Meaning |
|---------|---------|---------|
| *(default)* | **strict** | A conflict **stops the run**. Nothing overwritten, nothing skipped. |
| `--skip-conflicts` (alias `--skip-existing`) | — | Skip conflicting objects and continue (the old "safe skip" — now an explicit opt-in). |
| `--overwrite` | — | Replace the destination object (staging + atomic rename, verified before the swap). |
| `--truncate` | — | Truncate then reload. |
| `--append` | — | Append rows to the destination object. |
| `--conflict-action` | — | Pick one of the above explicitly. |

The **default `ConflictAction` is `SkipExisting` with `--skip-conflicts` OFF**,
so a bare `gpstream run` **stops on the first conflict** rather than silently
skipping or overwriting. To proceed you opt in: `--skip-conflicts` to keep what's
already there, or `--overwrite`/`--truncate`/`--append` to replace it.

Two further controls protect you from silent clobbering:

- `--skip-conflicts` — **off by default (strict)**. When off, conflicts **stop
  the run**. When on, they are skipped and reported in `conflicts.jsonl`.
- `--overwrite-only-conflicts` — only overwrite the objects that actually conflict.

Also, `--rename-db src=dst`, `--rename-schema src=dst`, and
`--rename-table src=dst` (all `KV` form) remap names into the destination so a
run never lands on a live name it should not touch.

> Rule of thumb: in production, run strict (leave `--skip-conflicts` off), let
> the run stop on the first conflict, review `conflicts.jsonl`, and only then
> re-run with an explicit policy.

---

## 5. Verification

After each unit is streamed and landed, gpstream can verify it:

| Mode | Flag | Behaviour |
|------|------|-----------|
| `full` | `--verify full` | Verify the whole object (checksums). |
| `sample` | `--verify sample` **default** | Verify a sample of units. |
| `off` | `--verify off` | No verification. |

The default is **`sample`**. Each verified unit records a checksum (a unit cannot
be marked completed without one), and the journal persists it, so a resumed run
knows what was already verified. For a critical migration, run `--verify full`.

Consistency options let you copy an object atomically:

| Flag | Meaning |
|------|---------|
| `--consistent-db` | Keep each DB consistent during the copy. |
| `--consistent-cluster` | Keep the whole cluster consistent during the copy. |

---

## 6. Performance knobs you actually turn

| Flag | Default | What it does |
|------|---------|--------------|
| `--rate-limit` | `0` (no ceiling) | Total throughput ceiling in Gbps (accepts `bps`/`mbit`/`gbit` units). |
| `--adaptive-rate-limit` | off | Let gpstream reduce the rate on host overload (see §7). |
| `--workers-per-pair` | `2` | Concurrent units per host-pair. |
| `--data-port-range` | — | Restrict the data plane to a port range, e.g. `1000-2000`. |
| `--compress` | `zstd` | `none` / `zlib` / `zstd`. |
| `--compress zstd:9` | level 3 | zstd level **1–19** (validated). |

Default compression is **`zstd` at level 3**. A higher level compresses more but
uses more CPU — with `--adaptive-rate-limit` on, gpstream will *degrade* the
compression level when the host is CPU-saturated, and that change is observable
as a metric (see §8).

---

## 7. Back-pressure and adaptive rate-limiting

The stream uses **back-pressure**: the source agent blocks when the destination
cannot keep up, so gpstream never buffers the whole table in RAM — only small
in-memory buffers. There is no intermediate disk.

With `--adaptive-rate-limit`, one limiter runs per host-pair. It samples host
load (CPU iowait, disk await/utilisation, TCP retransmits) and:

- **reduces** the rate by a step (~25%, floor ~1 Gbps) on sustained overload;
- **recovers** toward the baseline when load subsides (~10% per healthy interval).

A reduction is a diagnostic message (it carries the triggering metric values), so
"why did the run slow down?" has an answer in the logs and the metrics endpoint.
Typical trigger thresholds: iowait > 30%, disk utilisation > 90%, TCP
retransmits > 100/s.

---

## 8. Watching a run

Run the metrics server alongside a long job:

```bash
gpstream dry-run --plan-file plan.yaml --metrics --metrics-addr :9090
```

The metrics package serves a Prometheus textformat endpoint (`/metrics`) via the
address you pass to `--metrics-addr`. The exposed series include:

| Metric | Type | Meaning |
|--------|------|---------|
| `gpstream_bytes_total` | counter | Bytes transferred (compressed on the wire). |
| `gpstream_bytes_decoded_total` | counter | Logical bytes after decompression. |
| `gpstream_errors_total` | counter | Transfer errors. |
| `gpstream_frame_errors_total` | counter | CRC / framing errors. |
| `gpstream_rate_limit_gbits` | gauge | Current rate limit (Gbps). |
| `gpstream_throughput_bytes` | gauge | Recent throughput (bytes/sec, ~60s window). |
| `gpstream_compression_level` | gauge | Current zstd level. |
| `gpstream_compression_mode` | gauge | Current mode (`none`/`zlib`/`zstd`). |

`gpstream status --job <jobid>` shows the live job state, and
`gpstream report --job <jobid> --report-json run.json` (or `--report-csv` /
`--report-text`) gives a final report you can archive.

### The diagnostic event log (`events.jsonl`)

Every run appends a per-operation diagnostic stream to
`<state-dir>/<operation-id>/events.jsonl` (next to `journal.jsonl` and
`conflicts.jsonl`) — one JSON object per line: `{"ts", "kind", ...fields}`.
It is the operator's audit trail for the run's self-diagnosis (spec F-04/F-14):

| `kind` | When | Key fields |
|--------|------|-----------|
| `rate_limit_shift` | every adaptive rate-limit change (F-14) | `from_gbps`, `to_gbps`, `reason` (cpu %, disk latency, queue) |
| `conflict` | a destination collision is hit (F-04) | `schema`, `table`, `action` = `stop` (strict default) or `skip` (`--skip-conflicts`) |
| `unit_failed` | a unit fails or panics (retried by resume/re-run) | `schema`, `table`, `attempts`, `on_unit_failure` (`continue`/`abort`, spec F-06), `error` |
| `unit_reread` | a resume re-reads a table under the fresh snapshot (T5) | `schema`, `table`, `reason`, `epoch` |

It is a diagnostic log, not the durable record (that is the journal), so a
write failure never fails the copy. With no `--state-dir` journal mirror the
log is a no-op. Consume it with `jq -c . events.jsonl` or any JSONL tooling.

---

## 9. Resuming, cancelling, rolling back

| Command | When to use |
|---------|-------------|
| `gpstream resume --job <jobid>` | A run stopped or crashed; resume only the incomplete units. |
| `gpstream cancel --job <jobid>` | Stop an in-flight run cleanly. |
| `gpstream rollback --job <jobid>` | Roll back a partially-swapped staging table after a bad run. |
| `gpstream cleanup --job <jobid>` | Clear the run's service files after it is done. |

Resume is bounded by `--auto-resume N`. **`N = 0` means no auto-resume**
(the default). On resume, gpstream replays the journal and restarts every unit
not in a terminal state; units already in a terminal state are skipped. A worker
dying **does not abort the whole job** — the manager reassigns the dead
worker's units to a live one. `--auto-cleanup` removes service files
automatically on success.

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
| `running` | In progress. |
| `completed` | Data + checksum verified. |
| **`swapped`** | Completed unit atomically renamed to its final name — **terminal success**. |
| **`failed`** | Unit exhausted its retries — **terminal failure**. |

There is **no `done` state**. A job's terminal outcomes are `swapped` (success)
and `failed` (failure). This matters for `resume`: a unit that is already
`swapped` (or `failed`) will not be re-copied.

---

## 11. Indexes

Indexes are handled separately from the data copy:

| Flag | Meaning |
|------|---------|
| `--background-indexes` | Rebuild indexes in the background as units land. |
| `--index-workers` | Number of background index workers. |
| `--no-indexes` | Skip index work for this run. |
| `--index-priority` | Index priority (e.g. `manual`, `size`). |

A common pattern is `--metadata-only` first (to land the objects), then a
separate `gpstream indexes` / background-index run so index builds do not
contend with the bulk copy.

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
