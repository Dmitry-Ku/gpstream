# gpstream — Failover, Resume & Crash Recovery

This document describes how gpstream survives a crash mid-copy and how an
operator recovers. Everything below is grounded in the current implementation
(`internal/manifest`, `internal/manifest/resume`, `cmd/gpstream/main.go`).

## The core idea: failover *is* resume

gpstream has **no primary/standby role and no cluster switchover**. It is a
streaming copy tool: source (PROD) → network → destination (PREPROD), with
**no intermediate storage** (F-01). A "failover" in gpstream terms means
**resuming an interrupted operation from its journal**, not promoting a
standby. If a worker dies, the operation does not abort (F-06): the manager
reassigns the dead worker's units to live workers, and every unit transition
is journaled so the operation can be picked up later.

## The unit state machine

Each copied unit (a database / schema / table / partition / index) moves
through the states in `internal/manifest/job.go`:

| State         | Meaning                                        | Terminal? |
|---------------|------------------------------------------------|-----------|
| `pending`     | not yet started                                | no        |
| `copying`     | data is in flight over the stream              | no        |
| `verifying`   | awaiting checksum verification                 | no        |
| `completed`   | data + checksum verified (ADR-001 §4.4)        | **yes**   |
| `swapped`     | completed unit atomically renamed to final     | **yes**   |
| `failed`      | exhausted retries / a genuine fault            | **yes**   |
| `skipped`     | deliberately not copied (conflict skip / identical overwrite) | **yes** |

There is **no `StateDone`**. A unit is "done" only once it reaches
`completed` (data + checksum) and then `swapped` (renamed to its final
name). `failed` is the only terminal failure state. This matters for
resume: **a unit is re-run only if it is not terminal**, and
`completed` / `swapped` / `failed` / `skipped` are the terminal set
(`isTerminal` in `internal/manifest/resume/resume.go`).

## Two journal writers — the operation survives a lost destination

The operation journal (ADR-001 §4.3) has **two writers**:

1. **Primary record** — a table in the destination service DB,
   `gpstream_units` (`job_id, unit_id, "db", "schema", "table", "part",
   kind, state, checksum, row_count, byte_count, attempts, epoch, updated`,
   primary key `(job_id, unit_id)`), created with
   `CREATE TABLE IF NOT EXISTS`.
2. **File mirror** — a local JSON-lines file at
   `<state-dir>/<operation-id>/journal.jsonl` (default
   `/var/lib/gpstream/<operation-id>/journal.jsonl`).

The **file mirror is the durable fallback (F-07)**: if the destination DB
becomes unreachable, the mirror still makes the operation resumable. `SetState`
writes the primary record best-effort and always appends one frozen record
to the mirror, so a single changed unit serialises in O(1) — never a full
re-scan (F-03/F-07).

Alongside the journal the operation directory also holds `events.jsonl`
(diagnostic event stream, F-04/F-14), `audit.log` (operator actions, F-13),
`conflicts.jsonl` (F-10), `runstats.json` (heartbeat), and `jobmeta.json`
(snapshot-epoch metadata read by `status`/`resume`).

## Resume (the primary recovery path)

### `--auto-resume N` on a live copy

Pass `--auto-resume N` to `run`. If, after the run, any unit did not reach a
terminal state, gpstream loops: resume the incomplete units, re-run the
manager, repeat — up to **N** times. The attempt counter enforces the limit;
once `attempts >= N` the resume manager refuses
(`auto-resume: max attempts (N) reached`).

Default `AutoResume` is **0** (no auto-resume). Set it to, e.g., `3` to let
transient network/CPU faults heal themselves.

### `gpstream resume`

```
gpstream resume --operation-id <id> [--auto-resume N]
```

Reopens the journal (the file mirror, optionally the live dest-DB journal)
and re-queues every unit **not** in a terminal state — it **skips** units
already `completed` / `swapped` / `failed` (they are not re-copied). Output:

```
resume: operation <id>: re-queued 4 unit(s)
```

or, when everything is already terminal:

```
resume: all units terminal — nothing to resume
```

`--operation-id <id>` is **required** for `resume`. `--auto-resume N` bounds
how many times the same operation may be re-queued. The re-read semantics are
fixed: a table is kept only if it was verified under one snapshot epoch (DESIGN §2.2).

## Cleanup

```
gpstream cleanup --operation-id <id> --yes
```

Destructive — requires `--yes`. Removes the operation's journal directory and
state. `--all-incomplete` removes every operation directory that still has
non-terminal units (no `--operation-id` needed); `--mode drop` ADDITIONALLY
drops the destination-side objects the operation created — surviving staging
tables, renamed-in or gpstream-created final tables, and renamed-in schemas —
derived conservatively from the journal (a pre-existing, conflicted
destination table is never touched), restoring the receiver to its
pre-operation state (F-02). `--mode drop` requires `--dest <connstring>`
(`--ssl-ca` is honoured). Each removal is recorded to `audit.log` **before**
the state dir is removed (F-13).

```
gpstream cleanup --operation-id <id> --mode drop --dest "$DST" --yes
```

## Stopping a run (operator-side cancel)

There is **no `cancel` subcommand.** To stop a run you no longer want, kill
the process (`kill <pid>`); the non-terminal units are left in the journal and
`gpstream resume --operation-id <id>` will re-queue them (or `cleanup` removes
the operation's footprint). Terminal (verified/swapped) units are preserved
either way — the journal is the source of truth (F-07).

## Rollback (operator-side, via resume + cleanup)

There is **no `rollback` subcommand.** The crash-recovery behaviour that an
older "rollback" described is built into `resume`: any unit that was **not**
in a terminal state at crash time (i.e. not verified) is re-queued and
re-copied, and completed-but-not-swapped staging objects are the target of
recovery. To discard a bad run's destination objects entirely, use
`gpstream cleanup --operation-id <id> --mode drop --dest <connstring> --yes`.

## Integrity after a crash (verify + swap)

Recovery is only safe because of two guarantees in `runUnit`
(`internal/manifest/manager.go`):

1. **Verify before swap.** With `--verify on`, after each unit lands the
   content checksum is re-derived on both sides and compared **without
   re-copying the table** (F-02). A checksum mismatch sets the unit to
   `failed` and **drops the staging table** — no partially-correct data is
   ever swapped in. The default is `--verify off` (no content verification;
   the COPY row count is the landed-data check), so a default run reads each
   source table exactly once.
2. **Swap failure is safe.** Data is written to a staging table
   (`StagingName(jobID, unitID)`), verified, reindexed (F-17), and only then
   **atomically renamed** to its final name. If the swap fails *after* a
   verified copy, the staging table still holds the verified data — the run
   reports the error and a resume retries the swap, so no data is lost.

In-place mode (`--resolve-conflicts truncate`) copies into the final table
directly after `TRUNCATE`; it is idempotent, so a retry re-copies from a clean
table.

## Fault isolation (one bad unit does not kill the batch)

- **Panic in a unit** is a genuine fault (CR-7): the worker recovers it,
  records the unit `failed` (a resume can retry it, F-06/F-07), and
  **continues** with the next unit.
- **Panic in the data path** is converted to an error; the other workers
  continue (F-06).
- **First error is reported only after the in-flight units finish** — the
  pool does not cancel the batch, so a single failure does not abort the run.
  `--on-unit-failure continue|abort` selects whether a *failed* unit stops the
  run (`abort`) or lets the rest finish (`continue`, default).

## Consistency across a crash

With `--consistent-db` / `--consistent-cluster`, gpstream pins a read-only
`REPEATABLE READ` transaction (source read snapshot) **before** any unit is
copied, so every `COPY ... TO STDOUT` of the database reads from the same
point-in-time (F-02). The snapshot is released only after every unit has been
copied. If a crash happens mid-snapshot, resume re-establishes it; a snapshot
error is non-fatal (the copy proceeds) but is logged so the operator can see
the consistency mode.

## Recommended recovery recipe

```bash
OP=my-copy          # the --operation-id used for the run

# 1. See where the operation stood:
gpstream status --operation-id "$OP"

# 2. Decide:
gpstream resume --operation-id "$OP" --auto-resume 3        # re-run incomplete units
# (to discard the operation's footprint and dest objects instead:)
gpstream cleanup --operation-id "$OP" --mode drop --dest "$DST" --yes

# 3. After a clean, verified completion, discard the operation's footprint:
gpstream cleanup --operation-id "$OP" --yes
```

`gpstream status --operation-id <id>` prints each unit's current state
(`unit <id>  <state>  <kind>  db.schema.table  <bytes>  epoch=<n>`).

## Honest notes

- **Failover is resume, not switchover.** gpstream never promotes a standby;
  recovery is journal-driven and idempotent.
- The file mirror (`journal.jsonl`) is the durable fallback; the
  `gpstream_units` primary record is best-effort when the destination DB is
  reachable.
- There is **no** `cancel`, `rollback`, or `logs` subcommand — stop the
  process to cancel, `resume` to recover, `cleanup --mode drop` to discard
  destination objects, and `status` / `events.jsonl` for the log.
- These are the **real** commands and their **real** output strings as
  shipped; a live cluster is required for an actual copy (the fake connection
  has no data plane, so `run` never falls back to it).
