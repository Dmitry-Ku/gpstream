# gpstream — Failover, Resume & Crash Recovery (v2)

This document describes how gpstream survives a crash mid-copy and how an
operator recovers. Everything below is grounded in the v2 implementation
(`internal/job`, `internal/resume`, `cmd/gpstream/main.go`).

## The core idea: failover *is* resume

gpstream has **no primary/standby role and no cluster switchover**. It is a
streaming copy tool: source (PROD) → network → destination (PREPROD), with
**no intermediate storage** (FR-01). A "failover" in gpstream terms means
**resuming an interrupted job from its journal**, not promoting a standby.
If a worker dies, the job does not abort (FR-06): the manager reassigns the
dead worker's units to live workers, and every unit transition is journaled
so the job can be picked up later.

## The job state machine

Each copied unit (a database / schema / table / partition / index) moves
through the states in `internal/job/job.go`:

| State         | Meaning                                        | Terminal? |
|---------------|------------------------------------------------|-----------|
| `pending`     | not yet started                                | no        |
| `copying`     | data is in flight over the stream            | no        |
| `verifying`   | awaiting checksum verification               | no        |
| `completed`   | data + checksum verified (ADR-001 §4.4)      | **yes**   |
| `swapped`     | completed unit atomically renamed to final   | **yes**   |
| `failed`      | exhausted retries / a genuine fault          | **yes**   |

There is **no `StateDone`**. A unit is "done" only once it reaches
`completed` (data + checksum) and then `swapped` (renamed to its final
name). `failed` is the only terminal failure state. This matters for
resume: **a unit is re-run only if it is not terminal**, and
`completed` / `swapped` / `failed` are the terminal set
(`isTerminal` in `internal/resume/resume.go`).

## Two journal writers — the job survives a lost destination

The operation journal (ADR-001 §4.3) has **two writers**:

1. **Primary record** — a table in the destination service DB,
   `gpstream_units` (`job_id, unit_id, "db", "schema", "table", "part",
   kind, state, checksum, row_count, byte_count, attempts, updated`,
   primary key `(job_id, unit_id)`), created with
   `CREATE TABLE IF NOT EXISTS`.
2. **File mirror** — a local JSON-lines file at `--mirror <path>`.
   When `--mirror` is not given, the copy defaults to
   `/tmp/gpstream_<plan-hash>.mirror`.

The **file mirror is the durable fallback (FR-07)**: if the destination DB
becomes unreachable, the mirror still makes the job resumable. `SetState`
writes the primary record best-effort and always appends one frozen record
to the mirror, so a single changed unit serialises in O(1) — never a full
re-scan (FR-03/FR-07).

## Resume (the primary recovery path)

### `--auto-resume N` on a live copy

Pass `--auto-resume N` to `copy`. If, after the run, any unit did not reach
a terminal state, gpstream loops: resume the incomplete units, re-run the
manager, repeat — up to **N** times. The attempt counter enforces the
limit; once `attempts >= N` the resume manager refuses
(`auto-resume: max attempts (N) reached`).

Default `AutoResume` is **0** (no auto-resume). Set it to, e.g., `3` to let
transient network/CPU faults heal themselves.

### `gpstream resume`

```
gpstream resume --mirror <path> [--auto-resume N]
```

Reopens the journal (the file mirror, optionally the live dest-DB journal)
and re-queues every unit **not** in a terminal state — it **skips** units
already `completed` / `swapped` / `failed` (they are not re-copied). Output:

```
resume: re-queued 4 unit(s) (attempt 1, max 3)
```

or, when everything is already terminal:

```
resume: all units terminal — nothing to resume
```

`--mirror` is **required** for `resume`. `--auto-resume N` bounds how many
times the same job may be re-queued.

## Cancel

```
gpstream cancel --mirror <path> [--job <id>] [--yes]
```

Destructive — requires `--yes`. Marks every **non-terminal** unit
`failed` so the manager will not re-run it, while **leaving terminal units
intact** (the journal is the source of truth, FR-07). Output:

```
cancel: 6 unit(s) marked failed; terminal units preserved
```

Use `cancel` to stop a run you no longer want, while keeping the verified
units it already completed.

## Rollback

```
gpstream rollback --mirror <path> [--job <id>] [--yes]
```

Destructive — requires `--yes`. Runs the crash-recovery sequence
(`CrashRecovery`, FR-07): any unit that was **not** in a terminal state at
crash time (i.e. not verified) is rolled back to `pending` so it will be
re-copied. Completed-but-not-swapped staging objects are the target of
recovery. Output:

```
rollback: 3 unit(s) rolled back to pending
```

This is the **minimal rollback**: only unverified units are touched.
Verified units (`completed` / `swapped`) are left as-is.

## Cleanup

```
gpstream cleanup --mirror <path> [--job <id>] [--yes] [--auto-cleanup]
```

Destructive — requires `--yes`. Removes the job's journal entries (from the
file mirror and, when reachable, the `gpstream_units` table) and its
staging objects. Output:

```
cleanup: removed journal for job "..."
```

`--auto-cleanup` additionally removes orphaned staging objects from a
**previous** job when a new copy starts (mirrors the gpcopy
`--truncate-source-after --yes` model). Destructive actions are gated
behind `--yes` (the spec's cleanup guard).

## Integrity after a crash (verify + swap)

Recovery is only safe because of two guarantees in `runUnit`
(`internal/job/manager.go`):

1. **Verify before swap.** The content checksum is re-derived on both sides
   and compared **without re-copying the table** (FR-02). The verify mode
   is `--verify full|sample|off` (default `sample`; `full` for small or
   critical units). A checksum mismatch sets the unit to `failed` and
   **drops the staging table** — no partially-correct data is ever
   swapped in. `--append` is the one case where an exact source checksum
   cannot match (the final holds existing + source rows), so verify is
   skipped there.
2. **Swap failure is safe.** Data is written to a staging table
   (`StagingName(jobID, unitID)`), verified, reindexed (FR-17), and only
   then **atomically renamed** to its final name. If the swap fails
   *after* a verified copy, the staging table still holds the verified
   data — the run reports the error and a resume retries the swap, so no
   data is lost.

In-place modes (`--truncate` / `--append`) copy into the final table
directly; `--truncate` is idempotent, so a retry re-copies from a clean
table.

## Fault isolation (one bad unit does not kill the batch)

- **Panic in a unit** is a genuine fault (CR-7): the worker recovers it,
  records the unit `failed` (a resume can retry it, FR-06/FR-07), and
  **continues** with the next unit.
- **Panic in the data path** is converted to an error; the other workers
  continue (FR-06).
- **First error is reported only after the in-flight units finish** — the
  pool does not cancel the batch, so a single failure does not abort the
  run.

## Consistency across a crash

With `--consistent-db` / `--consistent-cluster`, gpstream pins a read-only
`REPEATABLE READ` transaction (source read snapshot) **before** any unit is
copied, so every `COPY ... TO STDOUT` of the database reads from the same
point-in-time (FR-02). The snapshot is released only after every unit has
been copied. If a crash happens mid-snapshot, resume re-establishes it; a
snapshot error is non-fatal (the copy proceeds) but is logged so the
operator can see the consistency mode.

## Recommended recovery recipe

```bash
# 1. See where the job stood:
gpstream status  --mirror /tmp/gpstream_<hash>.mirror
gpstream logs    --mirror /tmp/gpstream_<hash>.mirror

# 2. Decide:
gpstream resume  --mirror /tmp/gpstream_<hash>.mirror --auto-resume 3   # re-run incomplete units
gpstream rollback --mirror /tmp/gpstream_<hash>.mirror --yes          # roll unverified -> pending
gpstream cancel  --mirror /tmp/gpstream_<hash>.mirror --yes          # stop, keep terminal units

# 3. After a clean, verified completion, discard the job's footprint:
gpstream cleanup --mirror /tmp/gpstream_<hash>.mirror --yes
```

`gpstream status` prints each unit's current state
(`unit %4d  state  kind  db.schema.table  N B`); `gpstream logs` prints
one line per transition (`timestamp unit %d state kind db.schema.table
attempts=N checksum`). Both require `--mirror`.

## Honest notes

- **Failover is resume, not switchover.** gpstream never promotes a
  standby; recovery is journal-driven and idempotent.
- The file mirror is the durable fallback; the `gpstream_units` primary
  record is best-effort when the destination DB is reachable.
- These are the **real** commands and their **real** output strings as
  shipped in v2; a live cluster is required for an actual copy (the fake
  connection has no data plane, so `copy` never falls back to it).
