# gpstream — Resume Semantics (snapshot epochs, re-read rules)

This document explains what a `gpstream resume` actually does: the snapshot
**epoch** model, the **re-read rules** (DESIGN §2.2), and the operator
procedure for a crashed or interrupted job. It is grounded in
`internal/manifest/epoch.go` (T5) and live-verified on the lab
(E3: kill -9 mid-copy → resume → exact counts, no duplicates — see the
checkpoint `28.09 ~02:30 UTC`).

---

## 1. The core idea: snapshot epochs

A copy reads the source under a snapshot (`--consistent-db` pins one
REPEATABLE READ snapshot per database). If the job dies and is resumed,
the fresh run takes a **new** snapshot — the source may have moved on.
Which tables must be re-read, and which can be kept, is decided by the
**epoch** each table was completed under.

- An **epoch** is an integer, per operation, starting at **1**.
- Every **fresh** run (no journal yet) establishes epoch `1`.
- Every **resume** establishes `lastEpoch + 1` — and writes it **before
  any unit is processed**, so the epoch is fixed no matter where the job
  dies next (a crash can never leave an ambiguous "which epoch am I in?").
- Every unit that reaches a verified state is stamped with its epoch
  (`Unit.Epoch`, persisted in the journal, `gpstream_units.epoch`).
- The operation's current epoch is durably mirrored to
  `jobmeta.json` (next to the journal) and the `gpstream_jobs` table, so
  the epoch survives even a partial journal loss. A torn/missing mirror
  is conservative: it forces **one extra re-read**, never data loss.

`gpstream status --operation-id <id>` prints the current `snap-epoch`
and the re-read mode, so an operator can always see which epoch a run is
in.

## 2. The re-read rules (DESIGN §2.2)

At resume time, gpstream recomputes, per table, whether the table's units
were all completed under the **fresh snapshot** or under an older one.
The mode is `--resume-consistency relaxed|strict` (default **relaxed**).

### `relaxed` (default)

| Table state at resume | Decision |
|---|---|
| Completed in **one** epoch, all units terminal | **keep** — re-verify the aggregate, swap. No re-read. |
| Completed across **different** epochs | **re-read** the whole table under the fresh snapshot. |
| Has any **non-terminal** unit | **re-read** the whole table under the fresh snapshot. |
| Already **swapped** | **keep** — its point-in-time is the swap moment; re-swapping would change the timestamp, not the data. |

**Invariant relaxed preserves: table integrity.** Every unit of a kept
table was verified under one snapshot, so the table is internally
consistent — even though different *tables* may be from different
snapshots. This is the safe default: a 1 PB re-read on every failure is
unacceptable, and most jobs only need per-table consistency.

### `strict`

| Table state at resume | Decision |
|---|---|
| Any table with journal records under the fresh snapshot | **re-read every such table** under the fresh snapshot — including already-swapped ones. |

**strict gives: the whole database = one point in time.** At the cost of
a full re-read of everything the operation touched. Sensible for
cross-table consistency requirements (foreign-key-style invariants across
tables), never the default.

### Within a single run (no failure)

Both modes behave identically — there is only one snapshot, so nothing
is re-read. The mode only matters when a job is interrupted and resumed.

Every re-read is also observable: the run appends a `unit_reread` event
(`schema`, `table`, `reason`, `epoch`) to `events.jsonl` (see
`docs/OPERATOR_GUIDE.md` §8), so an operator can audit exactly which tables
were re-read and why.

## 3. Crash recovery: what happens to a half-copied table

This is the part that bites in practice, and it was live-verified (E3):

1. A unit's data is copied into a **destination staging table**
   (`_gs_<...>`), not the final table. The swap (rename to the final
   name) is the last, atomic step.
2. If the process is killed **mid-COPY** (`kill -9`), the COPY's partial
   rows **remain in the staging table** — GPDB 6.x does not roll back a
   killed COPY.
3. On resume, the manager sees the surviving staging table. Because the
   staging table is **scratch for the in-flight copy**, it is
   **cleared (TRUNCATE) before the re-copy** — a re-copy on top of the
   partial rows would double the data and fail verification
   ("want 10000, got 20000"). This fix is `a56be8b`.
4. The re-copied unit is then verified against the fresh snapshot's
   checksum; on success it is swapped. **No duplicates, no gaps.**

So the operator-visible guarantee is:

> **A resumed job completes to the same final counts as an uninterrupted
> run — every table is either kept from a verified single-epoch copy or
> re-read from scratch under the fresh snapshot. Rows are never doubled
> and never lost.**

(E3 live result: t1 killed genuinely mid-copy, staging partial after the
kill, resume re-queued exactly 1 unit, re-run exit 0, final counts exact
t1=100000 / t2=10000 / t3=1000, no leftover staging.)

## 4. Operator procedure

### Resume a crashed job

```bash
# 1. See where it stopped (snap-epoch + per-unit state).
gpstream status --operation-id <id>

# 2. Resume. It replays the journal, re-establishes the epoch (last+1),
#    recomputes the re-read set, and re-runs only the non-terminal /
#    re-read units. Swapped units are never re-copied.
gpstream resume --operation-id <id>

# 3. For cross-table point-in-time consistency, choose strict before
#    resuming (it is a run-level flag):
gpstream resume --operation-id <id> --resume-consistency strict
```

### Auto-resume (no operator in the loop)

`--auto-resume N` (F-07) makes `run` resume itself up to **N** times when
any unit does not reach a terminal state, so a job can complete without a
human re-invoking it. `N = 0` (the default) disables auto-resume — a
failed run then waits for an explicit `resume`.

### Cleanup after a finished (or abandoned) job

```bash
gpstream cleanup --operation-id <id> --yes
```

Removes the operation's state directory (journal, `jobmeta.json`,
checksums). It is destructive — `--yes` is required. `cleanup` does **not**
touch destination data; the destination staging objects of a fully
swapped job are already renamed, and any leftover staging of an
abandoned job must be dropped explicitly (the manager reports how many
dest staging objects a journal recorded).

## 5. Vacuum pressure (the real cost of a long snapshot)

`--consistent-db` holds a REPEATABLE READ snapshot for the whole copy.
On a live source, that snapshot **pins the catalog**: dead tuples from
writes that are newer than the snapshot cannot be vacuumed until the
snapshot is gone. A multi-hour copy of a hot table will accumulate
dead tuples on the **source**.

Mitigations (in order of preference):

1. **Maintenance window** — run the big copies when the source's write
   load is lowest; the snapshot lifetime is shortest.
2. **Watch the source's age** — alert on the source's oldest-xact age /
   dead-tuple ratio while a long job runs (see METRICS.md for the job-side
   gauges; the source-side age is a Greenplum operator metric).
3. **Prefer `relaxed`** — if a job must be resumable, relaxed re-reads
   only the tables that actually need it, so a resume's *fresh* snapshot
   is short-lived (it only has to cover the re-read set, not the whole DB).
4. **Stop the job if the source's age trends up** — a snapshot-age cap is a
   DESIGN §7 risk-mitigation (a job that exceeds it is stopped rather than
   allowed to bloat the source indefinitely); no such flag is implemented in
   v0.1, so today the operator enforces the bound by watching the source age
   and killing the run (resume re-queues the incomplete units).

The design point: the snapshot is **per-database and per-run**, and a
resume's snapshot covers only the re-read set — so the vacuum pressure of
a *resume* is bounded by the size of what must be re-read, not by the
whole operation.

## 6. Honest notes

- The re-read rules and epoch model are unit-tested (the §2.2 rule table,
  fresh→resume epoch, keep-single-epoch, mixed-epochs re-read, strict
  full re-read) and the crash-recovery path is **live-verified** on the
  lab (E3). The 1 PB-scale vacuum-pressure figures are design
  intent; validate on a live stand before quoting them.
- `strict` is a cost trade (full re-read for full point-in-time), not a
  correctness upgrade — `relaxed` is already table-consistent.
- A resume re-reads under a **fresh** snapshot; it is not a
  "continue from the same snapshot" — the old snapshot is gone with the
  dead process.
