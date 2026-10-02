# gpstream — Doctor & Capability Matrix

`gpstream doctor` (FR-05) is the pre-flight self-diagnostic. It runs a
fixed set of checks against the source and destination clusters and prints
each as `PASS` / `WARN` / `FAIL` with an actionable recommendation. This
document is the **capability matrix**: what each check probes, what each
severity means for your run, and how the segment-direct capability
(`on_segment`) composes from its three prerequisites.

Grounded in `internal/doctor/doctor.go` and
`internal/transfer/segstream` (probe), live-verified on the lab.

---

## 1. The check matrix

| # | Check | Side | Probes | FAIL means | WARN means |
|---|-------|------|--------|-----------|------------|
| 1 | `reachability` | source, dest | `Ping` the coordinator | Cannot connect — verify network, host/port, credentials | — |
| 2 | `version` | source, dest | `SELECT version()` | Query failed (privilege or connection) | — |
| 3 | `segments` | source, dest | `gp_segment_configuration` present? count of `status='u'` | Catalog unreadable, or **0 up segments** (cannot copy) | Not a Greenplum cluster (plain PostgreSQL single-node mode — expected for the kernel PoC) |
| 4 | `route` | src→dst | segment counts S, D; eligible (s,d) pair count; S==D? | — (never fails) | Endpoint is plain PostgreSQL — no route to report (coordinator path only) |
| 5 | `on_segment` | source | the 3-prerequisite probe below (only when `--seg-binary` is supplied) | — (never fails the doctor) | Capability degraded — the job runs on the coordinator path instead |
| 6 | `disk` | dest | disk probe (sentinel in the test bench; live = free space on the dest) | Probe failed / no data | — |
| 7 | `permissions` | source, dest | `has_database_privilege(current_database(), 'CREATE')` / `'CONNECT'` | — | Role lacks CREATE/CONNECT — `GRANT CREATE, CONNECT ON DATABASE` |
| 8 | `collation` | network | `datcollate` source vs dest | — | `LC_COLLATE` differs — affects routing; review before start (FR-10) |
| 9 | `tls` | network | configured minimum TLS version | — | Always informational in v0.1 — provide `--ssl-ca` (CA bundle), and `--ssl-cert`/`--ssl-key` for mTLS (FR-13) |
| 10 | `extensions` | network | `pg_extension` set: source vs dest | — | A source extension is missing in the destination (MADlib/PostGIS/orafce or any other) — install it in the receiver before the data load (FR-05/FR-11) |
| 11 | `column-collation` | network | distinct explicit `COLLATE` names (public schema) source vs `pg_collation` dest | — | An explicit column collation on the source is not installed in the destination — `CREATE COLLATION` in the receiver (or accept the DB default) before the data load (FR-16) |

Overall health: the doctor is **healthy iff there is no FAIL**. WARNs do
not block a run — they degrade an *option* (segment-direct, collation
review) and say so in the recommendation.

### The `column-collation` check (FR-16, column level)

The `collation` check (row 8) compares the **database-level** `LC_COLLATE`
/`LC_CTYPE`. `column-collation` covers the **column level**: it lists the
distinct explicit `COLLATE` names used by the source's public-schema tables
and warns for any name the destination database does not have installed in
`pg_collation`. This is the cheap pre-flight signal that an explicit
`COLLATE` would be lost; the per table×column detail lives in the dry-run
plan (`plan.Collations`, `collation_diffs`) and in the run's final report
and `events.jsonl` (`collation_diff` events). Columns on the database
default collation are never reported — a DB-level difference is a `collation`
finding, not a column one.

### The `route` check (informational, M4)

`route` never fails: routing is a **planning** concern — the per-table
mode is resolved by the plan, not by doctor. On a GP6 pair it prints,
e.g.:

```
route  src->dst  PASS  route-mode: src=2 dst=2 segments, 2 eligible (s,d) pair(s) — direct_1to1 possible
```

- **S == D** → `direct_1to1 possible`: the hash fast path is available
  (same segment count, the GP jump-hash is identical on both sides,
  s == d for every row, zero per-row routing cost).
- **S != D** → `direct_1to1 not possible (S!=D) — router_full`: every
  (s,d) pair is reachable under the GP jump-hash (the fast-path
  congruence math of the original design is **disproven** — see
  `ANSWER-T4B-FASTPATH-DISPROVEN-2026-09-27.md`), so the destination
  write must re-hash per row. The routing *kernel* (`internal/route`)
  is implemented and live-validated (T0-2: 2000/2000 match); the
  **wire-up of a direct dest-segment write is backlog R5** — today the
  dest write always goes through the dest coordinator, so this line is
  informational for planning, not a live capability.

Doctor output format (`printReport`, FAILs first, then WARNs, then
PASSes, alphabetical within severity):

```
gpstream doctor
===============
  [WARN] source      on_segment   segment-direct capability degraded: GUC ycmdb.yc_allow_copy_to_program = "off" (want "on"; set in postgresql.conf of EVERY data-dir, then gpstop -ar); ON SEGMENT dispatch: ok; gpstream-segreceiver present on all segment hosts
           -> the job will fall back to the coordinator path; fix the items above to enable the 2.0x segment-direct fast path
  [PASS] src->dst    route        route-mode: src=2 dst=2 segments, 2 eligible (s,d) pair(s) — direct_1to1 possible
...
overall: PASS (no FAIL)
```

## 2. The `on_segment` capability (segment-direct read, T-B)

`--copy-mode on_segment` streams the **source segments** directly to the
controller (`COPY ... TO PROGRAM ... ON SEGMENT` under the coordinator's
snapshot, delivered by `gpstream-segreceiver` on the segment hosts),
while the destination write still goes through the dest coordinator.
Doctor probes it **only when `--seg-binary` is supplied** (the binary
path on the segment hosts) and **only on a Greenplum source**.

It composes from **three prerequisites**, all reported in the check
message:

| Prerequisite | Probe | What a failure looks like |
|--------------|-------|---------------------------|
| **1. GUC** | `current_setting('ycmdb.yc_allow_copy_to_program')` == `on` | `forbidden to COPY to or from an external program` — set the GUC in `postgresql.conf` of **all** data dirs (master + segments) and restart the cluster (`gpstop -ar`). Required on the open-gpdb 6.x fork; it is a `PGC_POSTMASTER` superuser GUC. |
| **2. Dispatch** | `COPY (SELECT 1) TO PROGRAM 'true <SEGID>' ON SEGMENT` (no-op program) | Any dispatch error — the kernel path `CdbDispatchUtilityStatement` → primary-only gang is broken (permissions, segment state). |
| **3. Binary** | `COPY (SELECT 1) TO PROGRAM '<seg-binary> --check --content <SEGID>' ON SEGMENT` — `gpstream-segreceiver --check` must exit 0 on **every primary segment host** | The binary is missing/not executable on one or more segment hosts — install the `gpstream` package (recommended: same version as the controller, so `rpm -qa`/`dnf upgrade` keep hosts in lockstep) or copy the single static `gpstream-segreceiver` file to every host in the segment topology. |

All three OK → `on_segment` PASS and `--copy-mode on_segment` is
available (~2× throughput on the source side, T0-4). Any one missing →
`on_segment` WARN with the precise fix; **the job still runs on the
coordinator path** — that path always works and is the default.

Live-verified on the lab: with the GUC off, the probe reports the
`forbidden ... external program` error; with it on, the probe passes and
the E2 e2e (`--copy-mode on_segment`, 111 000 rows, trailer cross-check)
is green.

### Choosing `--copy-mode`

| Situation | Use |
|-----------|-----|
| Doctor `on_segment` = **PASS** | `--copy-mode on_segment` — source segments stream directly (the fast path) |
| Doctor `on_segment` = **WARN** (or no `--seg-binary`) | default coordinator path — correct, slower on the source side |
| Plain PostgreSQL endpoint | coordinator path only — there are no segments to read from |

## 3. How to read a doctor run

```
$ gpstream doctor \
    --source "host=prod-coordinator   port=5432 user=gpstream dbname=postgres" \
    --dest   "host=preprod-coordinator  port=5432 user=gpstream dbname=postgres" \
    --seg-binary /usr/bin/gpstream-segreceiver
```

- **Any FAIL** → fix before running. The recommendation string names the
  exact step (credentials, `gp_segment_configuration`, disk, grants).
- **`on_segment` WARN** → decide: run coordinator-path now (safe), or
  apply the named fix (GUC / dispatch / binary) and re-run doctor to
  enable the fast path.
- **`topology` WARN (single-controller data path, ADR-002)** → the data
  stream transits the controller process and the network legs carry raw
  binary COPY; run `--topology two-agent` (with `--source-agent-binary` /
  `--dest-agent-binary`) to move the data plane into the agents — the check
  then reports **PASS** (data runs source agent → dest agent, compressed on
  the wire; the controller holds no data connections). The controller is
  still placed where it may be trusted.
- **`route` PASS with `direct_1to1 possible`** → if/when the R5 dest
  wire-up lands, this topology gets the zero-cost fast path; today it
  still means "source and destination layouts match 1:1", which also
  makes the coordinator path's redistribution trivial (none).
- **`collation` WARN** → do not ignore on a data migration: differing
  `LC_COLLATE` changes sort/routing semantics (FR-10).
- **`extensions` WARN** → the destination is missing an extension the source
  database has. As with a backup restore, dependent extensions (MADlib,
  PostGIS, orafce, ...) must be installed in the receiver BEFORE the data
  load, or extension-typed columns cannot be restored (FR-05/FR-11). The
  finding names the exact extension(s): `CREATE EXTENSION <name>` in the
  destination database.

## 4. Honest notes

- `disk` on a live dest is a probe of dest free space; the test bench
  seeds it as a sentinel (CR-7: the doctor must detect a no-space fault).
- `tls` in v0.1 reports the configured minimum and defers full
  certificate-chain validation; provide `--ssl-ca` (CA bundle), and
  `--ssl-cert`/`--ssl-key` for mTLS (FR-13).
- The doctor never mutates the clusters — every probe is a read or a
  no-op program (`true`, `--check`).
