# gpstream dry-run — Capability Matrix

> These pre-flight checks (FR-05) run inside **`gpstream dry-run`**, before the plan is built:
> any FAIL aborts with the report, WARNs are printed, then the plan summary
> renders. This document is the **capability matrix**: what each check
> probes, what each severity means for your run, and how the segment-direct
> capability (`on_segment`) composes from its three prerequisites.

The diagnostic (FR-05) runs a
fixed set of checks against the source and destination clusters and prints
each as `PASS` / `WARN` / `FAIL` with an actionable recommendation.

Grounded in `internal/dryrun/sides.go` (the check matrix) and
`internal/agent/dryrun_checks.go` (the A+ segreceiver probe), live-verified on the lab.

---

## 1. The check matrix

| # | Check | Side | Probes | FAIL means | WARN means |
|---|-------|------|--------|-----------|------------|
| 1 | `reachability` | source, dest | `Ping` the coordinator | Cannot connect — verify network, host/port, credentials | — |
| 2 | `version` | source, dest | `SELECT version()` | Query failed (privilege or connection) | — |
| 3 | `segments` | source, dest | `gp_segment_configuration` present? count of `status='u'` | **0 up segments** (cannot copy) | Not a Greenplum cluster (plain PostgreSQL single-node mode — expected for the kernel PoC) |
| 4 | `disk` | source, dest | disk probe (sentinel in the test bench; live = free space) | Probe failed / no data | — |
| 5 | `permissions` | source, dest | `has_database_privilege(current_database(), 'CREATE')` / `'CONNECT'` | — | Role lacks CREATE/CONNECT — `GRANT CREATE, CONNECT ON DATABASE` |
| 6 | `statement-timeout` | source, dest | `statement_timeout` / `idle_in_transaction_session_timeout` | — | An operator statement/idle timeout is set — long-lived COPY sessions can be killed on a slow network; raise it for the copy duration or leave it 0 |
| 7 | `max_connections` | source (only with `--workers N>1`) | `max_connections` vs `N + 4` (N pinned snapshot conns + pool headroom) | — | `max_connections` below what `--workers N` needs — raise it or lower `--workers` |
| 8 | `route` | src→dst | segment counts S, D; eligible (s,d) pair count; S==D? | — (never fails) | Endpoint is plain PostgreSQL or has no segments — coordinator path only |
| 9 | `collation` | network | `datcollate` source vs dest | — | `LC_COLLATE` differs — affects routing; review before start (FR-10) |
| 10 | `collation-behavior` | network | behavioral sort probe (same locale name, F-16) | — | Same locale name but different glibc sort behavior — string sort / ORDER BY / partition-routing semantics differ on the destination; the data migrates byte-for-byte but its ordering changes |
| 11 | `column-collation` | network | distinct explicit `COLLATE` names (public schema) source vs `pg_collation` dest | — | An explicit column collation on the source is not installed in the destination — `CREATE COLLATION` in the receiver (or accept the DB default) before the data load (FR-16) |
| 12 | `encoding` | network | database `encoding` source vs dest | — | Database encodings differ — an encoding mismatch reinterprets string bytes on the destination (DATA DESTRUCTIVE); create the destination database with the source's encoding (F-16) |
| 13 | `extensions` | network | `pg_extension` set: source vs dest | — | A source extension is missing in the destination (MADlib/PostGIS/orafce or any other) — install it in the receiver before the data load (FR-05/FR-11) |
| 14 | `on_segment` | source (mesh mode) | the 3-prerequisite probe below (only when the segreceiver binary resolves and the source is Greenplum) | — (never fails the diagnostic) | Capability degraded — the job runs on the coordinator path instead |
| 15 | `copy_to_program_guc` | source | the GUC read from `pg_settings` (`ycmdb.yc_allow_copy_to_program`; a missing GUC is distinguishable from a present-but-off one) | GUC present and not `on` **in mesh mode** — the run cannot start (the segreceiver dispatch is rejected) | GUC present and not `on` (coordinator mode) — the mesh path would not work |
| 16 | `name-resolution` | src→dst (mesh mode, Greenplum dest) | destination segment hostnames resolve from the source segment hosts | A destination segment hostname does not resolve from a source segment host — the mesh run as configured cannot succeed | The name checks were requested but no agent probed them (older agent version) — unverified |
| 17 | `mini-ready` | dest | mini-agent readiness on the D segment hosts (when reported) | — | Mini agents not ready on every D segment host — the mesh mini path needs the `gpstream-dest-agent` binary (and its ports) on every D segment host |
| 18 | `tls` | network | configured minimum TLS version | — | Always informational — provide `--ssl-ca` (CA bundle), and `--ssl-cert`/`--ssl-key` for mTLS (FR-13) |
| 19 | `dest-dsn-form` | dest (mesh mode; only when `--dest` is given in keyword/value form) | `--dest` is a URL form (`postgres://user@host:port/db`) | — | `--dest "…"` is not URL-form — mesh segment-to-segment writes (routed, direct_1to1) need a URL-form `--dest` or the run degrades to the merge path |

Overall health: the diagnostic is **healthy iff there is no FAIL**. WARNs do
not block a run — they degrade an *option* (segment-direct, collation
review) and say so in the recommendation.

### The `column-collation` check (FR-16, column level)

The `collation` check (row 8) compares the **database-level** `LC_COLLATE`
/`LC_CTYPE`. `column-collation` covers the **column level**: it lists the
distinct explicit `COLLATE` names used by the source's public-schema tables
and warns for any name that is not installed as a built-in collation on the
destination (`pg_catalog` only, namespace 11 — user-created collations in other
schemas of the destination database are not visible to the check). This is the cheap pre-flight signal that an explicit
`COLLATE` would be lost; the per table×column detail lives in the dry-run
plan (`plan.Collations`, `collation_diffs`) and in the run's final report
and `events.jsonl` (`collation_diff` events). Columns on the database
default collation are never reported — a DB-level difference is a `collation`
finding, not a column one.

### The `route` check (informational, M4)

`route` never fails: routing is a **planning** concern — the per-table
mode is resolved by the plan, not by the diagnostic. On a GP6 pair it prints,
e.g.:

```
route: mesh — src=2 dst=2 segments, 2 eligible (s,d) pair(s) — direct_1to1 possible
```
(in coordinator mode the line is instead `route: coordinator — one COPY via the destination coordinator (per-table route modes not used); layout: src=2 dst=2 segments`)

- **S == D** → `direct_1to1 possible`: the hash fast path is available
  (same segment count, the GP jump-hash is identical on both sides,
  s == d for every row, zero per-row routing cost).
- **S != D** → `direct_1to1 not possible (S!=D) — router_full`: every
  (s,d) pair is reachable under the GP jump-hash (the fast-path
  congruence math of the original design is **disproven** — see
  `ANSWER-T4B-FASTPATH-DISPROVEN-2026-09-27.md`), so the destination
  write must re-hash per row. The routing *kernel* (`internal/route`)
  is implemented and live-validated (T0-2: 2000/2000 match), and the
  **direct dest-segment write is live** in mesh mode: the destination
  mini-agents perform per-segment utility COPYs with **no coordinator in
  the data path** (recorded as `mesh_direct_write` / `mesh_routed_write`
  events).

Diagnostic output format (`printReport`, FAILs first, then WARNs, then
PASSes, alphabetical within severity):

```
gpstream dry-run — diagnostics
=============================
  [WARN] source      on_segment   segment-direct capability degraded: vm1: segreceiver binary present+executable, listen base port 50000 free; vm2: segreceiver binary MISSING or not executable; kernel ON SEGMENT support: NOT checked in dry-run (no real dispatch; verified by the first real run)
           -> the job will fall back to the coordinator path; fix the items above to enable the 2.0x segment-direct fast path
  [PASS] src->dst    route        route: mesh — src=2 dst=2 segments, 2 eligible (s,d) pair(s) — direct_1to1 possible
...
overall: PASS (no FAIL)
```

## 2. The `on_segment` capability (segment-direct read, T-B)

`--copy-mode mesh` reads the **source segments** directly
(`COPY ... TO PROGRAM ... ON SEGMENT` under the coordinator's snapshot,
launched by `gpstream-segreceiver` on the segment hosts), then moves the
data **segment-to-segment** to the destination mini-agents: with
`direct_1to1` (S == D, equal hash keys) each segreceiver s streams to the
mini-agent of destination segment s (one slice per segment pair); with
`router_full` (S != D or unequal keys) the segreceivers route by
cdbhash/JCH in S*D*W segment-to-segment streams. The destination write is
a per-segment utility COPY on the destination mini-agents — **no
coordinator in the data path**; the coordinator path remains only as a
fallback (a `mesh_coordinator_fallback` event records when a table fell
back to it and why). The diagnostic probes the segment-direct read
**only in mesh mode, only when the segreceiver binary resolves** (the
binary path on the segment hosts), and **only on a Greenplum source**.

It composes from the **A+ probe**: the source agent SSHes to each source
**primary** segment host and runs a no-op script, all reported in the check
message:

| Step | Probe | What a failure looks like |
|------|-------|---------------------------|
| **1. Binary** | `test -x <seg-binary>` + `<seg-binary> --version` (matched against the agent's version) on every primary segment host | The binary is missing/not executable on one or more segment hosts, or its version mismatches the agent — install the `gpstream` package (recommended: same version, so `rpm -qa`/`dnf upgrade` keep hosts in lockstep) or copy the single static `gpstream-segreceiver` file to every host in the segment topology. |
| **2. Base port** | bash `/dev/tcp` connect probe of the `--seg-listen` base port (only when the flag is set explicitly) | the base port is already busy on the host |
| **3. Dest names** (mesh) | `getent hosts` for the destination segment names from the source hosts | a destination name does not resolve from a source host — the mesh run as configured cannot succeed |

All three OK → `on_segment` PASS and `--copy-mode mesh` is
available (~2× throughput on the source side, T0-4). Any one missing →
`on_segment` WARN with the precise fix; **the job still runs on the
coordinator path** — that path always works and is the default.

Two boundaries of this check:
- **The kernel's ON SEGMENT dispatch is NOT exercised in dry-run** (no real
dispatch) — the check's message says so explicitly, and the dispatch path is
verified by the first real run (the E2 e2e, `--copy-mode mesh`, 111 000 rows,
trailer cross-check, is green).
- The GUC that gates the dispatch is checked **separately** by the
`copy_to_program_guc` row (read from `pg_settings`). With the GUC off, that
check FAILs in mesh mode; launching the segreceivers at runtime with the GUC
off fails with `forbidden to COPY to or from an external program` (set it in
`postgresql.conf` of **all** data dirs, `gpstop -ar`).

### Choosing `--copy-mode`

| Situation | Use |
|-----------|-----|
| dry-run `on_segment` = **PASS** | `--copy-mode mesh` — source segments stream directly (the fast path) |
| dry-run `on_segment` = **WARN** (or the segreceiver does not resolve) | default coordinator path — correct, slower on the source side |
| Plain PostgreSQL endpoint | coordinator path only — there are no segments to read from |

## 3. How to read a dry-run diagnostic

```
$ gpstream dry-run \
    --source "host=prod-coordinator   port=5432 user=gpstream dbname=postgres" \
    --dest   "host=preprod-coordinator  port=5432 user=gpstream dbname=postgres" \
    # the segreceiver path resolves by convention (sibling of the gpstream binary); pass it via GPSTREAM_SEG_BINARY only for non-standard installs
```

- **Any FAIL** → fix before running. The recommendation string names the
  exact step (credentials, `gp_segment_configuration`, disk, grants).
- **`on_segment` WARN** → decide: run coordinator-path now (safe), or
  apply the named fix (GUC / dispatch / binary) and re-run the dry-run to
  enable the fast path.
- **`route` PASS with `direct_1to1 possible`** → source and destination
  layouts match 1:1, so `--copy-mode mesh` gets the zero-cost `direct_1to1`
  fast path (one slice per segment pair), and the coordinator path's
  redistribution is trivial (none).
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
  seeds it as a sentinel (CR-7: the diagnostic must detect a no-space fault).
- `tls` reports the configured minimum and defers full certificate-chain
  validation (the check message says so: "full cert validation in M2");
  provide `--ssl-ca` (CA bundle), and `--ssl-cert`/`--ssl-key` for mTLS
  (FR-13).
- The diagnostic never mutates the clusters — every probe is a read or a
  remote no-op script (`test -x`, `--version`, `getent hosts`, a bash port
  probe).
