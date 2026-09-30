# ADR-001: Architecture

- **Status:** Accepted
- **Date:** 2026-09-02
- **Milestone:** M0

## Context

gpstream copies logical data between two Greenplum 6 clusters over a direct
high-speed network **without any intermediate storage** (FR-01), with snapshot
consistency (FR-02), resilience/crash-recovery (FR-06/FR-07), monitoring
(FR-04), background index rebuild (FR-17), redistribution accounting (FR-15),
and a fully deterministic `--dry-run` plan (FR-10). The specification (§4)
prescribes the component decomposition and the frame framing.

## Decision

The following components, mapping to the `internal/` packages:

```
CLI (cmd/gpstream) ── parses flags, binds config, dispatches subcommands
  │
  ├── planner      build the full object graph BEFORE start; estimate volume /
  │                time; detect conflicts + collation differences; emit an
  │                idempotent plan (FR-10). No time/randomness in the plan hash.
  │
  ├── job          Job Manager: journal (DB + file mirror), checkpoints, staging
  │                + atomic swap, cleanup, auto-resume (FR-02/FR-06/FR-07)
  │
  ├── stream       host-pair workers (separate processes), frame framing + crc
  │                (FR-01/§4.2)
  │
  ├── source       source agents: COPY TO from segments (gp_segment_connect),
  │                checksum (FR-01/FR-02)
  │
  ├── dest         dest agents: COPY FROM into staging, verify (FR-01/FR-02)
  │
  ├── indexer      background index builder (FR-17)
  │
  ├── limiter      adaptive rate limiter from /proc metrics (FR-14)
  │
  ├── metrics      progress + JSON/CSV + optional /metrics (FR-04)
  │
  └── doctor       self-diagnostic, PASS/FAIL/WARN (FR-05/CR-7)
```

Plus shared packages: `config` (defaults + flag/env binding, FR-08), `pg`
(connection + pluggable `Querier` so components are testable without a live
cluster), `selftest` (internal checks, FR-05).

### Data path (FR-01)

```
source segments
  → source agent (COPY TO, binary, checksum)
  → frame {job_id, unit_id, table_oid, part_id, seq, crc32, len}
  → encrypted stream (100 GbE, ≥ 80% of channel)
  → dest agent (COPY FROM into _gs_<jobid>_<unit> staging)
  → verify (gp_table_checksum + row count)
  → atomic ALTER ... RENAME to final name
  → dest segments
```

No intermediate disk storage; only RAM buffers with back-pressure.

### Framing (§4.2)

Every frame carries a crc over its body so corruption is detected **without**
re-copying the whole table (implemented and tested in `selftest` in M0; the
production stream layer reuses this in M2).

### Journal (§4.3)

Primary journal lives in the destination service DB (e.g. `postgres`); a local
file mirror is the fallback when the destination DB is unreachable. Records:
job_id, unit, state, checksums, row/byte counts, timestamps, checkpoints.

### "Unit done" definition (§4.4)

A unit is done when **data + checksum + (optionally) swap** are complete; a job
is done when all units are done plus a final report.

## Consequences

- Separate worker processes (FR-06): a worker death does not kill the job;
  systemd `Restart=on-failure` + an internal watchdog.
- The `pg.Querier` interface lets `doctor`, `planner`, and the source/dest
  agents be unit-tested with `pg.FakeConn` (no live cluster) — this is how
  CR-7 (detect all seeded faults) and plan-determinism (CR-4) are verified.
- The plan hash is a pure function of (units, compression, workers, …) with no
  time or randomness, so two `--dry-run` on an unchanged topology are
  byte-identical (FR-10/CR-4).
