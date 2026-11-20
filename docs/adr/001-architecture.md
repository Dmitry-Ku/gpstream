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
  ├── plan         build the full object graph BEFORE start; estimate volume /
  │                time; detect conflicts + collation differences; emit an
  │                idempotent plan (FR-10). No time/randomness in the plan hash.
  │
  ├── manifest     Job Manager: journal (DB + file mirror, in `journaldb`),
  │                checkpoints, staging + atomic swap, cleanup, auto-resume
  │                (FR-02/FR-06/FR-07)
  │
  ├── transfer     frame framing + crc, compressed agent↔agent stream
  │                (FR-01/§4.2)
  │
  ├── agent        source + dest agents: COPY TO from segments
  │                (gp_segment_connect), COPY FROM into staging, checksum +
  │                verify (FR-01/FR-02)
  │
  ├── index        background index builder (FR-17)
  │
  ├── ratelimit    adaptive rate limiter from /proc metrics (FR-14)
  │
  ├── monitor      progress + JSON/CSV + optional /metrics (FR-04)
  │
  └── dryrun       self-diagnostic for dry-run, PASS/FAIL/WARN (FR-05/CR-7)
```

Plus shared packages: `config` (defaults + flag/env binding, FR-08),
`transfer/pg` (connection + pluggable `Querier` so components are testable
without a live cluster).

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
re-copying the whole table (the production stream layer implements this in
`internal/transfer/segstream`; M0 carried a stand-alone selftest, removed 08.10).

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
- The `transfer/pg.Querier` interface lets `dryrun`, `plan`, and the
  source/dest agents be unit-tested with `transfer/pg.FakeConn` (no live
  cluster) — this is how
  CR-7 (detect all seeded faults) and plan-determinism (CR-4) are verified.
- The plan hash is a pure function of (units, compression, workers, …) with no
  time or randomness, so two `--dry-run` on an unchanged topology are
  byte-identical (FR-10/CR-4).

## Post-decision changes (verified against the code, 2026-11-19)

- **Data path:** the two-agent data path (ADR-002 remediation) is now the
  only data path since the `--topology` flag was removed (CHANGELOG.md);
  the controller holds no data connections.
- **Read from source segments:** `COPY ... TO PROGRAM 'gpstream-segreceiver
  ...' ON SEGMENT` dispatch (internal/transfer/segstream) —
  `gp_segment_connect` (the mechanism named above) never appeared in the
  code.
- **Frame fields:** the wire frame is v2 — `{job_id, unit_id, seq,
  content_id, flags}` plus the on-wire length and crc32
  (internal/transfer/stream.go). The `table_oid`/`part_id` fields named
  above were never shipped; the legacy v1 layout (still decodable for
  backward compatibility) carries only job_id, unit_id and seq.
- **Verification:** content digests on the data path — the Trailer carries
  the per-slice SHA256, the per-row XOR digest and the row count, compared
  source-vs-destination (internal/transfer/stream.go,
  internal/agent/destserver.go) — plus row count; `gp_table_checksum`
  (named above) is not used.
- **Worker model:** units run on a bounded in-process worker pool
  (`cfg.WorkersPerPair`, internal/manifest/manager.go); the separate
  processes are the source/dest agents launched by the controller
  (internal/launch).
- **Ratelimit:** diagnostic-only in the two-agent data path — the token-bucket
  pacer is not wired in (backlog); the flow is paced by backpressure only
  (internal/manifest/manager.go:151-158).
- **Monitor outputs:** JSON only (events.jsonl, runstats.json) + optional
  Prometheus /metrics; no CSV output in the code.
- **Deployment:** the repo ships no systemd unit (host-side deployment,
  operator-managed); the watchdogs live in code: the persistent-channel
  Ping/Pong (internal/agent/persistent.go) and the L2 data-idle watchdog
  (internal/agent/mini.go).
