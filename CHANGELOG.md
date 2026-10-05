# Changelog

All notable changes to gpstream are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]
### Removed: `--copy-mode direct` (breaking)

The `direct` (P5) copy-mode is removed from the user surface. Its per-segment
write is subsumed by `on_segment`'s auto-selected per-table sub-paths
(`direct_1to1` 1:1, `routed`, `merge`), so the capability is unchanged — only
the redundant top-level mode is gone. `--copy-mode` now accepts exactly two
values: `coordinator | on_segment`. `gpstream run --copy-mode direct` now
fails at validation, and `config.Validate` rejects `CopyMode "direct"`
(regression-tested). See the P5/P8-1 entries below for what the `direct`
capability became.

### Added: RST-resume (frame-ACK + RST-resume) on the production data paths

The `ReliablePipe` transport (DESIGN §4.6 Tier-1: frame-level ACK + bounded
retransmit window + RST-resume) is now wired into the **production** data
paths, not just the test harness:
- the coordinator two-agent path,
- the `on_segment` merge path, and
- the `on_segment` routed segment-to-segment path.

On a mid-stream RST the receiver re-dials carrying its identity
(`job_id` + `unit_id` + `content` + `last-acked Seq`); a multi-tenant
resume router on the destination maps the re-dial back to the correct pipe,
so a torn connection resumes instead of failing the unit. `--frame-ack` is
`on` by default and was confirmed throughput-neutral in a T7 soak A/B
(28.09). The in-flight window is `--retransmit-buffer-max` (default 1 GiB per
stream; lower it for short LAN hops).

### Fixed: agent reports a routable data address when the listener is a wildcard

The remote-launch (SSH) path computes `--data-listen 0.0.0.0:0` (the
controller does not know the agent host's IP), so the bound listener is
`[::]:port` and the agent's hello reported `data=[::]:port`; the source agent
then dialed the wildcard host as `localhost` and the data connection was
refused (cross-VM SSH-launch A/B: `dial [::]:34595: connection refused`). The
agent now reports its routable primary non-loopback IP in the hello.

### Docs: CLI contract, man page, and `--help` reconciled with the code

Full cross-surface audit (man for all four binaries, `--help` for all four,
README/CLI_REFERENCE/docs vs `cmd/gpstream` + `internal/config`): added the
missing `--control-tls-cert/key/ca` and `--data-port-range` to `gpstream
--help` and the man page; removed a duplicate `--operation-id` definition;
corrected a stale "no `--data-port-range` flag" claim; the `Commands` table
now lists exactly the 10 dispatch subcommands (`bench` added, `man` — the OS
man page, not a dispatch entry — removed). Every flag parsed by the code is
now documented in every surface; no dead or duplicated flags remain.

### Removed: the `gpstream.service` systemd unit

The packaged unit (`/usr/lib/systemd/system/gpstream.service`) is removed: it
wrapped a one-shot `gpstream run --config /etc/gpstream/gpstream.toml` and was
not enabled anywhere (it is superseded by the controller + long-lived agent
architecture, where runs are supervised externally). The package no longer
ships the unit; the lifecycle scripts are no-ops. Unattended / periodic runs
are scheduled by the operator's own cron / process manager.

### Added: `direct_1to1` fast-path write (P8)

When the planner proves a table's route is `direct_1to1` (source and
destination have the same number of up primary segments, both tables are
`DISTRIBUTED BY` the same keys in the same order, same GP major version,
non-legacy hashops), `--copy-mode on_segment` now takes the **fast-path
write**: the destination agent loads the N content-tagged segment slices with
N per-segment utility-mode COPYs (one per destination segment, routed by
content-id) instead of merging them into one coordinator COPY. No SliceMux,
no destination re-distribution. The source side is the same ON SEGMENT
dispatch (the kernel streams the same content-tagged slices either way).
Each such table logs `direct_1to1_fastpath` to the run event stream. A
partitioned parent, or a live destination segment-count mismatch (a primary
down since planning), degrades to the on_segment merge path — the copy is
always correct, only the speed changes. Measured on GP6 (4 segments,
3.2 GB): the fast-path removes the coordinator from the write path
(~1.5× warm) and the read already runs per-segment (~3.5× vs coordinator),
so end-to-end ≈ 1.5–1.8× at 4 segments and higher at larger segment counts.

### Added: `direct_1to1` read fast-path in the `direct` branch (P8-1)

> **Superseded (v0.2.0):** the `direct` copy-mode and this read fast-path were
> removed — `direct_1to1` reads now run through the `on_segment` ON SEGMENT
> dispatch (see "Removed: `--copy-mode direct`"). This entry is kept as the
> historical record of how the read fast-path was first built and validated.

The `--copy-mode direct` branch reads `direct_1to1` tables through the
coordinator (`COPY TO STDOUT`) and re-hashes every row with the RowRouter
(cdbhash + jump-consistent-hash, ~19 ns/row) — pure overhead for
`direct_1to1`, where source segment *s* maps to destination segment *s* for
every row. When the plan proves `direct_1to1` **and** the ON SEGMENT dispatch
is available (`--seg-binary` + `--seg-listen`, i.e. the segreceiver is deployed
on the source segment hosts), the source read now uses the kernel's ON SEGMENT
dispatch — each source segment streams its own slice, **0 CPU on hashing** —
and the write is the same per-segment utility-mode COPYs (the R5 endgame,
coordinator out of the write path, is preserved). A `router_full` table, or a
`direct` run without the seg config, keeps the coordinator + RowRouter path.
Each fast-path table logs `direct_1to1_read_fastpath` to the run event stream.

Live-validated on the lab (S==D=3 pair, 1.29M rows / 180 MB,
`DISTRIBUTED BY (id)`): the fast path completed in 6 s (~30 MB/s) with the
per-segment hash placement on the destination **identical** to the source
(430147/429563/430290 on both) and the distribution key preserved — the
`d==s` invariant for every row that the read fast-path relies on.


### Removed: `--topology` flag — the two-agent data path is now the only one

The single-controller data path (the ADR-002 debt) is fully removed.
`--topology single|two-agent` no longer exists: every `run`/`bench` copies
through the source + destination agent binaries (required for `run`/`bench`;
`doctor`/`dry-run`/`status`/`cleanup` do not need them — they are control
plane only). The controller keeps only the control plane: catalog, DDL,
journal, verify, swap.

### Removed: `--consistent-db` / `--consistent-cluster` / `--consistent-tables`

All three consistency flags are gone. The only behavior is the default one:
a REPEATABLE READ database snapshot pinned on the source agent's connection
for the whole run (FR-02). Per-table and cluster-wide scopes are no longer
exposed — the run-wide DB snapshot is what every operator wants and it is
what the data path guarantees.

### Changed: REPEATABLE READ snapshot point is pinned at run start

`BEGIN ISOLATION LEVEL REPEATABLE READ` alone does not establish the
snapshot in PostgreSQL — it is established at the first query in the
transaction. The source agent now runs `SELECT 1` right after BEGIN, so the
snapshot point is the run start, not the first unit's data COPY. Without
this, concurrent source writes committed between the run start and the first
COPY would leak into the run (FR-02 violation; caught by the scale-24
consistent-snapshot integration test).

### Changed: test strategy — data-path and integration tests under the
`gp6lab` build tag

- All data-path unit tests that faked the transport (fake-conn
  `internal/manifest` suite) are deleted; their coverage is replaced by the
  two-agent integration + crash-matrix suites and the live lab e2e.
- Every data-path/integration test (`tests/integration`, the transfer live
  tests, `two_agent_*`) now carries `//go:build gp6lab`. The default
  `go test ./...` runs only pure logic (config, plan, journal, DSN,
  doctor, framing, ratelimit, epoch rules) — no database required, fast,
  CI-stable.
- `go test -tags gp6lab ./...` runs the full stack against a real
  PostgreSQL (plain 9.4 works locally; GP6 on the testbed). CI vets the tag
  (`go vet -tags gp6lab ./...`) so it can never rot.

### Added: `pg.Pool.DSN()`

The pool now retains its normalized connection string and exposes it, so
tests and the controller can hand the two-agent runner the exact DSN the
pool uses.

### Added: two-agent crash-recovery matrix (kill any of the three processes mid-copy)

The ADR-002 remediation's hard part is now tested: `TestTwoAgentCrashRecoveryMatrix`
kills each of the three processes while a copy is in flight and verifies the
operation recovers idempotently by re-running the same `--operation-id`:

- **kill the source agent mid-COPY** — the controller's stream call errors,
  the unit is left non-terminal in the durable journal, and the re-run
  re-copies it under a fresh snapshot (epoch+1).
- **kill the dest agent mid-COPY** — the partial staging is truncated on
  resume (the dest agent truncates a surviving staging before re-creating it),
  so the re-copy replaces rather than appends.
- **kill the controller between journal writes** — the source agent detects
  the dropped control connection and **exits, rolling back its pinned
  REPEATABLE READ snapshot** (no orphan holding a server-side backend); the
  dest agent's one-shot serve ends; the re-run resumes from the durable
  journal.

In every case the already-swapped table is KEPT (the T5 keep rule — a unit
verified under one snapshot epoch) and only the in-flight table is re-copied;
the destination ends up exactly right (row count + content digest match, no
staging left). The test drives the real three binaries as subprocesses over a
real (loopback) socket, so the process boundaries — the thing the crash
matrix is about — are the production ones.

### Fixed: binary two-agent coordinator path failed every unit

`parseDestReport` (the process-runner coordinator path) required a 5-field
destination report (`n == 5`) but the coordinator report has always been four
fields (`unit rows logical <verdict> ok`, no `wire=`) — the on-wire volume is
the source agent's, reported separately. The binary `--topology two-agent`
coordinator copy therefore failed every unit with "no dest report line found".
It was latent because the in-proc runner returns counts directly and the
segment/direct paths use their own 5-field inline parsers; the new crash
matrix (which exercises the binary coordinator path) caught it. Fixed to
`n == 4` + a regression test.

### Changed: ADR-002 closed — two-agent is the remediation, docs + metrics + doctor updated (P6)

With P3/P4/P5 the two-agent data path is live for all three copy modes, so
ADR-002 (the single-controller data-path debt) is **closed**: the
`--topology two-agent` option is the remediation and the operator-facing
documentation now states the actual per-topology behavior instead of calling
the two-agent design "planned".

- **doctor:** the `topology` check takes the run's `--topology` value.
  `two-agent` (cross-host) now reports **PASS** (data runs source agent →
  dest agent, compressed on the wire; the controller holds no data
  connections); `single` (default) keeps the WARN naming the two-hop,
  raw-leg debt, with the recommendation now pointing at `--topology
  two-agent` instead of a planned design.
- **`gpstream_bytes_total`** semantics are documented per topology: two-agent
  = the on-wire agent-to-agent socket volume (the ADR-002 acceptance
  criterion), single = the in-memory framed pipe (the network legs stay raw).
  The metric's `# HELP` line, METRICS.md, OPERATOR_GUIDE and PERFORMANCE all
  state this.
- **Docs:** OPERATOR_GUIDE (topology note rewritten; two-agent requires
  `--source-agent-binary`/`--dest-agent-binary`), SECURITY.md (controller as
  data trust root is now scoped to the single topology; in two-agent the data
  trust roots are the two agent hosts), DOCTOR.md (the `topology` finding and
  its remediation), CLI_REFERENCE + CONFIGURATION + `man/gpstream.8`
  (the `--topology` / `--source-agent-binary` / `--dest-agent-binary` flags
  are now documented — they were previously only in the CLI help), and
  ADR-002 gains a **Resolution** section recording the three acceptance
  criteria and the GP6-measured wire/logical volumes (P3 15.9×, P4 8.8×,
  P5 ~3.1×).
- The three-process crash-recovery matrix (idempotent unit state machine
  under a death of any of the three processes) is recorded in ADR-002 as the
  remaining follow-up; the idempotent staging+swap / resume core it builds on
  is already in place.

### Added: direct data path through the agents (P5)

> **Superseded (v0.2.0):** the `direct` copy-mode was removed (see "Removed:
> `--copy-mode direct`"). The per-segment write capability lives on as the
> `direct_1to1` fast-path sub-path of `on_segment`. Kept as the historical
> record of the first fully segment-direct data path.

The ADR-002 remediation reaches the fully segment-direct (`--copy-mode
direct`, R5 endgame) path. In two-agent topology the controller no longer
routes rows: it computes the routing key (the table's distribution key, mapped
to its column positions + type OIDs) and the per-segment destination DSNs
(utility-mode, one per up primary) on its control-plane connections, hands the
routing key to the source agent and the launch payload (staging shape + DSNs)
to the destination agent, and keeps the DDL / journal / verify / swap
decision. The source agent reads the table with `COPY ... TO STDOUT`, routes
each row to one of D destination segments (the RowRouter reproduces the
kernel's cdbhash + jump-consistent-hash, so every row lands on the segment the
destination kernel would place it on) and streams D per-segment COMPRESSED
slices (content-tagged) to the destination agent; the destination agent
performs D utility-mode per-segment `COPY ... FROM STDIN` into the staging
table. The destination coordinator is out of the write path, so the copy is
bounded by the segment hosts, not the coordinator. A table that cannot be
routed (non-hash distribution, an unsupported dist-key type, a key column
absent from the source, or a non-Greenplum destination) degrades to the
two-agent coordinator path (correct, just not direct) and the decision is
logged (`direct_degraded`). Verified live on GP6 (A→C, 3 destination
segments): 20,000 rows, `--verify on` checksum match, 0 degraded, per-segment
balance max/avg ≈ 1.02, on-wire volume compressed ~3× vs the logical volume.

- `internal/agent/server.go` — `handleDirect`: the source agent's direct data
  path (read the table on the pinned FR-02 snapshot conn, route each row to a
  destination segment, stream D per-segment compressed slices over D
  connections to the destination agent; reports the router's row count +
  per-segment distribution + the on-wire byte volume it wrote).
- `internal/agent/direct.go` — `ServeDirect`: the destination agent's direct
  mode (builds the staging table on the coordinator conn, opens D utility-mode
  per-segment COPYs, accepts the D compressed slice connections and routes each
  by content-id to its segment; reports rows + raw on-network wire + logical
  bytes + the per-segment distribution).
- `internal/agent/control.go` + `internal/agent/spec.go` — the `direct` control
  op (`DirectReq`/`DirectResp`) and the `DirectLaunch` payload the controller
  hands the destination agent at launch (`--direct-spec`).
- `internal/manifest/twoagent.go` — `TwoAgentRunner.StreamDirect` (process +
  in-proc) + `unitDirectRequest`.
- `internal/manifest/manager.go` — the two-agent direct branch:
  `streamTwoAgentDirect` (routing key + per-segment DSNs computed on the
  control plane; degrades to the two-agent coordinator path when unroutable).
- `cmd/gpstream-dest-agent/main.go` — `--direct-spec` (the direct-mode launch
  payload).

### Added: on_segment data path through the agents (P4)

The ADR-002 remediation reaches the segment-direct (`--copy-mode on_segment`)
path. In two-agent topology the controller no longer owns the N-way segment
merge: it reads the source's primary-segment count + table shape on its source
pool, passes the instruction to the agents, and keeps the DDL / journal /
verify / swap decision. The source agent triggers the ON SEGMENT dispatch on
its FR-02 snapshot connection (the kernel launches `gpstream-segreceiver` on
every primary source segment); each segreceiver compresses its slice and
streams it over the network to the per-unit destination agent, which merges
the N slices (SliceMux) and loads the staging table. The controller holds no
data connection; it reads back the destination's authoritative row count + the
on-network wire volume (`gpstream_bytes_total`).

- `internal/transfer/segstream/segstream_compressed.go` — `SendCompressed` /
  `ReceiveCompressed` / `ReadStreamCompressed`: per-slice compression over the
  framed transport, the mode carried in the trailer (a mismatch is an error,
  never a silent mis-decode).
- `internal/agent/segments.go` — `ServeSegments`: the destination agent's
  segment mode (accepts N compressed segment slices, merges them with
  SliceMux, loads the staging table, reports rows + raw on-network wire +
  logical bytes).
- `internal/agent/control.go` + `internal/agent/server.go` — the `onsegment`
  control op: the source agent runs `COPY ... TO PROGRAM '<segreceiver>
  --listen <dest> --compress <mode>' ON SEGMENT` on its pinned FR-02 snapshot
  connection.
- `cmd/gpstream-segreceiver/main.go` — `--compress` (per-slice compression) +
  a bounded connect with dial-retry (the destination agent may still be
  binding its listen socket).
- `cmd/gpstream-dest-agent/main.go` — segment-mode flags (`--segments`,
  `--spec`, `--data-ssl-*`).
- `internal/manifest/twoagent.go` — `TwoAgentRunner.StreamSegment` (process +
  in-proc); the destination agent binds the routable `--seg-listen` host with
  port base+unitID (the segreceivers run on the source segment hosts, a
  different machine, so the port must be reachable over the network).
- `internal/manifest/manager.go` — the two-agent on_segment branch:
  `streamTwoAgentSegment` (segment count + shape read on the control-plane
  pool; the destination agent owns the staging DDL).

### Added: two-agent data path — controller as control plane (P3)

The ADR-002 remediation reaches the controller: `--topology two-agent` moves
the data plane out of the `gpstream` process. In two-agent topology the
controller keeps plan / journal / status / resume / metrics and the DDL +
verify + swap decision, but it **never runs a data COPY**: the long-lived
source agent (owns the source pg pool + the run-wide FR-02 snapshot) streams
each unit's `COPY ... TO STDOUT` framed + compressed over a real socket to a
per-unit destination agent (owns the destination conn + staging DDL +
`COPY ... FROM STDIN`). The controller orchestrates the agents over a
short-lived control protocol (JSON over TCP: `stream` / `checksum` /
`release`) and reads back the destination's authoritative row count + the
wire volume. `--topology single` (default) keeps the current in-process data
path as a fallback.

- `internal/agent/control.go` — the controller↔agent control protocol
  (`StreamReq`/`StreamResp`, `ChecksumReq`, `ReleaseReq`, `ControlClient`).
- `internal/agent/server.go` — `SourceServer`: the long-lived source agent
  (source pool + FR-02 snapshot on a pinned conn, control listener, per-unit
  stream over a real socket, `heal` re-BEGIN after a unit failure).
- `internal/agent/source.go` — `dialDestRetry` (the source retries its dial
  while the destination is still binding its listen socket).
- `internal/manifest/twoagent.go` — the `TwoAgentRunner` control-plane
  interface + `processTwoAgentRunner` (drives the real agent binaries as
  subprocesses, pre-binding loopback ports) + `InProcTwoAgentRunner` (drives
  the agent package in-process over a real TCP socket, for the integration
  test) + `NewInProcTwoAgentRunner`.
- `internal/manifest/manager.go` — `--topology two-agent` branch: the
  controller skips its own FR-02 snapshot (the source agent holds it), the
  data COPY runs through `streamTwoAgent`, the `--verify` source checksum is
  read from the source agent (same snapshot as the copy), and the in-place
  copy target (`--truncate` / `--data-only`) is rejected (the two-agent path
  is staging+swap).
- `cmd/gpstream-source-agent` — `--serve` long-lived mode (the controller's
  two-agent topology drives it over the control socket).
- `cmd/gpstream` — `--topology single|two-agent`, `--source-agent-binary`,
  `--dest-agent-binary`.
- P3 acceptance (`tests/integration/two_agent_manager_test.go`): the Manager
  itself runs the two-agent data path end to end — destination table correct
  (row count + content), staging gone (controller's swap landed), on-wire
  volume compressed (< logical, 15.9× in the test) with the manager's
  `bytes_total` metric equal to the wire volume, and `--verify on` working
  through the agent-side checksum.

### Added: two-agent data path — destination agent (P2)

The ADR-002 remediation continues: this change lands the **destination
agent** — the `gpstream-dest-agent` binary + `internal/agent` dest core that
runs on a destination-cluster host. It listens for the source agent's
connection, reads the `UnitSpec` header (destination schema + the source
table shape: columns, distribution, partition, compression), builds the
staging table, loads it from the **compressed** framed stream (decompressing
on the wire, FR-12), and — when instructed (`--swap`) — performs the atomic
swap. The swap stays a controller decision (the journal's terminal-state
semantics live in the controller), matching ADR-002's plan.

- `internal/agent/spec.go` — `UnitSpec` header + `ReadUnitSpec`/`WriteUnitSpec`
  (the schema the source sends before the framed stream).
- `internal/agent/dest.go` — `agent.Serve`: dest conn + listen (optionally a
  TLS/mTLS server) + staging DDL + COPY FROM + optional swap; reports rows +
  logical (the authoritative destination counts).
- `cmd/gpstream-dest-agent/` — the binary (`--check` doctor probe; SIGINT /
  SIGTERM unwinds the stream).
- `.goreleaser.yaml` — packages the dest agent (4th binary).
- P2 acceptance (`tests/integration/two_agent_roundtrip_test.go`): a real
  two-agent round trip over a real TCP socket — source agent push + dest agent
  serve — the destination table is correct (count + content digest) after the
  swap, the staging table is gone, and the on-wire volume is the compressed
  volume (< logical).

Together with P1 (the source agent) the two-agent coordinator data path is
complete end to end; the controller control-plane wiring (P3) is next. The
single-controller path is untouched.

### Added: two-agent data path — source agent (P1)

The ADR-002 remediation (moving the data plane out of the controller) is
under way. This change lands the **source agent**: a new
`gpstream-source-agent` binary + `internal/agent` core that runs on a
source-cluster host, owns the source pg connection and the FR-02 snapshot,
streams `COPY ... TO STDOUT (FORMAT binary)`, and pushes the bytes
**compressed over a real socket** (optionally TLS) to the destination agent.
The network now carries the compressed volume (FR-12), which the
single-controller build does not (its pipe is in-memory).

- `internal/agent/source.go` — `agent.Push`: source conn + FR-02 snapshot +
  COPY TO STDOUT + framed/compressed push; reports rows + wire/logical.
- `cmd/gpstream-source-agent/` — the binary (dials the dest agent; `--check`
  doctor probe; SIGINT/SIGTERM unwinds the stream).
- `.goreleaser.yaml` — packages `gpstream-source-agent` (3rd binary).
- P1 acceptance (`tests/integration/source_agent_test.go`): over a real TCP
  socket the on-wire bytes are the compressed volume (strictly < logical),
  the reconstructed payload is byte-for-byte the source table, and the row
  count matches.

The destination agent (P2) and the controller control-plane wiring (P3)
follow; the single-controller path (`--topology single`) is untouched.

### Documented: single-controller data path is an acknowledged debt (ADR-002)

The implemented build runs both PostgreSQL connections in one process, so
the frame/compress transport is an in-memory pipe: both TCP legs carry
uncompressed binary COPY, `--compress` costs controller CPU without
reducing inter-cluster bandwidth, the `gpstream_bytes_total`/`wire` numbers
are the compressed in-memory volume (not the network volume), and a
controller on an external host is a data trust root (two hops, data in
controller RAM). New:

- `docs/adr/002-single-controller-data-path.md` — the debt, its
  consequences, and the two-agent remediation target (ADR-001 data path).
- `docs/plans/two-agent-transport.md` — phased remediation plan (P0
  transport interface → P6 metrics/docs) with acceptance criteria
  (on-network compressed bytes, controller with no data connections).
- `doctor` now emits a `topology` WARN for cross-host copies (single
  controller in the data path); same-endpoint copies are unaffected.
- OPERATOR_GUIDE (`--compress`), METRICS, and SECURITY (threat model +
  controller-as-trust-root) state the actual topology.

### Fixed: resume now completes a unit a crashed run left `completed`

A run that died in the window between the `completed` journal write and the
atomic staging->final rename left the unit `completed` (verified data sitting
in the staging table) while the destination never received the table. A
re-run treated `completed` as terminal and skipped it, so the journal said
"done" but the table was missing — contradicting the run's own comment that a
resume should retry the swap. Now a re-run finishes the swap:

- staging table present -> the atomic rename is performed (same overwrite
  decision as the normal path), then sequences/segment-balance/index build
  run as for a normal swap; the unit is counted as a completed unit.
- staging gone, final present -> the rename already landed but the `swapped`
  journal write was lost; the state is simply recorded.
- staging and final both gone -> the verified data is lost (e.g. an operator
  cleanup); the unit is marked `failed` (a re-copy is required) and the
  anomaly is reported instead of claiming success.

New events: `resume_swap_completed`, `resume_swap_recorded`,
`resume_swap_data_lost`, `resume_swap_failed`. Test:
TestResumeCompletesUnswappedUnit (integration, all three cases).

### Fixed: primary key constraints are now restored (F-11)

The post-swap constraint pass read only foreign keys and user CHECK
constraints (`contype 'f','c'`), so a migrated table ended up with its PK's
unique index (built by the FR-17 pool) but **no `pg_constraint` row** — the
table had no primary key on the destination. Now the pass also restores the
primary key constraint (`contype 'p'`), with two accompanying changes:

- The FR-17 pool builds only the **secondary** indexes. The PK's unique index
  is created atomically by `ALTER TABLE ... ADD CONSTRAINT <name> PRIMARY
  KEY`; PostgreSQL does not adopt a pre-existing standalone unique index on
  that statement, so a pool-built PK index would be a 42P07 collision.
- PK constraints are applied in a **first pass, before** foreign keys and
  CHECKs, so an FK referencing a migrated table's key column finds the unique
  index. On a rename the PK constraint is rewritten to the destination
  table's naming (same rule as index names: `t_m1_pkey` → `t_m1_demo_pkey`).

Tests: TestPKConstraintRestoredOnLivePG (integration, no-rename + map-table
rename, asserts exactly one index and the `pg_constraint` row),
TestManagerRestoresPKConstraintRename / TestManagerRestoresPKConstraintNoRename
(unit).

### Fixed: map-table index builds — wrong-table REINDEX and name collisions

Two defects in the background index build (F-17) for renamed tables
(`--map-table`), found by a live B→C copy where the destination already
carried a prior full copy of the same source table:

- The existence check matched an index by **name + schema only**, so an
  index of the same name on *another* table in the destination schema
  (the prior copy) was treated as "already built" and `REINDEX`ed — while
  the renamed table itself was left with **no indexes and no primary key**,
  yet journaled `index_done: true`. The check is now scoped to the target
  table via `pg_index.indrelid`.
- Index names are unique **per schema**, so keeping the source name on a
  renamed table would collide with the other table's index. On a rename the
  index name is now rewritten to the destination table's naming
  (`t_m1_pkey` on `t_m1` → `t_m1_demo_pkey` on `t_m1_demo`; a name with no
  `<table>_` prefix gets the destination table name prefixed).

Tests: TestIndexRenameCollisionOnLivePG (integration, two real PG instances),
TestCreateIndexesRenameCollision / TestIndexExistsScopedToTable /
TestDestIndexName (unit); TestCreateIndexesNew and
TestManagerRunIndexesHonourRename updated for the renamed index names.

### Changed: `--overwrite` always rewrites; `--verify` is now `on`/`off` (default `off`)

Removed the pre-copy identical-data shortcut on `--overwrite`. Previously a
conflicting table whose data already matched the source was detected via a
full `COPY ... TO STDOUT` digest of **both** the source and the destination
and then skipped. That meant two extra full-table reads before any data moved
— prohibitive on very large tables. Now `--overwrite` (staging + swap) and
`--truncate` **always rewrite** the destination; there is no pre-copy content
comparison. `--resolve-conflicts skip` still skips conflicting units, and the
journal's completed-unit skip on a re-run is unchanged.

`--verify` now takes `on`/`off` (was `full`/`sample`/`off`) and is **`off` by
default**. With `on`, after each unit lands the content checksum is re-derived
on the source (same snapshot as the data COPY) and on the destination and
compared; a mismatch sets the unit to `failed` and drops the staging table —
no unverified data is ever swapped in. With `off` (the default) the COPY
protocol's row count is the landed-data check, so a default run reads each
source table exactly once. This makes the flag actually do what it says:
previously `full`/`sample`/`off` were parsed but the post-copy verify always
ran in full, so `off` did not disable it.

Unit tests: TestManagerOverwriteAlwaysRewrites (identical overwrite target is
re-copied + swapped, not skipped), TestManagerResolve (verify follows
`--verify`), TestManagerChecksumMismatch / TestManagerUnitFailureEmitsEvent
(under `--verify on`).

### Changed: operation identity is a timestamp by default; `--plan-file` removed

`--operation-id` is no longer defaulted to the deterministic plan hash. When
omitted on `run`, a **timestamp** id is generated (UTC,
`YYYYMMDDTHHMMSSZ`, e.g. `20261001T153000Z`; a `-2`, `-3`, … suffix on a
same-second collision), so the state directory is a **chronological history**
of runs — "find the latest run" is just the newest folder.

- `--operation-id latest` on `status`/`resume`/`cleanup` resolves to the
  newest operation (by timestamp folder name; mtime for operator-chosen
  names).
- Explicit human-readable ids are unchanged (first-class, not just
  timestamps).
- `--plan-file` is removed. The deterministic plan (CR-4) is now **always**
  persisted to `<state-dir>/<operation-id>/plan.json`, next to the journal,
  so every operation is self-describing.
- The plan hash is still computed and printed (and still used as the durable
  `job_id` in the destination journal tables); it is no longer the default
  folder name. A re-run of the same plan with no explicit id is therefore a
  **new** operation — pass the id (or `latest`) to resume an earlier one.

Unit tests: TestDefaultOperationIDFormat / TestResolveOperationID* /
TestLatestOperationID* / TestParseOperationArgsLatest / TestFolderTimestamp /
TestPersistPlan.

### Added: `gpstream bench` — input-stream benchmark (F-14)

A new `bench` command measures the source's production rate *before* the
data leaves for the destination: it streams the selected tables through the
real data path (`COPY ... TO STDOUT` -> frame v2 -> compression -> adaptive
rate limiter) and discards the payload instead of writing a staging table.
Only the source cluster is used (`--dest`, if given, is ignored); all
selection/performance options (CLI > env > toml) are honored exactly as for
`run`. Per-table and total rows, wire/logical bytes, elapsed time and MB/s
are printed to the screen. `--timeout S` is the benchmark's wall-time limit
(unlimited when not passed — a benchmark capped by default would truncate
the number it reports).

Use it to decide `--workers` / `--rate-limit` against the input ceiling:
if `bench` is already slower than the link, the destination is never the
bottleneck.

Unit tests: TestBenchSelectsSourceTables / TestBenchHonorsSelectionFlags /
TestBenchEmptySelection / TestBenchAllStreamsAndDiscards /
TestBenchAllTimeoutCutsShort / TestBenchAllEmptyUnits.

### Fix: overwrite drop of a destination table referenced by foreign keys
### (F-02/F-11, non-deterministic under parallel workers)

With `--resolve-conflicts overwrite`, the swap dropped the existing final
table with a plain `DROP TABLE`. If that table was referenced by foreign
keys on OTHER destination tables (a prior run's restored FK set, or an
operator table), the drop failed with `2BP01` (cannot drop table ... other
objects depend on it) — and because swap order depends on copy speed with
parallel workers, the failure was non-deterministic (which referenced
table got swapped first). The overwrite drop now removes the dependent FK
constraints first (surgically — not a blanket `DROP ... CASCADE`, so
operator views and other dependent objects are untouched); the manager's
post-load restore pass re-applies the source FK set (F-11), so the change
is lossless for the migrated tables.

Regression: live T30 run 2 (B→C) conflict matrix — `overwrite` over a
destination seeded with an FK graph (t_s09 → t_s01 + self-FK) failed with
2BP01 on t_s01/t_s09; after the fix the matrix is deterministic. Unit
tests: TestSwapOverwriteDropsDependentFKs /
TestSwapOverwriteNoDependentFKs.

### Removed the `man` subcommand (redundant)

Per the maintainer: the `gpstream man` subcommand is redundant — the man page is
installed to /usr/share/man/man8 by the package (standard `man gpstream`).
The subcommand also had a real bug: it read man/gpstream.8 relative to the
current working directory, so an installed binary printed a two-line
stub instead of the page. Removed from main.go (case + cmdMan), help,
the man COMMANDS section, the man-family generator (no gpstream-man.8),
tests, and docs (README, QUICKSTART now point at `man gpstream`).

### Docs: removed internal spec references from man page and `gpstream help`

Per the maintainer: user-facing output should not cite internal spec items. Removed
all `F-xx`/`FR-xx`/`v4 §N` references from `man/gpstream.8` (13 places) and
`printHelp` (6 places) — the descriptions now stand on their own.

### Fix: man seg-flags + full `gpstream help` (help/man vs code consistency)

`gpstream help` was stale: it omitted ~22 flags the CLI actually accepts
(`--config`, `--metrics-addr`, `--no-constraints`, `--verify`,
`--zstd-level`, `--exclude`, `--plan-file`, `--timeout`, `--dry-run`,
`--consistent-db/cluster`, `--ssl-cert/key/min-tls`, `--seg-tls-*`,
`--all-incomplete`, `--mode`, `--yes`, ...). `printHelp` is now rewritten
against the real flag set (verified: every flag `parseConfig` accepts is
present in the help text, no phantoms).

The man page squashed the five segment-direct flags into one unnamed pipe
line ("Segment-direct (on_segment) options"); they are now individual
entries with descriptions (`--seg-listen`, `--seg-binary`,
`--seg-tls-cert`, `--seg-tls-key`, `--seg-tls-ca`), rendered and verified
with groff on the lab.

### F-05 release pipeline: goreleaser + man-page family + version stamp

One-command release artifact build (tar.gz + RPM + DEB, amd64+arm64) via
goreleaser (pinned v1.24.0), plus the subcommand man-page family the spec
requires. `make package` / `scripts/package.sh` produce into `dist/`:
`gpstream-<ver>-linux-amd64.tar.gz`,
`gpstream-segreceiver-<ver>-linux-amd64.tar.gz`,
`gpstream-<ver>-amd64.{deb,rpm}`.

> **01.10 update:** arm64 dropped from the build matrix (no ARM hosts in the
> target environments). The F-05 pipeline originally produced amd64+arm64;
> it now produces linux/amd64 only.

- **Version stamp**: `main.version` is now an `ldflags`-overridable `var`
  (was a hard-coded `const`); the build stamps `{{ .Version }}` from the git
  tag, so `gpstream version` reports the release version.
- **Man family** (`scripts/gen-man-pages.sh`): the single source of truth is
  `man/gpstream.8`; each subcommand page is a `.so` alias so `man
  gpstream-run` etc. render the full manual under the subcommand name. The
  family is packaged into `/usr/share/man/man8/`.
- **`.goreleaser.yaml`**: two static builds (gpstream, gpstream-segreceiver),
  archives, and deb+rpm nfpms carrying the binary, systemd unit, example
  config, logrotate, and the man family; no runtime library dependency
  (static, proven on EL7/glibc 2.17 + EL8/glibc 2.28, F-16).
- **`release.yml`** workflow: on tag `v*`, builds and publishes artifacts to
  the GitHub release (goreleaser with `GITHUB_TOKEN`).
- **Packaging scripts** (`packaging/scripts/{postinstall,preremove,postremove}.sh`):
  systemd unit reload/stop/cleanup hooks.

**F-05 acceptance (clean host) verified on the lab (CentOS 7 / glibc
2.17):** `rpm -i gpstream-<ver>-amd64.rpm` installs the binary, segreceiver,
systemd unit, `/etc/gpstream/gpstream.toml`, logrotate, and the full man
family (`man gpstream-run` works); `gpstream version`/`selftest` pass from the
packaged binary; a second config file drives `doctor` (exact problem list on a
deliberately broken environment) and `dry-run`/`run`/`status` complete a real
operation. The spec's "clean host + package + 2 config files ⇒ working
operation" and "doctor on a broken environment ⇒ exact list of problems"
criteria are met.

### Fix: `skipped` is now a terminal unit state

A unit the run deliberately did not copy (a `--resolve-conflicts skip`, or an
`--overwrite` target whose data is already identical on the destination) was
counted as `skipped` in the run summary but left `state: pending` in the
journal. Consequence: after a successful run `status` reported `0/N units
terminal` (wrong), and `resume` would re-queue a unit the operator chose not
to copy. Now the unit is journaled `skipped` (a new terminal state): `status`
counts it as done, `resume`/`crash-recovery` leave it alone, and the
`--overwrite` checksum-skip emits a `unit_skipped` event for the audit trail.
`status`/`metrics`/`cleanup --all-incomplete` and the docs (OPERATOR_GUIDE,
TROUBLESHOOTING, FAILOVER, man) all treat `skipped` as terminal.

### Added `GPSTREAM_CONSISTENT_DB`/`_CLUSTER` env vars + `consistent_db`/`consistent_cluster` file keys; fixed consistency-scope resolution

the maintainer: the per-table env var had no sibling for the other two scopes
(asymmetry vs the naming rule), and `GPSTREAM_CONSISTENT_TABLES=1` was a
hard error ("mutually exclusive with --consistent-db") because the default
`ConsistentDB=true` could not be told apart from an explicit request — the
documented per-table fallback was effectively dead. Now: `Validate()`
resolves the scopes — `--consistent-db` stays the default, an explicit
`--consistent-cluster`/`--consistent-tables` (CLI/env/file) overrides it,
and only two *explicitly* requested scopes conflict. Added
`RequestConsistentDB()` (CLI `--consistent-db`), the two env vars and the two
file keys. Tests: `TestConsistentScopeResolution` (7-case matrix),
`TestConsistentEnvVars`, `TestLoadFileConsistentKeys`; the old
"tables+default-db errors" expectations inverted. Live-verified on GP6:
`GPSTREAM_CONSISTENT_TABLES=1` dry-run now succeeds, `--consistent-db
--consistent-cluster` still errors.

### Removed `--locale-check` (flag + env + file key)

the maintainer: collation/locale differences are always detected and reported
(`detectCollations` runs unconditionally; the dry-run always prints the
collation block), so the flag only added a redundant LC_TYPE count line.
Removed the CLI flag, `GPSTREAM_LOCALE_CHECK`, the `locale_check` file key,
the `Config.LocaleCheck` field and the dry-run locale line. File keys: 21→22
(net: −1 locale_check, +2 consistent_db/_cluster); env vars: 18→19 (−1
LOCALE_CHECK, +2 CONSISTENT_DB/_CLUSTER). Docs, man, CHANGELOG updated. All
tests green, English guard OK.

### Removed `--skew-threshold` and `--no-redistribute` (flags + file keys); raw skew metric kept

the maintainer (variant b): the threshold only drove a dry-run boolean warning (and a
plan-hash input) and never affected data or routing; `--skew-threshold -1` was
a bug (always-warn), not an off-switch, and `--no-redistribute` was
reporting-only (redistribution is done by the destination at COPY). The
`SkewThreshold`/`NoRedistribute` fields, the `--skew-threshold`/
`--no-redistribute` flags, the `skew_threshold` file key, the
`Redistribution.NeedsRedistrib`/`Threshold` fields and the `redistribCount`
warning were removed. The FR-15 metric stays: `detectRedistribution` now
reports the raw segment skew plus a layout verdict
(`identical` / `differs` / `cannot confirm`), always printed in the dry-run
(`redistribution: skew N% — <verdict>`), and the plan hash uses the raw skew
only (so report knobs no longer change the fingerprint). Tests: threshold
cases removed/converted (planner, planner_skew, main coverage, config). Docs,
man, packaging example updated. All tests green, English guard OK.

### Removed `--resume-consistency` (flag + file key); relaxed is now the only resume re-read rule

the maintainer: the strict mode (re-read the ENTIRE database on every resume) was a
rare, very expensive choice; F-02 already gives one-point-in-time within a
run, and relaxed re-reads exactly the tables that are not guaranteed to come
from a single snapshot (non-terminal or mixed-epoch). The flag, the
`resume_consistency` file key, the `JobMeta.ResumeCons` field and the
`gpstream_jobs.resume_cons` column were removed; `reReadTables` now takes no
mode. `status` no longer prints the mode. Tests: the strict cases were
removed/converted (epoch rule table, t7 resume run 3 now asserts idempotent
relaxed resume). Docs, man, CHANGELOG updated.

### Removed `--segment-balance-threshold` (flag, file key, env var)

the maintainer: the knob did not earn its place. The F-15 live segment-fill metric
stays — it is still measured after every swap, recorded in the final report
and events.jsonl, and the WARNING line now fires at a fixed 1.5 gate
(`dest.DefaultBalanceThreshold`). Removed: the CLI flag, the
`segment_balance_threshold` config-file key, and
`GPSTREAM_SEGMENT_BALANCE_THRESHOLD`. Docs, man page and tests updated;
the fixed-gate behavior is covered by TestManagerBalanceUsesFixedGate.

### Bug fixes found by the config-file semantic audit

- `--no-redistribute` no longer short-circuits `Validate()`: an early
  `return nil` made every later check (verify mode, copy mode, frame-ack
  values) unvalidated whenever the flag was present.
- Default `--timeout` is now **120s**, matching the spec (F-10 §6
  "--timeout (default 120 s)"); it was 60s.
- Config FILE `small_table_threshold` now accepts the same size notation as
  the CLI (`1M`, `512K`, `2G`, `1T`, or plain bytes) instead of plain bytes
  only.
- `--consistent-tables` / `GPSTREAM_CONSISTENT_TABLES` still error against
  the default DB scope (documented mutual exclusion kept); this remains a
  known sharp edge (see docs/CONFIGURATION.md).

### Aligned the config-file key and env var to the documented CLI name

The CLI flag stays `--workers` (as documented). Per the naming rule
(*the doc name without the leading `--` is the file key*), the
config-file key was renamed `workers_per_pair` → `workers` and the env
var `GPSTREAM_WORKERS_PER_PAIR` → `GPSTREAM_WORKERS`, so all three
layers carry one name: `--workers` / `workers` / `GPSTREAM_WORKERS`.


### Added

- `run` CLI contract per v4 §8: `--metadata-only` / `--data-only` (F-11;
  no `--include` flag — it duplicated the two shorthands and the default),
  `--zstd-level` (1–19), `--no-redistribute`, `--streams` / `--workers`.
- TOML configuration file support (`--config file.toml`, F-05) alongside YAML;
  precedence CLI > env > file > defaults.
- `doctor` self-diagnosis (F-16) with precise problem list.
- `internal/topology` package: single source of truth for the Greenplum
  catalog (`gp_segment_configuration`, 8 columns). `IsGreenplum`/`Read`/
  `SegmentCount`/`UpCount`/`ProbeSQL`; `doctor` now reuses it instead of
  duplicating catalog SQL (dedup, v4 §9 layout).
- `SECURITY.md`, `CONTRIBUTING.md`.
