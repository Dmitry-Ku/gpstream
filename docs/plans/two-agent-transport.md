# Two-agent transport — remediation plan (ADR-002)

- **Status:** Executed (phases P0–P6; see the v2 note and Post-decision changes)
- **Date:** 2026-10-02
- **Remediates:** ADR-002 (single-controller data path)
- **Restores:** ADR-001 data path (source agent ↔ dest agent stream)
> SUPERSEDED v2 (2026-10-04): the phase plan below (P0–P6) is executed and
historical. The TARGET architecture it must be read against is: (1) TWO
copy modes only — `coordinator` and `mesh` (renamed from `on_segment`);
the `direct` mode (P5) is removed, its per-segment write survives as the
per-table AUTO sub-paths of `mesh` — 1:1 (S==D, equal hash keys), routed
(S!=D, hash key), merge (non-routable fallback: non-hash distribution,
non-GP destination, partitioned parents); (2) agents run on the CLUSTER HOSTS
where their pg connections open — source agent on the source master
(long-lived, FR-02 snapshot, ON SEGMENT dispatch) and the dest agent
(supervisor) on the destination coordinator — both launched by the
controller (remotely over SSH (multi-host GP6 layout) or as a local
subprocess (single-host / gp6lab layout); internal/launch); the D
destination-segment mini-agents (mesh) are launched by the dest agent itself
(cmd/gpstream-dest-agent/minilaunch.go, local or --mini-ssh-user) — the
controller does not read the destination catalog and does not launch the
minis (it does not know D); (3) ports: the dest agent binds one data + one control port (auto `:0`,
real address reported to the controller in the control handshake and embedded
in the ON SEGMENT ProgramCommand); the source agent binds the control port
only — its data path is dial-out to the dest agent's listeners; units multiplex by unit_id frames on the listener —
per-unit ports (base+unitID) are removed from the live path (the
`segstream.UnitListen` helper remains in the code, unused by production
paths). The WAN leg carries only the
compressed framed stream; the controller holds no data connections.

## Goal

Move the data plane out of the controller process:

```
controller (any host): plan, journal, status, resume, metrics  — control plane only
source agent(s) (source cluster hosts): pg conn + COPY TO STDOUT + frame v2 + zstd
dest agent(s) (destination cluster hosts): receive + decompress + COPY FROM STDIN
agent<->agent stream: TLS (mTLS), compressed, one hop (A -> C)
```

Acceptance (the ADR-002 debt is closed when):
1. `gpstream_bytes_total` equals the bytes measured **on the network**
   (integration test samples `ss -i` / socket counters on the agent↔agent
   stream) within tolerance — i.e. the network carries the compressed
   volume.
2. The controller process holds **no data connections** (no pg conn to
   source/dest during the copy; only agent control conns).
3. All copy modes work through agents (`coordinator` / `mesh`, with the
   per-table 1:1 / routed / merge sub-paths); the two-agent path is the
   only data path — the `--topology` flag (and the `--topology single`
   fallback) was removed.

## Design

### Processes

| Process | Host | Role |
|---|---|---|
| `gpstream` (controller) | operator's choice | plan, journal, orchestration, status/resume/cleanup, metrics |
| `gpstream-source-agent` | source master (both modes; triggers the ON SEGMENT dispatch — `gpstream-segreceiver`, not the source agent, runs on the source segments) | opens the source pg conn, holds the FR-02 snapshot, `COPY ... TO STDOUT` (coordinator) or the ON SEGMENT dispatch (mesh), frame v2 + zstd, pushes to the dest agent |
| `gpstream-dest-agent` | destination coordinator (supervisor; the mesh minis run on the destination segments) | receives the stream, decompresses, `COPY ... FROM STDIN` (coordinator) or per-segment utility `COPY` (mesh 1:1 / routed) |

`gpstream-segreceiver` (existing) stays the on_segment source-side
program (it is already a per-segment binary); it gains a compressed-output
option instead of framing raw.

### Control protocol (controller ↔ agents)

The controller talks to the agents over short-lived control connections
(JSON over TLS, same cert bundle as the pg plane):

1. `build-plan`: the long-lived source agent builds the plan (units,
   staging names, distribution, compression, rate limit) and returns it to
   the controller (the source agent runs `plan.Build` with its local source
   pool + a remote destination querier; the controller opens no destination
   connection for planning).
2. Per unit, the controller sends the source agent a `stream` (coordinator
   path) or `onsegment` (mesh path: the source agent runs the ON SEGMENT
   dispatch) instruction `{schema, table, job_id, unit_id, dest, staging,
   compress}` and the dest agent a `serve-unit` (+ immediate `ack`). The
   source agent dials the dest agent (address from the unit instruction),
   performs the agent handshake (mTLS, unit_id + job_id binding, as
   `segstream.HandshakeFilter` already does), and streams. The FR-02
   snapshot is run-wide on the source agent (there is no per-unit snapshot
   token).
3. `unit-done {unit_id, rows, logical, wire, retransmitted, reconnects,
   swapped, verified, events, digest, src_digest}`: the dest agent reports
   the unit back to the controller via its control conn. The swap: on the
   mesh path the dest agent (supervisor) compares the source vs dest
   content digests and swaps itself; on the coordinator path the
   controller journals `completed` and swaps (runUnit step 9).
4. Heartbeat + cancel: the controller can cancel a unit (`cancel-unit`;
   the agent aborts the stream); a dead agent is detected by heartbeat
   timeout and the unit is marked failed (resume/re-run semantics
   unchanged).

Why the source dials the dest (not the other way): the source side owns
the snapshot and the data; the dest agent publishes a per-unit listen
endpoint from the plan (the `segstream.UnitListen` pattern already
proves this).

### Data protocol

Unchanged from today, but on a real socket: frame v2 (job_id, unit_id,
seq, content_id, crc) + zstd + optional ReliablePipe (ack/retransmit) —
i.e. `FramedPipe` over a `net.Conn` instead of a `net.Pipe`. The
`NewFramedPipe(writeConn, readConn net.Conn, ...)` constructor already
takes real conns; only the *selection* is in-memory. This is the main
reason the refactor is mechanical, not a rewrite.

### Snapshot & consistency

The FR-02 REPEATABLE READ snapshot moves into the source agent (it owns
the source pg conn). The pinned-conn logic (`beginSnapshot` per pinned
connection, `heal` re-establish after a unit failure) moves with it. The
`--consistent-db/--consistent-cluster/--consistent-tables` scopes were
removed (P7): the source agent always runs one run-wide REPEATABLE READ
snapshot per database. The controller no longer touches source data.

### Journal

Unchanged: primary journal in the destination service DB + file mirror on
the controller. The dest agent reports states; the controller writes the
journal (single writer preserved).

### Modes

- **coordinator (default):** 1 source agent (master) ↔ 1 dest agent
  (master), N units pipelined over the same mTLS stream (multiplexed by
  unit_id frames, as the on_segment SliceMux already multiplexes).
- **mesh (formerly `on_segment`):** segreceiver on each source segment
  pushes compressed slice streams to the dest agent; the dest agent merges
  (SliceMux moves to the dest agent) and COPYs via the coordinator.
- **direct (removed — see v2 note; the per-segment write survives as the
  1:1 / routed sub-paths of `mesh`):** the dest agent fans out per-segment
  `COPY FROM STDIN` to destination segments (today's dest-side direct load
  — `ServeDirect`, internal/agent/direct.go — moves into the dest agent);
  the row router runs in `gpstream-segreceiver` (RoutedSender), not in the
  source agent.

### Deployment

Per spec §5: agents installed on all hosts of both clusters over SSH
(gpssh-exkeys style); the controller discovers agent endpoints from the
plan (coordinator: `gp_segment_configuration`); TLS certs for the
agent↔agent stream reuse `--ssl-cert/--ssl-key/--ssl-ca` (mTLS).

## Phases (each phase = one green milestone)

- **P0 — transport interface.** Extract `transport.Stream` (framing +
  compression + reliability over any `net.Conn`); add a TCP
  `FramedPipe` factory; in-memory stays for tests. No behavior change.
  Acceptance: full suite green + a TCP-transport unit test (two processes,
  loopback) proving bytes on the socket == compressed volume.
- **P1 — source agent binary.** `cmd/gpstream-source-agent`: control
  protocol + snapshot + COPY TO STDOUT + framed/compressed push to a
  given endpoint. Acceptance: integration test (pgtest src + dummy
  socket server) — streamed volume on the socket is compressed, rows +
  trailer match.
- **P2 — dest agent binary.** `cmd/gpstream-dest-agent`: control
  protocol + receive + decompress + COPY FROM + swap-on-instruction +
  trailer reporting. Acceptance: integration test (dummy source socket →
  pgtest dest) — destination table correct, journal states reported.
- **P3 — controller as control plane.** `--topology two-agent` (new
  default candidate): controller drops its data conns, orchestrates the
  agents; single-controller stays as `--topology single` (default until
  two-agent is M7-accepted). Acceptance: full e2e on GP6 (A→C), T30
  dataset, `--verify on`, all three copy modes, crash/re-kill/resume
  matrix (kill each of the three processes mid-copy).
- **P4 — on_segment through agents.** SliceMux into the dest agent,
  segreceiver compressed output. Acceptance: on_segment e2e + the
  existing on_segment integration tests adapted.
- **P5 — direct through agents.** RowRouter into the source agent,
  per-segment dest COPYs in the dest agent. Acceptance: direct e2e +
  balance checks (F-15).
- **P6 — metrics + docs.** `gpstream_bytes_total` = on-wire bytes;
  OPERATOR_GUIDE/SECURITY/METRICS updated (ADR-002 closed); the dry-run
  topology WARN removed (replaced by `topology: two-agent` PASS).
  Acceptance: the ADR-002 acceptance (network-measured bytes).

## Risks

- **Three-process crash recovery** is the hard part: the protocol must
  make every unit state machine idempotent under a death of *any* of the
  three processes (source agent mid-COPY, dest agent mid-verify,
  controller between journal writes). Mitigation: keep the journal as the
  single source of truth; agents report monotonic state; resume =
  controller re-issues the unit (`stream`/`onsegment` + `serve-unit`;
  staging truncate-on-resume already exists in runUnit).
- **Snapshot lifetime** now spans an agent process: agent death must
  COMMIT/ROLLBACK the pinned snapshot (the `heal` re-establish logic moves to
  the agent + a controller-side watchdog for a dead agent).
- **Test surface grows** (process orchestration in integration tests).
  Mitigation: the pgtest harness + a loopback agent pair already cover
  most of it; keep the in-memory transport for unit tests.
- **Deployment complexity** (agents on both clusters). Mitigation: the
  segreceiver deployment path (spec §5, already built) is the template.

## Out of scope

- Multi-pair (controller on one host, several source/dest pairs in
  parallel) — possible later on the same protocol.
- Compression of the pg-plane legs (coordinator ↔ agent) — those become
  short control conns in two-agent mode.

## Post-decision changes (verified against the code, 2026-11-19)

- The `--topology` flag referenced by P3 (historical phase) was removed:
  the two-agent data path is now the only one (CHANGELOG.md: "Removed:
  `--topology` flag — the two-agent data path is now the only one").
- **Mini launch (mesh):** the D destination-segment mini-agents are launched
  by the dest agent itself (cmd/gpstream-dest-agent/minilaunch.go: local
  subprocess or SSH to the segment hosts via --mini-ssh-user); the controller
  only allocates the report-listen port and waits for the D-entry address book
  through the mini-map op. The "launched by the controller" wording for the
  minis (v2 note) is superseded (architecture 08.10).
- **Swap (both paths):** the destination agent performs the atomic swap for
  both paths — mesh: digest compare + swapMiniFinal; coordinator: since 2B-1
  ("the destination performs the atomic swap itself"). The controller's
  runUnit step 9 remains only as a fallback/resume-recovery (dstSwap via the
  dest agent's ddl op) for units the destination did not swap.
- **Dial-direction rationale:** per-unit listen endpoints were removed from
  the live path (v2 note): coordinator — the persistent-coordinator listener
  with unit_id-frame multiplexing (P1c); mesh — the D-entry mini address book
  embedded in the ON SEGMENT ProgramCommand (ProgramCommandMesh). The
  `segstream.UnitListen` helper remains in the code, unused.
- **Journal writer (full zero-DB, 08.10):** the dest agent is the writer of
  record — it writes the gpstream_units rows itself (journalUnitRef /
  journalMiniUnit) and the controller routes journal dest-DB writes through
  the dest agent (journal op); the controller opens no destination connection
  for the journal. The file mirror on the controller remains the durable
  resume source.
- **Mesh data path:** each source segreceiver streams its slice to its
  destination segment's mini-agent (1:1 — segreceiver s → mini s; routed S≠D —
  S×D×W sub-streams); the minis do per-segment utility COPY. Non-routable
  tables (hash-mismatch / non-hash distribution, partitioned parent,
  non-Greenplum side) take the COORDINATOR path (supervisor fallback);
  SliceMux (kind=segments) is unavailable in supervisor mode.
- **Staging truncate-on-resume:** lives in the dest agent (ensureMiniStaging
  truncates a surviving staging before re-copy), not in runUnit — the
  controller holds no data connections.
- **Instruction fields:** `stream` (coordinator) carries {schema, table,
  job_id, unit_id, dst_schema, dst_table, staging, compress, overwrite,
  verify_digest}; `onsegment` (mesh) carries {schema, table, job_id, unit_id,
  dest, segbin, compress, …} — the staging name reaches the dest agent/minis
  via MiniLaunch, not the onsegment instruction.
