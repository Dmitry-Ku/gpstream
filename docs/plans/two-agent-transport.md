# Two-agent transport — remediation plan (ADR-002)

- **Status:** Proposed
- **Date:** 2026-10-02
- **Remediates:** ADR-002 (single-controller data path)
- **Restores:** ADR-001 data path (source agent ↔ dest agent stream)
> SUPERSEDED v2 (2026-10-04): the phase plan below (P0–P6) is executed and
historical. The TARGET architecture it must be read against is: (1) TWO
copy modes only — `coordinator` and `on_segment`; the `direct` mode (P5)
is removed, its per-segment write survives as the per-table AUTO
sub-paths of `on_segment` — 1:1 (S==D, equal hash keys), routed (S!=D,
hash key), merge (non-routable fallback: non-hash distribution, non-GP
destination, partitioned parents); (2) agents run on the CLUSTER HOSTS
where their pg connections open — source agent on the source master
(long-lived, FR-02 snapshot, ON SEGMENT dispatch), dest agent on the
destination coordinator (coordinator mode) or destination segments
(on_segment sub-paths), launched remotely by the controller (SSH/daemon)
— NOT as local controller subprocesses; (3) ports: each long-lived agent
binds one data + one control port (auto `:0`, real address reported to the
controller in the control handshake and embedded in the ON SEGMENT
ProgramCommand); units multiplex by unit_id frames on the listener —
per-unit ports (base+unitID) are removed. The WAN leg carries only the
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
3. All three copy modes work through agents (coordinator / on_segment /
   direct), and the single-controller mode keeps working as a fallback
   (`--topology single`).

## Design

### Processes

| Process | Host | Role |
|---|---|---|
| `gpstream` (controller) | operator's choice | plan, journal, orchestration, status/resume/cleanup, metrics |
| `gpstream-source-agent` | source master (coordinator mode) / source segments (on_segment) | opens the source pg conn, holds the FR-02 snapshot, `COPY ... TO STDOUT`, frame v2 + zstd, pushes to the dest agent |
| `gpstream-dest-agent` | destination master (coordinator) / destination segments (direct) | receives the stream, decompresses, `COPY ... FROM STDIN` (coordinator) or per-segment `COPY` (direct) |

`gpstream-segreceiver` (existing) stays the on_segment source-side
program (it is already a per-segment binary); it gains a compressed-output
option instead of framing raw.

### Control protocol (controller ↔ agents)

The controller talks to the agents over short-lived control connections
(JSON over TLS, same cert bundle as the pg plane):

1. `plan-ready`: controller sends the plan (units, staging names,
   distribution, compression, rate limit) to each agent once.
2. `unit-start {unit_id, table, target, snapshot_token}`: per unit, the
   controller tells the source agent which table to stream and hands the
   dest agent the target staging name. The source agent dials the dest
   agent (address from the plan), performs the agent handshake (mTLS,
   unit_id + job_id binding, as `segstream.HandshakeFilter` already does),
   and streams.
3. `unit-done {unit_id, rows, wire, logical, trailer_sha256}`: the dest
   agent reports the trailer (rows + sha256) back to the controller via
   its control conn; the controller journals `completed`, then instructs
   the dest agent to swap (`ALTER ... RENAME`) — the swap stays a
   controller decision (journal semantics unchanged).
4. Heartbeat + cancel: the controller can cancel a unit (agent aborts the
   stream); a dead agent is detected by heartbeat timeout and the unit is
   marked failed (resume/re-run semantics unchanged).

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
the source pg conn). The pinned-conn logic (`establishSnapshot`,
`unitSource`, `--consistent-tables` per-table snapshots) moves with it,
unchanged. The controller no longer touches source data.

### Journal

Unchanged: primary journal in the destination service DB + file mirror on
the controller. The dest agent reports states; the controller writes the
journal (single writer preserved).

### Modes

- **coordinator (default):** 1 source agent (master) ↔ 1 dest agent
  (master), N units pipelined over the same mTLS stream (multiplexed by
  unit_id frames, as the on_segment SliceMux already multiplexes).
- **on_segment:** segreceiver on each source segment pushes compressed
  slice streams to the dest agent; the dest agent merges (SliceMux moves
  to the dest agent) and COPYs via the coordinator.
- **direct:** the dest agent fans out per-segment `COPY FROM STDIN` to
  destination segments (today's `copyStreamDirect` dest side moves into
  the dest agent); the RowRouter runs in the source agent.

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
  OPERATOR_GUIDE/SECURITY/METRICS updated (ADR-002 closed); doctor
  topology WARN removed (replaced by `topology: two-agent` PASS).
  Acceptance: the ADR-002 acceptance (network-measured bytes).

## Risks

- **Three-process crash recovery** is the hard part: the protocol must
  make every unit state machine idempotent under a death of *any* of the
  three processes (source agent mid-COPY, dest agent mid-verify,
  controller between journal writes). Mitigation: keep the journal as the
  single source of truth; agents report monotonic state; resume =
  controller re-issues `unit-start` (staging truncate-on-resume already
  exists in runUnit).
- **Snapshot lifetime** now spans an agent process: agent death must
  COMMIT/ROLLBACK the pinned snapshot (the `healSnapshot` logic moves to
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
