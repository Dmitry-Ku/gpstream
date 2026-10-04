# ADR-002: Single-controller data path (debt vs ADR-001 two-agent design)

- **Status:** Closed (remediated by the two-agent data path — the only data path since the `--topology` flag was removed; ADR-001)
- **Date:** 2026-10-02
- **Supersedes (partially):** ADR-001 data-path section
> NOTE (2026-10-04): this ADR is historical (the debt is closed). Its
"Two-agent design" section describes THREE copy modes and agents launched
locally by the controller — both superseded by the target architecture:
TWO modes (`coordinator`, `on_segment` with per-table 1:1/routed/merge
sub-paths) and agents running on the CLUSTER HOSTS where their pg
connections open (source agent on the source master, dest agent on the
destination coordinator or segments). See
docs/plans/two-agent-transport.md (v2 note).
- **Remediated:** 2026-10-02 — the two-agent data path is live for all three
  copy modes (`coordinator` P3, `on_segment` P4, `direct` P5); see the
  "Resolution" section at the end.

## Context

ADR-001 describes the intended data path as **two agents on the two
clusters** connected by an encrypted stream: source agent (COPY TO + frame +
compress) on the source side, dest agent (decompress + COPY FROM) on the
destination side, with `--compress` compressing the traffic **between the
hosts** (spec F-12: "compression between hosts"; spec §5: "installing the
agents on all hosts of both clusters").

The implemented M0–M7 build runs a **single controller process** that holds
BOTH PostgreSQL connections (source pool and destination pool) in one
process. The `FramedPipe` (frame v2 + zstd) is built over an **in-memory**
`net.Pipe` inside that process (`manager.framedPipe`), so:

1. **Compression does not touch the network.** Both TCP legs carry
   *uncompressed* binary COPY: source coordinator → controller (raw),
   controller → destination (raw). zstd compresses and decompresses in the
   controller's memory. `--compress` therefore consumes controller CPU
   without reducing inter-cluster bandwidth.
2. **The `gpstream_bytes_total` / `progress: X / Y` metrics are
   misleading**: they count the compressed bytes of the *in-memory* pipe,
   not the bytes on the wire. A status line like `512.4 MB / 34.7 MB` looks
   like a 15× network reduction but the network carried the full ~512 MB.
3. **Man-in-the-middle topology.** With the controller on an external host,
   the entire data stream transits the controller's memory (plaintext in
   RAM), and the data takes two hops (A→K, K→C) instead of one (A→C):
   doubled network volume, and the controller host becomes a trust root for
   the data, not just for the control plane.
4. In `on_segment` mode the source leg (segment → controller) is framed but
   also uncompressed; in `direct` mode the controller→segment legs are raw
   binary COPY. The same debt applies to all three copy modes.

The single-process model was a deliberate M0–M2 trade: it made the whole
pipeline testable in-process (pgtest embedded clusters), kept deployment to
one binary, and validated the frame/compress/verify/swap logic. The comment
in `framedPipe` ("in production this is a net.Conn") records the intended
production transport, which is not yet wired.

## Decision

Keep the single-controller model for now; **record the debt explicitly** and
plan the two-agent remediation:

- The in-memory `FramedPipe` stays (it validates frame v2, compression,
  back-pressure, and verify end-to-end), but the docs and metrics no longer
  claim the network is compressed:
  - `gpstream_bytes_total` is renamed in meaning to "bytes pushed through
    the framed transport (compressed in-memory)"; the operator-facing
    progress line keeps both numbers but the docs state the wire is raw.
  - OPERATOR_GUIDE (`--compress` section) and SECURITY.md (threat model)
    state the actual topology.
- The remediation target is ADR-001's two-agent design:
  - **source agent** process(es) on the source cluster hosts: source
    connection, `COPY ... TO STDOUT`, frame v2 + zstd, push over the network.
  - **dest agent** process(es) on the destination cluster hosts: receive,
    decompress, `COPY ... FROM STDIN` (coordinator) or per-segment `COPY`
    (direct).
  - **controller** on any host: plan, journal, status, resume, metrics —
    control plane only, out of the data path.
  - Key exchange across cluster hosts as in spec §5 (gpssh-exkeys style);
    TLS (mTLS) on the agent↔agent stream.

## Consequences

- Until the two-agent model lands, `--compress` is CPU cost on the
  controller with no bandwidth benefit; operators on bandwidth-bound links
  should expect the raw volume on the wire.
- The controller must be treated as a data trust root (SECURITY.md §threat
  model): place it where the data is allowed to be read.
- `doctor` warns (WARN) when the controller host differs from both cluster
  hosts and compression is on (the debt is most visible there).
- The two-agent refactor is tracked as the next major work item (plan in
  `docs/adr/002-...` follow-up / T30 backlog); acceptance: compressed bytes
  measured **on the network** (e.g. `ss`/`nload` sampling in an integration
  test) equal `gpstream_bytes_total` within tolerance, and the controller
  process holds no data connections.

## Resolution (2026-10-02, plan `docs/plans/two-agent-transport.md` P0–P5)

The two-agent design is implemented and live behind `--topology two-agent`
(default remains `single`, the fallback). All three acceptance criteria of
this ADR are met:

1. **`gpstream_bytes_total` is the on-network volume.** In two-agent mode the
   counter records the bytes measured on the real agent-to-agent socket
   (source agent `SendCompressed` counting writer; dest agent `ReadStreamCompressed`
   raw counter), not an in-memory pipe. Measured on GP6 (A→C): coordinator
   P3 — 105,755 wire vs 1,677,552 logical (15.9×); on_segment P4 — 120,526
   vs 1,064,915 (8.8×); direct P5 — 1,437,130 vs ~4.44 MB logical (~3.1×,
   low-entropy 200-byte payload).
2. **The controller holds no data connections.** The controller's
   PostgreSQL connections are control plane only (plan, journal, catalog
   reads: segment enumeration + distribution policy; the checksum reads in
   two-agent mode are the agents' catalog/snapshot reads, not the data
   stream). The data stream runs source agent → dest agent over one socket;
   in `direct` mode the destination coordinator is out of the write path
   (per-segment utility-mode COPYs).
3. **All three copy modes work through the agents.** `coordinator` (P3),
   `on_segment` (P4, SliceMux in the dest agent), `direct` (P5, RowRouter in
   the source agent + per-segment utility COPYs). `--topology single`
   remains available as the fallback, still documented as the ADR-002 debt.

`doctor` reports the topology check as **PASS** under `--topology two-agent`
(data runs agent → agent, compressed on the wire; the controller holds no
data connections) and keeps the WARN for the single-controller build.
OPERATOR_GUIDE, SECURITY.md, METRICS.md, CLI_REFERENCE and CONFIGURATION are
updated to state the actual per-topology topology.

Remaining debt recorded as a follow-up (not a blocker for closing this ADR):
the three-process crash-recovery matrix (idempotent unit state machine under
a death of any of the three processes — source agent mid-COPY, dest agent
mid-verify, controller between journal writes) is tracked in the plan's
Risks section and the T30 backlog.
