# ADR-002: Single-controller data path (debt vs ADR-001 two-agent design)

- **Status:** Accepted (debt acknowledged, remediation planned)
- **Date:** 2026-10-02
- **Supersedes (partially):** ADR-001 data-path section

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
