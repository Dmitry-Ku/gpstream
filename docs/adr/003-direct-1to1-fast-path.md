# ADR-003: `direct_1to1` fast-path — combine the proven on_segment read and direct write

- **Status:** Accepted
- **Date:** 2026-10-03
- **Part of:** ADR-001 (two-agent data path), ADR-002 (remediated debt)
- **Supersedes:** none

## Context

The two-agent data path (ADR-001/002, closed) has three copy modes. Two of
them each solve half of the "segment-to-segment, no coordinator in the data
path" goal:

- **`on_segment` (P4)** reads in parallel: the kernel's ON SEGMENT dispatch
  launches a segreceiver on every up primary source segment, and each streams
  its content-tagged compressed slice to the destination agent. But the
  **write** still centralises: the destination agent merges the N slices
  (SliceMux) into one coordinator `COPY FROM`, and the destination kernel
  re-distributes the rows (a second hash).
- **`direct` (P5)** writes in parallel: the source agent's RowRouter hashes
  each row to one of D destination segments and the destination performs D
  per-segment utility-mode COPYs (dest coordinator out of the write path).
  But the **read** still centralises: one coordinator `COPY TO STDOUT`, and
  the RowRouter hashes every row.

When the source and destination clusters are **identical in the ways that
matter** — same number of up primary segments (S==D), both tables
`DISTRIBUTED BY` the same keys in the same order, same GP major version (the
jump-hash is identical), non-legacy hashops — the kernel distributed the rows
**identically** on both clusters. Source segment `s`'s local slice is exactly
destination segment `s`'s local slice. The planner already proves this case
and labels it `direct_1to1` (`internal/route.ResolveMode`); the data path did
not use it.

The "almost 1:1" variants (D divides S, congruence mod g) were **disproven**
for the non-legacy GP jump-hash (a `JCH(h, S) mod D != JCH(h, D)` brute force
over 2^32 in CI; see `ANSWER-T4B-FASTPATH-DISPROVEN-2026-09-27`). Exact S==D
with equal keys is the only correct fast-path.

Measured on GP6 (4 segments, 3.2 GB / 1.29 M rows): the coordinator read
bottleneck is ~3.5× (per-segment read parallelism), the coordinator write
bottleneck is ~1.55× warm (per-segment write), and the per-row hash is
~19 ns (negligible below ~100 M rows). So the fast-path — per-segment read
**and** per-segment write — removes the coordinator from the data path
entirely, the ADR-001 "one hop, not two" endgame, for ≈ 1.5–1.8× end-to-end
at 4 segments and higher at larger segment counts (the read component grows
roughly linearly with segment count).

## Decision

Reuse the two already-implemented, live-verified components; add only the
glue. When the plan proves a table's route is `direct_1to1`:

- **Read:** the source agent runs the **same ON SEGMENT dispatch** as
  `on_segment` (kernel launches the segreceivers; each streams its
  content-tagged compressed slice). No change.
- **Write:** the destination agent loads the N content-tagged slices with N
  **per-segment utility-mode COPYs** (routed by content-id) instead of
  merging them (SliceMux) into one coordinator COPY. No merge, no destination
  re-distribution.
- **Gate:** `plan.Table.RouteMode == direct_1to1` (carried onto the unit as
  `Unit.RouteMode`). Only `direct_1to1` qualifies — never `router_full`,
  `direct_fanin`, or `router_congruence`.
- **Degradation:** a partitioned parent (utility-mode `COPY FROM` is rejected
  on a partition-hierarchy root), or a live destination segment-count
  mismatch (a primary down since planning), falls back to the `on_segment`
  merge path. The copy is always correct; only the speed changes. The run
  logs `direct_1to1_fastpath` to the event stream per qualifying table.

This is the `on_segment` branch of the fast-path (its write side loses the
most: the merge + re-distribution). The `direct` branch (ON SEGMENT read
instead of the coordinator read + RowRouter) is a follow-up and reuses the
same gate.

## Consequences

- The destination coordinator is out of the data path for qualifying tables
  in both the read and the write — the ADR-001 endgame for the S==D case.
- The `direct_1to1` write side gains its first in-repo test
  (`TestTwoAgentDirect1to1Serve`, `gp6lab`): N content-tagged compressed
  slices → N per-segment COPYs, per-segment distribution exact, wire <
  logical (compressed on the network), swap lands. The read side
  (ON SEGMENT kernel dispatch) is Greenplum-only and is validated live on GP6
  (e2e requires an S==D cluster pair).
- The fast-path is invisible to the operator: it is selected by the planner's
  route proof, not a flag. No new CLI surface.
- Per-segment balance (F-15) is unchanged for S==D equal-key tables: the
  per-segment fill on the source and the destination is identical (the same
  hash key), so there is no balance degradation from the fast-path.
- The per-segment write is a utility-mode COPY into the **staging** table
  only; the swap remains the controller's decision (runUnit step 9), exactly
  as in the other two-agent paths.
