# ADR-003: `direct_1to1` fast-path — combine the proven on_segment read and direct write

- **Status:** Accepted
- **Date:** 2026-10-03
- **Part of:** ADR-001 (two-agent data path), ADR-002 (remediated debt)
- **Supersedes:** none
> SUPERSEDED (2026-10-04): the copy-mode surface is now TWO modes —
`coordinator` and `mesh` (renamed from `on_segment`). The `direct` mode
described here is removed as redundant; the `direct_1to1` fast-path
survives as the per-table 1:1 write sub-path of `mesh` (auto-selected when
S==D with equal hash keys), alongside the routed (S!=D) and merge
(non-routable) sub-paths. See docs/plans/two-agent-transport.md (v2 note)
and the target-architecture section of that plan.

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
over 2^32 in CI; see the `ANSWER-T4B-FASTPATH-DISPROVEN-2026-09-27` note
cited in `internal/route/mode.go` and `internal/plan/route.go`). Exact S==D
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
  `Unit.RouteMode`). Only `direct_1to1` qualifies — never `router_full` or
  `merge` (the resolver's full mode set; fan-in and congruence variants
  were disproven and are not modes).
- **Degradation:** a live destination segment-count mismatch (a primary down
  since planning) falls back to the coordinator path (event
  `mesh_coordinator_fallback`). The copy is always correct; only the speed
  changes. The run logs `mesh_direct_write` to the event stream per
  qualifying table (routed units log `mesh_routed_write`).

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

## Post-decision changes (verified against the code, 2026-11-19)

- **Copy-mode names:** the second copy mode is `mesh` in the code (renamed
  from `on_segment`); `direct` was removed (noted above). Current set:
  `coordinator` / `mesh`, with the per-table sub-paths `direct_1to1` /
  `router_full` / `merge` (internal/config/config.go, internal/route/mode.go).
- **Fallback:** implemented as the COORDINATOR path (event
  `mesh_coordinator_fallback`, internal/manifest/manager.go). The mesh merge
  serve (SliceMux, `serve-unit` kind=segments) is not available in
  supervisor mode — the destination agent uses kind=mini
  (internal/agent/destserver.go).
- **Partitioned-parent gate:** not implemented for the direct_1to1 branch —
  the planner does not exclude partition parents from direct_1to1
  (internal/plan/route.go), and the branch gates only on the live
  destination segment count (internal/manifest/manager.go). The partition
  gate (`part.Spec == ""`) exists only for the router_full branch
  (wantsRoutedWrite).
- **Swap (mesh path):** the destination agent owns verify + swap for mesh
  units — "runUnit step 9 moved to the agent" (architecture 08.10): MiniLaunch
  carries Final/Verify and the dest agent performs the atomic swap itself
  (internal/agent/destserver.go: swapMiniFinal). The controller's step-9
  dstSwap runs only as a fallback for units the destination did not swap
  (meshSwapped/coordSwapped, internal/manifest/manager.go:1729-1740). The
  Consequences line "the swap remains the controller's decision (runUnit step
  9)" is superseded — for both paths (the coordinator path since 2B-1).
