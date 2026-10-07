# gpstream — Performance & Throughput (v2)

This document covers the performance characteristics of the gpstream data
path and the controls that shape throughput. It is grounded in the v2
implementation (`internal/transfer`, `internal/ratelimit`,
`internal/manifest`) and in the `gp6lab`-tagged integration tests
(`tests/integration`).

## The target — and an honest caveat

The stated performance target is:

- **~100 Gbps** network between two Greenplum 6 clusters, and
- **100 TB – 1 PB** of data moved, with **no intermediate storage**
  (direct cluster-to-cluster streaming, FR-01).

**This target is a live-stand goal, not a verified number.** gpstream
cannot be benchmarked against a real 100 Gbps channel without a live stand,
and none is claimed here. The `gp6lab` integration suite does **not** measure
100 Gbit/s throughput; it characterizes the *behaviour* the data path must
have (completion in each copy mode, batch under the worker pool, crash
recovery, positive throughput) against **real Greenplum clusters** on the lab
stand. See the "What the perf tests do and do not prove"
section and `RC-CHECKLIST.md` (CR-7) for the honest limits.

Treat the figures in this document as **design intent and controls**, and
validate absolute throughput on a live stand before quoting it.

## The data path (what "no intermediate storage" means)

```
source agent --(framed, compressed)--> FramedPipe --(framed, decompressed)--> dest agent
  COPY ... TO STDOUT                       (net.Pipe / net.Conn)            COPY ... FROM STDIN
```

- The source agent runs `COPY ... TO STDOUT`; the destination runs
  `COPY ... FROM STDIN` **in parallel** over a single connection
  (`copyStream`).
- The transport is a `net.Pipe` (tests) or a `net.Conn` (production,
  optionally TLS, FR-13). It is **back-pressure-respecting** and holds
  data **only in RAM** — there is no intermediate storage and **no full DB
  dump on disk** (FR-01).
- Only small **service** temp files exist (metadata / journal / checksums
  / status / checkpoints); the table bytes never touch local storage.

### Backpressure

The `FramedPipe` is both the `io.Writer` (source) and the `io.Reader`
(destination) over one connected pair. If the destination (or the network)
slows, the source `COPY ... TO STDOUT` blocks on the pipe write —
backpressure propagates naturally, so gpstream never over-buffers a slow
receiver into disk. This backpressure is currently the *only* form of flow
pacing (see "Rate limiting" below for the status of the planned token-bucket
pacer).

### CRC framing (corruption without a re-copy)

Each frame carries a CRC (FR-01 / §4.2). The destination frames-verifies
and **decompresses**; a CRC mismatch yields `ErrCorrupt`, which is
**surfaced, not swallowed** — a corrupted frame is not silently accepted
(observable as `gpstream_frame_errors_total`, see METRICS.md).

## Rate limiting

### `--rate-limit <rate>`

Sets a **baseline** in **Gbps** (the flag accepts `bps`, `mbit`, and `gbit`
units, e.g. `50gbit`, `40000mbit`). Default is **0** — no baseline.

**What it does today (honest status):** the value drives the **ETA estimate**
in the dry-run plan and seeds the **adaptive load diagnostics** below. It does
**not** cap the actual flow: the data path is paced by **backpressure only**
(the unbuffered pipe blocks the source when the destination lags). The
token-bucket pacer that would enforce the ceiling is implemented and
unit-tested, but it is **not wired into the two-agent data path yet** (backlog;
see "Enforced pacing (backlog)").

### Adaptive load diagnostics (FR-14)

When a rate-limit is set, `NewManager` builds an adaptive limiter over the
controller host's metrics provider (`internal/ratelimit`):

```
if cfg.RateLimit > 0 {
    lim := ratelimit.New(ratelimit.Options{Baseline: cfg.RateLimit, Provider: ratelimit.NewProcProvider()})
}
```

Every second a sampler calls `lim.Update()`, which reads the **controller
host's** load from `/proc/stat` (CPU iowait), `/proc/diskstats` (disk await /
util), and TCP retransmits (decoupled via a `Provider` interface so it is
unit-testable), and moves the *current* rate by AIMD:

- **Reduce** by a **~25% step** (floor **~1 Gbps**) on sustained overload:
  - CPU **iowait > 30%**,
  - **disk util > 90%**,
  - **TCP retransmits > 100/s**, or
  - a destination **receive-queue write-latency > 50 ms** (recovery only
    above **20 ms** — hysteresis).
- **Recover** by **~10% per healthy interval** back toward the baseline,
  never exceeding it.
- Every change is a **diagnostic event** (`rate_limit_shift` with
  from/to/reason + the metric values) in `events.jsonl` and a gauge movement
  on `gpstream_rate_limit_gbits` (FR-04) — so load pressure on the controller
  host is observable.

Two honest caveats until the pacer is wired in: the metrics come from the
**controller** host (the copy load sits on the **agent** hosts), and the
receive-queue signal is not fed from the agent data path — so the reduce
reasons that can fire today are the controller-host ones.

### Enforced pacing (backlog)

The **pacer** (`internal/ratelimit/pacer.go`) is a token bucket that refills
at the limiter's **current** rate (burst ≈ 1 second of the current rate,
minimum 1 MiB); it never throttles to 0 and honours context cancellation.
Wiring it into the agent send loops — with each agent sampling **its own**
host's metrics, so the ceiling is enforced where the load actually is — is
backlog work; the correct home is the agents (ADR-002 moved the data path out
of the controller).

## Workers per pair

`--workers N` controls the **bounded worker pool** (FR-03).
`Manager.Run` dispatches `N` workers over a channel of units; `N` is
validated (a value `< 1` is clamped to `1`). The flow is paced by
backpressure; the planned shared pacer (one for the whole pool, so the
*total* flow — not each worker — stays under the ceiling) is backlog work
(FR-14).

A higher `--workers` spreads more tables across the connection
pool (useful for the "many small tables" case); a lower value limits
concurrency on a constrained link. A dead worker's units are re-assigned
to live workers (FR-06).

## Measuring the input stream (`gpstream bench`)

To isolate the **source-side** rate from the link/destination, `gpstream
bench` streams the selected source tables through the real data path (read →
frame → compress) and **discards** the payload — no
destination is opened. It applies the same selection and performance options
as `run` (`--workers`, `--compress`; `--rate-limit` is accepted and reported
but does **not** throttle the benchmark — the pacer is not wired into the
data path yet), and
prints per-table and total rows, wire/logical bytes, elapsed and MB/s.
`--timeout S` bounds the benchmark wall-time. Use it to answer "how fast can
the source feed the pipeline?" before committing to a full run.

## Data ports

Data ports are allocated automatically: every long-lived agent binds `:0`
and reports its real (routable) address in the control handshake, so the
stream needs no pre-planned port. A run uses exactly 3 ports in coordinator
mode (source control + dest data + dest control) — the on_segment
segreceivers dial the dest agent's shared data listener, so the segment
count adds no ports. In mesh (direct_1to1) mode a run uses **4+D** ports
(source control + dest control + dest report + dest data + D mini-agent data
ports, one per destination segment). For strict firewalls, restrict the
auto-allocated
ports with `--port-range <LO-HI>`: same-host agents get pre-bound
ports from the range, remote (SSH-launched) agents bind their auto-picked
ports inside it (the operator opens exactly that range).

**Status:** the two-agent data path (source agent + destination agent, the
controller holds no data connection) is the only data path. Two copy modes
are **implemented and live-verified** on the lab (`coordinator` and
`on_segment`; the former `direct` mode was removed — its per-segment write
lives on as the per-table auto sub-paths of `on_segment`):

- **`--copy-mode coordinator` (default)** — the source agent reads the table
  with one `COPY ... TO STDOUT` on the source coordinator; the destination
  agent loads the staging table with one `COPY ... FROM STDIN` on the dest
  coordinator. All data transits the two coordinator hosts.
- **`--copy-mode mesh` (T-B, source-direct read)** — the source agent
  triggers the ON SEGMENT dispatch (`COPY ... TO PROGRAM ... ON SEGMENT`
  under the FR-02 run snapshot): the kernel launches `gpstream-segreceiver`
  on every up primary source segment, and each streams its compressed
  content-tagged slice to the destination agent; the destination agent merges
  the N slices (SliceMux) and loads the staging table via the dest
  coordinator. Live-verified (E2: 111 000 rows, trailer cross-check OK);
  measured **~2–3.5× on the source-read side** (the coordinator is the
  read bottleneck; grows with segment count). Requirements: the
  `gpstream-segreceiver` binary on the source segment hosts
  (the `gpstream` package on every source segment host, optionally `--seg-listen`) and, on the open-gpdb 6.x fork, the GUC
  `ycmdb.yc_allow_copy_to_program=on` (postgres superuser). `gpstream dry-run`
  reports the capability as `on_segment` (PASS / WARN — WARN only degrades
  the *option*, the coordinator path always works).
- **`direct_1to1` fast-path** — when the planner proves a table's route
  is `direct_1to1` (S==D primaries, both tables `DISTRIBUTED BY` the same
  keys in the same order, same GP major, non-legacy hashops), `--copy-mode
  on_segment` additionally takes the **fast-path write**: the destination
  agent loads the N content-tagged slices with N per-segment utility-mode
  COPYs (one per dest segment, routed by content-id) instead of merging them
  into one coordinator COPY — **no SliceMux, no destination re-distribution**.
  The source side is the same ON SEGMENT dispatch (the kernel streams the
  same slices). The run logs `direct_1to1_fastpath` per such table. A
  partitioned parent, or a live destination segment-count mismatch (a primary
  down since planning), degrades to the on_segment merge path. The hash-aware
  routing kernel (`internal/route`, `ResolveMode`) is the single source of
  the mode; see `ANSWER-T4B-FASTPATH-DISPROVEN-2026-09-27.md` (only
  `direct_1to1` (S==D) and `router_full` are correct under the GP jump-hash).

## Content verification (`--verify on`)

`--verify on` (FR-02) checks the copied content, not just the row count. The
digest is a **row-order-independent** content hash — the XOR of one SHA-256
per row over the row's on-the-wire binary-COPY bytes, combined with the row
count (XOR is commutative, so the result is invariant to the row order the
distributed source gathers rows in; the row count distinguishes two
multisets that happen to XOR the same). Format: `"<rows>:<hex>"`.

**How it is derived (P9-E, since v0.2.0):** on the **coordinator** two-agent
data path (the default `--copy-mode coordinator`), the digest is computed
**inline, on the data path, on BOTH sides** as the rows stream — the source
agent tees the `COPY ... TO STDOUT` stream and the destination agent tees the
stream it feeds into `COPY ... FROM STDIN`, each through an O(1)-memory
incremental writer. The controller then compares the two inline digests
directly, so **verification re-reads neither table**. This is the M7-relevant
behavior: on the 100 TB → 1 PB pilot, the legacy post-copy verify would have
re-scanned the source table *and* the destination staging table (a full extra
read of ~100 TB); the inline digest removes both re-reads for the cost of a
per-row SHA-256 that the copy already pays to touch the bytes.

**Honest scope:** the inline path covers the coordinator data path (including
the DISTRIBUTED REPLICATED fallback into it). The **on_segment** and **routed**
sub-paths (multi-slice, per-segment utility COPYs) keep the **legacy post-copy
re-read** (re-derive on the source snapshot + verify the destination staging
table) — wiring the inline digest through the N-slice merge is the remaining
follow-up. When a copy mode does not report inline digests, the verify step
transparently falls back to the re-read, so `--verify on` is always correct,
only the re-read cost differs by path. Off by default (`off`), where the
COPY protocol's row count is the landed-data check.

## Compression

`--compress none|zlib|zstd[:1-19]` (FR-12). The source **compresses** and
the destination **decompresses**; only compressed bytes travel the wire.

- Default is **zstd**, **level 3** (config default `ZstdLevel=3`).
- Higher levels reduce wire bytes but cost CPU. **Auto-degrading the level
  under CPU saturation is not implemented yet** (backlog): the level is
  static for a run. The `gpstream_compression_level` /
  `gpstream_compression_mode` gauges exist and will report the level once the
  degrade path is wired in.

The metrics collector records both **wire bytes** (compressed) and
**decoded bytes** (logical) so the compression ratio is visible
(`gpstream_bytes_total` vs `gpstream_bytes_decoded_total`). In the two-agent
transport (the only data path) the wire counter is the on-wire agent-to-agent
volume, so the ratio between the two counters is the real inter-cluster
compression ratio.

## What the perf tests do and do not prove

The data-path characteristics are exercised by the `gp6lab`-tagged
integration suite (real clusters, real agent sockets; the in-controller
data path no longer exists — ADR-002). The relevant ones:

- **`TestTwoAgentManagerRun`** / **`TestTwoAgentOnSegmentServe`** /
  **`TestTwoAgentRoutedServe`** / **`TestTwoAgentDirect1to1Serve`** — real
  copies over the two-agent data path in each copy mode / sub-path; data is
  copied and verified, nothing fails (FR-01/FR-02).
- **`TestPoolParallelWorkersLive`** — a bounded worker pool over a live
  pair completes a batch of units (FR-03), the "many small tables" case.
- **`TestTwoAgentCrashRecoveryMatrix`** / **`TestSoakThroughputLive`** —
  crash-recovery and a live throughput soak (positive bytes, no stall).
- The **rate-limit limiter** itself (AIMD reduce/recover, floor, hysteresis)
  is covered by **unit** tests in `internal/ratelimit` with a faked
  `Provider` (deterministic iowait/util/retransmit inputs). Those prove the
  *limiter logic*; they do **not** prove the limiter bounds the data path,
  because the pacer is not wired in (backlog, above).

**These are not a 100 Gbit/s / 100 TB–1 PB benchmark.** They prove the
*shape* of the data path (that copies complete in each mode, that a batch
finishes under the worker pool, that bytes moved, that crash/recovery
holds). Validating the absolute target requires a live stand on a real
100 Gbps link between two Greenplum 6 clusters; **do not cite the perf
suite as evidence for the target.**

## Tuning cheatsheet

| Goal                                   | Knobs |
|----------------------------------------|-------------------------------------------|
| Reduce load on a side (prod protection) | `--workers N` down, `--compress` level up, schedule the run (an enforced `--rate-limit` ceiling is backlog) |
| Push a fast link, let it adapt         | `--workers N` up; with `--rate-limit` set, watch `rate_limit_shift` events for controller-host load |
| Many small tables / more concurrency   | `--workers N` up |
| Direct segment→segment routing        | `--copy-mode mesh` (source segments read in parallel; per-segment writes are auto-selected sub-paths); `direct_1to1` (S==D, equal hash keys) takes the fast-path write automatically |
| Trade CPU for wire bytes              | `--compress zstd[:level]` (default zstd:3) |
| Protect integrity during the push     | `--verify on` for critical copies (`off` default — the COPY row count is the check; `on` adds a content digest: on the coordinator path it is computed **inline on the data path** (no re-read of either table, P9-E); the on_segment/routed paths keep a post-copy re-read) |

## Honest notes

- The 100 Gbps / 100 TB–1 PB numbers are a **live-stand target**, not
  measured here.
- The `gp6lab` integration suite runs against **real Greenplum clusters** on
  the lab stand (real `postgres://` DSNs, real agent sockets); it
  characterises behaviour (completeness, recovery, concurrency), not
  absolute throughput on a production-grade link.
- Absolute throughput, latency, and the interaction of rate-limit +
  compression + backpressure on a real 100 Gbps link must be validated on
  a live stand before being quoted.
