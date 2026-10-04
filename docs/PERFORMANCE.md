# gpstream — Performance & Throughput (v2)

> NOTE (2026-10-04): the copy-mode surface is being simplified to TWO modes —
> coordinator and on_segment. The direct mode described in this document is
> redundant (its per-segment write is subsumed by on_segment's auto-selected
> per-table sub-paths: 1:1, routed, merge) and will be removed. This page
> still describes the CURRENT (shipped) behavior.

This document covers the performance characteristics of the gpstream data
path and the controls that shape throughput. It is grounded in the v2
implementation (`internal/stream`, `internal/limiter`, `internal/compress`,
`internal/job/manager.go`) and in the performance-characterization tests
(`tests/perf/perf_test.go`).

## The target — and an honest caveat

The stated performance target is:

- **~100 Gbps** network between two Greenplum 6 clusters, and
- **100 TB – 1 PB** of data moved, with **no intermediate storage**
  (direct cluster-to-cluster streaming, FR-01).

**This target is a live-stand goal, not a verified number.** gpstream
cannot be benchmarked against a real 100 Gbps channel without a live stand,
and none is claimed here. The `tests/perf` suite does **not** measure
100 Gbit/s throughput; it characterizes the *behaviour* the data path must
have (rate-limiting, batch completion, positive throughput) using a
**faked SQL layer**. See the "What the perf tests do and do not prove"
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
receiver into disk. The limiter's pacer adds a second, rate-based form of
backpressure (below).

### CRC framing (corruption without a re-copy)

Each frame carries a CRC (FR-01 / §4.2). The destination frames-verifies
and **decompresses**; a CRC mismatch yields `ErrCorrupt`, which is
**surfaced, not swallowed** — a corrupted frame is not silently accepted
(observable as `gpstream_frame_errors_total`, see METRICS.md).

## Rate limiting

### `--rate-limit <Gbps>`

Caps the total flow at a ceiling, in **Gbps**. The flag accepts `bps`,
`mbit`, and `gbit` units (e.g. `50gbit`, `40000mbit`). Default is **0** —
**no ceiling** (unlimited).

When a rate-limit is set, `NewManager` builds an adaptive limiter over the
host metrics provider and a pacer that throttles the data path (FR-14):

```
if cfg.RateLimit > 0 {
    lim   := limiter.New(limiter.Options{Baseline: cfg.RateLimit, Provider: ...})
    pacer := limiter.NewPacer(lim)   // throttles the source write
}
```

When `RateLimit == 0` the pacer is a **no-op** (unlimited), so an
unbounded copy is never accidentally throttled.

### Adaptive rate limit (FR-14)

The limiter (`internal/limiter/limiter.go`) reads **host load** and adapts
the *current* rate, independent of the raw network. It samples, on a live
host, from `/proc/stat` (CPU iowait), `/proc/diskstats` (disk await /
util), and TCP retransmits (decoupled via a `Provider` interface so it is
unit-testable).

Behaviour:

- **Reduce** by a **~25% step** (floor **~1 Gbps**) on sustained overload:
  - CPU **iowait > 30%**,
  - **disk util > 90%**, or
  - **TCP retransmits > 100/s**.
- **Recover** by **~10% per healthy interval** back toward the baseline,
  never exceeding the baseline.
- Every change is a **diagnostic message** carrying the metric values,
  emitted to the metrics collector (FR-04) — so a rate change is
  observable as `gpstream_rate_limit_gbits`.

The **pacer** (`internal/limiter/pacer.go`) is a token bucket that
refills at the limiter's **current** rate. Burst ≈ 1 second of the current
rate (minimum 1 MiB). When the limiter reduces the rate on host overload,
the pacer slows the data flow; when load subsides, it recovers. It never
throttles to 0 (a ~1 byte/s floor avoids a permanent stall) and honours
context cancellation.

The adaptive behaviour is **always active** (F-14) — there is no
`--adaptive-rate-limit` flag; without a rate-limit the limiter still
builds, it simply has no ceiling to enforce and only manages back-pressure
and the diagnostic rate-shift events.

## Workers per pair

`--workers N` controls the **bounded worker pool** (FR-03).
`Manager.Run` dispatches `N` workers over a channel of units; `N` is
validated (a value `< 1` is clamped to `1`). When a rate-limit is set,
**all workers share one pacer**, so the *total* flow across the pool
stays under the ceiling (FR-14) rather than each worker racing to it.

A higher `--workers` spreads more tables across the connection
pool (useful for the "many small tables" case); a lower value limits
concurrency on a constrained link. A dead worker's units are re-assigned
to live workers (FR-06).

## Measuring the input stream (`gpstream bench`)

To isolate the **source-side** ceiling from the link/destination, `gpstream
bench` streams the selected source tables through the real data path (read →
frame → compress → adaptive rate limiter) and **discards** the payload — no
destination is opened. It applies the same selection and performance options
as `run` (`--workers`, `--compress`, `--rate-limit`), and
prints per-table and total rows, wire/logical bytes, elapsed and MB/s.
`--timeout S` bounds the benchmark wall-time. Use it to answer "how fast can
the source feed the pipeline?" before committing to a full run.

## Data ports

There is **no `--data-port-range` flag** — the stream manages its own data
ports per **host-pair** connection (source-host → destination-host)
internally, bounded by the worker pool (`--workers`).

**Status:** the two-agent data path (source agent + destination agent, the
controller holds no data connection) is the only data path. Three copy modes
are **implemented and live-verified** on the lab:

- **`--copy-mode coordinator` (default)** — the source agent reads the table
  with one `COPY ... TO STDOUT` on the source coordinator; the destination
  agent loads the staging table with one `COPY ... FROM STDIN` on the dest
  coordinator. All data transits the two coordinator hosts.
- **`--copy-mode on_segment` (T-B, source-direct read)** — the source agent
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
  `ycmdb.yc_allow_copy_to_program=on` (postgres superuser). `gpstream doctor`
  reports the capability as `on_segment` (PASS / WARN — WARN only degrades
  the *option*, the coordinator path always works).
- **`--copy-mode direct` (P5, dest-segment write)** — the source agent reads
  with one coordinator `COPY ... TO STDOUT` and the RowRouter hashes each row
  to one of the D destination segments, streaming D per-segment compressed
  slices; the destination agent performs D utility-mode per-segment COPYs
  into the staging table. The dest coordinator is out of the write path. A
  table that cannot be routed (no distribution key, a partitioned parent, or a
  destination without all primaries up) **degrades to the coordinator path**
  (correct, just not direct) and the run logs `direct_degraded`.

- **`direct_1to1` fast-path (P8)** — when the planner proves a table's route
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

## Compression

`--compress none|zlib|zstd[:1-19]` (FR-12). The source **compresses** and
the destination **decompresses**; only compressed bytes travel the wire.

- Default is **zstd**, **level 3** (config default `ZstdLevel=3`).
- Compression is **auto-degraded on CPU saturation** (FR-12/FR-14): the
  level can drop under load, and that change is **observable as a metric**
  (`gpstream_compression_level` / `gpstream_compression_mode`).
- Higher levels reduce wire bytes but cost CPU; the adaptive limiter and
  compression degrade together so a saturated host sheds compression
  before it sheds throughput.

The metrics collector records both **wire bytes** (compressed) and
**decoded bytes** (logical) so a compression change is visible
(`gpstream_bytes_total` vs `gpstream_bytes_decoded_total`). In the two-agent
transport (the only data path) the wire counter is the on-wire agent-to-agent
volume, so the ratio between the two counters is the real inter-cluster
compression ratio.

## What the perf tests do and do not prove

The data-path characteristics are exercised by the `gp6lab`-tagged
integration suite (real clusters, real agent sockets; the in-controller
data path no longer exists in P7):

- **`TestBatchManyUnitsComplete`** — a batch of 50 units over 4 workers
  all complete, data is copied, nothing fails (FR-01/FR-03). This is the
  "many small tables" characteristic, reduced to a fast, deterministic
  batch.
- **`TestRateLimitBoundsDataPath`** — with a 100 Gbps ceiling (floor 10,
  step 0.25) and a sustained iowait of 90%, the limiter holds the current
  rate at or below the ceiling and **steps down below baseline**, then
  **recovers** on a healthy interval without exceeding the baseline
  (FR-14).
- **`TestThroughputIsPositive`** — a real copy produces **positive**
  throughput and records bytes (not a fake "no bytes transferred")
  (FR-04).

**These are not a 100 Gbit/s / 100 TB–1 PB benchmark.** They prove the
*shape* of the data path (that a rate-limit actually bounds it, that a
batch completes, that bytes moved). Validating the absolute target
requires a live stand on a real 100 Gbps link between two Greenplum 6
clusters; **do not cite the perf suite as evidence for the target.**

## Tuning cheatsheet

| Goal                                   | Knobs |
|----------------------------------------|-------------------------------------------|
| Cap a link so it doesn't starve prod  | `--rate-limit <Gbps>` (the limiter auto-degrades, F-14) |
| Push a fast link, let it adapt         | `--rate-limit` high, let the limiter auto-degrade |
| Many small tables / more concurrency   | `--workers N` up |
| Direct segment→segment routing        | `--copy-mode on_segment` (source segments read in parallel) + `--copy-mode direct` (per-segment writes); `direct_1to1` (S==D, equal hash keys) takes the fast-path write automatically |
| Trade CPU for wire bytes              | `--compress zstd[:level]` (default zstd:3) |
| Protect integrity during the push     | `--verify on` for critical copies (`off` default — the COPY row count is the check; `on` adds a post-copy checksum of both sides) |

## Honest notes

- The 100 Gbps / 100 TB–1 PB numbers are a **live-stand target**, not
  measured here.
- The perf suite uses a **faked SQL path**; it characterises behaviour,
  not absolute throughput.
- Absolute throughput, latency, and the interaction of rate-limit +
  compression + backpressure on a real 100 Gbps link must be validated on
  a live stand before being quoted.
