# gpstream — Metrics & Observability

This document covers gpstream's observability: the Prometheus `/metrics`
endpoint, the exact counters it exposes, and the on-disk operation logs.
Everything below is grounded in the current implementation
(`internal/monitor/metrics.go`, `internal/monitor/events.go`,
`internal/manifest/job.go`, `cmd/gpstream/main.go`).

## The `/metrics` endpoint

Pass `--metrics-addr <addr>` to **`run`**. The run starts an HTTP server that
exposes the live metrics for the operation (F-04) and prints, right after
launching the server goroutine:

```
metrics:     127.0.0.1:9100/metrics
```

The printed line is `<addr>/metrics` (no `http://` prefix), and it is
printed **before** the listen is known to have succeeded — a listen
failure is reported later, on stderr, and is non-fatal (below).

Behaviour of the metrics server (`cmd/gpstream/main.go`):

- The server is `http.Server{Addr: addr, Handler: m.Handler()}` and is
  started in a goroutine; it returns a stop function that
  `srv.Shutdown(ctx)`s the server when the run finishes.
- **A listen failure is non-fatal (F-04 is observability, not core):** the
  run proceeds and the error is logged to stderr
  (`gpstream: metrics: <err>`).
- The exposition format is the **Prometheus textformat v0.0.4**, written
  **in-tree with no third-party metrics dependency** (F-08).

> **There is no separate `metrics` subcommand.** The Prometheus endpoint is
> served *during* `run` when `--metrics-addr` is set; scrape it with `curl`
> or Prometheus while the copy is in flight.

## About `/health` — an honest note

gpstream's metrics server has a **single root handler** that returns the
Prometheus exposition for **any** path. That means:

- `GET http://<addr>/metrics` returns the Prometheus text (the documented,
  intended path).
- There is **no distinct `/health` route with health-specific semantics.**
  A request to `http://<addr>/health` against the same server returns the
  **same Prometheus text** — it is **not** a readiness/liveness probe with
  `200`/`5xx` health semantics.

Do **not** treat `http://<addr>/health` as a real health endpoint. For
health / readiness use the commands below, which carry real semantics:

| Check | Command | What it tells you |
|-------|---------|-------------------|
| Liveness / operation state | `gpstream status --operation-id <id>` | per-unit state |
| Cluster readiness | `gpstream dry-run` | reachability + `PASS`/`FAIL` |
| Live counters (Prometheus) | scrape `http://<addr>/metrics` (served by `run --metrics-addr`) | throughput / latency / error rate |
| Error health | `gpstream_errors_total`, `gpstream_frame_errors_total` (via `/metrics`) | rising errors = unhealthy |

If you need a probe, scrape `/metrics` and alert on
`gpstream_frame_errors_total` / `gpstream_errors_total`, or use
`gpstream dry-run`. A dedicated `/health` route is **not** currently wired
into the metrics server.

## The counters (Prometheus exposition)

`m.Handler()` sets `Content-Type: text/plain; version=0.0.4` and renders the
following series (exact `# HELP` / `# TYPE` lines from
`internal/monitor/metrics.go`):

```
# HELP gpstream_bytes_total Bytes written to the data transport (compressed; two-agent: the on-wire agent-to-agent socket volume, single: the in-memory framed pipe).
# TYPE gpstream_bytes_total counter
# HELP gpstream_bytes_decoded_total Logical bytes after decompression.
# TYPE gpstream_bytes_decoded_total counter
# HELP gpstream_rows_total Rows copied (FR-04).
# TYPE gpstream_rows_total counter
# HELP gpstream_errors_total Transfer errors.
# TYPE gpstream_errors_total counter
# HELP gpstream_frame_errors_total CRC/framing errors.
# TYPE gpstream_frame_errors_total counter
# HELP gpstream_checksum_errors_total Content-checksum mismatches in the verify phase (FR-04).
# TYPE gpstream_checksum_errors_total counter
# HELP gpstream_auto_recovery_total Auto-recovery actions (resume re-reads, retries) (FR-04).
# TYPE gpstream_auto_recovery_total counter
# HELP gpstream_rate_limit_gbits Current rate limit in Gbps (FR-14).
# TYPE gpstream_rate_limit_gbits gauge
# HELP gpstream_receive_write_latency_ms Average data-path write latency (ms) — destination receive-queue depth as the controller observes it (FR-14).
# TYPE gpstream_receive_write_latency_ms gauge
# HELP gpstream_throughput_bytes Current throughput in bytes/sec.
# TYPE gpstream_throughput_bytes gauge
# HELP gpstream_eta_seconds Estimated remaining seconds at the recent throughput (FR-04).
# TYPE gpstream_eta_seconds gauge
# HELP gpstream_compression_level Current compression level (FR-12).
# TYPE gpstream_compression_level gauge
# HELP gpstream_compression_mode Current compression mode (FR-12).
# TYPE gpstream_compression_mode gauge
# HELP gpstream_index_queue Background index builds pending (FR-17).
# TYPE gpstream_index_queue gauge
# HELP gpstream_index_done_total Background index builds finished (FR-17).
# TYPE gpstream_index_done_total counter
# HELP gpstream_active_flows Units currently copying (FR-04 “number of active flows”).
# TYPE gpstream_active_flows gauge
# HELP gpstream_frames_retransmitted_total Frames retransmitted after loss/corruption (§4.6).
# TYPE gpstream_frames_retransmitted_total counter
# HELP gpstream_reconnects_total Data-path transport reconnects (§4.6).
# TYPE gpstream_reconnects_total counter
# HELP gpstream_ack_lag_max_seconds Worst observed ACK lag (§4.6).
# TYPE gpstream_ack_lag_max_seconds gauge
# HELP gpstream_tables_by_status Units by state (FR-04).
# TYPE gpstream_tables_by_status gauge
# HELP gpstream_table_rows Rows copied per table (FR-04).
# TYPE gpstream_table_rows counter
# HELP gpstream_table_bytes_wire Wire bytes per table (FR-04).
# TYPE gpstream_table_bytes_wire counter
# HELP gpstream_table_bytes_logical Logical bytes per table (FR-04).
# TYPE gpstream_table_bytes_logical counter
# HELP gpstream_table_state Current unit state per table (1 = in this state) (FR-04).
# TYPE gpstream_table_state gauge
# HELP gpstream_pair_up Host pair of the running copy (1 = up) (FR-04).
# TYPE gpstream_pair_up gauge
```

Summary of the series:

| Series | Kind | Labels | Meaning |
|--------|------|--------|---------|
| `gpstream_bytes_total` | counter | — | bytes written to the data transport (compressed): **two-agent** — the on-wire agent-to-agent socket volume (ADR-002 remediated); **single** — the in-memory framed pipe, while the network legs stay raw (ADR-002 debt) |
| `gpstream_bytes_decoded_total` | counter | — | logical bytes after decompression |
| `gpstream_rows_total` | counter | — | rows copied |
| `gpstream_errors_total` | counter | — | transfer errors |
| `gpstream_frame_errors_total` | counter | — | CRC / framing errors (F-01 / §4.2) |
| `gpstream_checksum_errors_total` | counter | — | content-checksum mismatches in the verify phase |
| `gpstream_auto_recovery_total` | counter | — | auto-recovery actions (resume re-reads, retries) |
| `gpstream_rate_limit_gbits` | gauge | — | current rate limit in Gbps (F-14) |
| `gpstream_receive_write_latency_ms` | gauge | — | avg data-path write latency (ms) = destination receive-queue depth (F-14) |
| `gpstream_throughput_bytes` | gauge | — | current throughput in bytes/sec |
| `gpstream_eta_seconds` | gauge | — | estimated remaining seconds at recent throughput |
| `gpstream_compression_level` | gauge | — | current zstd level (F-12) |
| `gpstream_compression_mode` | gauge | `{mode}` | current mode (`none`/`zlib`/`zstd`) |
| `gpstream_index_queue` | gauge | — | background index builds pending (F-17) |
| `gpstream_index_done_total` | counter | — | background index builds finished (F-17) |
| `gpstream_active_flows` | gauge | — | units currently copying |
| `gpstream_frames_retransmitted_total` | counter | — | frames retransmitted after loss/corruption (§4.6) |
| `gpstream_reconnects_total` | counter | — | data-path transport reconnects (§4.6) |
| `gpstream_ack_lag_max_seconds` | gauge | — | worst observed ACK lag (§4.6) |
| `gpstream_tables_by_status` | gauge | `{status}` | units by state |
| `gpstream_table_rows` | counter | `{table}` | rows copied per table |
| `gpstream_table_bytes_wire` | counter | `{table}` | wire bytes per table |
| `gpstream_table_bytes_logical` | counter | `{table}` | logical bytes per table |
| `gpstream_table_state` | gauge | `{table}`, `{status}` | current unit state per table (1 = in this state) |
| `gpstream_pair_up` | gauge | `{pair}` | host pair of the running copy (1 = up) |

### How the gauges are computed

- **`gpstream_throughput_bytes`** — over a rolling **~60 s window**: the
  collector keeps cumulative byte samples and returns
  `(last.bytes − first.bytes) / (last.t − first.t)`; with fewer than two
  samples it returns `0`.
- **`gpstream_eta_seconds`** — remaining volume divided by the average
  throughput since run start (not the 60 s rolling window); `0` until bytes
  flow.
- **`gpstream_rate_limit_gbits`** — set by the limiter on every rate change
  via `SetRateLimit(to)` (F-14), so the AIMD movement is visible as a gauge
  movement. Diagnostic only until the pacer is wired in (see PERFORMANCE.md).
- **`gpstream_receive_write_latency_ms`** — the destination's receive queue
  as the controller observes it (F-14 "receive queue … COPY metrics"): the
  average time a source-side frame write takes to drain into the
  destination over the last second (the data path is an unbuffered pipe).
  **Not populated yet** (the `SetReceiveWriteMs` hook is never called in the
  shipped code), so it reads 0; it is the intended reduce signal with reason
  `dest-receive-queue` (above 50 ms) once the data-path wiring lands.
- **`gpstream_compression_level` / `gpstream_compression_mode`** — the
  `SetCompression` hook exists in `internal/monitor/metrics.go` but is
  **never called** in the shipped code, so the exposition is static:
  `gpstream_compression_level 0` and `gpstream_compression_mode{mode=""} 1`.
  Treat them as unpopulated until the compression-degradation work wires
  the hook in (F-12).
- **`gpstream_active_flows`** — number of units currently copying.
- **`gpstream_ack_lag_max_seconds`** — worst observed ACK lag over the
  running window (§4.6 blackhole detection).
- **`gpstream_pair_up`** — rendered as `gpstream_pair_up{pair="..."} 1`
  once the run labels the host pair (`SetPair`, a single call at run
  start). There is no disconnect path that clears or zeroes the label:
  the series is either absent (pair not set yet) or `1` — `0` is never
  rendered.

The limiter and the compression share the same collector (F-04/F-12/F-14).
**A rate-limit change is observable as a metric** (`gpstream_rate_limit_gbits`
is set by the limiter on every change via `SetRateLimit`), but a
compression-level change is **not** yet — the compression gauges are
unpopulated (above). A slowed data path is visible without re-running
anything; a compression change is not, yet.

## Scrape configuration (Prometheus)

Since the exposition is the standard textformat v0.0.4, a plain scrape
works. Example `scrape_configs` entry:

```yaml
- job_name: gpstream
  static_configs:
    - targets: ['127.0.0.1:9100']
      labels: { side: preprod }
  metrics_path: /metrics
```

Suggested alerts (using the real series names):

```yaml
- alert: GpstreamFrameErrors
  expr: rate(gpstream_frame_errors_total[5m]) > 0
- alert: GpstreamTransferErrors
  expr: rate(gpstream_errors_total[5m]) > 0
- alert: GpstreamChecksumMismatch
  expr: rate(gpstream_checksum_errors_total[5m]) > 0
```

## The on-disk operation logs

There is **no `logs` subcommand**. Every operation writes durable,
human-readable files under its state directory
(`<state-dir>/<operation-id>/`, default `/var/lib/gpstream/<operation-id>/`):

| File | Content |
|------|---------|
| `events.jsonl` | the diagnostic event stream (F-04/F-14): one JSON line per rate-limit shift, conflict, small-table auto-disable (`compression_off`), collation diff, unit failure, mesh route decision, and other notable transitions. Retransmits are **not** events — they are the `gpstream_frames_retransmitted_total` counter (and a `runstats.json` field). |
| `runstats.json` | the cumulative counters written at unit/run completion: rows / wire / logical / bytes, the frame-ack observability (`retransmitted`, `reconnects`) and, with `--sender-stats` on, the sender-side mirror (`sender_retransmitted`, `sender_reconnects`, plus the last unit's per-segment detail in `sender_seg_stats`). |
| `audit.log` | the F-13 audit log of **operator actions** on the operation (`run`, `resume`, `cleanup`) — mode `0600`, recorded before the state dir is removed. |
| `jobmeta.json` | the snapshot-epoch metadata only (`job_id`, `snap_epoch`, `snap_id`, `copy_mode`) — which generation of the source snapshot the job is on. |

`gpstream status --operation-id <id>` renders the journal (with the
snapshot-epoch line from `jobmeta.json`) as a compact per-unit state view;
`tail -f events.jsonl` is the human view of the event stream. The
**journal mirror** (`journal.jsonl`) — not `jobmeta.json` — is the source of
truth for resume and cleanup (FAILOVER.md).

## Related observability commands

| Command | Purpose |
|---------|---------|
| `run --metrics-addr <A>` | serves the live Prometheus endpoint during the copy |
| `gpstream status --operation-id <id>` | per-unit state from the durable manifest |
| `gpstream dry-run` | reachability + `PASS`/`FAIL` on both clusters |
| `tail -f <state-dir>/<operation-id>/events.jsonl` | per-transition event stream (with rate-shifts, conflicts, etc.) |

## Honest notes

- The `/metrics` endpoint is real and standard; a dedicated `/health`
  route is **not** wired into the metrics server — a request to
  `/health` returns the same Prometheus text, not a health probe.
- A metrics listen failure is **non-fatal**; the run continues.
- The endpoint is the live source for scraping; there is no separate
  `gpstream metrics` command to print a snapshot.
- The counters and the `events.jsonl` / `audit.log` output strings are the
  **real** ones as shipped.
