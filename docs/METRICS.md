# gpstream — Metrics & Observability

This document covers gpstream's observability: the Prometheus `/metrics`
endpoint, the exact counters it exposes, and the on-disk operation logs.
Everything below is grounded in the current implementation
(`internal/monitor/metrics.go`, `internal/monitor/events.go`,
`internal/manifest/job.go`, `cmd/gpstream/main.go`).

## The `/metrics` endpoint

Pass `--metrics-addr <addr>` to **`run`**. The run starts an HTTP server that
exposes the live metrics for the operation (F-04). On a successful listen it
prints:

```
metrics:     http://127.0.0.1:9100/metrics (F-04)
```

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
| Cluster readiness | `gpstream doctor` | reachability + `PASS`/`FAIL` |
| Live counters (Prometheus) | scrape `http://<addr>/metrics` (served by `run --metrics-addr`) | throughput / latency / error rate |
| Error health | `gpstream_errors_total`, `gpstream_frame_errors_total` (via `/metrics`) | rising errors = unhealthy |

If you need a probe, scrape `/metrics` and alert on
`gpstream_frame_errors_total` / `gpstream_errors_total`, or use
`gpstream doctor`. A dedicated `/health` route is **not** currently wired
into the metrics server.

## The counters (Prometheus exposition)

`m.Handler()` sets `Content-Type: text/plain; version=0.0.4` and renders the
following series (exact `# HELP` / `# TYPE` lines from
`internal/monitor/metrics.go`):

```
# HELP gpstream_bytes_total Bytes transferred (compressed on the wire).
# TYPE gpstream_bytes_total counter
# HELP gpstream_bytes_decoded_total Logical bytes after decompression.
# TYPE gpstream_bytes_decoded_total counter
# HELP gpstream_rows_total Rows copied (F-04).
# TYPE gpstream_rows_total counter
# HELP gpstream_errors_total Transfer errors.
# TYPE gpstream_errors_total counter
# HELP gpstream_frame_errors_total CRC/framing errors.
# TYPE gpstream_frame_errors_total counter
# HELP gpstream_checksum_errors_total Content-checksum mismatches in the verify phase (F-04).
# TYPE gpstream_checksum_errors_total counter
# HELP gpstream_auto_recovery_total Auto-recovery actions (resume re-reads, retries) (F-04).
# TYPE gpstream_auto_recovery_total counter
# HELP gpstream_rate_limit_gbits Current rate limit in Gbps (F-14).
# TYPE gpstream_rate_limit_gbits gauge
# HELP gpstream_receive_write_latency_ms Average data-path write latency (ms) — destination receive-queue depth as the controller observes it (F-14).
# TYPE gpstream_receive_write_latency_ms gauge
# HELP gpstream_throughput_bytes Current throughput in bytes/sec.
# TYPE gpstream_throughput_bytes gauge
# HELP gpstream_eta_seconds Estimated remaining seconds at the recent throughput (F-04).
# TYPE gpstream_eta_seconds gauge
# HELP gpstream_compression_level Current compression level (F-12).
# TYPE gpstream_compression_level gauge
# HELP gpstream_compression_mode Current compression mode (F-12).
# TYPE gpstream_compression_mode gauge
# HELP gpstream_index_queue Background index builds pending (F-17).
# TYPE gpstream_index_queue gauge
# HELP gpstream_index_done_total Background index builds finished (F-17).
# TYPE gpstream_index_done_total counter
# HELP gpstream_active_flows Units currently copying (F-04).
# TYPE gpstream_active_flows gauge
# HELP gpstream_frames_retransmitted_total Frames retransmitted after loss/corruption (§4.6).
# TYPE gpstream_frames_retransmitted_total counter
# HELP gpstream_reconnects_total Data-path transport reconnects (§4.6).
# TYPE gpstream_reconnects_total counter
# HELP gpstream_ack_lag_max_seconds Worst observed ACK lag (§4.6).
# TYPE gpstream_ack_lag_max_seconds gauge
# HELP gpstream_tables_by_status Units by state (F-04).
# TYPE gpstream_tables_by_status gauge
# HELP gpstream_table_rows Rows copied per table (F-04).
# TYPE gpstream_table_rows counter
# HELP gpstream_table_bytes_wire Wire bytes per table (F-04).
# TYPE gpstream_table_bytes_wire counter
# HELP gpstream_table_bytes_logical Logical bytes per table (F-04).
# TYPE gpstream_table_bytes_logical counter
# HELP gpstream_table_state Current unit state per table (1 = in this state) (F-04).
# TYPE gpstream_table_state gauge
# HELP gpstream_pair_up Host pair of the running copy (1 = up) (F-04).
# TYPE gpstream_pair_up gauge
```

Summary of the series:

| Series | Kind | Labels | Meaning |
|--------|------|--------|---------|
| `gpstream_bytes_total` | counter | — | bytes pushed through the framed transport (compressed in-memory; see ADR-002 — the network legs are uncompressed in the single-controller build) |
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
| `gpstream_table_state` | gauge | `{table}`, `{state}` | current unit state per table (1 = in this state) |
| `gpstream_pair_up` | gauge | `{pair}` | host pair of the running copy (1 = up) |

### How the gauges are computed

- **`gpstream_throughput_bytes`** — over a rolling **~60 s window**: the
  collector keeps cumulative byte samples and returns
  `(last.bytes − first.bytes) / (last.t − first.t)`; with fewer than two
  samples it returns `0`.
- **`gpstream_eta_seconds`** — remaining volume divided by the recent
  throughput; `0` until enough samples exist to be meaningful.
- **`gpstream_rate_limit_gbits`** — set by the limiter on every rate change
  via `SetRateLimit(to)` (F-14), so an adaptive degradation is visible as
  a gauge movement.
- **`gpstream_receive_write_latency_ms`** — the destination's receive queue
  as the controller observes it (F-14 "receive queue … COPY metrics"): the
  average time a source-side frame write takes to drain into the
  destination over the last second (the data path is an unbuffered pipe).
  Refreshed by the limiter sampler each tick; a rising value means the
  receiver is falling behind and (above 50 ms) will trigger an adaptive
  reduction with reason `dest-receive-queue`.
- **`gpstream_compression_level` / `gpstream_compression_mode`** — set by
  `SetCompression` on each compression-mode / level change (F-12/F-14
  auto-degradation); the change is **counted and observable** (F-04).
- **`gpstream_active_flows`** — number of units currently copying.
- **`gpstream_ack_lag_max_seconds`** — worst observed ACK lag over the
  running window (§4.6 blackhole detection).
- **`gpstream_pair_up`** — `1` while the host pair is up, `0` after a
  disconnect (labelled by pair).

Because the limiter and the compression degrade against the same collector
(F-04/F-12/F-14), **a rate-limit change and a compression-level change
are both observable as metrics** — a slowed or compressed data path is
visible without re-running anything.

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
| `events.jsonl` | the diagnostic event stream (F-04/F-14): one JSON line per rate-limit shift, conflict, small-table auto-disable, collation diff, retransmit, unit failure, and other notable transitions. |
| `audit.log` | the F-13 audit log of **operator actions** on the operation (`run`, `resume`, `cleanup`) — mode `0600`, recorded before the state dir is removed. |
| `jobmeta.json` | the durable job metadata (snap-epoch, per-unit id/state/kind/`db.schema.table`/bytes/epoch) that `status` and `resume` read. |

`gpstream status --operation-id <id>` renders the `jobmeta.json` as a
compact per-unit state view; `tail -f events.jsonl` is the human view of the
event stream. The `jobmeta.json` is the source of truth for resume/cleanup
(FAILOVER.md).

## Related observability commands

| Command | Purpose |
|---------|---------|
| `run --metrics-addr <A>` | serves the live Prometheus endpoint during the copy |
| `gpstream status --operation-id <id>` | per-unit state from the durable manifest |
| `gpstream doctor` | reachability + `PASS`/`FAIL` on both clusters |
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
