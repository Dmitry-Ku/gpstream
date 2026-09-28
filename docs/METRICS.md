# gpstream — Metrics, Health & Logs (v2)

This document covers gpstream's observability: the Prometheus `/metrics`
endpoint, how to check health, the exact counters exposed, and the `logs`
command. Everything below is grounded in the v2 implementation
(`internal/monitor/metrics.go`, `internal/limiter`, `cmd/gpstream/main.go`).

## The `/metrics` endpoint

Pass `--metrics-addr <addr>` (or `GPSTREAM_METRICS_ADDR`) to `copy`. The
copy starts an HTTP server that exposes the live metrics for the job
(FR-04). On a successful listen it prints:

```
metrics:     http://127.0.0.1:9100/metrics (FR-04)
```

Behaviour of `startMetricsServer` (`cmd/gpstream/main.go`):

- The server is `http.Server{Addr: addr, Handler: m.Handler()}` and is
  started in a goroutine; it returns a stop function that
  `srv.Shutdown(ctx)`s the server when the copy finishes.
- **A listen failure is non-fatal (FR-04 is observability, not core):** the
  copy proceeds and the error is logged to stderr
  (`gpstream: metrics: <err>`).
- The exposition format is the **Prometheus textformat v0.0.4**, written
  **in-tree with no third-party metrics dependency** (FR-08).

## About `/health` — an honest note

gpstream's metrics server has a **single root handler** that returns the
Prometheus exposition for **any** path. That means:

- `GET http://<addr>/metrics` returns the Prometheus text (the documented,
  intended path).
- There is **no distinct `/health` route with health-specific semantics**.
  A request to `http://<addr>/health` against the same server returns the
  **same Prometheus text** — it is **not** a readiness/liveness probe with
  `200`/`5xx` health semantics.

Do **not** treat `http://<addr>/health` as a real health endpoint. For
health / readiness use the commands below, which carry real semantics:

| Check | Command | What it tells you |
|-------|---------|-------------------|
| Liveness / job state | `gpstream status --mirror <path>` | per-unit state |
| Cluster readiness | `gpstream doctor` | reachability + `PASS`/`FAIL` |
| Live counters (text) | `gpstream metrics [--metrics-addr <addr>]` | throughput / latency / error rate |
| Error health | `gpstream_errors_total`, `gpstream_frame_errors_total` (via `/metrics`) | rising errors = unhealthy |

If you need a probe, scrape `/metrics` and alert on
`gpstream_frame_errors_total` / `gpstream_errors_total`, or use
`gpstream doctor`. A dedicated `/health` route is **not** currently wired
into the metrics server.

## The counters (Prometheus exposition)

`m.Handler()` sets `Content-Type: text/plain; version=0.0.4` and renders
the following series (exact `# HELP` / `# TYPE` lines from
`internal/monitor/metrics.go`):

```
# HELP gpstream_bytes_total Bytes transferred (compressed on the wire).
# TYPE gpstream_bytes_total counter
gpstream_bytes_total <n>

# HELP gpstream_bytes_decoded_total Logical bytes after decompression.
# TYPE gpstream_bytes_decoded_total counter
gpstream_bytes_decoded_total <n>

# HELP gpstream_errors_total Transfer errors.
# TYPE gpstream_errors_total counter
gpstream_errors_total <n>

# HELP gpstream_frame_errors_total CRC/framing errors.
# TYPE gpstream_frame_errors_total counter
gpstream_frame_errors_total <n>

# HELP gpstream_rate_limit_gbits Current rate limit in Gbps (FR-14).
# TYPE gpstream_rate_limit_gbits gauge
gpstream_rate_limit_gbits <g>

# HELP gpstream_throughput_bytes Current throughput in bytes/sec.
# TYPE gpstream_throughput_bytes gauge
gpstream_throughput_bytes <b>

# HELP gpstream_compression_level Current compression level (FR-12).
# TYPE gpstream_compression_level gauge
gpstream_compression_level <lvl>

# HELP gpstream_compression_mode Current compression mode (FR-12).
# TYPE gpstream_compression_mode gauge
gpstream_compression_mode{mode="zstd"} 1

# HELP gpstream_index_queue Background index builds pending (FR-17).
# TYPE gpstream_index_queue gauge
gpstream_index_queue <n>

# HELP gpstream_index_done_total Background index builds finished (FR-17).
# TYPE gpstream_index_done_total counter
gpstream_index_done_total <n>
```

Summary of the series:

| Series | Kind | Meaning |
|--------|------|---------|
| `gpstream_bytes_total` | counter | bytes transferred (compressed on the wire) |
| `gpstream_bytes_decoded_total` | counter | logical bytes after decompression |
| `gpstream_errors_total` | counter | transfer errors |
| `gpstream_frame_errors_total` | counter | CRC / framing errors (FR-01 / §4.2) |
| `gpstream_rate_limit_gbits` | gauge | current rate limit in Gbps (FR-14) |
| `gpstream_throughput_bytes` | gauge | current throughput in bytes/sec |
| `gpstream_compression_level` | gauge | current zstd level (FR-12) |
| `gpstream_compression_mode` | gauge | current mode, labelled `{mode="..."}` |
| `gpstream_index_queue` | gauge | background index builds pending (FR-17) |
| `gpstream_index_done_total` | counter | background index builds finished (FR-17) |

### How the gauges are computed

- **`gpstream_throughput_bytes`** — over a rolling **~60 s window**: the
  collector keeps cumulative byte samples and returns
  `(last.bytes − first.bytes) / (last.t − first.t)`; with fewer than two
  samples it returns `0`.
- **`gpstream_rate_limit_gbits`** — set by the limiter on every rate change
  via `SetRateLimit(to)` (FR-14), so an adaptive degradation is visible as
  a gauge movement.
- **`gpstream_compression_level` / `gpstream_compression_mode`** — set by
  `SetCompression` on each compression-mode / level change (FR-12/FR-14
  auto-degradation); the change is **counted and observable** (FR-04).
- **Error rate** (via `gpstream metrics`) — `errors / bytes` over the job
  (`ErrorRate()`), `0` when no bytes have been transferred.
- **Latency** (via `gpstream metrics`) — the **job duration**
  (`time.Since(start)`); a job-level latency, not per-request.

Because the limiter and the compression degrade against the same collector
(FR-04/FR-12/FR-14), **a rate-limit change and a compression-level change
are both observable as metrics** — a slowed or compressed data path is
visible without re-running anything.

## The `metrics` command

`gpstream metrics [--metrics-addr <addr>]` prints a live snapshot
(`cmd/gpstream/main.go`):

```
gpstream metrics
================
throughput:   41.2 Gbps
latency:      5m12s
error rate:   0.0001
endpoint:     http://127.0.0.1:9100/metrics (prometheus)
```

- **With `--metrics-addr`**, it prints the endpoint line
  (`endpoint: http://<addr>/metrics (prometheus)`).
- **Without `--metrics-addr`**, it prints
  `endpoint: set --metrics-addr to serve the Prometheus endpoint`.
- Note: the `metrics` command prints counters from a **fresh** collector
  snapshot; the **live** per-job numbers come from scraping `/metrics`.

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
```

## The `logs` command

`gpstream logs --mirror <path> [--job <id>]` prints the **operation
journal** of a job as a human-readable log — one line per unit transition
(v2 §4.5 / FR-07). Each line is:

```
2026-09-04T10:15:00Z  unit    4  copied     table  db.public.t4  attempts=1  <checksum>
```

Field layout: `timestamp  unit <id>  <state>  <kind>  db.schema.table
attempts=<n> <checksum>`. The `checksum` is the per-unit content checksum
recorded when the unit was verified (FR-02).

- **`--mirror` is required**; without a populated journal it prints
  `logs: no units recorded (journal empty or --mirror not populated)`.
- **`--job <id>`** filters to one job.
- The journal is the source of truth for resume / rollback / cancel
  (FAILOVER.md); `logs` is the human view of it, `status` the compact
  per-unit state view.

## Related observability commands

| Command | Purpose |
|---------|---------|
| `gpstream metrics` | live throughput / latency / error-rate snapshot |
| `gpstream status` | per-unit state from the journal |
| `gpstream doctor` | reachability + `PASS`/`FAIL` on both clusters |
| `gpstream logs`   | per-transition operation journal (with checksums) |

## Honest notes

- The `/metrics` endpoint is real and standard; a dedicated `/health`
  route is **not** wired into the metrics server — a request to
  `/health` returns the same Prometheus text, not a health probe.
- A metrics listen failure is **non-fatal**; the copy continues.
- The `metrics` command and the `/metrics` endpoint read the same
  collector, but the command's printed snapshot is a fresh render — the
  endpoint is the live source for scraping.
- The counters and the `logs` output strings are the **real** ones as
  shipped in v2.
