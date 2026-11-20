# gpstream — Configuration Reference

This document is the configuration reference for **gpstream**, the open-source
Go tool that performs high-throughput **LOGICAL** data copy between two
Greenplum 6 clusters over a direct, high-speed network via `COPY (binary)`
with **no intermediate storage** (it is *not* `gpbackup`/`gprestore`/`gpcopy`).

It describes the `Config` struct and its defaults, the three sources that fold
into it (TOML/YAML file, environment, CLI), the precedence that resolves them,
the validation rules, and the secret-handling rule (F-08). See
[CLI_REFERENCE.md](CLI_REFERENCE.md) for the command and flag reference.

---

## 1. The Config struct

`config.Config` (defined at `internal/config/config.go`) is the fully resolved
runtime configuration and the single source of truth consumed by every
component. Every CLI flag, environment variable, and file key folds into it.

### 1.1 Connection
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `Source` | string | `""` | CLI `--source` / file `source` | Source connstring. May hold secrets but **never** via argv in production (F-08). |
| `Dest` | string | `""` | CLI `--dest` / file `dest` | Destination connstring. |

There is **no** `GPSTREAM_SOURCE`/`GPSTREAM_DEST` env var; the endpoints come
from the flags or the file.

### 1.2 Selection (F-11)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `Full` | bool | `false` | (internal) |
| `DBs` | []string | `[]` | `--dbs` (comma-separated) |
| `Schemas` | []string | `[]` | `--schemas` (comma-separated, `db.schema`) |
| `Tables` | []string | `[]` | `--tables` (comma-separated, `db.schema.table`) |
| `Exclude` | []string | `[]` | `--exclude` (comma-separated) |

### 1.3 Metadata / data (F-11)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `MetadataOnly` | bool | `false` | `--metadata-only` (mutually exclusive with `DataOnly`) |
| `DataOnly` | bool | `false` | `--data-only` (mutually exclusive with `MetadataOnly`) |

### 1.4 Conflict / safety (F-10)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `ConflictAction` | `ConflictAction` | **`stop`** | `--resolve-conflicts` / `--overwrite` / `--truncate` | One of `stop`/`skip`/`overwrite`/`truncate`. `stop` (default) halts the run on the first collision; `skip` skips conflicting units; `overwrite` = staging + swap; `truncate` = in-place after TRUNCATE. |

### 1.5 Rename (F-11)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `RenameDB` | map[string]string | `{}` | `--map-db src=dst` |
| `RenameSchema` | map[string]string | `{}` | `--map-schema src=dst` |
| `RenameTable` | map[string]string | `{}` | `--map-table src=dst` |

### 1.6 Performance (F-01/F-12/F-14)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `Compression` | `Compression` | **`zstd`** | `--compress` / env / file | `none`/`zlib`/`zstd`. |
| `ZstdLevel` | int | **`3`** | `--zstd-level` / `--compress zstd:N` / file | Must be **1–19** (validate). |
| `SmallTableThreshold` | int64 | **`1MiB`** | `--small-table-threshold` / env / file | F-12: a table whose estimated size is below this copies with compression **off**. `0` disables the auto-disable. |
| `RateLimit` | float64 | `0` (none) | `--rate-limit` / env / file | Gbps baseline for the ETA + adaptive load diagnostics (AIMD, F-14). Does not cap the flow — backpressure paces it; the enforced ceiling (pacer) is backlog. |
| `WorkersPerPair` | int | **`1`** | `--workers` / env / file | Must be **≥ 1** (validate). 1 = strict (one snapshot point); N>1 = snapshot-epoch. |
| `MaxSnapshotDelta` | time.Duration | **`1s`** | `--max-snapshot-delta` / env / file | P1d: for `--workers N > 1`, the N REPEATABLE READ pins must be taken within this spread (seconds) or the run refuses to start. `0` disables the check; no effect in strict mode (`--workers 1`). |
| `DataIdleTimeout` | time.Duration | **`5m`** | `--data-idle-timeout` / env / file | L2 data-plane watchdog: no byte on a unit's data stream for this long → the destination declares the stream dead and fails the unit (idle-based, size- and link-independent). `0` disables. |

### 1.6a Transport (two-agent, ADR-002)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `SourceAgentSSH` | string | `""` | `--source-agent-ssh` / file `source_agent_ssh` | P1e: `user@host[:port]` of the source cluster's master. **May be omitted** (or name a local host) → the source agent runs as a local subprocess on the gpstream host. Remote host → launched over SSH. No env source (the Inc2c env vars were removed, P1e). |
| `DestAgentSSH` | string | `""` | `--dest-agent-ssh` / file `dest_agent_ssh` | P1e: `user@host[:port]` of the destination cluster's coordinator. Same semantics as `SourceAgentSSH` (file key `dest_agent_ssh`, no env source). |
| `SourceAgentBinary` | string | `""` → resolved | `source_agent_binary` / `GPSTREAM_SOURCE_AGENT_BINARY` | gpstream-source-agent path. Not a CLI flag: resolved by convention (sibling of the `gpstream` binary, then `PATH`) before `run` / `bench`. |
| `DestAgentBinary` | string | `""` → resolved | `dest_agent_binary` / `GPSTREAM_DEST_AGENT_BINARY` | gpstream-dest-agent path. Resolved by convention as above. |

The data path is always the two-agent transport: the data plane lives in the
agents, the controller holds no data connections. The agent binaries are
invoked by the **resolved path** (sibling of the `gpstream` binary first, then `PATH` — the packaged install). Each agent verifies
before serving that it runs on the coordinator of exactly the cluster its
DSN points to (P1e coordinator guard; a violation aborts the agent and the
`run`/`bench` fails).

### 1.7 Resilience (F-06/F-07)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `AutoResume` | int | **`0`** | `--auto-resume` / env / file | `N ≥ 0`; `0` = manual only. |
| `OnUnitFailure` | string | `""` → `continue` | `--on-unit-failure` / env / file | `continue` (default) \| `abort` (F-06). |
| `Verify` | `VerifyMode` | **`on`** | `--verify` / file | `on`/`off` (post-copy content verification). On by default — digests are always computed; `off` only disables the swap gate. |

### 1.8 Indexes (F-17)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `IndexWorkers` | int | `0` | `--index-workers` | Background index-build parallelism. |
| `NoIndexes` | bool | `false` | `--no-indexes` | Set to `true` to skip index creation entirely. |
| `IndexPriority` | string | **`size`** | `--index-priority` / file | `size` (default) \| `fifo`. |

### 1.9 Constraints (F-11)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `NoConstraints` | bool | `false` | `--no-constraints` | Disable the constraint-restore pass (FK/PK/unique re-applied post-load by default). |

### 1.10 Redistribution (F-15)

The FR-15 redistribution accounting has no configuration knob: the measured
segment skew (fraction of the copied data that moves to a different segment
under the destination layout) and the layout verdict are always computed in
the `dry-run` and reported as-is — the operator reads the number and decides.
The former `--no-redistribute` / `--skew-threshold` flags (and the
`NoRedistribute` / `SkewThreshold` fields) were removed: the raw metric stays,
the gate did not earn its place (it only drove a dry-run warning and a plan-
hash input, and `--skew-threshold -1` was a bug, not an off-switch).

The F-15 live segment-fill warning gate (max/avg over the destination
segments, measured after each swap) is **fixed at 1.5**
(`dest.DefaultBalanceThreshold`): the metric is always recorded in the final
report and events.jsonl, and a `WARNING` line is emitted when a table's
max/avg exceeds 1.5. The `--segment-balance-threshold` flag (and its file
key / env var) was removed — the metric stays, the knob did not earn its
place.

### 1.11 TLS (F-13)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `SSLCert` | string | `""` | `--ssl-cert` / env / file | **Secret — env/file, never argv (F-08).** |
| `SSLKey` | string | `""` | `--ssl-key` / env / file | **Secret — env/file, never argv (F-08).** |
| `SSLCA` | string | `""` | `--ssl-ca` / env / file | **Secret — env/file, never argv (F-08).** |
| `SSLMinTLS` | string | **`1.2`** | `--ssl-min-tls` / file | Minimum TLS version. The pg plane uses `sslmode=verify-full` by default. |
| `ControlTLSCert` | string | `""` | `--control-tls-cert` / env / file | Control-channel TLS: the client cert the controller presents (mTLS, when an agent requires a client certificate). |
| `ControlTLSKey` | string | `""` | `--control-tls-key` / env / file | **Secret — env/file, never argv (F-08).** |
| `ControlTLSCA` | string | `""` | `--control-tls-ca` / env / file | **Secret — env/file, never argv (F-08).** Trusted CA bundle the controller verifies the agents' control-socket certificates against. |

### 1.12 Transport (T-B, §1.3)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `CopyMode` | `CopyMode` | `""` → `coordinator` | `--copy-mode` | `coordinator` \| `mesh`. |
| `SegListen` | string | `""` → auto | `--seg-listen` / `seg_listen` / `GPSTREAM_SEG_LISTEN` | Base listen address for `on_segment`: the dry-run probes that the base port is free on the source segment hosts; in mesh the per-segment mini-agents bind their own data listeners at launch (auto, or `--port-range`) and the segreceivers dial the per-segment address book. When empty, auto-resolved to the controller's primary non-loopback IP + a free port (must be routable from the source segment hosts). |
| `SegBinary` | string | `""` → resolved | `seg_binary` / `GPSTREAM_SEG_BINARY` | gpstream-segreceiver path on the SOURCE segment hosts. Not a CLI flag: resolved by convention (sibling of the `gpstream` binary, then `PATH`) before `on_segment` runs. |
| `SegTLSCert` | string | `""` | `--seg-tls-cert` / env / file | Controller serving cert (segment data plane). |
| `SegTLSKey` | string | `""` | `--seg-tls-key` / env / file | Controller serving key. |
| `SegTLSCA` | string | `""` | `--seg-tls-ca` / env / file | CA for the segment data plane. |
| `PortRange` | string | `""` → auto | `--port-range` / env / file | Restricts the auto data/control port allocation to a `LO-HI` range (or a single `LO` base) for strict-firewall environments. Empty = fully automatic (every long-lived agent binds `:0` and reports its address in the handshake). The range size is checked against the mode's requirement at run/dry-run time (coordinator: **5** ports — source control + dest control + dest data + dest persist + dest peer; mesh: **5 + D**). |
| `NoRoutedWrite` | bool | `false` | `--no-routed-write` (CLI only) | P9: disables the routed segment-to-segment data path for S≠D topologies — on_segment then always merges through the destination coordinator (SliceMux) instead of routing in the segreceivers. |

### 1.13 Frame-ack / reliability (§4.6)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `FrameAck` | string | `on` | `--frame-ack` / env | `on` (default) \| `off` (plain pipe). |
| `AckTimeout` | string | `30s` | `--ack-timeout` / env | `15s` \| `30s` \| `adaptive` (3x median RTT). |
| `MaxReconnects` | int | `20` | `--max-reconnects` / env | Reconnect budget; exhausted = unit failed. |
| `RetransmitBufferMax` | int64 | `64MiB` | `--retransmit-buffer-max` / env | In-flight window per stream. |

### 1.14 Dry-run / planning (F-10)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `DryRun` | bool | `false` | `--dry-run` | Plan only, do not execute. |
| `Timeout` | time.Duration | **`120s`** | `--timeout` / file | `run`/`dry-run`: bounds plan formation (F-10 §6 default). `bench`: the benchmark wall-time limit (unlimited when not passed). |
| `OperationID` | string | `""` | `--operation-id` | Operation identity. `run`: a name, or omitted = a generated timestamp id (`YYYYMMDDTHHMMSSZ` UTC, collision-suffixed) so the state dir is a chronological history. `status`/`resume`/`cleanup`: a name or `latest` (newest operation). `run` persists the plan JSON to `<state-dir>/<operation-id>/plan.json`; `dry-run` prints the plan without persisting it. |

### 1.15 Observability (F-04)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `MetricsAddr` | string | `""` | `--metrics-addr` | Serve the Prometheus endpoint during `run`. |

### 1.16 Consistency (F-02)

At `--workers 1` (strict) the source agent reads under one run-wide
REPEATABLE READ snapshot per database; every table's data COPY and checksum
read share that single point in time. At `--workers N>1` the N REPEATABLE READ
pins are taken in an ε-window bounded by `--max-snapshot-delta` (§1.6) — no
other scope is configurable.

### 1.17 Operational / state (v4 §8)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `ConfigFile` | string | `""` | `--config` |
| `OperationID` | string | `""` | `--operation-id` |
| `StateDir` | string | **`/var/lib/gpstream`** | `--state-dir` |

---

## 2. Defaults (the `Default()` function)

`config.Default()` seeds the following fields; all other fields default to
their zero values (`""`, `false`, `0`, `nil`, `[]`).

| Field | Default |
|-------|---------|
| `ConflictAction` | `stop` |
| `Compression` | `zstd` |
| `SmallTableThreshold` | `1 MiB` |
| `ZstdLevel` | `3` |
| `AutoResume` | `0` |
| `Verify` | `on` |
| `Timeout` | `120s` |
| `IndexPriority` | `size` |
| `SSLMinTLS` | `1.2` |
| `WorkersPerPair` | `1` |
| `MaxSnapshotDelta` | `1s` |
| `DataIdleTimeout` | `5m` |
| `StateDir` | `/var/lib/gpstream` |

`Validate()` additionally normalises the frame-ack defaults in place:
`AckTimeout` → `30s`, `MaxReconnects` → `20`, `RetransmitBufferMax` → `64 MiB`
when unset, so the data path always sees a usable config.

---

## 3. The three sources and precedence

The precedence is **CLI > env > file > defaults**. `parseConfig`
(`cmd/gpstream/main.go`) assembles the `Config` in this order:

1. **defaults** — `cfg := config.Default()`.
2. **file** — `cfg.LoadFile(configFile)` overlays the `--config` file onto the
   defaults (the file is the base layer).
3. **env** — `cfg.ApplyEnv()` overlays `GPSTREAM_*` variables. Values already
   set by higher-precedence sources are **not** clobbered.
4. **CLI** — a second pass over `os.Args` applies CLI flags last, so **CLI
   flags win**.

Because the file is loaded before env, **env beats file**; because CLI is
applied last, **CLI beats env**. Unknown file keys are ignored.

### 3.1 Config file (`--config file.toml` or `file.yaml`)

`LoadFile` reads the file (TOML by `.toml` extension, otherwise YAML) and
overlays known scalar leaves via `assignConfigString`. Only **scalar leaves**
are honoured (non-scalar nodes are skipped). The accepted keys (all lowercase)
are:

**Naming rule:** for every key that has a CLI flag, the file key is the flag
name with dashes turned into underscores — `--index-priority` ↔
`index_priority`, `--workers` ↔ `workers`. The 31 keys below are exactly the
set `assignConfigString` accepts: 28 of them mirror a CLI flag, and the three
binary-path keys (`source_agent_binary`, `dest_agent_binary`, `seg_binary`)
have no CLI flag (the binaries resolve by convention). Several CLI flags
(`--frame-ack`, `--ack-timeout`, `--max-reconnects`,
`--retransmit-buffer-max`, `--dry-run`, `--operation-id`, …) have no file
key. Env vars are the same names uppercased under the `GPSTREAM_` prefix
where they exist (env covers a different subset — see §3.2).

| File key | Field | Notes |
|----------|-------|-------|
| `source` | `Source` | |
| `dest` | `Dest` | |
| `rate_limit` | `RateLimit` | parsed as float |
| `workers` | `WorkersPerPair` | parsed as int |
| `max_snapshot_delta` | `MaxSnapshotDelta` | int seconds |
| `data_idle_timeout` | `DataIdleTimeout` | parsed as duration |
| `compress` | `Compression` | `none`/`zlib`/`zstd` |
| `zstd_level` | `ZstdLevel` | parsed as int (1–19) |
| `small_table_threshold` | `SmallTableThreshold` | int64 bytes, or a size with a `K`/`M`/`G`/`T` suffix (same notation as the CLI) |
| `auto_resume` | `AutoResume` | parsed as int |
| `on_unit_failure` | `OnUnitFailure` | `continue`/`abort` |
| `verify` | `Verify` | `on`/`off` |
| `ssl_cert` | `SSLCert` | **secret** |
| `ssl_key` | `SSLKey` | **secret** |
| `ssl_ca` | `SSLCA` | **secret** |
| `ssl_min_tls` | `SSLMinTLS` | e.g. `1.2` |
| `seg_tls_cert` | `SegTLSCert` | **secret** |
| `seg_tls_key` | `SegTLSKey` | **secret** |
| `seg_tls_ca` | `SegTLSCA` | **secret** |
| `control_tls_cert` | `ControlTLSCert` | **secret** |
| `control_tls_key` | `ControlTLSKey` | **secret** |
| `control_tls_ca` | `ControlTLSCA` | **secret** |
| `source_agent_ssh` | `SourceAgentSSH` | `user@host[:port]`; no env source (P1e) |
| `dest_agent_ssh` | `DestAgentSSH` | `user@host[:port]`; no env source (P1e) |
| `source_agent_binary` | `SourceAgentBinary` | binary path; no CLI flag |
| `dest_agent_binary` | `DestAgentBinary` | binary path; no CLI flag |
| `seg_binary` | `SegBinary` | binary path; no CLI flag |
| `seg_listen` | `SegListen` | `host:port` |
| `port_range` | `PortRange` | `LO-HI` or a single `LO` base |
| `index_priority` | `IndexPriority` | `size`/`fifo` |
| `timeout` | `Timeout` | parsed as duration |

A file example:

```toml
source = "host=src port=5432 dbname=app"
dest   = "host=dst port=5432 dbname=app"
compress = "zstd"
zstd_level = 3
rate_limit = 10
workers = 2
verify = "on"
ssl_ca   = "/secrets/ca.pem"
```

### 3.2 Environment layer (`GPSTREAM_*`)

`ApplyEnv` reads `GPSTREAM_*` variables. Recognised variables:

| Env var | Field | Parsing |
|---------|-------|---------|
| `GPSTREAM_RATE_LIMIT` | `RateLimit` | float |
| `GPSTREAM_WORKERS` | `WorkersPerPair` | int |
| `GPSTREAM_COMPRESS` | `Compression` | string |
| `GPSTREAM_SMALL_TABLE_THRESHOLD` | `SmallTableThreshold` | int64 bytes |
| `GPSTREAM_AUTO_RESUME` | `AutoResume` | int |
| `GPSTREAM_ON_UNIT_FAILURE` | `OnUnitFailure` | string |
| `GPSTREAM_SSL_CERT` | `SSLCert` | string (**secret**) |
| `GPSTREAM_SSL_KEY` | `SSLKey` | string (**secret**) |
| `GPSTREAM_SSL_CA` | `SSLCA` | string (**secret**) |
| `GPSTREAM_SEG_TLS_CERT` | `SegTLSCert` | string (**secret**) |
| `GPSTREAM_SEG_TLS_KEY` | `SegTLSKey` | string (**secret**) |
| `GPSTREAM_SEG_TLS_CA` | `SegTLSCA` | string (**secret**) |
| `GPSTREAM_FRAME_ACK` | `FrameAck` | `on`/`off` |
| `GPSTREAM_ACK_TIMEOUT` | `AckTimeout` | string |
| `GPSTREAM_MAX_RECONNECTS` | `MaxReconnects` | int |
| `GPSTREAM_RETRANSMIT_BUFFER_MAX` | `RetransmitBufferMax` | size (e.g. `64M`) |
| `GPSTREAM_MAX_SNAPSHOT_DELTA` | `MaxSnapshotDelta` | int seconds |
| `GPSTREAM_DATA_IDLE_TIMEOUT` | `DataIdleTimeout` | duration |
| `GPSTREAM_SOURCE_AGENT_BINARY` | `SourceAgentBinary` | string (path) |
| `GPSTREAM_DEST_AGENT_BINARY` | `DestAgentBinary` | string (path) |
| `GPSTREAM_SEG_BINARY` | `SegBinary` | string (path) |
| `GPSTREAM_SEG_LISTEN` | `SegListen` | `host:port` |
| `GPSTREAM_PORT_RANGE` | `PortRange` | `LO-HI` or a single `LO` base |
| `GPSTREAM_CONTROL_TLS_CERT` | `ControlTLSCert` | string (**secret**) |
| `GPSTREAM_CONTROL_TLS_KEY` | `ControlTLSKey` | string (**secret**) |
| `GPSTREAM_CONTROL_TLS_CA` | `ControlTLSCA` | string (**secret**) |

ApplyEnv does not clobber values already set by higher-precedence sources, so
CLI beats env and env beats file.

> **Endpoints are not env-driven:** there is no `GPSTREAM_SOURCE`/
> `GPSTREAM_DEST`. The source/destination connstrings come from the
> `--source`/`--dest` flags or the file.

> **`GPSTREAM_DATA_LOG` is not a `Config` key:** it is read directly by the
> data path (`internal/transfer/datalog.go`, Inc3.4) — an opt-in data-path
> debug log (frame-ack transport events: reconnect attempts, ring replay
> size, resume routing, duplicate frames) written to stderr. Off by default;
> enable with `GPSTREAM_DATA_LOG=1` (any non-empty value other than `0`/
> `off`). The agent's stderr is captured in the controller's run log, the
> segreceiver's stderr lands in the GPDB segment log. See `gpstream(8)`.

---

## 4. Validation rules

`Config.Validate()` reports configuration errors independent of the live
clusters. It is called by `parseConfig` after assembling the `Config`. A
failed validation aborts the command.

| Rule | Condition | Error |
|------|-----------|-------|
| zstd level range | `ZstdLevel` must be **1–19** | `zstd level must be 1-19, got N` |
| small-table threshold | `SmallTableThreshold` must be **≥ 0** | `small-table-threshold must be >= 0 …` |
| workers | `WorkersPerPair` must be **≥ 1** | `workers must be >= 1, got N` |
| auto-resume | `AutoResume` must be **≥ 0** | `auto-resume must be >= 0` |
| on-unit-failure | must be `continue`/`abort` (or empty) | `unknown on-unit-failure "…" (want continue\|abort)` |
| metadata vs data | `--metadata-only` and `--data-only` are **mutually exclusive** | `--metadata-only and --data-only are mutually exclusive` |
| conflict action | must be `stop`/`skip`/`overwrite`/`truncate` | `unknown --resolve-conflicts value "…" (want stop\|skip\|overwrite\|truncate)` |
| verify mode | must be `on`/`off` | `unknown verify mode "…"` |
| copy-mode mesh | `--seg-listen` must be a valid `host:port` when given (auto-resolved when empty); the segreceiver binary must resolve | `--seg-listen must be host:port` / `on_segment: gpstream-segreceiver not found` |
| frame-ack | must be `on`/`off` (or empty) | `unknown --frame-ack "…" (want on\|off)` |
| ack-timeout | must be `15s`/`30s`/`adaptive` or a valid duration | `invalid --ack-timeout "…" (want 15s\|30s\|adaptive)` |
| max-snapshot-delta | must be **≥ 0 seconds** | `max-snapshot-delta must be >= 0 seconds…` |
| data-idle-timeout | must be **≥ 0** | `data-idle-timeout must be >= 0…` |
| index-priority | must be `size`/`fifo` | `unknown index-priority "…" (want size\|fifo)` |
| copy mode | must be `coordinator`/`mesh` | `unknown copy mode "…" (want coordinator\|mesh)` |
| port-range | must be a valid `LO-HI` (or `LO`) range | `invalid --port-range "…": …` |


---

## 5. Secrets and F-08

Secrets — the SSL certificate, key, and CA (`SSLCert`, `SSLKey`, `SSLCA`),
the segment-plane keypair/CA (`SegTLSCert`, `SegTLSKey`, `SegTLSCA`), and the
control-channel keypair/CA (`ControlTLSCert`, `ControlTLSKey`, `ControlTLSCA`)
— **come only from environment variables or a `0600` file, never from argv
(F-08).** Although `parseConfig` technically accepts them on the command
line, the production posture is to supply them via:

- env: `GPSTREAM_SSL_CERT`, `GPSTREAM_SSL_KEY`, `GPSTREAM_SSL_CA` (and
  `GPSTREAM_SEG_TLS_CERT`/`_KEY`/`_CA`, `GPSTREAM_CONTROL_TLS_CERT`/`_KEY`/
  `_CA`); or
- the config file: `ssl_cert`, `ssl_key`, `ssl_ca` (and `seg_tls_cert`/
  `seg_tls_key`/`seg_tls_ca`, `control_tls_cert`/`control_tls_key`/
  `control_tls_ca`) in a file with `0600` permissions.

This keeps secrets out of the process command line (visible via `ps`, shell
history, and job schedulers). `SSLMinTLS` (a non-secret version string,
default `1.2`) may be passed as `--ssl-min-tls` or via the `ssl_min_tls` key.

> **Credential provenance (F-13):** when a DSN with a password is loaded from
> the config *file* (tracked by `SourceFromFile`/`DestFromFile`),
> `SecurityWarnings()` emits an advisory — credentials belong in env /
> `.pgpass`, not a plaintext config. A CLI override clears the provenance.

---

## 6. Worked precedence example

Given `config.Default()`, a file, environment, and CLI, the final value of
`WorkersPerPair` resolves as:

- CLI `--workers 4` → **4** (wins).
- else env `GPSTREAM_WORKERS=3` → **3**.
- else file `workers = 2` → **2**.
- else default → **1** (default).

The same last-wins ordering applies to every field: **CLI > env > file >
defaults**.

---

*This reference is derived from `internal/config/config.go` and
`cmd/gpstream/main.go` (module `github.com/gpstream/gpstream`, Go 1.27).
Field names, defaults, env vars, file keys, and validation rules above match
the source exactly.*
