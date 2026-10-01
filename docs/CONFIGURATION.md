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
| `RateLimit` | float64 | `0` (no ceiling) | `--rate-limit` / env / file | Gbps total ceiling; adaptive AIMD (F-14) is always active. |
| `WorkersPerPair` | int | **`2`** | `--workers` / env / file | Must be **≥ 1** (validate). |

### 1.7 Resilience (F-06/F-07)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `AutoResume` | int | **`0`** | `--auto-resume` / env / file | `N ≥ 0`; `0` = manual only. |
| `OnUnitFailure` | string | `""` → `continue` | `--on-unit-failure` / env / file | `continue` (default) \| `abort` (F-06). |
| `Verify` | `VerifyMode` | **`sample`** | `--verify` / file | `full`/`sample`/`off`. |

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

### 1.12 Transport (T-B, §1.3)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `CopyMode` | `CopyMode` | `""` → `coordinator` | `--copy-mode` | `coordinator` \| `on_segment` \| `direct`. |
| `SegListen` | string | `""` | `--seg-listen` | Controller listen address; **required** for `on_segment`. |
| `SegBinary` | string | `""` | `--seg-binary` | gpstream-segreceiver path; **required** for `on_segment`. |
| `SegTLSCert` | string | `""` | `--seg-tls-cert` / env / file | Controller serving cert (segment data plane). |
| `SegTLSKey` | string | `""` | `--seg-tls-key` / env / file | Controller serving key. |
| `SegTLSCA` | string | `""` | `--seg-tls-ca` / env / file | CA for the segment data plane. |

### 1.13 Frame-ack / reliability (§4.6)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `FrameAck` | string | `on` | `--frame-ack` / env / file | `on` (default) \| `off` (plain pipe). |
| `AckTimeout` | string | `30s` | `--ack-timeout` / env / file | `15s` \| `30s` \| `adaptive` (3x median RTT). |
| `MaxReconnects` | int | `20` | `--max-reconnects` / env | Reconnect budget; exhausted = unit failed. |
| `RetransmitBufferMax` | int64 | `64MiB` | `--retransmit-buffer-max` / env | In-flight window per stream. |

### 1.14 Dry-run / planning (F-10)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `DryRun` | bool | `false` | `--dry-run` | Plan only, do not execute. |
| `Timeout` | time.Duration | **`120s`** | `--timeout` / file | `run`/`dry-run`: bounds plan formation (F-10 §6 default). `bench`: the benchmark wall-time limit (unlimited when not passed). |
| `OperationID` | string | `""` | `--operation-id` | Operation identity. `run`: a name, or omitted = a generated timestamp id (`YYYYMMDDTHHMMSSZ` UTC, collision-suffixed) so the state dir is a chronological history. `status`/`resume`/`cleanup`: a name or `latest` (newest operation). The plan JSON is always persisted to `<state-dir>/<operation-id>/plan.json`. |

### 1.15 Observability (F-04)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `MetricsAddr` | string | `""` | `--metrics-addr` | Serve the Prometheus endpoint during `run`. |

### 1.16 Consistency (F-02)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `ConsistentDB` | bool | **`true`** | `--consistent-db` / env / file — db scope (the default) |
| `ConsistentCluster` | bool | `false` | `--consistent-cluster` / env / file — cluster-wide freeze |
| `ConsistentTables` | bool | `false` | `--consistent-tables` / env / file — per-table fallback |

The scopes are resolved in `Validate()`: `--consistent-db` is the default, and an
explicit `--consistent-cluster` or `--consistent-tables` **overrides** it (so
`GPSTREAM_CONSISTENT_TABLES=1` on its own is a valid per-table run, not a
conflict). Only two *explicitly* requested scopes are mutually exclusive
(“—consistent-db, —consistent-cluster and —consistent-tables are mutually
exclusive”).

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
| `Verify` | `sample` |
| `Timeout` | `120s` |
| `ConsistentDB` | `true` |
| `IndexPriority` | `size` |
| `SSLMinTLS` | `1.2` |
| `WorkersPerPair` | `2` |
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

**Naming rule:** a file key is the CLI flag name with dashes turned into
underscores — `--index-priority` ↔ `index_priority`, `--workers` ↔
`workers`. The 22 keys below are 1:1 with 22 CLI flags, so the name
in the docs without the leading `--` is exactly the file key. Env vars are the
same names uppercased under the `GPSTREAM_` prefix where they exist (env
covers a different, session-level subset).

| File key | Field | Notes |
|----------|-------|-------|
| `source` | `Source` | |
| `dest` | `Dest` | |
| `rate_limit` | `RateLimit` | parsed as float |
| `workers` | `WorkersPerPair` | parsed as int |
| `compress` | `Compression` | `none`/`zlib`/`zstd` |
| `zstd_level` | `ZstdLevel` | parsed as int (1–19) |
| `small_table_threshold` | `SmallTableThreshold` | int64 bytes, or a size with a `K`/`M`/`G`/`T` suffix (same notation as the CLI) |
| `auto_resume` | `AutoResume` | parsed as int |
| `on_unit_failure` | `OnUnitFailure` | `continue`/`abort` |
| `verify` | `Verify` | `full`/`sample`/`off` |
| `ssl_cert` | `SSLCert` | **secret** |
| `ssl_key` | `SSLKey` | **secret** |
| `ssl_ca` | `SSLCA` | **secret** |
| `ssl_min_tls` | `SSLMinTLS` | e.g. `1.2` |
| `seg_tls_cert` | `SegTLSCert` | **secret** |
| `seg_tls_key` | `SegTLSKey` | **secret** |
| `seg_tls_ca` | `SegTLSCA` | **secret** |
| `index_priority` | `IndexPriority` | `size`/`fifo` |
| `timeout` | `Timeout` | parsed as duration |
| `consistent_db` | `ConsistentDB` | `true`/`1` |
| `consistent_cluster` | `ConsistentCluster` | `true`/`1` |
| `consistent_tables` | `ConsistentTables` | `true`/`1` |

A file example:

```toml
source = "host=src port=5432 dbname=app"
dest   = "host=dst port=5432 dbname=app"
compress = "zstd"
zstd_level = 3
rate_limit = 10
workers = 2
verify = "sample"
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
| `GPSTREAM_CONSISTENT_DB` | `ConsistentDB` | `true`/`1` |
| `GPSTREAM_CONSISTENT_CLUSTER` | `ConsistentCluster` | `true`/`1` |
| `GPSTREAM_CONSISTENT_TABLES` | `ConsistentTables` | `true`/`1` |
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

ApplyEnv does not clobber values already set by higher-precedence sources, so
CLI beats env and env beats file.

> **Endpoints are not env-driven:** there is no `GPSTREAM_SOURCE`/
> `GPSTREAM_DEST`. The source/destination connstrings come from the
> `--source`/`--dest` flags or the file.

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
| consistency scope | `--consistent-db` is the default; an explicit `--consistent-cluster`/`--consistent-tables` overrides it; two **explicitly** requested scopes are mutually exclusive | `--consistent-db, --consistent-cluster and --consistent-tables are mutually exclusive` |
| conflict action | must be `stop`/`skip`/`overwrite`/`truncate` | `unknown --resolve-conflicts value "…" (want stop\|skip\|overwrite\|truncate)` |
| verify mode | must be `full`/`sample`/`off` | `unknown verify mode "…"` |
| copy-mode on_segment | requires `--seg-binary` and `--seg-listen` | `--copy-mode on_segment requires --seg-binary …` / `… --seg-listen …` |
| frame-ack | must be `on`/`off` (or empty) | `unknown --frame-ack "…" (want on\|off)` |
| ack-timeout | must be `15s`/`30s`/`adaptive` or a valid duration | `invalid --ack-timeout "…" (want 15s\|30s\|adaptive)` |


---

## 5. Secrets and F-08

Secrets — the SSL certificate, key, and CA (`SSLCert`, `SSLKey`, `SSLCA`) and
the segment-plane keypair/CA (`SegTLSCert`, `SegTLSKey`, `SegTLSCA`) —
**come only from environment variables or a `0600` file, never from argv
(F-08).** Although `parseConfig` technically accepts them on the command
line, the production posture is to supply them via:

- env: `GPSTREAM_SSL_CERT`, `GPSTREAM_SSL_KEY`, `GPSTREAM_SSL_CA` (and
  `GPSTREAM_SEG_TLS_CERT`/`_KEY`/`_CA`); or
- the config file: `ssl_cert`, `ssl_key`, `ssl_ca` (and `seg_tls_cert`/
  `seg_tls_key`/`seg_tls_ca`) in a file with `0600` permissions.

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
- else default → **2** (default).

The same last-wins ordering applies to every field: **CLI > env > file >
defaults**.

---

*This reference is derived from `internal/config/config.go` and
`cmd/gpstream/main.go` (module `github.com/gpstream/gpstream`, Go 1.27).
Field names, defaults, env vars, file keys, and validation rules above match
the source exactly.*
