# gpstream — Configuration Reference

This document is the configuration reference for **gpstream**, the open-source
Go tool that performs high-throughput **LOGICAL** data copy between two
Greenplum 6 clusters over a direct, high-speed network via `COPY (binary)`
with **no intermediate storage** (it is *not* `gpbackup`/`gprestore`/`gpcopy`).

It describes the `Config` struct and its defaults, the three sources that fold
into it (YAML file, environment, CLI), the precedence that resolves them, the
validation rules, and the secret-handling rule (FR-08). See
[CLI_REFERENCE.md](CLI_REFERENCE.md) for the command and flag reference.

---

## 1. The Config struct

`config.Config` (defined at `internal/config/config.go` line 60) is the fully
resolved runtime configuration and the single source of truth consumed by
every component. Every CLI flag, environment variable, and YAML key folds into
it.

### 1.1 Connection
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `Source` | string | `""` | CLI `--source` / env / file | Source connstring. May hold secrets but **never** via argv in production (FR-08). |
| `Dest` | string | `""` | CLI `--dest` / env / file | Destination connstring. |

### 1.2 Selection (FR-11)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `Full` | bool | `false` | `--full` |
| `DBs` | []string | `[]` | `--db` (repeatable) |
| `Schemas` | []string | `[]` | `--schema` (repeatable, `db.schema`) |
| `Tables` | []string | `[]` | `--table` (repeatable, `db.schema.table`) |
| `Exclude` | []string | `[]` | `--exclude` (repeatable) |

### 1.3 Metadata / data (FR-11)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `MetadataOnly` | bool | `false` | `--metadata-only` (mutually exclusive with `DataOnly`) |
| `DataOnly` | bool | `false` | `--data-only` (mutually exclusive with `MetadataOnly`) |

### 1.4 Conflict / safety (FR-10/FR-11)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `ConflictAction` | `ConflictAction` | **`SkipExisting`** | `--skip-existing`/`--overwrite`/`--truncate`/`--append`/`--conflict-action` | One of `skip-existing`/`overwrite`/`truncate`/`append`. |
| `SkipConflicts` | bool | **`false` (strict)** | `--skip-conflicts` | Off by default = strict: conflicts are surfaced, not silently skipped. |

### 1.5 Rename (FR-11)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `RenameDB` | map[string]string | `{}` | `--rename-db src=dst` |
| `RenameSchema` | map[string]string | `{}` | `--rename-schema src=dst` |

### 1.6 Performance (FR-01/FR-12/FR-14)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `Compression` | `Compression` | **`zstd`** | `--compress` / env / file | `none`/`zlib`/`zstd`. |
| `ZstdLevel` | int | **`3`** | `--compress zstd:N` / file `zstd_level` | Must be **1–19** (validate). |
| `RateLimit` | float64 | `0` (no ceiling) | `--rate-limit` / env / file | Gbps total ceiling. |
| `WorkersPerPair` | int | **`2`** | `--workers-per-pair` / env / file | Must be **≥ 1** (validate). |
| `DataPortRange` | string | `""` | `--data-port-range` / file | `a-b` (low ≤ high, parsed by `ParsePorts`). |

### 1.7 Resilience (FR-06/FR-07)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `AutoResume` | int | **`0`** | `--auto-resume` / env / file | `N ≥ 0`; `0` = manual only. |
| `Verify` | `VerifyMode` | **`sample`** | `--verify` / file | `full`/`sample`/`off`. |

### 1.8 Indexes (FR-17)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `IndexWorkers` | int | `0` | `--index-workers` |
| `NoIndexes` | bool | `false` | `--no-indexes` / `--background-indexes` (sets false) |
| `IndexPriority` | string | **`size`** | `--index-priority` / file | `size`/`manual`. |

### 1.9 Redistribution (FR-15)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `NoRedistribute` | bool | `false` | `--no-redistribute` |
| `SkewThreshold` | float64 | **`0.05`** | `--skew-threshold` / file | Skew fraction. |

### 1.10 TLS (FR-13)
| Field | Type | Default | Source | Notes |
|-------|------|---------|--------|-------|
| `SSLCert` | string | `""` | `--ssl-cert` / env / file | **Secret — env/file, never argv (FR-08).** |
| `SSLKey` | string | `""` | `--ssl-key` / env / file | **Secret — env/file, never argv (FR-08).** |
| `SSLCA` | string | `""` | `--ssl-ca` / env / file | **Secret — env/file, never argv (FR-08).** |
| `SSLMinTLS` | string | **`1.2`** | `--ssl-min-tls` / file | Minimum TLS version. |

### 1.11 Dry-run / planning (FR-10)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `DryRun` | bool | `false` | `--dry-run` |
| `Timeout` | time.Duration | **`60s`** | `--timeout` / file | Bounds plan formation. |
| `PlanFile` | string | `""` | `--plan-file` |

### 1.12 Observability (FR-04)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `MetricsAddr` | string | `""` | `--metrics-addr` |

### 1.13 Operational
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `ConfigFile` | string | `""` | `--config` |
| `LogLevel` | `LogLevel` | **`info`** | `--log-level` / env / file |
| `Quiet` | bool | `false` | `--quiet` |
| `Debug` | bool | `false` | `--debug` |

### 1.14 Consistency (FR-02)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `ConsistentDB` | bool | **`true`** | `--consistent-db` (mutually exclusive with `ConsistentCluster` / `ConsistentTables`) |
| `ConsistentCluster` | bool | `false` | `--consistent-cluster` (mutually exclusive with `ConsistentDB` / `ConsistentTables`) |
| `ConsistentTables` | bool | `false` | `--consistent-tables` — per-table fallback: each table under its own short snapshot (per-table discrepancy, off by default) |

### 1.15 v2 conflict refinement (§6.3)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `OverwriteOnlyConflicts` | bool | `false` | `--overwrite-only-conflicts` / env / file | Refines `Overwrite`: only conflicting objects are re-written, never the whole target. |

### 1.16 v2 diagnostics / checks (§6.10, §4.13, §4.14, §4.16)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `CollationCheck` | bool | `false` | `--collation-check` / env / file |
| `LocaleCheck` | bool | `false` | `--locale-check` / env / file |
| `DistributionCheck` | bool | `false` | `--distribution-check` / env / file |
| `RedistributionPlan` | bool | `false` | `--redistribution-plan` / env / file |
| `AdaptiveRateLimit` | bool | `false` | `--adaptive-rate-limit` / env / file |

### 1.17 v2 checksum / dedup (§6.4)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `ChecksumAlgorithm` | string | `""` (spec default `sha256`) | `--checksum-algorithm` / env / file | `sha256`/`blake3`. |
| `Dedup` | bool | `false` | `--dedup` / env / file |
| `SkipIfSameChecksum` | bool | `false` | alias for `Dedup` |

### 1.18 v2 observability / reports (§4.4, §6.9)
| Field | Type | Default | Source |
|-------|------|---------|--------|
| `MetricsOn` | bool | `false` | `--metrics` / env / file |
| `ReportJSON` | string | `""` | `--report-json` / env / file |
| `ReportCSV` | string | `""` | `--report-csv` / env / file |
| `ReportText` | string | `""` | `--report-text` / env / file |

---

## 2. Defaults (the `Default()` function)

`config.Default()` (line 153) seeds the following fields; all other fields
default to their zero values (`""`, `false`, `0`, `nil`, `[]`).

| Field | Default |
|-------|---------|
| `ConflictAction` | `SkipExisting` |
| `Compression` | `zstd` |
| `ZstdLevel` | `3` |
| `AutoResume` | `0` |
| `Verify` | `sample` |
| `Timeout` | `60s` |
| `LogLevel` | `info` |
| `SkewThreshold` | `0.05` |
| `ConsistentDB` | `true` |
| `IndexPriority` | `size` |
| `SSLMinTLS` | `1.2` |
| `WorkersPerPair` | `2` |

Note: the struct comment documents `sha256` as the intended default for
`ChecksumAlgorithm`, but `Default()` does **not** set it — its zero value is
`""` (unset). Set it explicitly via `--checksum-algorithm` to be safe.

---

## 3. The three sources and precedence

The precedence is **CLI > env > file > defaults**. `parseConfig`
(`cmd/gpstream/main.go`) assembles the `Config` in this order:

1. **defaults** — `cfg := config.Default()`.
2. **file** — `cfg.LoadFile(configFile)` overlays the `--config` YAML file onto
   the defaults (the file is the base layer).
3. **env** — `cfg.ApplyEnv()` overlays `GPSTREAM_*` variables. Values already
   set by higher-precedence sources are **not** clobbered.
4. **CLI** — a second pass over `os.Args` applies CLI flags last, so **CLI
   flags win**.

Because the file is loaded before env, **env beats file**; because CLI is
applied last, **CLI beats env**. Unknown YAML keys are ignored.

### 3.1 YAML config file (`--config file.yaml`)

`LoadFile` reads the file and overlays known keys via `assignConfigString`.
Only **string leaves** are honoured by the file loader (non-string YAML nodes
are skipped). The accepted keys (all lowercase) are:

| YAML key | Field | Notes |
|----------|-------|-------|
| `source` | `Source` | |
| `dest` | `Dest` | |
| `rate_limit` | `RateLimit` | parsed as float |
| `workers_per_pair` | `WorkersPerPair` | parsed as int |
| `compress` | `Compression` | `none`/`zlib`/`zstd` |
| `zstd_level` | `ZstdLevel` | parsed as int (1–19) |
| `auto_resume` | `AutoResume` | parsed as int |
| `verify` | `Verify` | `full`/`sample`/`off` |
| `log_level` | `LogLevel` | `debug`/`info`/`warn`/`error` |
| `ssl_cert` | `SSLCert` | **secret** |
| `ssl_key` | `SSLKey` | **secret** |
| `ssl_ca` | `SSLCA` | **secret** |
| `ssl_min_tls` | `SSLMinTLS` | e.g. `1.2` |
| `data_port_range` | `DataPortRange` | `a-b` |
| `skew_threshold` | `SkewThreshold` | parsed as float |
| `index_priority` | `IndexPriority` | `size`/`manual` |
| `timeout` | `Timeout` | parsed as duration |
| `overwrite_only_conflicts` | `OverwriteOnlyConflicts` | `true`/`1` |
| `collation_check` | `CollationCheck` | `true`/`1` |
| `locale_check` | `LocaleCheck` | `true`/`1` |
| `distribution_check` | `DistributionCheck` | `true`/`1` |
| `redistribution_plan` | `RedistributionPlan` | `true`/`1` |
| `adaptive_rate_limit` | `AdaptiveRateLimit` | `true`/`1` |
| `checksum_algorithm` | `ChecksumAlgorithm` | `sha256`/`blake3` |
| `dedup` | `Dedup` | `true`/`1` |
| `metrics` | `MetricsOn` | `true`/`1` |
| `report_json` | `ReportJSON` | path |
| `report_csv` | `ReportCSV` | path |
| `report_text` | `ReportText` | path |

Boolean keys accept `true` or `1`. A file example:

```yaml
source: "host=src port=5432 dbname=app"
dest:   "host=dst port=5432 dbname=app"
compress: "zstd"
zstd_level: 3
rate_limit: 10
workers_per_pair: 2
verify: sample
log_level: info
ssl_cert: "/secrets/gpstream.pem"
ssl_key:  "/secrets/gpstream.key"
ssl_ca:   "/secrets/ca.pem"
ssl_min_tls: "1.2"
skew_threshold: 0.05
```

### 3.2 Environment layer (`GPSTREAM_*`)

`ApplyEnv` (line 207) reads `GPSTREAM_*` variables. Recognised variables:

| Env var | Field | Parsing |
|---------|-------|---------|
| `GPSTREAM_RATE_LIMIT` | `RateLimit` | float |
| `GPSTREAM_WORKERS_PER_PAIR` | `WorkersPerPair` | int |
| `GPSTREAM_COMPRESS` | `Compression` | string |
| `GPSTREAM_LOG_LEVEL` | `LogLevel` | string |
| `GPSTREAM_AUTO_RESUME` | `AutoResume` | int |
| `GPSTREAM_SSL_CERT` | `SSLCert` | string (**secret**) |
| `GPSTREAM_SSL_KEY` | `SSLKey` | string (**secret**) |
| `GPSTREAM_SSL_CA` | `SSLCA` | string (**secret**) |
| `GPSTREAM_OVERWRITE_ONLY_CONFLICTS` | `OverwriteOnlyConflicts` | `true`/`1` |
| `GPSTREAM_COLLATION_CHECK` | `CollationCheck` | `true`/`1` |
| `GPSTREAM_LOCALE_CHECK` | `LocaleCheck` | `true`/`1` |
| `GPSTREAM_DISTRIBUTION_CHECK` | `DistributionCheck` | `true`/`1` |
| `GPSTREAM_REDISTRIBUTION_PLAN` | `RedistributionPlan` | `true`/`1` |
| `GPSTREAM_ADAPTIVE_RATE_LIMIT` | `AdaptiveRateLimit` | `true`/`1` |
| `GPSTREAM_CHECKSUM_ALGORITHM` | `ChecksumAlgorithm` | string |
| `GPSTREAM_DEDUP` | `Dedup` | `true`/`1` |
| `GPSTREAM_METRICS` | `MetricsOn` | `true`/`1` |
| `GPSTREAM_REPORT_JSON` | `ReportJSON` | string |
| `GPSTREAM_REPORT_CSV` | `ReportCSV` | string |
| `GPSTREAM_REPORT_TEXT` | `ReportText` | string |

ApplyEnv does not clobber values already set by higher-precedence sources, so
CLI beats env and env beats file.

---

## 4. Validation rules

`Config.Validate()` (line 172) reports configuration errors independent of the
live clusters. It is called by `parseConfig` after assembling the `Config`. A
failed validation aborts the command.

| Rule | Condition | Error |
|------|-----------|-------|
| zstd level range | `ZstdLevel` must be **1–19** | `zstd level must be 1-19, got N` |
| workers-per-pair | `WorkersPerPair` must be **≥ 1** | `workers-per-pair must be >= 1, got N` |
| auto-resume | `AutoResume` must be **≥ 0** | `auto-resume must be >= 0` |
| metadata vs data | `--metadata-only` and `--data-only` are **mutually exclusive** | `--metadata-only and --data-only are mutually exclusive` |
| consistency scope | `--consistent-db`, `--consistent-cluster` and `--consistent-tables` are **mutually exclusive** (pairwise) | `--consistent-db and --consistent-cluster are mutually exclusive` / `--consistent-tables and --consistent-db are mutually exclusive` |
| conflict action | must be `skip-existing`/`overwrite`/`truncate`/`append` | `unknown conflict action "…"` |
| verify mode | must be `full`/`sample`/`off` | `unknown verify mode "…"` |

Note: when `NoRedistribute` is set and `SkewThreshold < 0`, validation returns
`nil` (this combination is fine) — it is a no-op guard, not an error.

---

## 5. Secrets and FR-08

Secrets — the SSL certificate, key, and CA (`SSLCert`, `SSLKey`, `SSLCA`) —
**come only from environment variables or a `0600` file, never from argv
(FR-08).** Although `parseConfig` technically accepts `--ssl-cert`/`--ssl-key`/
`--ssl-ca` on the command line, the production posture is to supply them via:

- env: `GPSTREAM_SSL_CERT`, `GPSTREAM_SSL_KEY`, `GPSTREAM_SSL_CA`; or
- the YAML config file: `ssl_cert`, `ssl_key`, `ssl_ca` (in a file with
  `0600` permissions).

This keeps secrets out of the process command line (visible via `ps`, shell
history, and job schedulers). `SSLMinTLS` (a non-secret version string,
default `1.2`) may be passed as `--ssl-min-tls` or via the `ssl_min_tls` key.

---

## 6. Worked precedence example

Given `config.Default()`, a file `file.yaml`, environment, and CLI, the final
value of `WorkersPerPair` resolves as:

- CLI `--workers-per-pair 4` → **4** (wins).
- else env `GPSTREAM_WORKERS_PER_PAIR=3` → **3**.
- else file `workers_per_pair: 2` → **2**.
- else default → **2** (default).

The same last-wins ordering applies to every field: **CLI > env > file >
defaults**.

---

*This reference is derived from `internal/config/config.go` and
`cmd/gpstream/main.go` (module `github.com/gpstream/gpstream`, Go 1.27).
Field names, defaults, env vars, YAML keys, and validation rules above match
the source exactly.*
