# gpstream — CLI Reference

This is the complete command and flag reference for **gpstream**, an open-source
Go tool that performs high-throughput **LOGICAL** data copy between two
Greenplum 6 clusters over a direct, high-speed network. gpstream streams
table data with `COPY (binary)` directly from source to destination with **no
intermediate storage** — it is *not* `gpbackup`, `gprestore`, or `gpcopy`.

Every subcommand that consumes the full flag contract is bound by a single
shared parser (`parseConfig`) so a flag is never silently ignored. The resolved
configuration is the single source of truth consumed by every component.

## Configuration precedence

A single `Config` is assembled from four layers, applied in this order
(lowest precedence first, highest last):

```
defaults  <  --config file.yaml  <  GPSTREAM_* env  <  CLI flags
```

So **CLI > env > file > defaults**. Concretely, `parseConfig` does:

1. Start from `config.Default()` (the specification defaults).
2. `LoadFile(--config)` — overlays YAML file values onto the defaults.
3. `ApplyEnv()` — overlays `GPSTREAM_*` environment variables (does not
   clobber values already set by higher-precedence sources).
4. Second pass over `os.Args` — **CLI flags win** and are applied last.

See [CONFIGURATION.md](CONFIGURATION.md) for the full field/precedence/env
reference and the YAML key table.

---

## Commands (19 dispatch entries)

The dispatcher (`run`) accepts 18 functional subcommands plus the `help`
group (`-h` / `--help` / `help`), i.e. 19 dispatch entries.

| # | Command | Class | Consumes full contract? |
|---|---------|-------|-------------------------|
| 1 | `copy` | core | yes (`parseConfig`) |
| 2 | `dry-run` | core | yes |
| 3 | `plan` | core | yes |
| 4 | `status` | journal | no (bespoke) |
| 5 | `cleanup` | journal | no (bespoke) |
| 6 | `doctor` | core | yes |
| 7 | `selftest` | utility | no |
| 8 | `man` | utility | no |
| 9 | `resume` | v2 | no (bespoke) |
| 10 | `cancel` | v2 | no (bespoke) |
| 11 | `rollback` | v2 | no (bespoke) |
| 12 | `metrics` | v2 | no (bespoke) |
| 13 | `logs` | v2 | no (bespoke) |
| 14 | `indexes` | v2 | no (bespoke) |
| 15 | `compatibility-check` | v2 | yes |
| 16 | `security-check` | v2 | yes |
| 17 | `package-info` | utility | no |
| 18 | `upgrade-check` | v2 | no (bespoke) |
| 19 | `help` | utility | no |

The commands that go through `parseConfig` (honouring **every** flag) are:
**`copy`, `dry-run`, `plan`, `doctor`, `compatibility-check`,
`security-check`**. The journal/v2 commands parse only a small subset of flags
(listed per-command below) and ignore the rest.

---

## Command reference

- **`copy`** — streams selected objects between clusters (FR-01/02/07/17); full
  contract. Opens **live** src/dest connections (never a fake — a fake has no
  data plane, FR-01/CR-7), builds the plan (bounded by `--timeout`), runs the
  Job Manager (src → stream → dest, no intermediate storage), verifies each
  unit, rebuilds indexes in the background, atomically swaps the staging table
  to its final name, journals every transition, serves Prometheus metrics
  (FR-04) and honours `--auto-resume N`.
- **`dry-run`** — plans **without executing** (FR-10); full contract. Renders
  the deterministic plan summary (units, volume, ETA, conflicts,
  redistribution, collation, plan hash); on `--timeout` prints the collected
  fragment as a forecast. Writes to `--plan-file` if set.
- **`plan`** — emits the deterministic plan as **JSON** (FR-10); full contract.
  Writes to `--plan-file` (0644) or stdout; on `--timeout` emits the partial
  fragment as a forecast.
- **`status`** — reports a job's journal state (FR-02/06/07) from the **file
  mirror** (unit id, state, kind, db.schema.table, bytes). **Flags:**
  `--mirror <path>` (req), `--job <ID>`.
- **`cleanup`** — removes a job's journal entries and staging objects (FR-02).
  **Destructive — requires `--yes`.** **Flags:** `--mirror <path>` (req),
  `--job <ID>`, `--yes` (req), `--auto-cleanup` (also clean prior orphans).
- **`doctor`** — self-diagnoses src/dest clusters (FR-05); full contract. Uses a
  live connection that **never** fabricates a healthy fake for a
  missing/unreachable cluster (CR-7); a missing connstring is a reachability
  FAIL, not a silent PASS. Exits non-zero if any FAIL remains.
- **`selftest`** — internal sanity checks; no flags, no cluster connection.
  Prints PASS/FAIL per case, exits non-zero on any failure.
- **`man`** — prints the built-in man page (FR-05): reads `man/gpstream.8` if
  present, else a short summary. No flags.
- **`resume`** (v2) — resumes an interrupted job (§4.2 / FR-06/07). Reopens the
  journal (file mirror, optionally the live dest-DB journal) and re-queues every
  non-terminal unit. **Flags:** `--mirror <F>` (req), `--auto-resume <N>`.
- **`cancel`** (v2) — cancels a running job (§4.2). Marks every non-terminal
  unit `failed` so the manager won't re-run them, leaving terminal units intact
  (FR-07: journal is the source of truth). **Destructive — requires `--yes`.**
  **Flags:** `--mirror <F>` (req), `--job <ID>`, `--yes` (req).
- **`rollback`** (v2) — rolls a job back (§4.2 / FR-07). Any unit not in a
  terminal state (not verified/swapped) is rolled back to `pending` to be
  re-copied, and completed-but-not-swapped staging objects are rolled back.
  **Destructive — requires `--yes`.** **Flags:** `--mirror <F>` (req),
  `--job <ID>`, `--yes` (req).
- **`metrics`** (v2) — prints the live metrics snapshot (§4.4 / FR-04). Without
  `--metrics-addr` prints process counters; with one, points at the Prometheus
  endpoint. **Flags:** `--metrics-addr <A>`.
- **`logs`** (v2) — prints the journal as a human-readable log (§4.5 / FR-07):
  one line per unit transition (timestamp, unit id, state, kind,
  db.schema.table, attempts, checksum). **Flags:** `--mirror <F>` (req),
  `--job <ID>`.
- **`indexes`** (v2) — reports/rebuilds indexes after a copy (§4.7 / FR-17).
  With `--rebuild`, queues index rebuilds for completed/swapped units (a live
  dest connection is required; the file mirror has none, so it reports intent
  rather than silently passing); without it, reports index state from the
  journal. **Flags:** `--mirror <F>` (req), `--rebuild`.
- **`compatibility-check`** (v2) — reports cross-cluster compatibility
  (§4.13/16): collation/locale/encoding and distribution differences; full
  contract. Prints collation/locale differences and the redistribution verdict
  (needs redistrib, skew %, threshold %).
- **`security-check`** (v2) — reports the TLS/security posture (§4.12); full
  contract. Connects live and checks the connection, honouring
  `--ssl-*`/`--ssl-min-tls`; a plain (non-TLS) connection is a WARN. Exits
  non-zero if any FAIL remains.
- **`package-info`** (v2) — prints package metadata (§4.5): build `version`,
  Go/runtime version, goos/arch, and the commands the build exposes. No flags.
- **`upgrade-check`** (v2) — compares the current version against the operator's
  target (§4.5). **Flags:** `--target <V>` (without it, "latest" is unknown).
  Reports whether an upgrade is available.
- **`help`** / `-h` / `--help` — prints built-in help (commands + flags). The
  19th dispatch entry.

---

## Full flag surface (shared contract)

The following flags are bound by `parseConfig` and honoured by
`copy`, `dry-run`, `plan`, `doctor`, `compatibility-check`, and
`security-check`. Values fold into the `Config` struct; defaults and bindings
below are the exact ones from the source.

### Connection
| Flag | Config field | Default |
|------|--------------|---------|
| `--source` | `Source` | `""` (source connstring; may hold secrets but **never** via argv in production, FR-08) |
| `--dest` | `Dest` | `""` (destination connstring) |

### Selection (FR-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--full` | `Full` | `false` (copy the full database) |
| `--db` / `--schema` / `--table` / `--exclude` | `DBs` / `Schemas` / `Tables` / `Exclude` | `[]` |

All four are **repeatable**. `--db` selects a database, `--schema` a `db.schema`,
`--table` a `db.schema.table`, and `--exclude` any exclude-* entry.

### Metadata / data (FR-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--metadata-only` | `MetadataOnly` | `false` (mutually exclusive with `--data-only`) |
| `--data-only` | `DataOnly` | `false` (mutually exclusive with `--metadata-only`) |

### Conflict / safety (FR-10/FR-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--skip-existing` | `ConflictAction` | `SkipExisting` (default action: never clobber) |
| `--overwrite` / `--truncate` / `--append` | `ConflictAction` | — (sets Overwrite/Truncate/Append) |
| `--conflict-action <v>` | `ConflictAction` | — (alias; skip-existing/overwrite/truncate/append) |
| `--skip-conflicts` | `SkipConflicts` | `false` (**off by default = strict**: conflicts are surfaced, not silently skipped) |

### Rename (FR-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--rename-db src=dst` | `RenameDB` | `{}` (map src→dst; split on first `=`) |
| `--rename-schema src=dst` | `RenameSchema` | `{}` (map src→dst; split on first `=`) |

### Performance (FR-01/FR-12/FR-14)
| Flag | Config field | Default |
|------|--------------|---------|
| `--compress none|zlib|zstd[:1-19]` | `Compression` / `ZstdLevel` | `zstd` / `3` (`zstd:level` sets the level, 1–19) |
| `--rate-limit <Gbps>` | `RateLimit` | `0` (no ceiling; total Gbps ceiling) |
| `--workers-per-pair <N>` | `WorkersPerPair` | `2` (must be `>= 1`, validate) |
| `--data-port-range <a-b>` | `DataPortRange` | `""` (port range `a-b`, low ≤ high) |

### Resilience (FR-06/FR-07)
| Flag | Config field | Default |
|------|--------------|---------|
| `--auto-resume <N>` | `AutoResume` | `0` (`N >= 0`; `0` = manual only) |
| `--verify full|sample|off` | `Verify` | `sample` |

### Consistency (FR-02)
| Flag | Config field | Default |
|------|--------------|---------|
| `--consistent-db` | `ConsistentDB` | `true` (db scope; mutually exclusive with `--consistent-cluster`) |
| `--consistent-cluster` | `ConsistentCluster` | `false` (cluster scope; mutually exclusive with `--consistent-db`) |

### Indexes (FR-17)
| Flag | Config field | Default |
|------|--------------|---------|
| `--index-workers <N>` | `IndexWorkers` | `0` (background index rebuild parallelism) |
| `--no-indexes` / `--background-indexes` | `NoIndexes` | `false` (the latter sets `NoIndexes=false`) |
| `--index-priority size|manual` | `IndexPriority` | `size` |

### Redistribution (FR-15)
| Flag | Config field | Default |
|------|--------------|---------|
| `--no-redistribute` | `NoRedistribute` | `false` (suppress redistribution accounting) |
| `--skew-threshold <X>` | `SkewThreshold` | `0.05` |
| `--redistribution-plan` | `RedistributionPlan` | `false` (surface redistribution in the plan) |
| `--distribution-check` | `DistributionCheck` | `false` (detect distribution mismatch) |

### Diagnostics / checks (v2)
| Flag | Config field | Default |
|------|--------------|---------|
| `--collation-check` | `CollationCheck` | `false` (detect LC_COLLATE/collation differences) |
| `--locale-check` | `LocaleCheck` | `false` (detect locale/encoding/timezone differences) |
| `--adaptive-rate-limit` | `AdaptiveRateLimit` | `false` (throttle per host-pair on a detected bottleneck) |

### Checksum / dedup (v2)
| Flag | Config field | Default |
|------|--------------|---------|
| `--checksum-algorithm sha256|blake3` | `ChecksumAlgorithm` | `""` (spec default `sha256`) |
| `--dedup` / `--skip-if-same-checksum` | `Dedup` | `false` (skip units whose checksum matches the target) |

### TLS (FR-13)
| Flag | Config field | Default |
|------|--------------|---------|
| `--ssl-cert <F>` | `SSLCert` | `""` (**secret** — prefer env/file, not argv, FR-08) |
| `--ssl-key <F>` | `SSLKey` | `""` (**secret** — prefer env/file, not argv, FR-08) |
| `--ssl-ca <F>` | `SSLCA` | `""` (**secret** — prefer env/file, not argv, FR-08) |
| `--ssl-min-tls <V>` | `SSLMinTLS` | `1.2` (minimum TLS version, e.g. `1.2`) |

### Dry-run / planning (FR-10)
| Flag | Config field | Default |
|------|--------------|---------|
| `--dry-run` | `DryRun` | `false` (plan only, do not execute) |
| `--timeout <S>` | `Timeout` | `60s` (bounds the plan-formation phase) |
| `--plan-file <F>` | `PlanFile` | `""` |

### Observability / reports (FR-04)
| Flag | Config field | Default |
|------|--------------|---------|
| `--metrics-addr <A>` | `MetricsAddr` | `""` (serve Prometheus endpoint at `A/metrics`) |
| `--metrics` | `MetricsOn` | `false` |
| `--report-json` / `--report-csv` / `--report-text` | `ReportJSON` / `ReportCSV` / `ReportText` | `""` |

### Operational
| Flag | Config field | Default |
|------|--------------|---------|
| `--config <file.yaml>` | `ConfigFile` | `""` (YAML config file; file layer of precedence) |
| `--log-level debug|info|warn|error` | `LogLevel` | `info` |
| `--quiet` / `--debug` | `Quiet` / `Debug` | `false` |

---

## v2 commands and their flags

The v2 command group was added on top of the base 8 commands. Their flags are
**bespoke** (not the full contract):

| Command | Flags |
|---------|-------|
| `resume` | `--mirror <F>` (required), `--auto-resume <N>` |
| `cancel` | `--mirror <F>` (required), `--job <ID>`, `--yes` (required) |
| `rollback` | `--mirror <F>` (required), `--job <ID>`, `--yes` (required) |
| `metrics` | `--metrics-addr <A>` |
| `logs` | `--mirror <F>` (required), `--job <ID>` |
| `indexes` | `--mirror <F>` (required), `--rebuild` |
| `compatibility-check` | full contract (`parseConfig`) |
| `security-check` | full contract (`parseConfig`) |
| `package-info` | none |
| `upgrade-check` | `--target <V>` |

The v2 **flags** (added to the shared contract): `--overwrite-only-conflicts`,
`--collation-check`, `--locale-check`, `--distribution-check`,
`--redistribution-plan`, `--checksum-algorithm`, `--dedup` (alias
`--skip-if-same-checksum`), `--metrics`, `--report-json`, `--report-csv`,
`--report-text`, `--adaptive-rate-limit`.

---

## Safety defaults

gpstream is deliberately conservative about overwriting destination data:

- **`ConflictAction` defaults to `SkipExisting`** — existing tables are never
  clobbered unless the operator explicitly chooses another action.
- **Data is never overwritten** unless the operator passes `--overwrite`
  (`ConflictAction=Overwrite`) or `--overwrite-only-conflicts`
  (`OverwriteOnlyConflicts`, which rewrites *only* conflicting objects, never
  the whole target). `--truncate` and `--append` are the other explicit actions.
- **`--skip-conflicts` is off by default (`SkipConflicts=false`) — strict mode:**
  conflicts are *surfaced*, not silently skipped, so a run cannot quietly drop
  data. Set `--skip-conflicts` only when you explicitly want conflicts ignored.
- **Secrets (`--ssl-cert`/`--ssl-key`/`--ssl-ca`) come from env or a `0600`
  file, never argv (FR-08).** Although the parser accepts them as flags, supply
  them via `GPSTREAM_SSL_CERT` / `GPSTREAM_SSL_KEY` / `GPSTREAM_SSL_CA` or the
  config file.

## Destructive commands

These modify or destroy journal state and **require `--yes`** (a hard guard in
the code): **`cancel`** (marks non-terminal units failed), **`rollback`** (rolls
units back to pending), and **`cleanup`** (removes journal entries and staging
objects; also offers `--auto-cleanup` for prior orphans). `resume`, `logs`,
`indexes`, `metrics`, and `status` are **not** destructive and do not require
`--yes`.

---

## Environment layer (summary)

Environment variables are read by `ApplyEnv()` (the env layer of
CLI > env > file > defaults). All are prefixed `GPSTREAM_`:
`RATE_LIMIT`, `WORKERS_PER_PAIR`, `COMPRESS`, `LOG_LEVEL`, `AUTO_RESUME`,
`SSL_CERT`, `SSL_KEY`, `SSL_CA`, `OVERWRITE_ONLY_CONFLICTS`, `COLLATION_CHECK`,
`LOCALE_CHECK`, `DISTRIBUTION_CHECK`, `REDISTRIBUTION_PLAN`,
`ADAPTIVE_RATE_LIMIT`, `CHECKSUM_ALGORITHM`, `DEDUP`, `METRICS`, `REPORT_JSON`,
`REPORT_CSV`, `REPORT_TEXT`. Boolean env vars accept `true`/`1`. See
[CONFIGURATION.md](CONFIGURATION.md) for the full env/YAML/precedence reference.

---

*This reference is derived from `cmd/gpstream/main.go` and
`internal/config/config.go` (module `github.com/gpstream/gpstream`, Go 1.27).
Flag names, defaults, and bindings above match the source exactly.*
