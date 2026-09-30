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
defaults  <  --config file.yaml|file.toml  <  GPSTREAM_* env  <  CLI flags
```

So **CLI > env > file > defaults**. Concretely, `parseConfig` does:

1. Start from `config.Default()` (the specification defaults).
2. `LoadFile(--config)` — overlays file values onto the defaults (YAML or
   TOML, selected by extension).
3. `ApplyEnv()` — overlays `GPSTREAM_*` environment variables.
4. Second pass over `os.Args` — **CLI flags win** and are applied last.

See [CONFIGURATION.md](CONFIGURATION.md) for the full field/precedence/env
reference and the YAML/TOML key table.

---

## Commands (11 dispatch entries)

The dispatcher (`run`) accepts the following subcommands. `copy`/`plan` from
older drafts are **not** separate commands: **`dry-run`** performs the
planning (without executing) and **`run`** performs the copy (with executing).
There is no `cancel`, `rollback`, `metrics`, `logs`, `indexes`,
`compatibility-check`, `security-check`, `package-info`, or `upgrade-check`
command — the capability they described is folded into `run`/`doctor`/
`status`/`resume` (see per-command notes).

| # | Command | Class | Consumes full contract? |
|---|---------|-------|-------------------------|
| 1 | `run` | core (copy) | yes (`parseConfig`) |
| 2 | `dry-run` | core (plan) | yes |
| 3 | `doctor` | core (diagnose) | yes |
| 4 | `status` | state/journal | no (bespoke) |
| 5 | `resume` | state/journal | no (bespoke) |
| 6 | `cleanup` | state/journal | no (bespoke) |
| 7 | `selftest` | utility | no |
| 8 | `version` | utility | no |
| 9 | `man` | utility | no |
| 10 | `help` / `-h` / `--help` | utility | no |

The commands that go through `parseConfig` (honouring **every** flag) are:
**`run`, `dry-run`, `doctor`**. The state/journal commands parse only a small
subset of flags (listed per-command below) and ignore the rest.

---

## Command reference

- **`run`** — streams selected objects between clusters (FR-01/02/03/07/17);
  full contract. Opens **live** src/dest connections (pgxpool-backed, one
  connection per worker — a single `pgx.Conn` is not safe for concurrent
  COPY, FR-03), builds the plan (bounded by `--timeout`), runs the Job
  Manager (src → stream → dest, no intermediate storage), verifies each unit,
  rebuilds indexes in the background (FR-17), atomically swaps the staging
  table to its final name, journals every transition to
  `<state-dir>/<operation-id>/journal.jsonl`, appends the diagnostic event
  stream to `<state-dir>/<operation-id>/events.jsonl` (FR-04/F-14), records
  the action to `<state-dir>/<operation-id>/audit.log` (F-13 audit log of
  actions: `run` + `run-finished`), serves
  Prometheus metrics at `--metrics-addr` (FR-04) and honours `--auto-resume N`
  (FR-07). On a Greenplum destination it also measures the live segment-fill
  balance of every copied table (max/avg over `gp_segment_id`, F-15/CR-5) and
  prints it in the final report, warning when a table exceeds
  `--segment-balance-threshold`. `run` is the command that used to be called `copy`.
- **`dry-run`** — plans **without executing** (FR-10); full contract. Renders
  the deterministic plan summary (units, volume, conflicts, redistribution,
  plan hash); on `--timeout` prints the collected fragment as a
  prediction/forecast. Writes the plan JSON to `--plan-file` if set. `dry-run`
  is the command that used to be called `plan`.
- **`doctor`** — self-diagnoses src/dest clusters (FR-05); full contract. Uses
  a live connection that **never** fabricates a healthy fake for a
  missing/unreachable cluster (CR-7); a missing connstring is a reachability
  FAIL, not a silent PASS. Reports the capability matrix (reachability,
  version, segments, route mode, `on_segment` transport, disk, permissions,
  collation, TLS). Exits non-zero if any FAIL remains.
- **`status`** — reports a job's journal state (FR-02/06/07) from the durable
  **file mirror** (operation-id, snap-epoch, per-unit id/state/kind/
  db.schema.table/bytes/epoch). **Flags:** `--operation-id <id>` (required,
  or legacy `--mirror <path>`), `--state-dir <dir>`.
- **`resume`** — resumes an interrupted operation (FR-06/07). Reopens the
  durable operation journal (file mirror, which survived the crash) and
  re-queues every non-terminal unit under the next snapshot epoch; the action
  is recorded to `audit.log` (F-13). **Flags:**
  `--operation-id <id>` (required, or legacy `--mirror <path>`),
  `--state-dir <dir>`, `--auto-resume <N>` (cap on resume attempts).
- **`cleanup`** — removes an operation's journal directory and state (v4 §8
  "State"). **Destructive — requires `--yes`.** `--all-incomplete` removes
  every operation directory that still has non-terminal units (no
  `--operation-id` needed); `--mode drop` ADDITIONALLY drops the
  destination-side objects the operation created — surviving staging tables,
  renamed-in or gpstream-created final tables, and renamed-in schemas —
  derived conservatively from the journal (a pre-existing, conflicted
  destination table is never touched), restoring the receiver to its
  pre-operation state (F-02). `--mode drop` requires `--dest <connstring>`
  (`--ssl-cert`/`--ssl-key`/`--ssl-ca`/`--ssl-min-tls` are honoured). Each
  removal is recorded to `audit.log` **before** the state dir is removed
  (F-13). **Flags:** `--operation-id <id>` (or `--all-incomplete` / legacy
  `--mirror <path>`), `--state-dir <dir>`, `--mode drop`, `--dest
  <connstring>`, `--ssl-cert/--ssl-key/--ssl-ca/--ssl-min-tls`, `--yes`
  (required).
- **`selftest`** — internal sanity checks; no flags, no cluster connection.
  Prints PASS/FAIL per case, exits non-zero on any failure.
- **`version`** — prints the gpstream build version. No flags.
- **`man`** — prints the built-in man page (FR-05): reads `man/gpstream.8` if
  present, else a short summary. No flags.
- **`help`** / `-h` / `--help` — prints built-in help (commands + flags).

> **Note on removed v2 commands.** Earlier design drafts listed `cancel`,
> `rollback`, `metrics`, `logs`, `indexes`, `compatibility-check`,
> `security-check`, `package-info` and `upgrade-check`. None of these are
> wired into the dispatcher. Cancellation is operator-side (kill the process,
> then `resume`); rollback is `resume` (re-copy non-terminal units); live
> metrics are served over HTTP at `--metrics-addr` during `run`; the journal
> log is what `status` prints; index rebuilds happen inside `run` (FR-17);
> compatibility/security posture is what `doctor` reports; and `version`
> covers package/version info.

---

## Full flag surface (shared contract)

The following flags are bound by `parseConfig` and honoured by
`run`, `dry-run`, and `doctor`. Values fold into the `Config` struct; the
defaults below are the exact ones from `config.Default()` and the flag
bindings in `cmd/gpstream/main.go`. Flags marked *alias* share a binding with
a canonical v4 §8 name.

### Connection
| Flag | Config field | Default |
|------|--------------|---------|
| `--source` | `Source` | `""` (source connstring; may hold secrets but **never** via argv in production, FR-08) |
| `--dest` | `Dest` | `""` (destination connstring) |

### Selection (v4 §8)
| Flag | Config field | Default |
|------|--------------|---------|
| `--dbs <csv>` | `DBs` | `[]` (comma-separated, repeatable) |
| `--schemas <csv>` | `Schemas` | `[]` (comma-separated, repeatable) |
| `--tables <csv>` | `Tables` | `[]` (comma-separated, repeatable) |
| `--db` / `--schema` / `--table` | (aliases of the above) | — (deprecated; still parsed) |
| `--exclude <csv>` | `Exclude` | `[]` (any exclude-* entry, comma-separated) |

All are **repeatable** and comma-separated. With none set, the full database
is selected.

### Object classes (v4 §8 / F-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--include metadata,data` | `Include` | `[]` (empty = both metadata and data) |
| `--metadata-only` / `--ddl-only` | `MetadataOnly` | `false` (mutually exclusive with `--data-only`) |
| `--data-only` | `DataOnly` | `false` (mutually exclusive with `--metadata-only`) |

### Rename (v4 §8 canonical + FR-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--map-db src=dst` | `RenameDB` | `{}` (map src→dst; split on first `=`) |
| `--map-schema src=dst` | `RenameSchema` | `{}` |
| `--map-table src=dst` | `RenameTable` | `{}` |
| `--rename-db` / `--rename-schema` / `--rename-table` | (aliases of the above) | — (FR-11 names; still parsed) |

### Conflict / safety (FR-10/FR-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--skip-existing` | `ConflictAction` | `SkipExisting` (default action: never clobber) |
| `--overwrite` / `--truncate` / `--append` | `ConflictAction` | — (sets Overwrite/Truncate/Append) |
| `--conflict-action <v>` | `ConflictAction` | — (alias; skip-existing/overwrite/truncate/append) |
| `--skip-conflicts` | `SkipConflicts` | `false` (**off by default = strict**: conflicts are surfaced, not silently skipped) |
| `--overwrite-only-conflicts` | `OverwriteOnlyConflicts` | `false` (rewrite *only* conflicting objects, never the whole target) |

### Performance (FR-01/FR-12/FR-14)
| Flag | Config field | Default |
|------|--------------|---------|
| `--compress none\|zlib\|zstd[:1-19]` (alias `--compression`) | `Compression` / `ZstdLevel` | `zstd` / `3` (`zstd:level` sets the level, 1–19) |
| `--small-table-threshold <size>` | `SmallTableThreshold` | `1MiB` (F-12: tables smaller than this copy with compression off; `0` disables the auto-disable; suffixes K/M/G) |
| `--rate-limit <rate>` (alias `--max-bw`) | `RateLimit` | `0` (no ceiling; accepts `40`, `40gbps`, `400mbps`, …) |
| `--workers-per-pair <N>` (alias `--workers`) | `WorkersPerPair` | `2` (must be `>= 1`, validated) |
| `--streams <N>` | `Streams` | `0` (v4 §8 canonical parallelism knob; mapped onto the worker pool) |
| `--data-port-range <a-b>` | `DataPortRange` | `""` (port range `a-b`, low ≤ high) |

### Resilience (FR-06/FR-07)
| Flag | Config field | Default |
|------|--------------|---------|
| `--auto-resume <N>` | `AutoResume` | `0` (`N >= 0`; `0` = manual only) |
| `--verify full\|sample\|off` | `Verify` | `sample` |
| `--resume-consistency relaxed\|strict` | `ResumeConsistency` | `""` → relaxed (T5, DESIGN §2.2) |
| `--on-unit-failure continue\|abort` | `OnUnitFailure` | `""` → continue (spec F-06) |

### Consistency (FR-02)
| Flag | Config field | Default |
|------|--------------|---------|
| `--consistent-db` | `ConsistentDB` | `true` (db scope; mutually exclusive with `--consistent-cluster` / `--consistent-tables`) |
| `--consistent-cluster` | `ConsistentCluster` | `false` (cluster scope; mutually exclusive with `--consistent-db` / `--consistent-tables`) |
| `--consistent-tables` | `ConsistentTables` | `false` (per-table fallback: each table under its own short snapshot; the per-table discrepancy is documented — see below) |

**Per-table fallback (`--consistent-tables`).** Instead of one long run-wide
`READ ONLY, REPEATABLE READ` snapshot per database, each table is copied under
its own **short** snapshot: the table's data COPY and its checksum read run on
a dedicated connection with one REPEATABLE READ snapshot, which is released
right after the table completes. **Documented trade-off (spec F-02):** the
tables are therefore *not mutually consistent* — table N is read at a slightly
different point in the source history than table N+1 (the discrepancy is
bounded by the copy time of the tables in between). In exchange, the run does
not hold a long snapshot, which avoids the vacuum pressure a long
`--consistent-db` snapshot puts on a busy PROD. Use it only when a per-table
discrepancy is acceptable (e.g. batch/analytics loads); for a strict
point-in-time copy keep the default `--consistent-db`. The flag is an explicit
fallback and is off by default.

### Transport (T-B, DESIGN §1.3)
| Flag | Config field | Default |
|------|--------------|---------|
| `--copy-mode coordinator\|on_segment` | `CopyMode` | `""` → coordinator (source-direct + dest-coordinator) |
| `--seg-listen <addr>` | `SegListen` | `""` (controller listen address the segreceivers dial; **required** for `on_segment`) |
| `--seg-binary <path>` | `SegBinary` | `""` (gpstream-segreceiver path on the segment hosts; **required** for `on_segment`) |

`on_segment` (T-B segment-direct transport) requires both `--seg-listen` and
`--seg-binary`; the source GUC `ycmdb.yc_allow_copy_to_program` must be `on`.
See [PERFORMANCE.md](PERFORMANCE.md).

### Indexes (FR-17)
| Flag | Config field | Default |
|------|--------------|---------|
| `--index-workers <N>` (alias `--index-parallel`) | `IndexWorkers` | `0` (background index rebuild parallelism) |
| `--no-indexes` | `NoIndexes` | `false` (set to `true`) |
| `--background-indexes` | `NoIndexes` | `false` (sets `NoIndexes=false`) |
| `--index-priority size\|manual` | `IndexPriority` | `size` |

### Redistribution / checks (FR-15)
| Flag | Config field | Default |
|------|--------------|---------|
| `--no-redistribute` | `NoRedistribute` | `false` (suppress redistribution accounting; sets skew gate off) |
| `--skew-threshold <X>` | `SkewThreshold` | `0.05` (negative = skew checks off) |
| `--segment-balance-threshold <X>` | `SegmentBalanceThreshold` | `1.5` (max/avg fill of the destination segments; warning above it; negative = metric only) |
| `--redistribution-plan` | `RedistributionPlan` | `false` (surface redistribution in the plan) |
| `--distribution-check` | `DistributionCheck` | `false` (detect distribution mismatch) |
| `--collation-check` | `CollationCheck` | `false` (detect LC_COLLATE/collation differences) |
| `--locale-check` | `LocaleCheck` | `false` (detect locale/encoding/timezone differences) |
| `--adaptive-rate-limit` | `AdaptiveRateLimit` | `false` (throttle per host-pair on a detected bottleneck) |
| `--zstd-level <1-19>` | `ZstdLevel` | `3` (F-12) |

### Checksum / dedup
| Flag | Config field | Default |
|------|--------------|---------|
| `--checksum-algorithm sha256\|blake3` | `ChecksumAlgorithm` | `""` (spec default `sha256`) |
| `--dedup` (alias `--skip-if-same-checksum`) | `Dedup` | `false` (skip units whose checksum matches the target) |

### TLS (FR-13)
| Flag | Config field | Default |
|------|--------------|---------|
| `--tls` | `SSL` | `false` (require an encrypted connection; plaintext rejected) |
| `--ssl-cert <F>` | `SSLCert` | `""` (**secret** — prefer env/file, not argv, FR-08) |
| `--ssl-key <F>` | `SSLKey` | `""` (**secret** — prefer env/file, not argv, FR-08) |
| `--ssl-ca <F>` (alias `--tls-ca`) | `SSLCA` | `""` (**secret** — prefer env/file, not argv, FR-08) |
| `--ssl-min-tls <V>` | `SSLMinTLS` | `1.2` (minimum TLS version) |

### Dry-run / planning (FR-10)
| Flag | Config field | Default |
|------|--------------|---------|
| `--dry-run` | `DryRun` | `false` (plan only, do not execute) |
| `--timeout <S>` | `Timeout` | `60s` (bounds the plan-formation phase) |
| `--plan-file <F>` | `PlanFile` | `""` (also used as the journal mirror path if set) |

### Operation identity / state (v4 §8 "State")
| Flag | Config field | Default |
|------|--------------|---------|
| `--operation-id <id>` | `OperationID` | `""` (defaults to the plan hash) |
| `--state-dir <dir>` | `StateDir` | `/var/lib/gpstream` (journal lives at `<state-dir>/<operation-id>/journal.jsonl`) |

### Observability / reports (FR-04)
| Flag | Config field | Default |
|------|--------------|---------|
| `--metrics-addr <A>` | `MetricsAddr` | `""` (serve the Prometheus endpoint at `A/metrics` during `run`) |
| `--metrics` | `MetricsOn` | `false` |
| `--report-json <F>` / `--report-csv <F>` / `--report-text <F>` | `ReportJSON` / `ReportCSV` / `ReportText` | `""` |

### Operational
| Flag | Config field | Default |
|------|--------------|---------|
| `--config <file.yaml\|file.toml>` | `ConfigFile` | `""` (config file; file layer of precedence) |
| `--log-level debug\|info\|warn\|error` | `LogLevel` | `info` |
| `--quiet` / `--debug` | `Quiet` / `Debug` | `false` |

---

## State/journal commands and their flags

The state commands do **not** go through `parseConfig`; their flags are
bespoke and shared:

| Command | Flags |
|---------|-------|
| `status` | `--operation-id <id>` (required, or legacy `--mirror <path>`), `--state-dir <dir>` |
| `resume` | `--operation-id <id>` (required, or legacy `--mirror <path>`), `--state-dir <dir>`, `--auto-resume <N>` |
| `cleanup` | `--operation-id <id>` (or `--all-incomplete` / legacy `--mirror <path>`), `--state-dir <dir>`, `--mode drop`, `--yes` (required) |

`--mirror <path>` is a legacy alias that names the journal file directly; it
wins over the `<state-dir>/<operation-id>/journal.jsonl` resolution when set.

---

## Safety defaults

gpstream is deliberately conservative about overwriting destination data:

- **`ConflictAction` defaults to `SkipExisting`** — existing tables are never
  clobbered unless the operator explicitly chooses another action.
- **Data is never overwritten** unless the operator passes `--overwrite`
  (`ConflictAction=Overwrite`) or `--overwrite-only-conflicts`
  (`OverwriteOnlyConflicts`, which rewrites *only* conflicting objects, never
  the whole target). `--truncate` and `--append` are the other explicit
  actions.
- **`--skip-conflicts` is off by default (`SkipConflicts=false`) — strict
  mode:** conflicts are *surfaced*, not silently skipped, so a run cannot
  quietly drop data. Set `--skip-conflicts` only when you explicitly want
  conflicts ignored.
- **Secrets (`--ssl-cert`/`--ssl-key`/`--ssl-ca`) come from env or a `0600`
  file, never argv (FR-08).** Although the parser accepts them as flags,
  supply them via `GPSTREAM_SSL_CERT` / `GPSTREAM_SSL_KEY` / `GPSTREAM_SSL_CA`
  or the config file.

## Destructive commands

Only **`cleanup`** modifies or destroys journal state and **requires
`--yes`** (a hard guard in the code). It removes the operation's journal
directory (and, with `--mode drop`, notes the dest-side staging objects).
`run`, `dry-run`, `doctor`, `status`, `resume`, `selftest`, `version`, `man`
and `help` are **not** destructive and do not require `--yes`.

---

## Environment layer (summary)

Environment variables are read by `ApplyEnv()` (the env layer of
CLI > env > file > defaults). All are prefixed `GPSTREAM_`:
`RATE_LIMIT`, `WORKERS_PER_PAIR`, `COMPRESS`, `LOG_LEVEL`, `AUTO_RESUME`,
`SSL_CERT`, `SSL_KEY`, `SSL_CA`, `OVERWRITE_ONLY_CONFLICTS`, `COLLATION_CHECK`,
`LOCALE_CHECK`, `DISTRIBUTION_CHECK`, `REDISTRIBUTION_PLAN`,
`ADAPTIVE_RATE_LIMIT`, `CHECKSUM_ALGORITHM`, `DEDUP`, `METRICS`, `REPORT_JSON`,
`REPORT_CSV`, `REPORT_TEXT`. Boolean env vars accept `true`/`1`. See
[CONFIGURATION.md](CONFIGURATION.md) for the full env/YAML/TOML/precedence
reference.

---

*This reference is derived from `cmd/gpstream/main.go` and
`internal/config/config.go` (module `github.com/gpstream/gpstream`, Go 1.27).
Command names, flag names, defaults, and bindings above match the source
exactly.*
