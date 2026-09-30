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
defaults  <  --config file.toml|file.yaml  <  GPSTREAM_* env  <  CLI flags
```

So **CLI > env > file > defaults**. Concretely, `parseConfig` does:

1. Start from `config.Default()` (the specification defaults).
2. `LoadFile(--config)` — overlays file values onto the defaults (TOML or
   YAML, selected by extension).
3. `ApplyEnv()` — overlays `GPSTREAM_*` environment variables.
4. Second pass over `os.Args` — **CLI flags win** and are applied last.

See [CONFIGURATION.md](CONFIGURATION.md) for the full field/precedence/env
reference and the TOML/YAML key table.

> **Endpoints:** `source` and `dest` come from the `--source`/`--dest` flags
> or the `source`/`dest` keys in the config file. There is **no**
> `GPSTREAM_SOURCE`/`GPSTREAM_DEST` environment variable.

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
| 4 | `status` | state | no (bespoke) |
| 5 | `resume` | state | no (bespoke) |
| 6 | `cleanup` | state | no (bespoke) |
| 7 | `selftest` | utility | no |
| 8 | `version` | utility | no |
| 9 | `man` | utility | no |
| 10 | `help` / `-h` / `--help` | utility | no |

The commands that go through `parseConfig` (honouring **every** flag) are:
**`run`, `dry-run`, `doctor`**. The state commands parse only a small
subset of flags (listed per-command below) and ignore the rest.

---

## Command reference

- **`run`** — streams selected objects between clusters (F-01/02/03/07/17);
  full contract. Opens **live** src/dest connections (pgxpool-backed, one
  connection per worker — a single `pgx.Conn` is not safe for concurrent
  COPY, F-03), builds the plan (bounded by `--timeout`), runs the transfer
  engine (src → stream → dest, no intermediate storage), verifies each unit,
  rebuilds indexes in the background (F-17), atomically swaps the staging
  table to its final name, records every transition to
  `<state-dir>/<operation-id>/`, appends the diagnostic event stream to
  `<state-dir>/<operation-id>/events.jsonl` (F-04/F-14), records the action to
  `<state-dir>/<operation-id>/audit.log` (F-13 audit log of actions: `run` +
  `run-finished`), serves Prometheus metrics at `--metrics-addr` (F-04) and
  honours `--auto-resume N` (F-07). On a Greenplum destination it also
  measures the live segment-fill balance of every copied table (max/avg over
  `gp_segment_id`, F-15) and prints it in the final report, warning when a
  table exceeds `--segment-balance-threshold`. `run` is the command that used
  to be called `copy`.
- **`dry-run`** — plans **without executing** (F-10); full contract. Renders
  the deterministic plan summary (units, volume, conflicts, redistribution,
  plan hash); on `--timeout` prints the collected fragment as a
  prediction/forecast. Writes the plan JSON to `--plan-file` if set. `dry-run`
  is the command that used to be called `plan`.
- **`doctor`** — self-diagnoses src/dest clusters (F-05); full contract. Uses
  a live connection that **never** fabricates a healthy fake for a
  missing/unreachable cluster (CR-7); a missing connstring is a reachability
  FAIL, not a silent PASS. Reports the capability matrix (reachability,
  version, segments, route mode, `on_segment` transport, disk, permissions,
  collation, TLS). Exits non-zero if any FAIL remains.
- **`status`** — reports an operation's state (F-02/06/07) from the durable
  manifest (operation-id, snap-epoch, per-unit id/state/kind/
  db.schema.table/bytes/epoch). **Flags:** `--operation-id <id>` (required),
  `--state-dir <dir>`.
- **`resume`** — resumes an interrupted operation (F-06/07). Reopens the
  durable operation manifest (which survived the crash) and re-queues every
  non-terminal unit under the next snapshot epoch; the action is recorded to
  `audit.log` (F-13). **Flags:** `--operation-id <id>` (required),
  `--state-dir <dir>`, `--auto-resume <N>` (cap on resume attempts),
  `--resume-consistency relaxed|strict`.
- **`cleanup`** — removes an operation's state directory (v4 §8 "State").
  `--all-incomplete` removes every operation directory that still has
  non-terminal units (no `--operation-id` needed); `--mode drop` ADDITIONALLY
  drops the destination-side objects the operation created — surviving staging
  tables, renamed-in or gpstream-created final tables, and renamed-in schemas —
  derived conservatively from the manifest (a pre-existing, conflicted
  destination table is never touched), restoring the receiver to its
  pre-operation state (F-02). `--mode drop` requires `--dest <connstring>`
  (`--ssl-ca` is honoured). Each removal is recorded to `audit.log` **before**
  the state dir is removed (F-13). **Flags:** `--operation-id <id>` (or
  `--all-incomplete`), `--state-dir <dir>`, `--mode drop`, `--dest
  <connstring>`, `--ssl-ca`.
- **`selftest`** — internal sanity checks; no flags, no cluster connection.
  Prints PASS/FAIL per case, exits non-zero on any failure.
- **`version`** — prints the gpstream build version. No flags.
- **`man`** — prints the built-in man page (F-05): reads `man/gpstream.8` if
  present, else a short summary. No flags.
- **`help`** / `-h` / `--help` — prints built-in help (commands + flags).

> **Note on removed v2 commands.** Earlier design drafts listed `cancel`,
> `rollback`, `metrics`, `logs`, `indexes`, `compatibility-check`,
> `security-check`, `package-info` and `upgrade-check`. None of these are
> wired into the dispatcher. Cancellation is operator-side (kill the process,
> then `resume`); rollback is `resume` (re-copy non-terminal units); live
> metrics are served over HTTP at `--metrics-addr` during `run`; the operation
> log is what `status` prints; index rebuilds happen inside `run` (F-17);
> compatibility/security posture is what `doctor` reports; and `version`
> covers package/version info.

---

## Full flag surface (shared contract)

The following flags are bound by `parseConfig` and honoured by
`run`, `dry-run`, and `doctor`. Values fold into the `Config` struct; the
defaults below are the exact ones from `config.Default()` and the flag
bindings in `cmd/gpstream/main.go`.

### Connection
| Flag | Config field | Default |
|------|--------------|---------|
| `--source` | `Source` | `""` (source connstring; may hold secrets but **never** via argv in production, F-08) |
| `--dest` | `Dest` | `""` (destination connstring) |

### Selection (v4 §8)
| Flag | Config field | Default |
|------|--------------|---------|
| `--dbs <csv>` | `DBs` | `[]` (comma-separated) |
| `--schemas <csv>` | `Schemas` | `[]` (comma-separated) |
| `--tables <csv>` | `Tables` | `[]` (comma-separated) |
| `--exclude <csv>` | `Exclude` | `[]` (any db/schema/table, comma-separated) |

All are **comma-separated** lists. With none set, the full database is
selected.

### Object classes (F-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--metadata-only` | `MetadataOnly` | `false` (structure only: DDL + sequences, no data; idempotent) |
| `--data-only` | `DataOnly` | `false` (data only, into the existing structure: rows replaced — TRUNCATE + load + verify; the structure must already exist) |

With neither flag both classes are copied. The two are mutually exclusive
(Validate). There is no `--include` flag: it duplicated the two shorthands
and the default, and was removed.

### Rename (v4 §8 canonical + F-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--map-db src=dst` | `RenameDB` | `{}` (map src→dst; split on first `=`) |
| `--map-schema src=dst` | `RenameSchema` | `{}` |
| `--map-table src=dst` | `RenameTable` | `{}` |

### Conflict / safety (F-10)
| Flag | Config field | Default |
|------|--------------|---------|
| `--resolve-conflicts stop\|skip\|overwrite\|truncate` | `ConflictAction` | `stop` (the run stops on the first collision and the operator decides) |
| `--overwrite` | `ConflictAction` | — (shorthand: sets `ConflictAction=overwrite`) |
| `--truncate` | `ConflictAction` | — (shorthand: sets `ConflictAction=truncate`) |

Conflict policy semantics:

- **`stop`** (default) — the run stops on the first conflict; nothing is
  clobbered, nothing is silently skipped.
- **`skip`** — the conflicting unit is skipped and the run continues.
- **`overwrite`** — copy to a staging table and swap over the final (verified
  before the swap).
- **`truncate`** — copy in-place into the final table after `TRUNCATE`, verify.

### Performance (F-01/F-12/F-14)
| Flag | Config field | Default |
|------|--------------|---------|
| `--compress none\|zlib\|zstd[:1-19]` | `Compression` / `ZstdLevel` | `zstd` / `3` (`zstd:level` sets the level, 1–19) |
| `--zstd-level <1-19>` | `ZstdLevel` | `3` |
| `--small-table-threshold <size>` | `SmallTableThreshold` | `1MiB` (F-12: tables smaller than this copy with compression off; `0` disables the auto-disable; suffixes K/M/G) |
| `--rate-limit <rate>` | `RateLimit` | `0` (no ceiling; accepts `40`, `40gbps`, `400mbps`, …) |
| `--workers <N>` | `WorkersPerPair` | `2` (must be `>= 1`, validated) |

### Resilience (F-06/F-07)
| Flag | Config field | Default |
|------|--------------|---------|
| `--auto-resume <N>` | `AutoResume` | `0` (`N >= 0`; `0` = manual only) |
| `--verify full\|sample\|off` | `Verify` | `sample` |
| `--resume-consistency relaxed\|strict` | `ResumeConsistency` | `""` → relaxed (T5, DESIGN §2.2) |
| `--on-unit-failure continue\|abort` | `OnUnitFailure` | `""` → continue (spec F-06) |

### Transport / frame-ack (T-B, §4.6)
| Flag | Config field | Default |
|------|--------------|---------|
| `--copy-mode coordinator\|on_segment\|direct` | `CopyMode` | `""` → coordinator (source-direct + dest-coordinator) |
| `--seg-listen <addr>` | `SegListen` | `""` (controller listen address the segreceivers dial; **required** for `on_segment`) |
| `--seg-binary <path>` | `SegBinary` | `""` (gpstream-segreceiver path on the SOURCE segment hosts; **required** for `on_segment`) |
| `--seg-tls-cert <F>` | `SegTLSCert` | `""` (controller serving cert for the segment data plane) |
| `--seg-tls-key <F>` | `SegTLSKey` | `""` (controller serving key) |
| `--seg-tls-ca <F>` | `SegTLSCA` | `""` (CA for the segment data plane) |
| `--frame-ack on\|off` | `FrameAck` | `on` (frame-level ACK + RST-resume; `off` falls back to the plain pipe) |
| `--ack-timeout 15s\|30s\|adaptive` | `AckTimeout` | `30s` (`adaptive` = 3x median RTT, min 1s) |
| `--max-reconnects <N>` | `MaxReconnects` | `20` (reconnect budget; exhausted = unit failed) |
| `--retransmit-buffer-max <size>` | `RetransmitBufferMax` | `64MiB` (in-flight window per stream; raise for long-haul WAN) |

`on_segment` (T-B segment-direct transport) requires both `--seg-listen` and
`--seg-binary`; the source GUC `ycmdb.yc_allow_copy_to_program` must be `on`.
`direct` (R5) routes rows to destination segments directly (dest coordinator
out of the write path) and degrades to the coordinator per table when a table
can't be routed. See [PERFORMANCE.md](PERFORMANCE.md).

### Indexes (F-17)
| Flag | Config field | Default |
|------|--------------|---------|
| `--index-workers <N>` | `IndexWorkers` | `0` (background index rebuild parallelism) |
| `--no-indexes` | `NoIndexes` | `false` (set to `true`) |
| `--index-priority size\|fifo` | `IndexPriority` | `size` |

`--no-indexes` is a master switch: with it set, `--index-workers` and
`--index-priority` are ignored (not an error — they are set on every run by
default, `workers 2` / `size`). An advisory on stderr names the ignored flag(s).

### Constraints / consistency (F-02/F-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--no-constraints` | `NoConstraints` | `false` (disable the F-11 constraint-restore pass: FK/PK/unique are re-applied post-load by default) |
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

### Redistribution / checks (F-15/F-16)
| Flag | Config field | Default |
|------|--------------|---------|
| `--no-redistribute` | `NoRedistribute` | `false` (suppress redistribution accounting; sets skew gate off) |
| `--skew-threshold <X>` | `SkewThreshold` | `0.05` (negative = skew checks off) |
| `--segment-balance-threshold <X>` | `SegmentBalanceThreshold` | `1.5` (max/avg fill of the destination segments; warning above it; negative = metric only) |
| `--locale-check` | `LocaleCheck` | `false` (detect locale/encoding/timezone differences, F-16) |

### TLS (F-13)
| Flag | Config field | Default |
|------|--------------|---------|
| `--ssl-ca <F>` | `SSLCA` | `""` (**secret** — prefer env/file, not argv, F-08; the pg plane uses `sslmode=verify-full` by default) |
| `--ssl-cert <F>` | `SSLCert` | `""` (**secret** — env/file only, F-08) |
| `--ssl-key <F>` | `SSLKey` | `""` (**secret** — env/file only, F-08) |
| `--ssl-min-tls <V>` | `SSLMinTLS` | `1.2` (minimum TLS version) |

### Dry-run / planning (F-10)
| Flag | Config field | Default |
|------|--------------|---------|
| `--dry-run` | `DryRun` | `false` (plan only, do not execute) |
| `--timeout <S>` | `Timeout` | `60s` (bounds the plan-formation phase) |
| `--plan-file <F>` | `PlanFile` | `""` (persist the plan JSON to F) |

### Operation identity / state (v4 §8 "State")
| Flag | Config field | Default |
|------|--------------|---------|
| `--operation-id <id>` | `OperationID` | `""` (defaults to the plan hash) |
| `--state-dir <dir>` | `StateDir` | `/var/lib/gpstream` (operation state lives at `<state-dir>/<operation-id>/`) |

### Observability (F-04)
| Flag | Config field | Default |
|------|--------------|---------|
| `--metrics-addr <A>` | `MetricsAddr` | `""` (serve the Prometheus endpoint at `A/metrics` during `run`) |

### Operational
| Flag | Config field | Default |
|------|--------------|---------|
| `--config <file.toml\|file.yaml>` | `ConfigFile` | `""` (config file; file layer of precedence) |

---

## State commands and their flags

The state commands do **not** go through `parseConfig`; their flags are
bespoke and shared:

| Command | Flags |
|---------|-------|
| `status` | `--operation-id <id>` (required), `--state-dir <dir>` |
| `resume` | `--operation-id <id>` (required), `--state-dir <dir>`, `--auto-resume <N>`, `--resume-consistency relaxed\|strict` |
| `cleanup` | `--operation-id <id>` (or `--all-incomplete`), `--state-dir <dir>`, `--mode drop`, `--dest <connstring>`, `--ssl-ca` |

`--operation-id <id>` names the operation directly; the state is resolved at
`<state-dir>/<operation-id>/`.

---

## Safety defaults

gpstream is deliberately conservative about overwriting destination data:

- **`ConflictAction` defaults to `stop`** — the run stops on the first
  collision; nothing is clobbered and nothing is silently skipped unless the
  operator explicitly chooses `skip`, `overwrite`, or `truncate`.
- **Data is never overwritten** unless the operator passes
  `--resolve-conflicts overwrite` (or the `--overwrite` shorthand) or
  `--resolve-conflicts truncate` (or the `--truncate` shorthand).
- **Secrets (`--ssl-cert`/`--ssl-key`/`--ssl-ca`) come from env or a `0600`
  file, never argv (F-08).** Although the parser accepts them as flags,
  supply them via `GPSTREAM_SSL_CERT` / `GPSTREAM_SSL_KEY` / `GPSTREAM_SSL_CA`
  or the config file.
- **The pg data plane is encrypted by default** (`sslmode=verify-full` is
  enforced unless the connstring sets an explicit `sslmode`).

## Destructive commands

Only **`cleanup`** modifies or destroys operation state. With `--mode drop`
it also removes destination-side objects, so use it deliberately. `run`,
`dry-run`, `doctor`, `status`, `resume`, `selftest`, `version`, `man` and
`help` are **not** destructive.

---

## Environment layer (summary)

Environment variables are read by `ApplyEnv()` (the env layer of
CLI > env > file > defaults). All are prefixed `GPSTREAM_`:

`RATE_LIMIT`, `WORKERS_PER_PAIR`, `COMPRESS`, `SMALL_TABLE_THRESHOLD`,
`AUTO_RESUME`, `ON_UNIT_FAILURE`, `CONSISTENT_TABLES`,
`SEGMENT_BALANCE_THRESHOLD`, `FRAME_ACK`, `ACK_TIMEOUT`, `MAX_RECONNECTS`,
`RETRANSMIT_BUFFER_MAX`, `SSL_CERT`, `SSL_KEY`, `SSL_CA`, `SEG_TLS_CERT`,
`SEG_TLS_KEY`, `SEG_TLS_CA`, `LOCALE_CHECK`.

Boolean env vars accept `true`/`1`. See
[CONFIGURATION.md](CONFIGURATION.md) for the full env/TOML/YAML/precedence
reference.

> **Endpoints are not env-driven:** there is no `GPSTREAM_SOURCE`/
> `GPSTREAM_DEST`. The source/destination connstrings come from the
> `--source`/`--dest` flags or the config file.

---

*This reference is derived from `cmd/gpstream/main.go` and
`internal/config/config.go` (module `github.com/gpstream/gpstream`, Go 1.27).
Command names, flag names, defaults, and bindings above match the source
exactly.*
