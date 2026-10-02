# Changelog

All notable changes to gpstream are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]
### Added: two-agent data path — source agent (P1)

The ADR-002 remediation (moving the data plane out of the controller) is
under way. This change lands the **source agent**: a new
`gpstream-source-agent` binary + `internal/agent` core that runs on a
source-cluster host, owns the source pg connection and the FR-02 snapshot,
streams `COPY ... TO STDOUT (FORMAT binary)`, and pushes the bytes
**compressed over a real socket** (optionally TLS) to the destination agent.
The network now carries the compressed volume (FR-12), which the
single-controller build does not (its pipe is in-memory).

- `internal/agent/source.go` — `agent.Push`: source conn + FR-02 snapshot +
  COPY TO STDOUT + framed/compressed push; reports rows + wire/logical.
- `cmd/gpstream-source-agent/` — the binary (dials the dest agent; `--check`
  doctor probe; SIGINT/SIGTERM unwinds the stream).
- `.goreleaser.yaml` — packages `gpstream-source-agent` (3rd binary).
- P1 acceptance (`tests/integration/source_agent_test.go`): over a real TCP
  socket the on-wire bytes are the compressed volume (strictly < logical),
  the reconstructed payload is byte-for-byte the source table, and the row
  count matches.

The destination agent (P2) and the controller control-plane wiring (P3)
follow; the single-controller path (`--topology single`) is untouched.

### Documented: single-controller data path is an acknowledged debt (ADR-002)

The implemented build runs both PostgreSQL connections in one process, so
the frame/compress transport is an in-memory pipe: both TCP legs carry
uncompressed binary COPY, `--compress` costs controller CPU without
reducing inter-cluster bandwidth, the `gpstream_bytes_total`/`wire` numbers
are the compressed in-memory volume (not the network volume), and a
controller on an external host is a data trust root (two hops, data in
controller RAM). New:

- `docs/adr/002-single-controller-data-path.md` — the debt, its
  consequences, and the two-agent remediation target (ADR-001 data path).
- `docs/plans/two-agent-transport.md` — phased remediation plan (P0
  transport interface → P6 metrics/docs) with acceptance criteria
  (on-network compressed bytes, controller with no data connections).
- `doctor` now emits a `topology` WARN for cross-host copies (single
  controller in the data path); same-endpoint copies are unaffected.
- OPERATOR_GUIDE (`--compress`), METRICS, and SECURITY (threat model +
  controller-as-trust-root) state the actual topology.

### Fixed: resume now completes a unit a crashed run left `completed`

A run that died in the window between the `completed` journal write and the
atomic staging->final rename left the unit `completed` (verified data sitting
in the staging table) while the destination never received the table. A
re-run treated `completed` as terminal and skipped it, so the journal said
"done" but the table was missing — contradicting the run's own comment that a
resume should retry the swap. Now a re-run finishes the swap:

- staging table present -> the atomic rename is performed (same overwrite
  decision as the normal path), then sequences/segment-balance/index build
  run as for a normal swap; the unit is counted as a completed unit.
- staging gone, final present -> the rename already landed but the `swapped`
  journal write was lost; the state is simply recorded.
- staging and final both gone -> the verified data is lost (e.g. an operator
  cleanup); the unit is marked `failed` (a re-copy is required) and the
  anomaly is reported instead of claiming success.

New events: `resume_swap_completed`, `resume_swap_recorded`,
`resume_swap_data_lost`, `resume_swap_failed`. Test:
TestResumeCompletesUnswappedUnit (integration, all three cases).

### Fixed: primary key constraints are now restored (F-11)

The post-swap constraint pass read only foreign keys and user CHECK
constraints (`contype 'f','c'`), so a migrated table ended up with its PK's
unique index (built by the FR-17 pool) but **no `pg_constraint` row** — the
table had no primary key on the destination. Now the pass also restores the
primary key constraint (`contype 'p'`), with two accompanying changes:

- The FR-17 pool builds only the **secondary** indexes. The PK's unique index
  is created atomically by `ALTER TABLE ... ADD CONSTRAINT <name> PRIMARY
  KEY`; PostgreSQL does not adopt a pre-existing standalone unique index on
  that statement, so a pool-built PK index would be a 42P07 collision.
- PK constraints are applied in a **first pass, before** foreign keys and
  CHECKs, so an FK referencing a migrated table's key column finds the unique
  index. On a rename the PK constraint is rewritten to the destination
  table's naming (same rule as index names: `t_m1_pkey` → `t_m1_demo_pkey`).

Tests: TestPKConstraintRestoredOnLivePG (integration, no-rename + map-table
rename, asserts exactly one index and the `pg_constraint` row),
TestManagerRestoresPKConstraintRename / TestManagerRestoresPKConstraintNoRename
(unit).

### Fixed: map-table index builds — wrong-table REINDEX and name collisions

Two defects in the background index build (F-17) for renamed tables
(`--map-table`), found by a live B→C copy where the destination already
carried a prior full copy of the same source table:

- The existence check matched an index by **name + schema only**, so an
  index of the same name on *another* table in the destination schema
  (the prior copy) was treated as "already built" and `REINDEX`ed — while
  the renamed table itself was left with **no indexes and no primary key**,
  yet journaled `index_done: true`. The check is now scoped to the target
  table via `pg_index.indrelid`.
- Index names are unique **per schema**, so keeping the source name on a
  renamed table would collide with the other table's index. On a rename the
  index name is now rewritten to the destination table's naming
  (`t_m1_pkey` on `t_m1` → `t_m1_demo_pkey` on `t_m1_demo`; a name with no
  `<table>_` prefix gets the destination table name prefixed).

Tests: TestIndexRenameCollisionOnLivePG (integration, two real PG instances),
TestCreateIndexesRenameCollision / TestIndexExistsScopedToTable /
TestDestIndexName (unit); TestCreateIndexesNew and
TestManagerRunIndexesHonourRename updated for the renamed index names.

### Changed: `--overwrite` always rewrites; `--verify` is now `on`/`off` (default `off`)

Removed the pre-copy identical-data shortcut on `--overwrite`. Previously a
conflicting table whose data already matched the source was detected via a
full `COPY ... TO STDOUT` digest of **both** the source and the destination
and then skipped. That meant two extra full-table reads before any data moved
— prohibitive on very large tables. Now `--overwrite` (staging + swap) and
`--truncate` **always rewrite** the destination; there is no pre-copy content
comparison. `--resolve-conflicts skip` still skips conflicting units, and the
journal's completed-unit skip on a re-run is unchanged.

`--verify` now takes `on`/`off` (was `full`/`sample`/`off`) and is **`off` by
default**. With `on`, after each unit lands the content checksum is re-derived
on the source (same snapshot as the data COPY) and on the destination and
compared; a mismatch sets the unit to `failed` and drops the staging table —
no unverified data is ever swapped in. With `off` (the default) the COPY
protocol's row count is the landed-data check, so a default run reads each
source table exactly once. This makes the flag actually do what it says:
previously `full`/`sample`/`off` were parsed but the post-copy verify always
ran in full, so `off` did not disable it.

Unit tests: TestManagerOverwriteAlwaysRewrites (identical overwrite target is
re-copied + swapped, not skipped), TestManagerResolve (verify follows
`--verify`), TestManagerChecksumMismatch / TestManagerUnitFailureEmitsEvent
(under `--verify on`).

### Changed: operation identity is a timestamp by default; `--plan-file` removed

`--operation-id` is no longer defaulted to the deterministic plan hash. When
omitted on `run`, a **timestamp** id is generated (UTC,
`YYYYMMDDTHHMMSSZ`, e.g. `20261001T153000Z`; a `-2`, `-3`, … suffix on a
same-second collision), so the state directory is a **chronological history**
of runs — "find the latest run" is just the newest folder.

- `--operation-id latest` on `status`/`resume`/`cleanup` resolves to the
  newest operation (by timestamp folder name; mtime for operator-chosen
  names).
- Explicit human-readable ids are unchanged (first-class, not just
  timestamps).
- `--plan-file` is removed. The deterministic plan (CR-4) is now **always**
  persisted to `<state-dir>/<operation-id>/plan.json`, next to the journal,
  so every operation is self-describing.
- The plan hash is still computed and printed (and still used as the durable
  `job_id` in the destination journal tables); it is no longer the default
  folder name. A re-run of the same plan with no explicit id is therefore a
  **new** operation — pass the id (or `latest`) to resume an earlier one.

Unit tests: TestDefaultOperationIDFormat / TestResolveOperationID* /
TestLatestOperationID* / TestParseOperationArgsLatest / TestFolderTimestamp /
TestPersistPlan.

### Added: `gpstream bench` — input-stream benchmark (F-14)

A new `bench` command measures the source's production rate *before* the
data leaves for the destination: it streams the selected tables through the
real data path (`COPY ... TO STDOUT` -> frame v2 -> compression -> adaptive
rate limiter) and discards the payload instead of writing a staging table.
Only the source cluster is used (`--dest`, if given, is ignored); all
selection/performance options (CLI > env > toml) are honored exactly as for
`run`. Per-table and total rows, wire/logical bytes, elapsed time and MB/s
are printed to the screen. `--timeout S` is the benchmark's wall-time limit
(unlimited when not passed — a benchmark capped by default would truncate
the number it reports).

Use it to decide `--workers` / `--rate-limit` against the input ceiling:
if `bench` is already slower than the link, the destination is never the
bottleneck.

Unit tests: TestBenchSelectsSourceTables / TestBenchHonorsSelectionFlags /
TestBenchEmptySelection / TestBenchAllStreamsAndDiscards /
TestBenchAllTimeoutCutsShort / TestBenchAllEmptyUnits.

### Fix: overwrite drop of a destination table referenced by foreign keys
### (F-02/F-11, non-deterministic under parallel workers)

With `--resolve-conflicts overwrite`, the swap dropped the existing final
table with a plain `DROP TABLE`. If that table was referenced by foreign
keys on OTHER destination tables (a prior run's restored FK set, or an
operator table), the drop failed with `2BP01` (cannot drop table ... other
objects depend on it) — and because swap order depends on copy speed with
parallel workers, the failure was non-deterministic (which referenced
table got swapped first). The overwrite drop now removes the dependent FK
constraints first (surgically — not a blanket `DROP ... CASCADE`, so
operator views and other dependent objects are untouched); the manager's
post-load restore pass re-applies the source FK set (F-11), so the change
is lossless for the migrated tables.

Regression: live T30 run 2 (B→C) conflict matrix — `overwrite` over a
destination seeded with an FK graph (t_s09 → t_s01 + self-FK) failed with
2BP01 on t_s01/t_s09; after the fix the matrix is deterministic. Unit
tests: TestSwapOverwriteDropsDependentFKs /
TestSwapOverwriteNoDependentFKs.

### Removed the `man` subcommand (redundant)

Per the maintainer: the `gpstream man` subcommand is redundant — the man page is
installed to /usr/share/man/man8 by the package (standard `man gpstream`).
The subcommand also had a real bug: it read man/gpstream.8 relative to the
current working directory, so an installed binary printed a two-line
stub instead of the page. Removed from main.go (case + cmdMan), help,
the man COMMANDS section, the man-family generator (no gpstream-man.8),
tests, and docs (README, QUICKSTART now point at `man gpstream`).

### Docs: removed internal spec references from man page and `gpstream help`

Per the maintainer: user-facing output should not cite internal spec items. Removed
all `F-xx`/`FR-xx`/`v4 §N` references from `man/gpstream.8` (13 places) and
`printHelp` (6 places) — the descriptions now stand on their own.

### Fix: man seg-flags + full `gpstream help` (help/man vs code consistency)

`gpstream help` was stale: it omitted ~22 flags the CLI actually accepts
(`--config`, `--metrics-addr`, `--no-constraints`, `--verify`,
`--zstd-level`, `--exclude`, `--plan-file`, `--timeout`, `--dry-run`,
`--consistent-db/cluster`, `--ssl-cert/key/min-tls`, `--seg-tls-*`,
`--all-incomplete`, `--mode`, `--yes`, ...). `printHelp` is now rewritten
against the real flag set (verified: every flag `parseConfig` accepts is
present in the help text, no phantoms).

The man page squashed the five segment-direct flags into one unnamed pipe
line ("Segment-direct (on_segment) options"); they are now individual
entries with descriptions (`--seg-listen`, `--seg-binary`,
`--seg-tls-cert`, `--seg-tls-key`, `--seg-tls-ca`), rendered and verified
with groff on the lab.

### F-05 release pipeline: goreleaser + man-page family + version stamp

One-command release artifact build (tar.gz + RPM + DEB, amd64+arm64) via
goreleaser (pinned v1.24.0), plus the subcommand man-page family the spec
requires. `make package` / `scripts/package.sh` produce into `dist/`:
`gpstream-<ver>-linux-amd64.tar.gz`,
`gpstream-segreceiver-<ver>-linux-amd64.tar.gz`,
`gpstream-<ver>-amd64.{deb,rpm}`.

> **01.10 update:** arm64 dropped from the build matrix (no ARM hosts in the
> target environments). The F-05 pipeline originally produced amd64+arm64;
> it now produces linux/amd64 only.

- **Version stamp**: `main.version` is now an `ldflags`-overridable `var`
  (was a hard-coded `const`); the build stamps `{{ .Version }}` from the git
  tag, so `gpstream version` reports the release version.
- **Man family** (`scripts/gen-man-pages.sh`): the single source of truth is
  `man/gpstream.8`; each subcommand page is a `.so` alias so `man
  gpstream-run` etc. render the full manual under the subcommand name. The
  family is packaged into `/usr/share/man/man8/`.
- **`.goreleaser.yaml`**: two static builds (gpstream, gpstream-segreceiver),
  archives, and deb+rpm nfpms carrying the binary, systemd unit, example
  config, logrotate, and the man family; no runtime library dependency
  (static, proven on EL7/glibc 2.17 + EL8/glibc 2.28, F-16).
- **`release.yml`** workflow: on tag `v*`, builds and publishes artifacts to
  the GitHub release (goreleaser with `GITHUB_TOKEN`).
- **Packaging scripts** (`packaging/scripts/{postinstall,preremove,postremove}.sh`):
  systemd unit reload/stop/cleanup hooks.

**F-05 acceptance (clean host) verified on the lab (CentOS 7 / glibc
2.17):** `rpm -i gpstream-<ver>-amd64.rpm` installs the binary, segreceiver,
systemd unit, `/etc/gpstream/gpstream.toml`, logrotate, and the full man
family (`man gpstream-run` works); `gpstream version`/`selftest` pass from the
packaged binary; a second config file drives `doctor` (exact problem list on a
deliberately broken environment) and `dry-run`/`run`/`status` complete a real
operation. The spec's "clean host + package + 2 config files ⇒ working
operation" and "doctor on a broken environment ⇒ exact list of problems"
criteria are met.

### Fix: `skipped` is now a terminal unit state

A unit the run deliberately did not copy (a `--resolve-conflicts skip`, or an
`--overwrite` target whose data is already identical on the destination) was
counted as `skipped` in the run summary but left `state: pending` in the
journal. Consequence: after a successful run `status` reported `0/N units
terminal` (wrong), and `resume` would re-queue a unit the operator chose not
to copy. Now the unit is journaled `skipped` (a new terminal state): `status`
counts it as done, `resume`/`crash-recovery` leave it alone, and the
`--overwrite` checksum-skip emits a `unit_skipped` event for the audit trail.
`status`/`metrics`/`cleanup --all-incomplete` and the docs (OPERATOR_GUIDE,
TROUBLESHOOTING, FAILOVER, man) all treat `skipped` as terminal.

### Added `GPSTREAM_CONSISTENT_DB`/`_CLUSTER` env vars + `consistent_db`/`consistent_cluster` file keys; fixed consistency-scope resolution

the maintainer: the per-table env var had no sibling for the other two scopes
(asymmetry vs the naming rule), and `GPSTREAM_CONSISTENT_TABLES=1` was a
hard error ("mutually exclusive with --consistent-db") because the default
`ConsistentDB=true` could not be told apart from an explicit request — the
documented per-table fallback was effectively dead. Now: `Validate()`
resolves the scopes — `--consistent-db` stays the default, an explicit
`--consistent-cluster`/`--consistent-tables` (CLI/env/file) overrides it,
and only two *explicitly* requested scopes conflict. Added
`RequestConsistentDB()` (CLI `--consistent-db`), the two env vars and the two
file keys. Tests: `TestConsistentScopeResolution` (7-case matrix),
`TestConsistentEnvVars`, `TestLoadFileConsistentKeys`; the old
"tables+default-db errors" expectations inverted. Live-verified on GP6:
`GPSTREAM_CONSISTENT_TABLES=1` dry-run now succeeds, `--consistent-db
--consistent-cluster` still errors.

### Removed `--locale-check` (flag + env + file key)

the maintainer: collation/locale differences are always detected and reported
(`detectCollations` runs unconditionally; the dry-run always prints the
collation block), so the flag only added a redundant LC_TYPE count line.
Removed the CLI flag, `GPSTREAM_LOCALE_CHECK`, the `locale_check` file key,
the `Config.LocaleCheck` field and the dry-run locale line. File keys: 21→22
(net: −1 locale_check, +2 consistent_db/_cluster); env vars: 18→19 (−1
LOCALE_CHECK, +2 CONSISTENT_DB/_CLUSTER). Docs, man, CHANGELOG updated. All
tests green, English guard OK.

### Removed `--skew-threshold` and `--no-redistribute` (flags + file keys); raw skew metric kept

the maintainer (variant b): the threshold only drove a dry-run boolean warning (and a
plan-hash input) and never affected data or routing; `--skew-threshold -1` was
a bug (always-warn), not an off-switch, and `--no-redistribute` was
reporting-only (redistribution is done by the destination at COPY). The
`SkewThreshold`/`NoRedistribute` fields, the `--skew-threshold`/
`--no-redistribute` flags, the `skew_threshold` file key, the
`Redistribution.NeedsRedistrib`/`Threshold` fields and the `redistribCount`
warning were removed. The FR-15 metric stays: `detectRedistribution` now
reports the raw segment skew plus a layout verdict
(`identical` / `differs` / `cannot confirm`), always printed in the dry-run
(`redistribution: skew N% — <verdict>`), and the plan hash uses the raw skew
only (so report knobs no longer change the fingerprint). Tests: threshold
cases removed/converted (planner, planner_skew, main coverage, config). Docs,
man, packaging example updated. All tests green, English guard OK.

### Removed `--resume-consistency` (flag + file key); relaxed is now the only resume re-read rule

the maintainer: the strict mode (re-read the ENTIRE database on every resume) was a
rare, very expensive choice; F-02 already gives one-point-in-time within a
run, and relaxed re-reads exactly the tables that are not guaranteed to come
from a single snapshot (non-terminal or mixed-epoch). The flag, the
`resume_consistency` file key, the `JobMeta.ResumeCons` field and the
`gpstream_jobs.resume_cons` column were removed; `reReadTables` now takes no
mode. `status` no longer prints the mode. Tests: the strict cases were
removed/converted (epoch rule table, t7 resume run 3 now asserts idempotent
relaxed resume). Docs, man, CHANGELOG updated.

### Removed `--segment-balance-threshold` (flag, file key, env var)

the maintainer: the knob did not earn its place. The F-15 live segment-fill metric
stays — it is still measured after every swap, recorded in the final report
and events.jsonl, and the WARNING line now fires at a fixed 1.5 gate
(`dest.DefaultBalanceThreshold`). Removed: the CLI flag, the
`segment_balance_threshold` config-file key, and
`GPSTREAM_SEGMENT_BALANCE_THRESHOLD`. Docs, man page and tests updated;
the fixed-gate behavior is covered by TestManagerBalanceUsesFixedGate.

### Bug fixes found by the config-file semantic audit

- `--no-redistribute` no longer short-circuits `Validate()`: an early
  `return nil` made every later check (verify mode, copy mode, frame-ack
  values) unvalidated whenever the flag was present.
- Default `--timeout` is now **120s**, matching the spec (F-10 §6
  "--timeout (default 120 s)"); it was 60s.
- Config FILE `small_table_threshold` now accepts the same size notation as
  the CLI (`1M`, `512K`, `2G`, `1T`, or plain bytes) instead of plain bytes
  only.
- `--consistent-tables` / `GPSTREAM_CONSISTENT_TABLES` still error against
  the default DB scope (documented mutual exclusion kept); this remains a
  known sharp edge (see docs/CONFIGURATION.md).

### Aligned the config-file key and env var to the documented CLI name

The CLI flag stays `--workers` (as documented). Per the naming rule
(*the doc name without the leading `--` is the file key*), the
config-file key was renamed `workers_per_pair` → `workers` and the env
var `GPSTREAM_WORKERS_PER_PAIR` → `GPSTREAM_WORKERS`, so all three
layers carry one name: `--workers` / `workers` / `GPSTREAM_WORKERS`.


### Added

- `run` CLI contract per v4 §8: `--metadata-only` / `--data-only` (F-11;
  no `--include` flag — it duplicated the two shorthands and the default),
  `--zstd-level` (1–19), `--no-redistribute`, `--streams` / `--workers`.
- TOML configuration file support (`--config file.toml`, F-05) alongside YAML;
  precedence CLI > env > file > defaults.
- `doctor` self-diagnosis (F-16) with precise problem list.
- `internal/topology` package: single source of truth for the Greenplum
  catalog (`gp_segment_configuration`, 8 columns). `IsGreenplum`/`Read`/
  `SegmentCount`/`UpCount`/`ProbeSQL`; `doctor` now reuses it instead of
  duplicating catalog SQL (dedup, v4 §9 layout).
- `SECURITY.md`, `CONTRIBUTING.md`.
