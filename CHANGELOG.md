# Changelog

All notable changes to gpstream are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]
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
