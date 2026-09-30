# Changelog

All notable changes to gpstream are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]
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
