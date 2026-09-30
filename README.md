<p align="center"><img src="assets/logo.svg" alt="gpstream" width="180"></p>

# gpstream

High-throughput **logical** data copy between two Greenplum 6 clusters over a
direct high-speed network — **no intermediate storage**, snapshot consistency,
crash-recovery, background index rebuild, redistribution accounting, and a
fully deterministic `--dry-run` plan.

> **Milestone status (this build):** M1 core **verified**. `run` now performs a
> real transfer — it is wired into the job Manager (source → stream → dest) and
> proven against live PostgreSQL clusters by the `tests/integration`
> suite (build tag `integration`). `doctor` reports real reachability failures
> (no silent fake fallback). Greenplum-specific paths (segment fan-out,
> `gp_segment_connect`) and live-cluster chaos (`kill -9` resume, 100 Gbit/s
> channel) are validated via the PG/fake substitute, not a live Greenplum — see
> `RC-CHECKLIST.md`.

## Why streaming, not `gpbackup`/`gpcopy`

- `gpbackup`/`gprestore` need external storage ≥ the size of the DB
  (100 TB–1 PB) — infeasible on cost.
- `gpcopy` is proprietary (Broadcom) — its code may not be used.
- **Logical streaming** via `COPY` (binary) is also **insensitive to
  differences in OS/kernel/glibc between clusters**: data is rewritten on the
  copy, not moved in binary form.

## Requirements at a glance

| ID | Requirement |
|---|---|
| FR-01 | Copy over the network, no intermediate storage |
| FR-02 | Snapshot consistency + cleanup of orphaned data |
| FR-03 | Performance independent of DB architecture (100 000 small AO/CO tables) |
| FR-04 | Monitoring and metrics |
| FR-05 | Packaging, self-diagnostic, man, documentation |
| FR-06 | Resilience, `--auto-resume N` |
| FR-07 | Crash-recovery: integrity check, minimal rollback |
| FR-08 | Minimal dependencies, free licenses only |
| FR-09 | Automated tests |
| FR-10 | `--dry-run`: availability, volume/time estimate, conflicts, plan |
| FR-11 | Selection, data safety, rename |
| FR-12 | zlib / zstd compression |
| FR-13 | TLS, certificates |
| FR-14 | Rate limits, bottleneck auto-detection |
| FR-15 | Redistribution accounting |
| FR-16 | Different OS/kernel/glibc, COLLATION |
| FR-17 | Background index rebuild |
| FR-18 | Project name `gpstream` |

## CLI contract (§5)

```
gpstream run
  --source "<connstr>" --dest "<connstr>"
  (--full | --dbs D... | --schemas D.S... | --tables D.S.T...) [--exclude ...]
  [--metadata-only | --data-only]
  [--skip-existing | --overwrite | --truncate | --append]      # default: skip-existing
  [--skip-conflicts | --conflict-action skip|fail]
  [--map-db src=dst] [--map-schema src=dst] [--map-table src=dst]
  [--compression none|zlib|zstd[:level]] [--zstd-level N]
  [--max-bw <Gbps>] [--workers-per-pair N] [--data-port-range a-b]
  [--auto-resume N] [--verify full|sample|off]
  [--index-workers N | --no-indexes] [--index-priority size|manual]
  [--no-redistribute | --skew-threshold X]
  [--tls] [--ssl-cert F --ssl-key F --ssl-ca F --ssl-min-tls V]
  [--timeout S] [--plan-file F]
  [--config file.toml] [--state-dir D] [--log-level debug|info|warn|error] [--quiet | --debug]

gpstream dry-run  [--timeout S] [--plan-file F]   # same selection flags as run
gpstream status   --operation-id <id>
gpstream resume   --operation-id <id>
gpstream cleanup  <operation-id> [--all-incomplete] [--mode drop]
gpstream doctor   [--source ...] [--dest ...]
gpstream selftest
gpstream man [topic]
```

## Build

```sh
go build -o bin/gpstream ./cmd/gpstream
# cross-compile
GOOS=linux GOARCH=arm64  go build -o bin/gpstream-arm64  ./cmd/gpstream
GOOS=linux GOARCH=amd64  go build -o bin/gpstream-amd64  ./cmd/gpstream
```

> The module proxy `proxy.golang.org` intermittently 403s
> `github.com/klauspost/compress`; the build env sets
> `GOPROXY=https://proxy.golang.org,direct` so the git-clone fallback works.

## Test

```sh
go test -cover ./...
```

## Layout

```
gpstream/
├── cmd/gpstream/            # main, subcommands
├── internal/
│   ├── planner/             # plan, volume estimate, collation, conflicts, idempotent
│   ├── job/                 # journal, checkpoint, swap, cleanup, auto-resume
│   ├── stream/              # host-pair workers, framing, crc
│   ├── source/              # agents, COPY TO, gp_segment_connect, checksum
│   ├── dest/                # agents, COPY FROM staging, verify
│   ├── indexer/             # background index builder
│   ├── limiter/             # adaptive rate limit, /proc metrics
│   ├── metrics/             # metrics, /metrics, json/csv
│   ├── doctor/              # self-diagnostic
│   ├── config/  pg/  selftest/  conn/
├── man/gpstream.8
├── docs/  (README, architecture, user/admin guide, ADR/, FAQ)
├── packaging/ (systemd, rpm/deb, logrotate, examples)
├── tests/ (unit, integration [docker 2×GP6], chaos, perf)
└── .github/workflows/
```

## License

Apache-2.0. See [LICENSE](LICENSE).
