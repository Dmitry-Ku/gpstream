<p align="center"><img src="assets/logo.svg" alt="gpstream" width="180"></p>

# gpstream

High-throughput **logical** data copy between two Greenplum 6 clusters over a
direct high-speed network — **no intermediate storage**, snapshot consistency,
crash-recovery, background index rebuild, redistribution accounting, and a
fully deterministic `--dry-run` plan.

<p align="center"><a href="assets/architecture.svg"><img src="assets/architecture.svg" alt="gpstream architecture: coordinator and mesh copy modes" width="900"></a></p>

*Architecture: two copy modes (coordinator / mesh), the control plane,
the encrypted WAN channel (mTLS, GPST v2 frames, zstd, frame-ACK), and the
capabilities — parallelism, checksums, RST-resume, resume, rate-limit,
encryption, observability. [Open full-size SVG](assets/architecture.svg).*

> **Milestone status (this build):** core **verified** on a real GP6
> two-cluster stand (1221 tables / 196M rows, coordinator and mesh modes,
> TLS + frame-ack, index rebuild). `run` performs a real transfer wired into
> the job Manager (source agent → stream → dest agent, zero-DB controller);
> `bench` measures the input stream in both copy modes. The
> `gp6lab`-tagged integration suite exercises the data path against real
> PostgreSQL/Greenplum (see `internal/pgtest`); `dry-run` reports real
> reachability failures (no silent fake fallback).

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
| F-01 | Copy over the network, no intermediate storage |
| F-02 | Snapshot consistency + cleanup of orphaned data |
| F-03 | Performance independent of DB architecture (100 000 small AO/CO tables) |
| F-04 | Monitoring and metrics |
| F-05 | Packaging, self-diagnostic, man, documentation |
| F-06 | Resilience, `--auto-resume N` |
| F-07 | Crash-recovery: integrity check, minimal rollback |
| F-08 | Minimal dependencies, free licenses only |
| F-09 | Automated tests |
| F-10 | `--dry-run`: availability, volume/time estimate, conflicts, plan |
| F-11 | Selection, data safety, rename |
| F-12 | zlib / zstd compression |
| F-13 | TLS, certificates |
| F-14 | Rate limits, bottleneck auto-detection |
| F-15 | Redistribution accounting |
| F-16 | Different OS/kernel/glibc, COLLATION |
| F-17 | Background index rebuild |
| F-18 | Project name `gpstream` |

## CLI contract (§8)

```
gpstream run
  --source "<connstr>" --dest "<connstr>"
  (--dbs D... | --schemas D.S... | --tables D.S.T...) [--exclude ...]
  [--resolve-conflicts stop|skip|overwrite|truncate]   # default: stop
  [--overwrite | --truncate]                           # explicit opt-ins
  [--map-db src=dst] [--map-schema src=dst] [--map-table src=dst]
  [--compress none|zlib|zstd[:level]] [--zstd-level N]
  [--small-table-threshold SIZE]                       # F-12, default 1MiB
  [--copy-mode coordinator|mesh]
  [--seg-listen HOST:PORT]
  [--seg-tls-cert F --seg-tls-key F --seg-tls-ca F]
  [--workers N] [--rate-limit <rate>]                 # e.g. 40, 40gbps, 400mbps
  [--auto-resume N]
  [--on-unit-failure continue|abort]
  [--frame-ack on|off] [--ack-timeout 15s|30s|adaptive]
  [--max-reconnects N] [--retransmit-buffer-max SIZE]
  [--verify on|off]                                    # post-copy verification; default: on
  [--index-workers N | --no-indexes] [--index-priority size|fifo]    # default: size
  [--ssl-ca F]                                         # pg plane TLS (connstring sslmode=verify-full default)
  [--operation-id <id|latest>] [--state-dir DIR]
  [--timeout S]
  [--config file.toml]

gpstream dry-run  [--timeout S]                     # same selection flags as run
gpstream bench    [--timeout S]                     # input-stream rate: read source, discard payload (no dest; --copy-mode coordinator|mesh)
gpstream status   --operation-id <id|latest>
gpstream resume   --operation-id <id|latest>
gpstream cleanup  --operation-id <id|latest> [--all-incomplete] [--mode drop] [--dest <conn>] --yes
gpstream version
man gpstream
```

Priority: CLI flag > environment > config file > default. Config file:
`/etc/gpstream/gpstream.toml`; state: `/var/lib/gpstream/<operation-id>/`.
The full flag reference is in [docs/CLI_REFERENCE.md](docs/CLI_REFERENCE.md).

## Build

```sh
go build -o bin/gpstream ./cmd/gpstream
# cross-compile (amd64 only; arm64 dropped from the build matrix, 01.10)
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
├── cmd/gpstream/            # main, subcommands, flag parsing, help
├── internal/
│   ├── plan/                # plan, volume estimate, routing, collation, conflicts
│   ├── route/               # GPDB 6.x hash-distribution reproduction (T0-2)
│   ├── topology/            # cluster/segment discovery
│   ├── transfer/            # framing, reliable pipe, stream engine
│   │   ├── pg/              # COPY TO/FROM, schema DDL, conn pool
│   │   ├── source/          # source-side read path
│   │   ├── dest/            # dest-side write path, segment balance
│   │   ├── segstream/       # segment-direct data plane (mesh / ON SEGMENT)
│   │   └── compress/        # zstd/zlib
│   ├── manifest/            # operation manifest, dest objects, resume
│   ├── monitor/             # metrics, events, /metrics
│   ├── ratelimit/           # adaptive rate limit (AIMD), /proc metrics
│   ├── index/               # background index builder
│   ├── dryrun/              # self-diagnostic (dry-run pre-flight)
│   ├── config/              # config file + env + defaults
│   ├── pgtest/              # live-PG test harness
├── man/gpstream.8
├── docs/                    # CLI reference, metrics, quickstart, guides
├── packaging/               # systemd, rpm/deb, logrotate, examples
├── tests/                   # unit, integration, chaos, perf
└── .github/workflows/       # CI
```

## License

Apache-2.0. See [LICENSE](LICENSE).
