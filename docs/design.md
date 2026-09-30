# Design: dependencies and stack (FR-08)

FR-08: "Static build; dependencies only from stdlib plus a small set of
free, vetted libraries. Each external package - pinned version with a
justification in `docs/design.md`. Forbidden: C libraries with a
runtime dependency, proprietary components, telemetry. Acceptance:
the binary runs on two hosts with different distros/glibc with no
dependency installation; `go mod verify` passes; the dependency list is < 10
packages."

## Direct dependencies (4 — well under the <10 limit)

All pinned to exact versions in `go.mod` (module `github.com/gpstream/gpstream`,
`go 1.27.1`). `go mod verify` passes; the build is
`CGO_ENABLED=0 go build -ldflags "-s -w"` (static, verified on two
distributions/glibc — see TESTBED-PROVISION-RUNBOOK.md: the same static
binary runs on the CentOS 7 host and the CentOS 8 VM without installing
anything).

| Module | Version | License | Why it is here |
|---|---|---|---|
| `github.com/jackc/pgx/v5` | v5.10.0 | MIT | The only pure-Go PostgreSQL driver with the features gpstream needs: `pgxpool` (one connection per operation — FR-03/FR-02 pool model), binary `COPY TO/FROM STDIN` streaming (FR-01 data plane), `sslmode=verify-full` with a custom CA bundle (F-13), and `gp_session_role=utility` DSN parameters for direct segment sessions (R5). A C-based driver (libpq) is exactly what FR-08 forbids (runtime C dependency). Verified against the target kernel, PostgreSQL 9.4.26 (GP6). |
| `github.com/klauspost/compress` | v1.20.0 | BSD-3 | zstd compression for the wire (FR-12: adaptive, with per-unit auto-disable for small tables). Pure-Go, no cgo — the spec names zstd explicitly as the example allowed library. The alternative (stdlib `compress/zlib`) is ~3–5× slower at the same ratio and would push the compression degradation thresholds (FR-14) into the noise on a 100 GbE channel. |
| `github.com/BurntSushi/toml` | v1.6.0 | MIT | Config file parsing (`/etc/gpstream/gpstream.toml` — F-05). Pure-Go, no cgo. TOML is the spec's primary config format; the parser is small and dependency-free. |
| `gopkg.in/yaml.v3` | v3.0.1 | Apache-2.0 | YAML config support (F-05 allows TOML/**or** YAML; `.yaml`/`.yml` config files are accepted with the same schema). Pure-Go, no cgo. |

## Indirect dependencies (transitive, all pinned by go.sum)

`github.com/jackc/pgpassfile` (`.pgpass` reading — F-13: credentials only from
env/.pgpass), `github.com/jackc/pgservicefile` (PG service files),
`github.com/jackc/puddle/v2` (pgxpool's connection pool),
`golang.org/x/sync` (errgroup), `golang.org/x/text` (unicode for pgx),
`github.com/kr/text` + `github.com/rogpeppe/go-internal` (test support only).
No C libraries, no telemetry, no proprietary components — nothing links a
runtime dependency; the binary is statically linked
(`file` → `statically linked`).

## What is deliberately NOT used

- **libpq / C drivers** — forbidden by FR-08 (C runtime dependency).
- **stdlib `compress/zlib`** — insufficient throughput for the 100 GbE target
  (see klauspost/compress above); kept as the documented degradation floor
  (`--compress none` / auto-disable for small tables, FR-12).
- **ORMs / query builders** — gpstream issues a small, stable set of catalog
  and COPY statements directly; a layer in between would obscure the exact
  SQL that must remain 9.4-compatible (GP6 kernel).
- **Any telemetry / metrics exporter** — metrics are local only
  (Prometheus scrape endpoint `--metrics-addr`, F-04); nothing leaves the
  host.

## Build and verification

- Static build: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags
  "-s -w" -o gpstream ./cmd/gpstream` (CI: `.github/workflows/ci.yml`,
  artifact `gpstream-linux-amd64`).
- Dependency integrity: `go mod verify` (checked in CI via `govulncheck`
  job) — all module hashes match go.sum.
- Two-distribution acceptance (FR-08): the same static binary runs on
  CentOS 7 (glibc 2.17) and CentOS 8 (glibc 2.28) hosts with zero installed
  dependencies — verified on the lab (TESTBED-PROVISION-RUNBOOK.md).

## Cross-references

- ADR-000 (language and stack decision) — `docs/adr/000-language-and-stack.md`.
- ADR-001 (architecture: streaming, journal, snapshot model) —
  `docs/adr/001-architecture.md`.
- SECURITY.md (credential handling, TLS, threat model) — F-13.
- CONFIGURATION.md (config file formats: TOML and YAML).
