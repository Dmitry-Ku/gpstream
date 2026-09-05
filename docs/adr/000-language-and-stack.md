# ADR-000: Implementation language and core stack

- **Status:** Accepted
- **Date:** 2026-09-02
- **Deciders:** project owner, engineering
- **Milestone:** M0

## Context

gpstream is a high-throughput data-copy tool between Greenplum 6 clusters
(100 TB–1 PB). The technical specification (FR-08) mandates:

- a **single static binary** (or RPM/DEB/tar.gz) with **minimal third-party
  dependencies**, all free (Apache-2.0/MIT/BSD);
- cross-compiled binaries for **linux amd64 and arm64**;
- an **I/O-bound** workload (≥ 80% of a 100 GbE channel — the bottleneck is the
  network and disk, not CPU);
- **100 000 small AO/CO partitioned tables** copied in ≤ 15 min, which demands a
  large concurrent connection pool to segments and DDL/COPY batching;
- **crash-recovery on 100 TB–1 PB** (FR-06/FR-07), which argues strongly for
  **memory safety**;
- a large scope (18 FRs, milestones M0–M4), so **development velocity** matters.

Candidate languages: Go, Rust, C.

## Decision

Implement gpstream in **Go 1.27+** (pinned `go 1.27.1` in `go.mod`).

### Rationale

| Criterion | Go | Rust | C |
|---|---|---|---|
| Single static binary, minimal deps (FR-08) | ✅ stdlib `crypto/tls`, `compress/zlib`; zstd = `klauspost/compress` (BSD) | ✅ but larger dependency surface | ❌ manual linking of zlib/zstd/openssl/pg — exactly what FR-08 forbids |
| ≥ 80% of a 100 GbE channel | ✅ bottleneck is network/disk, not CPU; G1 GC pauses are µs | ✅ marginally faster in raw CPU | ✅ |
| 100 000 small tables + connection pool + batching (FR-03) | ✅ **goroutines are purpose-built** for this | ⚠️ tokio or hand-rolled threads — far more code/bugs | ❌ manual concurrency |
| Separate worker processes; worker-death ≠ job-death (FR-06) | ✅ + trivial watchdog | ✅ | ✅ |
| Crash-recovery on 100 TB–1 PB (FR-07) | ✅ memory-safe | ✅ memory-safe | ❌ **unsafe = risk at petabyte scale** |
| Cross-compile amd64/arm64 | ✅ **verified in M0** | ✅ | ⚠️ harder |
| Dev velocity for 18 FR + 5 milestones | ✅ fastest | ⚠️ ×2–3 | ❌ |
| Reads `/proc/stat`, `/proc/diskstats` (FR-14) | ✅ | ✅ | ✅ |

**Rust** is the only realistic competitor (memory-safe, single binary). It loses
for this workload specifically: gpstream is I/O-bound, so Rust's CPU edge is not
on the critical path; its async runtime or manual threading is strictly more
complex than goroutines for the connection-pool/batching pattern; and the
single CPU-heavy feature (zstd) is explicitly handled by FR-14's adaptive
compression degradation. **C** is rejected: unsafe code on 100 TB–1 PB is a risk,
and "minimal dependencies + single binary + cross-compile" are hardest in C.

The specification itself recommends Go and permits Rust as an alternative; this
ADR records the choice so it can be revisited only via a new ADR.

## Consequences

- `go.mod` module `github.com/gpstream/gpstream`.
- Allowed external modules (all free-licensed):
  - `github.com/jackc/pgx/v5` (PostgreSQL protocol)
  - `github.com/klauspost/compress` (zstd, BSD)
  - stdlib only otherwise (`net`, `crypto/tls`, `compress/zlib`, `testing`,
    `crypto/sha256`).
- `GOPROXY=https://proxy.golang.org,direct` — `proxy.golang.org` intermittently
  403s `klauspost/compress`; the `direct` (git clone) fallback resolves it.
- `go test -cover ./...` gate: core packages ≥ 70% (FR-09/CР-8).
- Build matrix: `GOOS=linux GOARCH=amd64|arm64 go build -o gpstream ./cmd/gpstream`.
- Secrets never appear in argv (FR-08): only 0600 files or `GPSTREAM_*` env.
