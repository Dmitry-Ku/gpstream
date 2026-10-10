# gpstream — Installation & Build Guide

`gpstream` is an open-source Go tool that performs **high-throughput logical data
copy between two Greenplum 6 clusters** over a direct high-speed network, with
**no intermediate storage**. It streams logical data via `COPY (binary)` and is
insensitive to OS / kernel / glibc differences between the two clusters.

This guide covers prerequisites, building from source, cross-compilation,
packaging for Linux, log rotation, and how to verify a
build.

---

## 1. What gpstream is (and is not)

- **It is** a streaming logical-copy tool. Data flows `source -> stream -> dest`
  in-memory; nothing is written to an intermediate store.
- **It is not** `gpbackup` / `gprestore`. Those need external storage of at
  least the size of the database; gpstream needs none.
- **It is not** the proprietary `gpcopy`.
- It supports **snapshot-level consistency**, **crash recovery**, **background
  index rebuild**, **redistribution accounting**, and a **fully deterministic
  `--dry-run` plan**.

The Go module is `github.com/gpstream/gpstream` (Go 1.27). It is not a git
repository, so build directly from the source tree.

---

## 2. Prerequisites

| Requirement | Detail |
| --- | --- |
| **Go** | `go 1.27` (the module declares `go 1.27.1`). A Go 1.27 toolchain is required to build. |
| **Kernel** | Linux **6.1+** kernel for the Linux package/runtime targets. |
| **Network (build only)** | Module fetches need a reachable Go module proxy (see note below). |
| **Greenplum cluster** | **Not required** to build, and **not required** to run `--help`. A **reachable live cluster is required for `dry-run`** (it self-diagnoses both clusters and must not mask an unreachable one — CR-7) **and for `run`**; **`bench` needs only the source** (the payload is drained on the controller and discarded — no destination). |

### Module proxy note (important)

`proxy.golang.org` **intermittently returns HTTP 403** for the module
`github.com/klauspost/compress`. When that happens the proxy-only fetch fails.
The build environment therefore sets the proxy so the **direct (git clone)
fallback** can resolve that module:

```bash
export GOPROXY=https://proxy.golang.org,direct
```

These are the environment the build is tested against; export the same
variables (plus `GOFLAGS=-mod=mod`) for a working toolchain environment.

---

## 3. Build from source

The entry point is `./cmd/gpstream`. The standard build command is:

```bash
go build -o bin/gpstream ./cmd/gpstream
```

This produces a static binary at `bin/gpstream`. The binary links only the C
runtime it bundles (TLS / zlib are in-process, zstd is pure Go), so it has no
external runtime dependency.

Verify the build immediately:

```bash
./bin/gpstream --help
./bin/gpstream version
```

> A live Greenplum cluster is **not** needed for `--help` or `version`.
> `dry-run` (self-diagnostic + plan) and `run` require reachable clusters;
> `bench` needs only the source (no destination).

---

## 4. Makefile targets

The project ships a `Makefile` with the following targets:

| Target | Purpose |
| --- | --- |
| `all` | Default; runs `build`. |
| `build` | `go build -o bin/gpstream ./cmd/gpstream`. |
| `cross` | Cross-compile alias (builds `amd64`; arm64 was dropped from the build matrix, 01.10). |
| `amd64` | `go build -o bin/gpstream-amd64 -tags linux_amd64 ./cmd/gpstream`. |
| `test` | `go test ./...`. |
| `cover` | `go test -cover ./...`. |
| `vet` | `go vet ./...`. |
| `fmt` | Runs `gofmt -l .` then `gofmt -w .`. |
| `clean` | Removes `bin/`. |

The Makefile exports `GOPROXY ?= https://proxy.golang.org,direct` and sets
`GOFLAGS ?= -mod=mod`, so `make build` / `make cross` will use the correct proxy
fallback automatically.

---

## 5. Cross-compile (amd64)

Because gpstream is a Go static binary, cross-compilation is straightforward.
The build matrix is **linux/amd64 only** (arm64 was dropped from the build
matrix on 01.10 — no ARM hosts in the target environments):

```bash
make amd64     # -> bin/gpstream-amd64  (GOOS=linux GOARCH=amd64)
make cross     # alias for amd64
```

The resulting binary runs on any host with a Linux 6.1+ kernel; it is insensitive
to the host's glibc / kernel version (see §7).

---

## 6. Linux package install

The `packaging/` tree holds the distribution artifacts:

| Path | Contents |
| --- | --- |
| `packaging/logrotate.conf` | logrotate configuration for gpstream logs. |
| `packaging/examples/` | Example configuration `gpstream.toml`. |
| `packaging/scripts/` | Package lifecycle scripts (post-install/pre-upgrade/post-remove). |

### 6.1 Install the RPM

The RPM is built by `scripts/package.sh` (goreleaser v1.24 + nfpms, driven by
`.goreleaser.yaml`), not by a checked-in spec. It installs all four binaries
(`gpstream`, `gpstream-source-agent`, `gpstream-dest-agent`,
`gpstream-segreceiver`) to `/usr/bin`, the example config to
`/etc/gpstream/gpstream.toml` (a `%config(noreplace)` file — upgrades keep
operator edits), the logrotate rule to `/etc/logrotate.d/gpstream`, and the
man pages to `/usr/share/man/man8`.

### 6.2 Install the Debian package

The `.deb` is produced by the same `scripts/package.sh` flow (nfpms metadata
lives in `.goreleaser.yaml`). It places the same set of files as the RPM;
install with `dpkg -i gpstream-<ver>-amd64.deb` (or `apt install ./...`).
The binary is static (CGO_ENABLED=0), so the package carries no library
dependencies and installs cleanly on a wide range of Debian/Ubuntu hosts
with a Linux 6.1+ kernel.

## 6.3 Deployment model: what goes where

gpstream ships **four** binaries: the `gpstream` controller CLI, the
`gpstream-source-agent` / `gpstream-dest-agent` (the two-agent data path — the
only data path), and `gpstream-segreceiver` (the `on_segment` kernel helper).
Which of them a host needs depends on the `--copy-mode` you run:

| Where | What | When |
| --- | --- | --- |
| **Controller host** (the machine the operator runs gpstream from — it does *not* have to be a cluster host; any host with network access to both clusters) | `gpstream` (main CLI) + `gpstream-source-agent` + `gpstream-dest-agent` | always (the package installs all four) |
| **Source segment hosts** (every host in the source segment topology) | `gpstream-segreceiver` | only for `--copy-mode mesh` |
| **Destination hosts** | *nothing* by default, or `gpstream-dest-agent` for a cross-host dest placement | cross-host: the dest agent runs the per-unit `COPY FROM` on the destination cluster |

The data plane always runs in the agents (ADR-002 remediated; the
single-controller in-memory data path is gone). The source agent runs on the
controller host by default and reads the source cluster; the dest agent runs
on the controller host and writes the destination cluster (or, for a true
cross-host placement, on the destination cluster host). The controller is
control plane only. The agent binaries are resolved by convention (the packaged install puts all four in `/usr/bin`, beside each other) — no flags to set.
the agent paths (the package places both at `/usr/bin/gpstream-source-agent`
and `/usr/bin/gpstream-dest-agent`), so a single package install on the
controller host is enough for the default layout. The network carries the
compressed agent-to-agent volume and the controller holds no data
connections.

**Recommended:** install the full `gpstream` package on the controller host
*and* on every source segment host (for `on_segment`). The package places all
four binaries under `/usr/bin`, giving one canonical path for
the agent/segment binaries by convention (all four land in `/usr/bin`) and one
version-control surface (`rpm -qa gpstream` / `dnf upgrade gpstream` on every
host) — today there is no version handshake between controller and agents, so
the package is the only practical way to keep them in lockstep.

**Minimal-footprint alternative (on_segment):** copy the single static
`gpstream-segreceiver` file to every source segment host (one file,
unprivileged, no root); set `GPSTREAM_SEG_BINARY` to that path. This is the
F-15 "install = copy one file" mode; use it only if you deliberately want no
package on the segment hosts.

```bash
# controller (installs all four binaries)
rpm -i gpstream-<ver>-amd64.rpm            # or .deb
# on_segment: also on every source segment host
rpm -i gpstream-<ver>-amd64.rpm
gpstream dry-run --config /etc/gpstream/gpstream.toml
# agents are placed by the --*-agent-ssh flags (P1e): the source agent on the
# source master, the dest agent on the destination coordinator. Omit a flag
# to run that agent locally (when gpstream itself runs on that coordinator).
gpstream run --config /etc/gpstream/gpstream.toml \
  --source-agent-ssh gp@<source-master-host> \
  --dest-agent-ssh gp@<dest-coordinator-host>
```

The `dry-run` diagnostic probes the segreceiver on **every** segment host
(`--check` via `COPY ... TO PROGRAM ... ON SEGMENT`) and reports a WARN naming
the hosts where it is missing.

---

## 7. logrotate

The logrotate configuration is at `packaging/logrotate.conf`:

```
/var/log/gpstream/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
```

Install it (e.g. copy to `/etc/logrotate.d/gpstream`) and it will rotate
gpstream logs under `/var/log/gpstream/` daily, keeping 14 rotations.
`copytruncate` is used because the running gpstream process keeps the file open.

---

## 8. Verify the install

Two built-in commands confirm a working build:

### 9.1 `gpstream version`

Prints the build version — no cluster required:

```bash
gpstream version
```

### 9.2 `gpstream dry-run`

Runs the self-diagnostic against the source/dest clusters, **then** plans the copy without
executing. The diagnostic **must not** report healthy for an unconfigured or
unreachable cluster — a missing connstring is a reachability `FAIL`, not a
masked healthy fake. Provide reachable connstrings (via flags or `--config`):

```bash
gpstream dry-run --source "host=... user=... dbname=..." --dest "host=... user=... dbname=..."
```

It prints findings with severity (`FAIL` / `WARN` / `PASS`), the side
(source/dest), the name, and a recommendation, then an overall result:

```
overall: PASS (no FAIL)
```

or, if issues exist:

```
overall: N FAIL finding(s)
```

Any FAIL aborts the dry-run with the report and a non-zero exit; on a clean
diagnostic it goes on to render the plan summary (units, volume, conflicts,
redistribution, plan hash).

### 9.3 Makefile shortcuts

```bash
make build      # -> bin/gpstream
```

---

## 9. Configuration precedence

Config values resolve with precedence **CLI > env (`GPSTREAM_*`) > file
(`--config`) > defaults**. A few defaults (from `internal/config`):

| Setting | Default |
| --- | --- |
| `ConflictAction` | `stop` (the run halts on the first conflict; opt in to `skip`/`overwrite`/`truncate` with `--resolve-conflicts`) |
| `Compression` | `zstd` |
| `ZstdLevel` | `3` (range 1–19) |
| `AutoResume` | `0` (manual only) |
| `Verify` | `off` (post-copy content verification; `--verify on` enables it) |
| `Timeout` | `120s` |
| `ConsistentDB` | `true` |
| `IndexPriority` | `size` |
| `SSLMinTLS` | `1.2` |
| `WorkersPerPair` | `1` |

Secrets (SSL cert / key / CA) come from **env or file, never from argv**. The
example configuration is at `packaging/examples/gpstream.toml`; TLS paths in it
are shown commented with mode `0600` for the key file.

---

## 10. Quick checklist

- [ ] Go 1.27 toolchain present.
- [ ] `export GOPROXY=https://proxy.golang.org,direct` set (proxy 403 fallback).
- [ ] `go build -o bin/gpstream ./cmd/gpstream` succeeds; `make cross` builds both archs.
- [ ] `gpstream version` prints the build version.
- [ ] For a real copy: source and dest clusters are reachable (verify with `gpstream dry-run`).
