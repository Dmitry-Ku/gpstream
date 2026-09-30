# gpstream — Installation & Build Guide

`gpstream` is an open-source Go tool that performs **high-throughput logical data
copy between two Greenplum 6 clusters** over a direct high-speed network, with
**no intermediate storage**. It streams logical data via `COPY (binary)` and is
insensitive to OS / kernel / glibc differences between the two clusters.

This guide covers prerequisites, building from source, cross-compilation,
packaging for Linux, the systemd service, log rotation, and how to verify a
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
| **Greenplum cluster** | **Not required** to build, and **not required** to run `--help`, `dry-run`, or `plan` (these use a built-in fake connection). A **reachable live cluster is required for `copy`** (and for `doctor`, which must not mask an unreachable cluster). |

### Module proxy note (important)

`proxy.golang.org` **intermittently returns HTTP 403** for the module
`github.com/klauspost/compress`. When that happens the proxy-only fetch fails.
The build environment therefore sets the proxy so the **direct (git clone)
fallback** can resolve that module:

```bash
export GOPROXY=https://proxy.golang.org,direct
```

The repository's `env.sh` also sets these (plus `GOFLAGS=-mod=mod`); you can
source it for a working toolchain environment.

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
./bin/gpstream selftest
```

> A live Greenplum cluster is **not** needed for `--help`, `selftest`, `dry-run`,
> or `plan`. `copy` (and `doctor`) require a reachable cluster.

---

## 4. Makefile targets

The project ships a `Makefile` with the following targets:

| Target | Purpose |
| --- | --- |
| `all` | Default; runs `build`. |
| `build` | `go build -o bin/gpstream ./cmd/gpstream`. |
| `cross` | Cross-compiles the full matrix (builds both `amd64` and `arm64`). |
| `amd64` | `go build -o bin/gpstream-amd64 -tags linux_amd64 ./cmd/gpstream`. |
| `arm64` | `GOOS=linux GOARCH=arm64 go build -o bin/gpstream-arm64 ./cmd/gpstream`. |
| `test` | `go test ./...`. |
| `cover` | `go test -cover ./...`. |
| `vet` | `go vet ./...`. |
| `fmt` | Runs `gofmt -l .` then `gofmt -w .`. |
| `doctor` | Builds, then runs `./bin/gpstream doctor`. |
| `selftest` | Builds, then runs `./bin/gpstream selftest`. |
| `clean` | Removes `bin/`. |

The Makefile exports `GOPROXY ?= https://proxy.golang.org,direct` and sets
`GOFLAGS ?= -mod=mod`, so `make build` / `make cross` will use the correct proxy
fallback automatically.

---

## 5. Cross-compile amd64 / arm64

Because gpstream is a Go static binary, cross-compilation is straightforward.
Use the Makefile targets:

```bash
make amd64     # -> bin/gpstream-amd64
make arm64     # -> bin/gpstream-arm64  (GOOS=linux GOARCH=arm64)
make cross     # builds both amd64 and arm64
```

The resulting binary runs on any host with a Linux 6.1+ kernel; it is insensitive
to the host's glibc / kernel version (see §8).

---

## 6. Linux package install

The `packaging/` tree holds the distribution artifacts:

| Path | Contents |
| --- | --- |
| `packaging/deb/` | Debian package `control` file (package `gpstream`, v0.1.0, `database` section). Built from the cross-compiled static binary. |
| `packaging/rpm/` | RPM spec (`gpstream.spec`, v0.1.0, License Apache-2.0). |
| `packaging/systemd/` | systemd unit file `gpstream.service`. |
| `packaging/logrotate.conf` | logrotate configuration for gpstream logs. |
| `packaging/examples/` | Example configuration `gpstream.toml`. |

### 6.1 Install the RPM

The RPM spec builds the static binary and installs it to `%{_bindir}`
(typically `/usr/local/bin/gpstream`), the systemd unit to `%{_unitdir}`,
the example config to `/etc/gpstream/gpstream.toml`, and the man page
(`gpstream.8`) to `%{_mandir}/man8`. It sets the proxy fallback for the build
step (`export GOPROXY=https://proxy.golang.org,direct`; `go build -o bin/gpstream ./cmd/gpstream`).
Its `%post` / `%preun` / `%postun` scripts wire the systemd unit in and out via
`%systemd_post`, `%systemd_preun`, and `%systemd_postun`, so service management
is automatic.

### 6.2 Install the Debian package

The `packaging/deb/control` file describes the `gpstream` package
(`Section: database`, `Priority: optional`, `Architecture: amd64`,
`Depends: ${misc:Depends}`). Build the `.deb` from the cross-compiled static
binary using the deb packaging flow, then install with your package manager
(e.g. `dpkg -i gpstream_*.deb`), which places the binary, the systemd unit,
the example config, and the man page. The binary is static, so it installs
cleanly on a wide range of Debian/Ubuntu hosts with a Linux 6.1+ kernel.

---

## 7. systemd service

The service unit lives at `packaging/systemd/gpstream.service`. It runs a
continuous copy from a configuration file:

```ini
[Service]
Type=simple
# Worker processes are children; a worker death does not kill the job (FR-06).
ExecStart=/usr/local/bin/gpstream run --config /etc/gpstream/gpstream.toml
Restart=on-failure
RestartSec=3
# Internal watchdog + systemd restart together provide resilience (FR-06).
TimeoutStopSec=60
```

The unit is `WantedBy=multi-user.target` and `After=network-online.target`
(network-aware). To enable and start it after install:

```bash
systemctl enable --now gpstream.service
systemctl status gpstream.service
```

The RPM / deb packages install the unit and register it via the systemd
`%post` scripts, so a package install wires the service automatically.

---

## 8. logrotate

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
`copytruncate` is used because the running service keeps the file open.

---

## 9. Verify the install

Two built-in commands confirm a working build:

### 9.1 `gpstream selftest`

Runs internal sanity checks (no cluster required). Each case prints `PASS` or
`FAIL`, and the command exits non-zero if any case fails:

```bash
gpstream selftest
```

Expected tail:

```
selftest: OK
```

### 9.2 `gpstream doctor`

Runs the self-diagnostic against the source/dest clusters. Unlike `dry-run`,
`doctor` **must not** report healthy for an unconfigured or unreachable cluster —
a missing connstring is a reachability `FAIL`, not a masked healthy fake.
Provide reachable connstrings (via flags, `GPSTREAM_*` env, or `--config`):

```bash
gpstream doctor --source "host=... user=... dbname=..." --dest "host=... user=... dbname=..."
```

It prints findings with severity (`FAIL` / `WARN` / `INFO`), the side
(source/dest), the name, and a recommendation, then an overall result:

```
overall: PASS (no FAIL)
```

or, if issues exist:

```
overall: N FAIL finding(s)
```

### 9.3 Makefile shortcuts

```bash
make selftest   # build, then gpstream selftest
make doctor     # build, then gpstream doctor
```

---

## 10. Configuration precedence

Config values resolve with precedence **CLI > env (`GPSTREAM_*`) > file
(`--config`) > defaults**. A few defaults (from `internal/config`):

| Setting | Default |
| --- | --- |
| `ConflictAction` | `stop` (the run halts on the first conflict; opt in to `skip`/`overwrite`/`truncate` with `--resolve-conflicts`) |
| `Compression` | `zstd` |
| `ZstdLevel` | `3` (range 1–19) |
| `AutoResume` | `0` (manual only) |
| `Verify` | `sample` |
| `Timeout` | `60s` |
| `LogLevel` | `info` |
| `SkewThreshold` | `0.05` |
| `ConsistentDB` | `true` |
| `IndexPriority` | `size` |
| `SSLMinTLS` | `1.2` |
| `WorkersPerPair` | `2` |

Secrets (SSL cert / key / CA) come from **env or file, never from argv**. The
example configuration is at `packaging/examples/gpstream.toml`; TLS paths in it
are shown commented with mode `0600` for the key file.

---

## 11. Quick checklist

- [ ] Go 1.27 toolchain present.
- [ ] `export GOPROXY=https://proxy.golang.org,direct` set (proxy 403 fallback).
- [ ] `go build -o bin/gpstream ./cmd/gpstream` succeeds; `make cross` builds both archs.
- [ ] `gpstream selftest` prints `selftest: OK`.
- [ ] For a real copy: source and dest clusters are reachable (verify with `gpstream doctor`).
