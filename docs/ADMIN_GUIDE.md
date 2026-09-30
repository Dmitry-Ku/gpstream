# gpstream — Administrator Guide

This guide is for the person who deploys, configures, and maintains `gpstream`
on the boxes that run the copy. It covers deployment, Linux 6.1+ compatibility,
the systemd service, log rotation, the security model, and the upgrade path.

> Scope: administration and deployment. For the day-to-day operator, see
> `OPERATOR_GUIDE.md`. For every flag, see `CLI_REFERENCE.md`. For the config
> reference, see `CONFIGURATION.md`.

---

## 1. What an administrator owns

| Concern | Owner |
|---------|-------|
| Install / package / upgrade | Admin (this guide) |
| systemd unit, logrotate, restart policy | Admin |
| Credentials, TLS material, connection strings | Admin |
| Resource layout (ports, workers, rate-limit) | Admin |
| Running a copy, watching progress | Operator (`OPERATOR_GUIDE.md`) |
| Recovering from a crash | Operator + Admin |

`gpstream` is a single Go binary plus a small set of config/env inputs. It has
**no daemon of its own** — a copy runs as a foreground process and exits when
the job finishes. The systemd unit is an optional *invocation* wrapper, not a
service that lives forever.

---

## 2. Prerequisites

- **Go 1.27** to build from source.
- **Linux 6.1+ kernel** (the v2 target; glibc-aware). See §4 for the
  compatibility matrix.
- Module proxy note: `proxy.golang.org` intermittently 403s on
  `github.com/klauspost/compress`, so the build environment sets
  `GOPROXY=https://proxy.golang.org,direct`.
- A **live Greenplum 6 cluster is required for `copy`**, but **not** for
  `--help`, `dry-run`, `plan`, or `doctor` against a built-in fake connection.
  (See `INSTALL.md`.)

---

## 3. Install

Three supported routes:

### 3.1 Build from source
```
go build -o bin/gpstream ./cmd/gpstream
```

### 3.2 Cross-compile (amd64 / arm64)
```
make cross      # both
make amd64      # or individually
make arm64
```
The Makefile targets: `all build cross amd64 arm64 test cover vet fmt
doctor selftest clean`.

### 3.3 Linux package
`packaging/` ships `deb/`, `rpm/`, `systemd/`, and `logrotate.conf`. Install
the deb/rpm, then enable the systemd unit (optional; see §5).

Verify after install:
```
gpstream selftest   # internal consistency checks
gpstream doctor     # reachability + segment + glibc checks
```
`doctor` is **strict**: a missing connection string is a reachability **FAIL**,
and 0 up-segments is a **FAIL** (not a pass).

---

## 4. Linux 6.1+ compatibility

`gpstream` is a pure logical copy over `COPY` (binary) and is **insensitive to
OS / kernel / glibc differences** between source and destination — the streams
never touch the filesystem as data. Compatibility is checked by
`compatibility-check` and surfaced in `dry-run`:

| Check | Flag | What it looks at |
|-------|------|------------------|
| Collation / locale | `--locale-check` | `LC_COLLATE` / locale / encoding differences (warn, F-16) |
| Distribution / skew | (built into `dry-run`) | raw segment skew + layout verdict, always reported (F-15/CR-5) |

A **distribution** difference may require redistribution on the destination.
`gpstream` reports the raw segment skew and the layout verdict in the
`dry-run` (there is no gate flag). The `planner` includes the measured skew in
the deterministic plan hash, so a plan that must redistribute is
distinguishable from one that does not.

> Honesty note: the physical-scale target (100 TB–1 PB over a 100 Gbps link)
> and the fault-injection behaviors are validated on a **live stand**. The
> fake connection used by tests has no data plane, so `copy` never falls
> back to it.

---

## 5. systemd service

Reference unit: `packaging/systemd/gpstream.service`. It is an
**oneshot-style** invocation (a copy is a bounded job), not a long-lived
daemon. Typical layout:

- `ExecStart=/usr/bin/gpstream run --source ... --dest ...`
- Restart policy is usually **no** (or `on-failure` with an attempt cap),
  because the correct recovery mechanism is `resume`, not a silent restart
  (see `FAILOVER.md`).
- Pass secrets via the environment file / EnvironmentFile, **never** in
  `ExecStart` (see §7).

Enable:
```
systemctl enable --now gpstream
systemctl status gpstream
```

---

## 6. Log rotation

Reference: `packaging/logrotate.conf`. Rotate the job log / mirror journal
to keep them bounded on long-running copies.

---

## 7. Security model (admin)

The administrator's job is to keep **secrets out of argv and out of logs**.

| Secret | How it is supplied | Never via |
|--------|--------------------|-----------|
| Connection string (source/dest) | `--source`/`--dest` or the `source`/`dest` config keys (no `GPSTREAM_SOURCE`/`DEST` env) | argv in a committed file |
| TLS cert | `--ssl-cert` / `GPSTREAM_SSL_CERT` (file path) | argv |
| TLS key | `--ssl-key` / `GPSTREAM_SSL_KEY` | argv |
| TLS CA | `--ssl-ca` / `GPSTREAM_SSL_CA` | argv |
| Min TLS version | `--ssl-min-tls` / `GPSTREAM_SSL_MIN_TLS` (default `1.2`) | — |

The pg data plane is encrypted by default (`sslmode=verify-full`, minimum
**TLS 1.2** unless `--ssl-min-tls` is lowered). See `SECURITY.md` for the
full model.

Precedence for every setting: **CLI > env (GPSTREAM_*) > YAML (--config) >
defaults**.

---

## 8. Resource layout

| Knob | Flag / env | Default | Notes |
|------|-----------|---------|-------|
| Rate limit | `--rate-limit <Gbps>` (`GPSTREAM_RATE_LIMIT`) | 0 (no ceiling) | adaptive AIMD is always active (F-14) |
| Workers | `--workers N` (`GPSTREAM_WORKERS`) | 2 | parallel workers per host-pair |
| Compression | `--compress` (`GPSTREAM_COMPRESS`) | `zstd` | `none`/`zlib`/`zstd[:level]` |
| zstd level | `--zstd-level N` (or `--compress zstd:N`) | 3 | 1–19 |
| Metrics | `--metrics-addr <A>` | off | Prometheus `/metrics` served during `run` |

---

## 9. Upgrade path

There is **no `upgrade-check` or `package-info` subcommand**. Check the
installed version with `gpstream version`, and upgrade the RPM/deb package
with the normal package manager:

```
gpstream version
rpm -q gpstream            # or: dpkg -s gpstream
```

Before upgrading a running deployment:

1. Confirm `dry-run` still produces a sensible plan on the new version.
2. Confirm the `--verify` behavior is unchanged.
3. If a prior copy is mid-flight, prefer `resume` over a restart (see
   `FAILOVER.md`); a unit in terminal success state
   (`StateSwapped`) is **skipped** on resume.

---

## 10. Admin checklist

- [ ] Built or packaged for the target arch (`amd64`/`arm64`).
- [ ] `gpstream doctor` returns **PASS** against the real clusters.
- [ ] `compatibility-check` (collation/locale/distribution) clean or
      explained.
- [ ] Secrets supplied via env / EnvironmentFile, **not** argv.
- [ ] TLS enabled; `--ssl-min-tls >= 1.2`.
- [ ] logrotate installed; log level sane.
- [ ] `gpstream version` checked before/after an upgrade.
- [ ] Metrics endpoint reachable if used (see `METRICS.md`).

---

## 11. Read more

- `OPERATOR_GUIDE.md` — running a copy day-to-day.
- `SECURITY.md` — the security model in depth.
- `CLI_REFERENCE.md` — every command and flag.
- `CONFIGURATION.md` — config fields, defaults, precedence, validation.
- `METRICS.md` — the Prometheus endpoint and counters.
- `FAILOVER.md` — recovery from a crash.
