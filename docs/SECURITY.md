# gpstream — Security Model

This document describes how `gpstream` keeps credentials and secrets out of
command lines and logs, how it encrypts the data path, how it stays
non-destructive by default, and how it verifies integrity. It is the
security reference for v2; see `CONFIGURATION.md` for the full config
reference and `CLI_REFERENCE.md` for every flag.

---

## 1. Threat model

`gpstream` copies data between two Greenplum 6 clusters over a direct
high-speed network. The data plane is a **logical COPY (binary) stream**,
not a file on disk. The security concerns are therefore:

| Concern | gpstream posture |
|---------|------------------|
| Credential leakage (argv, logs, process list) | Secrets come from **env / file, never argv** (§2) |
| Wire eavesdropping / MITM | TLS always enforced, min **1.2** (§3) |
| Overwriting live data by accident | **Non-destructive default** (§4) |
| Corrupted / truncated copy | Checksum + verify (§5) |
| Destructive action without intent | `--yes` guard on cancel/rollback/cleanup (§6) |
| Wrong cluster / wrong target | `security-check` + `doctor` (§7) |

---

## 2. Secrets: environment / file, never argv

This is the single most important rule (v2 **FR-08**). A secret placed on the
command line is visible in the process list, in shell history, and in any
captured log line.

| Secret | Supply via | **Never** via |
|--------|-----------|---------------|
| Source connection string | `--source` or `GPSTREAM_SOURCE` | committed file / shared history |
| Dest connection string | `--dest` or `GPSTREAM_DEST` | argv |
| TLS cert | `--ssl-cert <file>` / `GPSTREAM_SSL_CERT` | argv |
| TLS key | `--ssl-key <file>` / `GPSTREAM_SSL_KEY` | argv |
| TLS CA | `--ssl-ca <file>` / `GPSTREAM_SSL_CA` | argv |
| Min TLS | `--ssl-min-tls <v>` / `GPSTREAM_SSL_MIN_TLS` | — |

In practice, put these in an `EnvironmentFile` for the systemd unit or in a
restricted-env wrapper, and keep the connection strings out of any committed
file or shell rc.

Config precedence is **CLI > env (GPSTREAM_*) > YAML (--config) > defaults**.
Because the CLI layer wins, a secret is *only* a risk when it appears on the
command line; the env and file layers keep it off argv.

---

## 3. Transport encryption (TLS)

`buildTLS` **always** enforces TLS on the connection, with a minimum of
**TLS 1.2** by default.

- `--ssl-cert` / `GPSTREAM_SSL_CERT` — client certificate (file path)
- `--ssl-key`  / `GPSTREAM_SSL_KEY`  — client key (file path)
- `--ssl-ca`   / `GPSTREAM_SSL_CA`   — CA bundle (file path)
- `--ssl-min-tls` / `GPSTREAM_SSL_MIN_TLS` — minimum TLS version (default `1.2`)
- `--tls` — explicitly **assert** the encrypted connection (a no-op if TLS is
  already enforced; useful to make the intent auditable)

A copy over an unencrypted link is refused. There is no
`InsecureSkipVerify` path in the shipped configuration.

---

## 4. Non-destructive by default

`gpstream` never overwrites existing data unless explicitly told to.

| Default | Meaning |
|---------|---------|
| `ConflictAction = SkipExisting` | an existing target is **skipped**, not overwritten |
| `--skip-conflicts` (default **off** = strict) | when off, a conflict is treated strictly |

To change behavior you must opt in:

| Flag | Effect |
|------|--------|
| `--overwrite` | overwrite an existing target |
| `--overwrite-only-conflicts` | overwrite **only** when there is a conflict |
| `--truncate` | truncate before load |
| `--append` | append to the target |
| `--skip-existing` | skip anything that already exists |
| `--conflict-action <a>` | explicit conflict policy |

`--overwrite` is **not** the default; the safe default is `SkipExisting`.

---

## 5. Integrity: checksum + verify

| Knob | Values | Default |
|------|--------|---------|
| `--checksum-algorithm` | `sha256` \| `blake3` | `sha256` |
| `--dedup` | dedupe by checksum / skip-if-same-checksum | off |
| `--verify` | `full` \| `sample` \| `off` | `sample` |

`--verify sample` checks a sample of rows; `full` checks every row; `off`
disables verification. A checksum mismatch surfaces as a frame / transfer
error on the metrics endpoint (see `METRICS.md`: `FrameErrors`, `Errors`).

---

## 6. Destructive commands require `--yes`

The commands that can destroy work require an explicit `--yes`:

| Command | Why it needs `--yes` |
|---------|---------------------|
| `cancel`  | abandons an in-flight job |
| `rollback` | undoes a completed/swap |
| `cleanup`  | removes abandoned data |

Without `--yes` these refuse to run. This is the guard against an
accidental destructive operation.

---

## 7. Pre-flight: `security-check` and `doctor`

```
gpstream security-check
gpstream doctor
```

`security-check` runs the diagnostic report with the configured
`MinTLS` (`--ssl-min-tls`) and prints a severity table:

```
gpstream security-check
=====================
  [FAIL ] source    reach    ...
  [WARN ] dest    collation ...
security: N FAIL finding(s)
```

- A **missing connection string is a reachability FAIL** (not a silent pass).
- **0 up-segments is a FAIL** (not a pass).
- `security-check` returns non-zero if there are any FAIL findings.

Use `security-check` before a copy and after any change to credentials or TLS
material.

---

## 8. Least privilege

- Use the **minimum** privilege connection strings required for the copy.
- Restrict the `--data-port-range` to the ports the copy actually uses.
- Keep the metrics endpoint (`--metrics-addr`) on a private interface; it
  exposes counters but is not an admin API.
- Do not run a copy as a more-privileged user than the data requires.

---

## 9. Security checklist

- [ ] Secrets via env / EnvironmentFile, **not** argv (FR-08).
- [ ] TLS enforced; `--ssl-min-tls >= 1.2`.
- [ ] `security-check` returns **PASS** against the real clusters.
- [ ] `ConflictAction = SkipExisting` (non-destructive) unless a specific
      copy intentionally opts into overwrite.
- [ ] `--verify` is `sample` or `full` for critical copies.
- [ ] `--checksum-algorithm` set (`sha256` or `blake3`).
- [ ] Destructive commands invoked only with `--yes`, deliberately.
- [ ] Metrics endpoint on a private interface.
- [ ] Logs redacted of connection strings / cert material.

---

## 10. Read more

- `CONFIGURATION.md` — config fields, defaults, precedence, validation.
- `ADMIN_GUIDE.md` — deployment, systemd, logrotate, upgrade.
- `METRICS.md` — the counters (including `Errors`/`FrameErrors`).
- `TROUBLESHOOTING.md` — diagnosing a failed `security-check`/`doctor`.
