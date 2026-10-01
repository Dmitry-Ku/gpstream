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
| Wrong cluster / wrong target | `doctor` (§7) |
| Undocumented operator action | **Audit log** (`audit.log`, §10) |

---

## 2. Secrets: environment / file, never argv

This is the single most important rule (v2 **FR-08**). A secret placed on the
command line is visible in the process list, in shell history, and in any
captured log line.

| Secret | Supply via | **Never** via |
|--------|-----------|---------------|
| Source connection string | `--source <connstring>` flag (or the config file) | argv beyond the flag / committed file / shared history |
| Dest connection string | `--dest <connstring>` flag (or the config file) | argv beyond the flag |
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
The pg data plane is **encrypted by default** (`sslmode=verify-full` is
enforced unless the connstring sets an explicit `sslmode`); there is no bare
`--tls` flag to assert it — the connection string carries the mode.

A copy over an unencrypted link is refused. There is no
`InsecureSkipVerify` path in the shipped configuration.

### Server identity: `verify-full` (hostname check)

PostgreSQL connections are made with `sslmode=verify-full` unless the DSN
explicitly sets another mode (`ensureVerifyFull`). With `verify-full`, the
server's certificate is validated **and** its hostname is checked against the
connection host: `buildTLS` sets the TLS `ServerName` from the DSN host, so a
certificate that does not cover the host is a hard, diagnosed failure
(`x509: certificate is valid for X, not <host>`) — the F-13 acceptance
"on a wrong certificate — a diagnosed refusal". An IP-literal host has no
verifiable name and degrades to chain verification (verify-ca). A
`--ssl-ca` bundle is loaded into the root store for chain validation; a
missing or certificate-less bundle (or a malformed client keypair) is a
**hard error at connect time** — gpstream never silently downgrades its TLS
guarantee.

---

## 4. Non-destructive by default

`gpstream` never overwrites existing data unless explicitly told to.

| Default | Meaning |
|---------|---------|
| `ConflictAction = stop` | an existing target **stops the run** — nothing overwritten, nothing skipped |

To change behavior you opt in with a single flag:

| Flag | Effect |
|------|--------|
| `--resolve-conflicts stop` | **default** — a conflict stops the run |
| `--resolve-conflicts skip` | skip conflicting objects (reported in `conflicts.jsonl`) |
| `--resolve-conflicts overwrite` | replace the target (staging + atomic swap, verified) |
| `--resolve-conflicts truncate` | truncate then reload (in-place) |
| `--overwrite` / `--truncate` | shorthand opt-ins for the matching policy |

`--overwrite` / `--truncate` are **not** the default; the safe default is
`stop`.

---

## 5. Integrity: checksum + verify

| Knob | Values | Default |
|------|--------|---------|
| `--verify` | `on` \| `off` | `off` |

With **`on`**, after each unit lands its content checksum is re-derived on both
sides (the source read shares the data COPY's snapshot) and compared; a mismatch
sets the unit to `failed` and drops the staging table — no unverified data is
ever swapped in. The default is **`off`** (no content verification; the COPY
protocol's row count is the landed-data check), so a default run reads each
source table exactly once — there is no pre-copy source/destination digest
comparison.

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

## 7. Pre-flight: `doctor`

There is **no separate `security-check` subcommand** — TLS findings are part
of `doctor` (the `tls` check), which prints a severity table:

```
gpstream doctor
===============
  [PASS] src->dst    route        route-mode: src=2 dst=2 segments ...
  [WARN] source      on_segment   segment-direct capability degraded: ...
  [PASS] network     tls          min TLS 1.2 required (full cert validation in M2)
overall: PASS (no FAIL)
```

- A **missing connection string is a reachability FAIL** (not a silent pass).
- **0 up-segments is a FAIL** (not a pass).
- `doctor` returns non-zero if there are any FAIL findings.

Use `doctor` before a copy and after any change to credentials or TLS
material.

---

## 8. Least privilege

- Use the **minimum** privilege connection strings required for the copy.
- Keep `--workers` bounded so the copy cannot saturate the shared NIC.
- Keep the metrics endpoint (`--metrics-addr`) on a private interface; it
  exposes counters but is not an admin API.
- Do not run a copy as a more-privileged user than the data requires.

---

## 9. Security checklist

- [ ] Secrets via env / EnvironmentFile, **not** argv (FR-08).
- [ ] TLS enforced; `--ssl-min-tls >= 1.2`.
- [ ] Server certificate covers the DSN host (`verify-full` hostname check).
- [ ] `doctor` returns **PASS** against the real clusters.
- [ ] `ConflictAction = stop` (non-destructive) unless a specific copy
      intentionally opts into `overwrite`/`truncate` via `--resolve-conflicts`.
- [ ] `--verify on` for critical copies.
- [ ] Destructive commands invoked only with `--yes`, deliberately.
- [ ] `audit.log` retained / reviewed per the operation's retention policy.
- [ ] Metrics endpoint on a private interface.
- [ ] Logs redacted of connection strings / cert material.

---

## 10. Audit log of actions (`audit.log`)

Every operator action on an operation is recorded in
`<state-dir>/<operation-id>/audit.log` (mode **0600**, alongside
`journal.jsonl` / `events.jsonl` / `conflicts.jsonl`) — one JSON object per
line: `{"ts", "action", ...fields}` (F-13 "audit log of actions").

| action | When | Fields |
|--------|------|--------|
| `run` | `gpstream run` starts (before any copy) | operation-id, units, copy-mode, on-unit-failure, compression, consistency mode |
| `run-finished` | `gpstream run` ends | units, rows, skipped, failed, error (if any) |
| `resume` | `gpstream resume` | operation-id, requeued, attempt (or error) |
| `cleanup` | `gpstream cleanup` (logged **before** the state dir is removed) | operation-id, mode, units |

`audit.log` records the operator's actions; `events.jsonl` records what the
run itself did (rate shifts, conflicts, failures — see OPERATOR_GUIDE §8).
Consume with `jq -c . audit.log`. The file is best-effort: a write failure
never fails the action.

---

## 11. Read more

- `CONFIGURATION.md` — config fields, defaults, precedence, validation.
- `ADMIN_GUIDE.md` — deployment, systemd, logrotate, upgrade.
- `METRICS.md` — the counters (including `Errors`/`FrameErrors`).
- `TROUBLESHOOTING.md` — diagnosing a failed `doctor` run.
