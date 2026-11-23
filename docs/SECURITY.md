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
| Wire eavesdropping / MITM | PG legs `verify-full` by default; agent data-socket TLS opt-in; min **1.2** (§3) |
| ~~**Controller host as data trust root**~~ | Removed in P7: the two-agent transport is the only data path, the data stream never transits the controller (ADR-002 closed) |
| Overwriting live data by accident | **Non-destructive default** (§4) |
| Corrupted / truncated copy | Checksum + verify (§5) |
| Destructive action without intent | `--yes` guard on `cleanup` (§6) |
| Wrong cluster / wrong target | `dry-run` (§7) |
| Undocumented operator action | **Audit log** (`audit.log`, §10) |
| Malicious / compromised peer (DoS, redirect) | **Peer-input bounds + frame CRC + fuzzing** (§12) |

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
| Min TLS | `--ssl-min-tls <v>` / file key `ssl_min_tls` (no env variant) | — |

In practice, put these in a restricted-env wrapper (your cron / process
manager / init unit of choice) and keep the connection strings out of any
committed file or shell rc.

Config precedence is **CLI > env (GPSTREAM_*) > YAML (--config) > defaults**.
Because the CLI layer wins, a secret is *only* a risk when it appears on the
command line; the env and file layers keep it off argv.

---

## 3. Transport encryption (TLS)

Encryption has **three planes with different defaults**:

- **PG legs (coordinator ↔ clusters):** encrypted **by default** —
  `sslmode=verify-full` is applied when the DSN sets no explicit `sslmode`
  (`ensureVerifyFull`). An explicit `sslmode` in the DSN (any value,
  including `disable`) is respected **as-is** — gpstream does not override
  a deliberately plaintext PG connection.
- **Agent data socket (source agent ↔ dest agent):** **plaintext by
  default**. TLS is opt-in and keyed on the serving keypair
  (`--seg-tls-cert` / `--seg-tls-key`, env `GPSTREAM_SEG_TLS_CERT` /
  `GPSTREAM_SEG_TLS_KEY`); `--seg-tls-ca` / `GPSTREAM_SEG_TLS_CA`
  additionally makes the hop mTLS (a client certificate is required).
- **Control channel (controller ↔ agent control sockets):** **plaintext by
  default**. Opt-in via `--control-tls-cert` / `--control-tls-key` /
  `--control-tls-ca` (env `GPSTREAM_CONTROL_TLS_CERT` / `GPSTREAM_CONTROL_TLS_KEY`
  / `GPSTREAM_CONTROL_TLS_CA`): the agents serve their control sockets with their
  own certificate, and the controller verifies against `--control-tls-ca` while
  presenting its `--control-tls-cert`/`--control-tls-key`.

PG-plane TLS settings:

- `--ssl-cert` / `GPSTREAM_SSL_CERT` — client certificate (file path)
- `--ssl-key`  / `GPSTREAM_SSL_KEY`  — client key (file path)
- `--ssl-ca`   / `GPSTREAM_SSL_CA`   — CA bundle (file path)
- `--ssl-min-tls` — minimum TLS version (default `1.2`; flag only, no env
  variant)

There is no bare `--tls` flag to assert encryption — the connection string
carries the PG-plane mode. There is no `InsecureSkipVerify` path in the
shipped configuration.

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

### Controller host as a data trust root (ADR-002)

The two-agent transport is the only data path: the data stream runs source
agent → dest agent over a real socket and the controller only holds
control-plane (catalog) connections. The socket is **plaintext by default** —
enable TLS for it with the `--seg-tls-*` trio above (§3). The controller
host then needs to be trusted for the *plan* (table names, DDL, state) but
not for the data; the data trust roots become the two agent hosts (one per
cluster), which are the hosts that run the COPYs.

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
| `--resolve-conflicts truncate` | **unsupported in the two-agent topology** — accepted by the CLI, but rejected at runtime (every unit fails); see `TROUBLESHOOTING.md` §4 |
| `--overwrite` | shorthand opt-in for `--resolve-conflicts overwrite` |
| `--truncate` | accepted (it sets the truncate policy) but the policy is rejected at runtime as above |

`--overwrite` / `--truncate` are **not** the default; the safe default is
`stop`.

---

## 5. Integrity: checksum + verify

| Knob | Values | Default |
|------|--------|---------|
| `--verify` | `on` \| `off` | `on` |

With **`on`** (the default), after each unit lands its content checksum is
re-derived on both sides (the source read shares the data COPY's snapshot)
and compared; a mismatch sets the unit to `failed` and drops the staging
table — no unverified data is ever swapped in. The digest is computed inline
on the data path (no re-read), so a default run still reads each source
table exactly once. `--verify off` disables the swap gate (the COPY
protocol's row count becomes the landed-data check).

---

## 6. Destructive commands require `--yes`

There is exactly one destructive command, and it requires an explicit
`--yes`:

| Command | Why it needs `--yes` |
|---------|---------------------|
| `cleanup`  | removes the operation's state (and, with `--mode drop`, destination objects) |

There is **no `cancel` subcommand** (stop a run by killing the process —
see `FAILOVER.md`) and **no `rollback` subcommand** (recovery is `resume`
plus `cleanup --mode drop`). Without `--yes`, `cleanup` refuses to run.
This is the guard against an accidental destructive operation.

---

## 7. Pre-flight: `dry-run` (self-diagnostic)

There is **no separate `security-check` subcommand** — TLS findings are part
of the `dry-run` self-diagnostic (the `tls` check), which prints a severity
table:

```
gpstream dry-run — diagnostics
=============================
  [PASS] src->dst    route        route-mode: src=2 dst=2 segments ...
  [WARN] source      on_segment   segment-direct capability degraded: ...
  [WARN] network     tls          min TLS 1.2 required (full cert validation in M2)
overall: PASS (no FAIL)
```

- A **missing connection string is a reachability FAIL** (not a silent pass).
- **0 up-segments is a FAIL** (not a pass).
- `dry-run` returns non-zero if there are any FAIL findings.

Use `dry-run` before a copy and after any change to credentials or TLS
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
- [ ] PG legs: `sslmode=verify-full` (default) or an explicit DSN mode; `--ssl-min-tls >= 1.2`; agent data socket: `--seg-tls-*` set when the link is untrusted.
- [ ] Server certificate covers the DSN host (`verify-full` hostname check).
- [ ] `dry-run` returns **PASS** against the real clusters.
- [ ] `ConflictAction = stop` (non-destructive) unless a specific copy
      intentionally opts into `overwrite` via `--resolve-conflicts`.
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
| `run` | `gpstream run` starts (before any copy) | operation-id, units, copy-mode, on-unit-failure, compression |
| `run-finished` | `gpstream run` ends | units, rows, skipped, failed, error (if any) |
| `resume` | `gpstream resume` | operation-id, requeued, attempt (or error) |
| `cleanup` | `gpstream cleanup` (logged **before** the state dir is removed) | operation-id, mode, units, dropped |

`audit.log` records the operator's actions; `events.jsonl` records what the
run itself did (rate shifts, conflicts, failures — see OPERATOR_GUIDE §8).
Consume with `jq -c . audit.log`. The file is best-effort: a write failure
never fails the action.

---

## 11. Read more

- `CONFIGURATION.md` — config fields, defaults, precedence, validation.
- `ADMIN_GUIDE.md` — deployment, logrotate, upgrade.
- `METRICS.md` — the counters (including `Errors`/`FrameErrors`).
- `TROUBLESHOOTING.md` — diagnosing a failed `dry-run` run.

---

## 12. Peer-input bounds and wire trust (DoS)

Every byte from a peer agent (data or control channel) is **attacker-shaped
input** — a compromised or malicious peer must not be able to crash, exhaust,
or redirect the agent. Three layers enforce that.

### 12.1 Frame size bound (data channel)

- `transfer.MaxFramePayload` = **1 MiB** (4x the 256 KiB production chunk
  size) bounds a single frame's payload.
- It is enforced in `checkFrameLen` (every `Decode`/`decodeV1`/`decodeV2`
  path) **and** in `ReadFrame` *before* the payload buffer is allocated.
- The frame length field is peer-controlled: without the bound, one forged
  header claiming a multi-giB payload made `ReadFrame` allocate ~4 GiB
  before the short read could fail — an OOM vector against the dest agent.
  Found by fuzzing (`FuzzReadFrame`, 2026-11); rejected with an error, no
  allocation, when a frame exceeds the cap.
- A legitimate frame is at most 256 KiB of rows (`segstream chunkSize`), so
  the 1 MiB cap leaves 4x headroom; a frame above the cap is malformed and
  refused.

### 12.2 Control-line size bound (control / peer channel)

- Every newline-delimited control and peer message is read through
  `readCappedLine` (cap `maxControlLine` = **1 MiB**, ~100x the largest
  legitimate onsegment request).
- `bufio.ReadBytes('\n')` grows to the full line length, so a peer streaming
  bytes without a newline could exhaust the agent's memory. `readCappedLine`
  bounds the accumulation: a line longer than the cap is rejected with an
  error (the peer is rejected, not absorbed), and in-bounds lines, a
  truncated final line at EOF, and a clean EOF keep the original semantics.
- Applies to all six receiver sites (control request loops, control/peer
  response readLoops, destination serve loop, source serve loop).

### 12.3 Control channel is the trust root

- The agent **executes what the control peer asks**: the onsegment dispatch
  runs `COPY ... TO PROGRAM` on the source PostgreSQL, and the request's
  `dest` field is the listen address the segreceivers dial — i.e. the
  control peer decides where the table data goes. There is **no
  per-message authentication** on the control channel of its own.
- **Default (lab layout):** plain TCP, no authentication. Acceptable only
  when the control port is reachable exclusively by the trusted controller
  (localhost bind, or a network segment the controller alone occupies).
- **Production / untrusted networks:** enable control-channel TLS with mTLS
  (agent `--control-tls-ca` requires and verifies the controller's client
  certificate; controller `--control-tls-cert/--control-tls-key`). With mTLS,
  merely reaching the control port is not enough to drive an agent.
- **Do not expose the control port to untrusted networks without mTLS.** A
  peer on the control channel can redirect an in-flight copy's destination
  (data exfiltration to an attacker-listener) and start onsegment dispatches.

### 12.4 Wire integrity and fuzzing

- **Frame CRC:** every data frame carries a 4-byte FNV-1a CRC over
  header+payload; a corrupted or truncated frame is rejected (`ErrCorrupt` /
  truncation error), never partially applied.
- **Fuzz targets** (native `testing.F`, go-fuzz compatible) cover the
  attacker-facing parse surfaces — `FuzzFrameDecode`, `FuzzReadFrame`,
  `FuzzFrameRoundTrip`, `FuzzHelloID` (internal/transfer) and
  `FuzzParseDestAddrs` (internal/transfer/segstream). The CI `fuzz` job runs
  60s per target on every push; a found failure persists as a corpus entry in
  `testdata/<target>/crashers`.
- **SSH probes** use `StrictHostKeyChecking=no` for compatibility with the
  lab controllers' OpenSSH 7.4 (predates `accept-new`). Key-based auth +
  `BatchMode=yes` limits the exposure (a MITM can forge a probe's *output*
  — e.g. a version string — but not inject commands or steal keys).
  Production hardening: pre-populate `known_hosts` and switch to
  `StrictHostKeyChecking=yes`.

### 12.5 Credentials and files (recap of the bounds that matter here)

- DSNs with passwords live only in the operator's config and in-memory agent
  state (see §2). The written `plan.json` deliberately carries **no
  credentials** (host:port pairs only).
- Manifest/journal files are created `0600`; operational status/stats files
  carry no secrets. No hardcoded secrets in the code base; protocol
  randomness uses `crypto/rand` (no `math/rand` in the module).
