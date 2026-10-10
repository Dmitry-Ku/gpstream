# gpstream — CLI Reference

This is the complete command and flag reference for **gpstream**, an open-source
Go tool that performs high-throughput **LOGICAL** data copy between two
Greenplum 6 clusters over a direct, high-speed network. gpstream streams
table data with `COPY (binary)` directly from source to destination with **no
intermediate storage** — it is *not* `gpbackup`, `gprestore`, or `gpcopy`.

Every subcommand that consumes the full flag contract is bound by a single
shared parser (`parseConfig`) so a flag is never silently ignored. The resolved
configuration is the single source of truth consumed by every component.

## Configuration precedence

A single `Config` is assembled from four layers, applied in this order
(lowest precedence first, highest last):

```
defaults  <  --config file.toml|file.yaml  <  GPSTREAM_* env  <  CLI flags
```

So **CLI > env > file > defaults**. Concretely, `parseConfig` does:

1. Start from `config.Default()` (the specification defaults).
2. `LoadFile(--config)` — overlays file values onto the defaults (TOML or
   YAML, selected by extension).
3. `ApplyEnv()` — overlays `GPSTREAM_*` environment variables.
4. Second pass over `os.Args` — **CLI flags win** and are applied last.

See [CONFIGURATION.md](CONFIGURATION.md) for the full field/precedence/env
reference and the TOML/YAML key table.

> **Endpoints:** `source` and `dest` come from the `--source`/`--dest` flags
> or the `source`/`dest` keys in the config file. There is **no**
> `GPSTREAM_SOURCE`/`GPSTREAM_DEST` environment variable.

---

## Commands (9 dispatch entries)

The dispatcher (`run`) accepts the following subcommands. `copy`/`plan` from
older drafts are **not** separate commands: **`dry-run`** performs the
planning (without executing) and **`run`** performs the copy (with executing).
There is no `cancel`, `rollback`, `metrics`, `logs`, `indexes`,
`compatibility-check`, `security-check`, `package-info`, or `upgrade-check`
command — the capability they described is folded into `run`/`dry-run`/
`status`/`resume` (see per-command notes). **`man` is not a subcommand** —
`man gpstream` renders the installed OS man page (`man/gpstream.8`), so it is
not counted here.

| # | Command | Class | Consumes full contract? |
|---|---------|-------|-------------------------|
| 1 | `run` | core (copy) | yes (`parseConfig`) |
| 2 | `dry-run` | core (diagnose + plan) | yes |
| 3 | `bench` | core (measure) | yes |
| 4 | `status` | state | no (bespoke) |
| 5 | `resume` | state | no (bespoke) |
| 6 | `cleanup` | state | no (bespoke) |
| 7 | `version` | utility | no |
| 8 | `help` / `-h` / `--help` | utility | no |

The commands that go through `parseConfig` (honouring **every** flag) are:
**`run`, `dry-run`, `bench`**. The state commands parse only a small
subset of flags (listed per-command below) and ignore the rest.

---

## Command reference

- **`run`** — streams selected objects between clusters (F-01/02/03/07/17);
  full contract. Opens **live** src/dest connections (pgxpool-backed, one
  connection per worker — a single `pgx.Conn` is not safe for concurrent
  COPY, F-03), builds the plan (bounded by `--timeout`), runs the transfer
  engine (src → stream → dest, no intermediate storage), verifies each unit,
  rebuilds indexes in the background (F-17), atomically swaps the staging
  table to its final name, records every transition to
  `<state-dir>/<operation-id>/`, appends the diagnostic event stream to
  `<state-dir>/<operation-id>/events.jsonl` (F-04/F-14), records the action to
  `<state-dir>/<operation-id>/audit.log` (F-13 audit log of actions: `run` +
  `run-finished`), serves Prometheus metrics at `--metrics-addr` (F-04) and
  honours `--auto-resume N` (F-07). On a Greenplum destination it also
  measures the live segment-fill balance of every copied table (max/avg over
  `gp_segment_id`, F-15) and prints it in the final report, warning when a
  table exceeds the fixed F-15 segment-fill gate (1.5). `run` is the command that used
  to be called `copy`.
- **`dry-run`** — **self-diagnoses src/dest clusters (F-05), then plans
  without executing** (F-10); full contract. Runs the full capability
  matrix (reachability, version, segments, route mode, `on_segment`
  transport, disk, permissions, collation, TLS, `max_connections` sizing,
  `--max-snapshot-delta`) over a live connection that **never** fabricates a
  healthy fake for a missing/unreachable cluster (CR-7); a missing connstring
  is a reachability FAIL, not a silent PASS. **Any FAIL aborts with the
  report (no plan is built).** On success it renders the deterministic plan
  summary (units, volume, conflicts, redistribution, plan hash); on
  `--timeout` prints the collected fragment as a prediction/forecast. The
  plan is **printed, not persisted** (the dry-run holds no state). `run`
  always persists the plan JSON to `<state-dir>/<operation-id>/plan.json`.
- **`bench`** — measures the **input stream** (F-14): streams the selected
  source tables through the real data path and **discards** the payload
  instead of writing a destination. The copy mode is honored
  (`--copy-mode coordinator|mesh`): **coordinator** streams one
  `COPY ... TO STDOUT` from the source coordinator; **mesh** runs the real
  `COPY ... TO PROGRAM (gpstream-segreceiver ...) ON SEGMENT`, so the GP
  kernel launches a segreceiver on every primary source segment and each
  streams its slice. The payload ends in a **drain sink on the controller**
  (frame v2 + compression + frame-ack + data TLS per the flags, so the
  transport overhead is in the measurement). Source only (`--dest`, if
  given, is ignored); all selection/performance options as for `run`
  (`--rate-limit` is reported but does not throttle the benchmark). Prints
  per-table and total rows, wire and logical bytes, elapsed and MB/s.
  **Metric source per mode:** coordinator rows/wire/logical come from the
  source agent's counters (one stream to the drain); mesh rows come from the
  GP kernel's `ON SEGMENT` row count (cross-checked against the per-slice
  trailer sum, a mismatch aborts the unit) and wire/logical are counted by
  the drain (the source agent cannot see the per-segment streams). The drain
  binds one listener per source content in mesh and serves the slices
  concurrently (mirroring the production per-segment mini-agent endpoints); a
  remote source agent dials the controller's primary non-loopback IPv4 + a
  free port, a local source agent (single-host layout) uses loopback.
  `--seg-tls-cert/--seg-tls-key` turn the drain into a TLS server (the
  certificate must be valid for the drain address the segreceivers dial).
  `--timeout S` is the benchmark wall-time limit (unlimited when not
  passed).
- **`status`** — reports an operation's state (F-02/06/07) from the durable
  manifest (operation-id, snap-epoch, per-unit id/state/kind/
  db.schema.table/bytes/epoch). **Flags:** `--operation-id <id|latest>`
  (required), `--state-dir <dir>`.
- **`resume`** — resumes an interrupted operation (F-06/07). Reopens the
  durable operation manifest (which survived the crash) and re-queues every
  non-terminal unit under the next snapshot epoch; the action is recorded to
  `audit.log` (F-13). **Flags:** `--operation-id <id|latest>` (required),
  `--state-dir <dir>`, `--auto-resume <N>` (cap on resume attempts),
  the re-read rule keeps a table only if it was verified under one snapshot epoch.
- **`cleanup`** — removes an operation's state directory (v4 §8 "State").
  `--all-incomplete` removes every operation directory that still has
  non-terminal units (no `--operation-id` needed; `<id>` also accepts
  `latest`); `--mode drop` ADDITIONALLY
  drops the destination-side objects the operation created — surviving staging
  tables, renamed-in or gpstream-created final tables, and renamed-in schemas —
  derived conservatively from the manifest (a pre-existing, conflicted
  destination table is never touched), restoring the receiver to its
  pre-operation state (F-02). `--mode drop` requires `--dest <connstring>`
  (`--ssl-ca` is honoured) and runs the drop on the destination coordinator
  through the **dest agent** — `--dest-agent-ssh user@host[:port]` names that
  host (omitted = local), `--dest-agent-binary` overrides the agent path
  (default: convention resolution); the controller opens no direct
  destination connection. Each removal is recorded to `audit.log` **before**
  the state dir is removed (F-13). **Flags:** `--operation-id <id>` (or
  `--all-incomplete`), `--state-dir <dir>`, `--mode drop`, `--dest
  <connstring>`, `--ssl-ca`, `--ssl-cert`, `--ssl-key`, `--ssl-min-tls`,
  `--dest-agent-ssh`, `--dest-agent-binary`.
- **`version`** — prints the gpstream build version. No flags.
- **`help`** / `-h` / `--help` — prints built-in help (commands + flags).
  The man page is installed by the package (`man gpstream`).

> **Note on removed v2 commands.** Earlier design drafts listed `cancel`,
> `rollback`, `metrics`, `logs`, `indexes`, `compatibility-check`,
> `security-check`, `package-info` and `upgrade-check`. None of these are
> wired into the dispatcher. Cancellation is operator-side (kill the process,
> then `resume`); rollback is `resume` (re-copy non-terminal units); live
> metrics are served over HTTP at `--metrics-addr` during `run`; the operation
> log is what `status` prints; index rebuilds happen inside `run` (F-17);
> compatibility/security posture is what `dry-run` reports; and `version`
> covers package/version info.

---

## Full flag surface (shared contract)

The following flags are bound by `parseConfig` and honoured by
`run`, `dry-run`, and `bench`. Values fold into the `Config` struct; the
defaults below are the exact ones from `config.Default()` and the flag
bindings in `cmd/gpstream/main.go`.

### Connection
| Flag | Config field | Default |
|------|--------------|---------|
| `--source` | `Source` | `""` (source connstring; may hold secrets but **never** via argv in production, F-08) |
| `--dest` | `Dest` | `""` (destination connstring) |

### Selection (v4 §8)
| Flag | Config field | Default |
|------|--------------|---------|
| `--dbs <csv>` | `DBs` | `[]` (comma-separated) |
| `--schemas <csv>` | `Schemas` | `[]` (comma-separated) |
| `--tables <csv>` | `Tables` | `[]` (comma-separated) |
| `--exclude <csv>` | `Exclude` | `[]` (any db/schema/table, comma-separated) |

All are **comma-separated** lists. With none set, the full database is
selected.

### Object classes (F-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--metadata-only` | `MetadataOnly` | `false` (structure only: DDL + sequences, no data; idempotent) |
| `--data-only` | `DataOnly` | `false` (data only, into the existing structure: rows replaced — TRUNCATE + load; the structure must already exist) |

With neither flag both classes are copied. The two are mutually exclusive
(Validate). There is no `--include` flag: it duplicated the two shorthands
and the default, and was removed.

### Rename (v4 §8 canonical + F-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--map-db src=dst` | `RenameDB` | `{}` (map src→dst; split on first `=`) |
| `--map-schema src=dst` | `RenameSchema` | `{}` |
| `--map-table src=dst` | `RenameTable` | `{}` |

### Conflict / safety (F-10)
| Flag | Config field | Default |
|------|--------------|---------|
| `--resolve-conflicts stop\|skip\|overwrite\|truncate` | `ConflictAction` | `stop` (the run stops on the first collision and the operator decides) |
| `--overwrite` | `ConflictAction` | — (shorthand: sets `ConflictAction=overwrite`) |
| `--truncate` | `ConflictAction` | — (shorthand: sets `ConflictAction=truncate`) |

Conflict policy semantics:

- **`stop`** (default) — the run stops on the first conflict; nothing is
  clobbered, nothing is silently skipped.
- **`skip`** — the conflicting unit is skipped and the run continues.
- **`overwrite`** — copy to a staging table and swap over the final. Always
  rewrites: an existing destination is re-copied even when its data already
  matches the source (no pre-copy content comparison — that would read the
  whole table twice; the only content check is the `--verify on` digest (inline
  on the data path, no re-read).
  If the destination table is referenced by foreign keys on
  other destination tables (a prior run's restored FK set, or an operator
  table), the dependent FK constraints are dropped before the swap (equivalent
  to `DROP ... CASCADE` for the constraints — the source FK set is re-applied
  in the post-load restore pass, F-11).
- **`truncate`** — copy in-place into the final table after `TRUNCATE`, verify.

### Performance (F-01/F-12/F-14)
| Flag | Config field | Default |
|------|--------------|---------|
| `--compress none\|zlib\|zstd[:1-19]` | `Compression` / `ZstdLevel` | `zstd` / `3` (`zstd:level` sets the level, 1–19) |
| `--zstd-level <1-19>` | `ZstdLevel` | `3` |
| `--small-table-threshold <size>` | `SmallTableThreshold` | `1MiB` (F-12: tables smaller than this copy with compression off; `0` disables the auto-disable; suffixes K/M/G) |
| `--rate-limit <rate>` | `RateLimit` | `0` (baseline for the ETA + adaptive load diagnostics; accepts `40`, `40gbps`, `400mbps`, …). Does **not** cap the flow — paced by backpressure; enforced ceiling is backlog (see PERFORMANCE.md) |
| `--workers <N>` | `WorkersPerPair` | `1` (must be `>= 1`, validated; **1 = strict** — one pinned REPEATABLE READ connection, one exact snapshot point, serial data phases; **N>1 = snapshot-epoch** — N pinned connections in an ε-window, N parallel data streams, bounded by `--max-snapshot-delta`) |
| `--max-snapshot-delta SEC` | `MaxSnapshotDelta` | `1` (seconds; the snapshot-epoch pin-spread limit for `--workers N>1`: the run's N REPEATABLE READ pins must be no more than this apart, else the run refuses to start with the actual Δ. `0` disables the check. The live pin spread is ~ms, so the default leaves ~three orders of magnitude of headroom. No effect at `--workers 1`) |

### Transport (two-agent, ADR-002)
| Flag | Config field | Default |
|------|--------------|---------|
| `--source-agent-ssh user@host[:port]` | `SourceAgentSSH` | `""` (P1e) |
| `--dest-agent-ssh user@host[:port]` | `DestAgentSSH` | `""` (P1e) |

**Agent placement (P1e).** The controller launches the agents on the hosts
named by these flags: `--source-agent-ssh` = the source cluster's master
(coordinator), `--dest-agent-ssh` = the destination cluster's coordinator.
**Either or both may be omitted** (or name a local host — `127.0.0.1`,
`localhost`, this host's name/IP): an omitted flag means that agent runs as a
**local subprocess** on the gpstream host (the case where gpstream itself runs
on a coordinator). A remote host is launched over **SSH** (key-based auth,
`~/.ssh` defaults — the administrator provisions the keys; no key/runas
flags). The transport is selected by the host in the flag: local = subprocess,
remote = SSH. The agent binary is invoked **by name from `PATH`** (packaged
install) — never by an absolute path.

**Coordinator guard (P1e).** Before an agent binds its control listener it
verifies it runs on the coordinator of **exactly the cluster its DSN points
to** (source agent — the source cluster, dest agent — the destination
cluster): the DSN host must be local, the local master must be responding,
and — for Greenplum — the coordinator row (`content = -1`) in the DSN
cluster's own `gp_segment_configuration` must name this host. A violation
aborts the agent, and `run`/`bench` fail with the verdict from the agent
log. A non-Greenplum (single-node) database always passes.

There are no `--source-agent-binary` / `--dest-agent-binary` flags: a packaged install always lays all four binaries in the same directory, so the controller resolves the agent binaries by convention (sibling of the `gpstream` binary, then `PATH`). Override via `GPSTREAM_SOURCE_AGENT_BINARY` / `GPSTREAM_DEST_AGENT_BINARY` (or config keys `source_agent_binary` / `dest_agent_binary`) for dev builds where the binaries are not beside each other.

The data path is always the two-agent transport: the source agent reads the
table and pushes a compressed (optionally TLS) stream over a real socket to
the dest agent; the controller orchestrates (plan, journal, verify, swap,
indexes) and holds no data connections. Works with both `--copy-mode`
values (`coordinator` / `mesh`); `gpstream_bytes_total` is
the on-wire agent-to-agent volume.

### Resilience (F-06/F-07)
| Flag | Config field | Default |
|------|--------------|---------|
| `--auto-resume <N>` | `AutoResume` | `0` (`N >= 0`; `0` = manual only) |
| `--verify on\|off` | `Verify` | `on` (content verification of the copied data; on by default — the digests are ALWAYS computed and `--verify off` only disables the swap gate. The digest is computed **inline on the data path** on both sides — the coordinator path (P9-E) and the mesh/mini path (the source digest rides the slice trailers, the destination mini tees the stream into the utility COPY) — so **no table is re-read** to verify) |
| `--on-unit-failure continue\|abort` | `OnUnitFailure` | `""` → continue (spec F-06) |

### Transport / frame-ack (T-B, §4.6)
| Flag | Config field | Default |
|------|--------------|---------|
| `--copy-mode coordinator\|mesh` | `CopyMode` | `""` → coordinator (one COPY through each coordinator) |
| `--seg-listen <addr>` | `SegListen` | `""` (auto: controller primary non-loopback IP + a free port; each unit binds `base+unitID`) |
| `--seg-tls-cert <F>` | `SegTLSCert` | `""` (controller serving cert for the segment data plane) |
| `--seg-tls-key <F>` | `SegTLSKey` | `""` (controller serving key) |
| `--seg-tls-ca <F>` | `SegTLSCA` | `""` (CA for the segment data plane) |
| `--frame-ack on\|off` | `FrameAck` | `on` (frame-level ACK + RST-resume; `off` falls back to the plain pipe) |
| `--no-routed-write` | `NoRoutedWrite` | `false` (disables the routed segment-to-segment data path for S≠D: mesh then always merges through the destination coordinator (SliceMux) instead of routing in the segreceivers; use for firewall/NAT fallbacks and A/B comparisons) |
| `--port-range <LO-HI\|LO>` | `PortRange` | `""` (restrict auto data/control port allocation to a fixed range for strict firewalls; empty = fully automatic: agents bind `:0` and report their real address in the control handshake; a run needs 3 ports in coordinator mode — source control + dest data + dest control, since mesh segreceivers dial the shared data listener — or **4+D** in mesh mode (+ dest report + D mini-agent data ports, one per destination segment); `dry-run` verifies the range holds that many) |
| `--ack-timeout 15s\|30s\|adaptive` | `AckTimeout` | `30s` (`adaptive` = 3x median RTT, min 1s) |
| `--max-reconnects <N>` | `MaxReconnects` | `20` (reconnect budget; exhausted = unit failed) |
| `--retransmit-buffer-max <size>` | `RetransmitBufferMax` | `64MiB` (in-flight window per stream; raise for long-haul WAN) |

`mesh` (T-B segment-direct transport) requires the `gpstream` package on
every SOURCE segment host (the kernel runs `gpstream-segreceiver` there); the
source GUC `ycmdb.yc_allow_copy_to_program` must be `on`. The
`--seg-listen` address must be routable from the segment hosts (auto-resolved
to the controller's primary IP when omitted). The `dry-run` reports this
capability under the name `on_segment` (the SQL mechanism name; the copy-mode
value itself is `mesh`). When the planner proves a table's route is
`direct_1to1` (S==D primaries, equal hash keys, same GP major, non-legacy
hashops), `mesh` takes the **fast-path write** automatically: the destination
loads the N content-tagged slices with N per-segment utility COPYs (no merge,
no re-distribution) — the run logs `direct_1to1_fastpath` per such table. See
[PERFORMANCE.md](PERFORMANCE.md).

### Mesh (per-segment mini-agents, `--copy-mode mesh`)

The `mesh` mode runs the data path **segment-to-segment**: the source agent
triggers the ON SEGMENT dispatch (the kernel launches `gpstream-segreceiver`
on every primary source segment), and each source segment's segreceiver
streams its compressed slice directly to the destination **segment** hosts —
no cluster coordinator is in the data path. On the destination, one
**mini-agent** per destination segment receives the slice(s) and loads them
with a local utility-mode `COPY` into its own staging slice.

> **Status (read this).** The mini-agent, the dest-agent **supervisor mode**,
> and the **runtime dispatch** are all **implemented and e2e-verified**
> (P2/P3/P4 + the P3 production-dispatch increment,
> `tests/integration/two_agent_mini_test.go`,
> `two_agent_supervisor_test.go`, `two_agent_mesh_routed_test.go`,
> `two_agent_mesh_direct_e2e_test.go`, `two_agent_mesh_coordinator_fallback_
> test.go`, `two_agent_mesh_flood_test.go` + the P5 crash matrix). A
> `--copy-mode mesh` `run` now launches the D mini-agents over SSH, builds the
> D-entry address book through the dest-agent (in supervisor mode), and routes
> the data segment-to-segment — NEITHER cluster coordinator is in the data
> path; a table that cannot be placed segment-to-segment degrades to the
> coordinator path (per table, `mesh_coordinator_fallback` event). The
> **live validation** on the multi-segment GP6 stand (the A/D/B dev matrix) is
> gated on the SVG-topology confirmation rule. See PERFORMANCE.md (mesh
> topology).

**Mini-agent (`gpstream-dest-agent --mini`).** The mini-agent is a mode of the
`gpstream-dest-agent` binary (there is no separate binary). It runs on a
destination **segment** host (not the coordinator), connects to its OWN
segment locally (`gp_session_role=utility`), and never touches the
coordinator for data. New flags (mini-agent mode):

| Flag | Default | Meaning |
|------|---------|---------|
| `--mini` | `false` | run as the per-segment mini-agent instead of the coordinator dest-agent |
| `--seg <N>` | `0` | this segment's ordinal (0..D-1) |
| `--report-addr <host:port>` | `""` (required in `--mini` mode) | the supervisor's (coordinator dest-agent's) report listener; the mini dials it **once** for the run |
| `--dest <connstring>` | `""` (required in `--mini` mode) | the utility-mode DSN to **this segment** (the mini's own local pg) |
| `--port-range <LO-HI>` | `""` | restricts the mini's auto-picked data port (one of the run's in-range ports) |

**Dest-agent supervisor mode (`gpstream-dest-agent --supervisor`).** The
dest-agent runs on the destination **coordinator** and coordinates the D
destination-segment mini-agents (the "supervisor" is a MODE of the dest-agent,
not a separate component or binary). It binds the control listener, the
persist listener (the coordinator fallback path) and the report listener; the
shared data listener stays OFF. Per unit it builds the distributed staging
table, broadcasts `mini-unit` to the D minis, and combines their reports
(XOR_D). New flags (supervisor mode):

| Flag | Default | Meaning |
|------|---------|---------|
| `--supervisor` | `false` | run as the mesh supervisor (coordinate the D mini-agents) instead of the plain coordinator dest-agent |
| `--mini-count <D>` | `0` | D, the number of destination segments / mini-agents (required with `--supervisor`) |
| `--report-listen <addr>` | `"0.0.0.0:0"` | the report listener the D minis dial (0 = pick a free port, restricted by `--port-range`; the real address is in the hello) |
| `--mini-hosts <h0,h1,...>` | `""` | the D segment hosts in seg order (the catalog's DstDSN hosts; the hello carries the data port only). Empty = `127.0.0.1` for all (single-host layout) |

The controller (the `run` process) launches the D mini-agents itself (SSH, one
per destination segment host) and reads the D-entry address book through the
dest-agent's `mini-map` control op before dispatching the ON SEGMENT read.

Per unit the mini accepts one compressed slice on its data listener,
decompresses it (teeing the content digest when `--verify` is on), loads it
with one utility-mode `COPY`, and reports `{unit, srcContrib, dstAcc, rows,
ok}` to the supervisor over the persistent report channel. A report-channel
drop (the supervisor died) is an **orphan-exit**: the mini exits immediately,
not by timeout. The coordinator dest-agent's **supervisor** (`internal/agent/
supervisor.go`) collects the D mini reports and combines them into the unit's
digests: `srcDigest = XOR_D(srcContrib)`, `dstDigest = XOR_D(dstAcc)` over the
summed row count — the per-row XOR accumulator is commutative/associative, so
segment order is immaterial and the mesh verifies **without re-reading any
table** (FR-02).

**Port formula (mesh = 4+D).** A mesh run uses **4+D** ports: the 3 base
ports (source control + dest control + dest data) **plus** the dest report
listener **plus** D mini-agent data ports (one per destination segment).
`--port-range` is validated against that count at `run`/`dry-run`.

**Routed mesh (S≠D).** When the source and destination segment counts differ
(or the distribution keys differ), the source segreceivers route each row to
its destination segment over W sub-streams (the `RoutedSender`), and each
destination mini-agent **merges** the S×W sub-streams into one binary-COPY
stream loaded by a single utility COPY. The segreceiver's `--dest-addr` is an
address book: one `host:port` (broadcast to the destination AGENT, the P9
coordinator-routed path) or D comma-separated `host:port` (one per
destination segment's mini-agent data endpoint, the mesh path). See
[PERFORMANCE.md](PERFORMANCE.md).

### Indexes (F-17)
| Flag | Config field | Default |
|------|--------------|---------|
| `--index-workers <N>` | `IndexWorkers` | `0` (background index rebuild parallelism) |
| `--no-indexes` | `NoIndexes` | `false` (set to `true`) |
| `--index-priority size\|fifo` | `IndexPriority` | `size` |

`--no-indexes` is a master switch: with it set, `--index-workers` and
`--index-priority` are ignored (not an error — they are set on every run by
default, `workers 2` / `size`). An advisory on stderr names the ignored flag(s).

### Constraints / consistency (F-02/F-11)
| Flag | Config field | Default |
|------|--------------|---------|
| `--no-constraints` | `NoConstraints` | `false` (disable the F-11 constraint-restore pass: FK/PK/unique are re-applied post-load by default) |

Consistency (F-02): the source agent always reads under one run-wide
REPEATABLE READ snapshot per database; every table's data COPY and checksum
read share that single point in time.

### Redistribution / checks (F-15/F-16)

Redistribution accounting (F-15/CR-5) is always computed and reported in the
`dry-run`: the raw segment skew (the fraction of data that moves to a
different segment under the destination layout) plus the layout verdict
(`identical` / `differs` / `cannot confirm`). There is no gate flag: the
operator reads the number and decides. The `--locale-check` flag was removed
— collation/locale differences are always detected (F-16).

### TLS (F-13)
| Flag | Config field | Default |
|------|--------------|---------|
| `--ssl-ca <F>` | `SSLCA` | `""` (**secret** — prefer env/file, not argv, F-08; the pg plane uses `sslmode=verify-full` by default) |
| `--ssl-cert <F>` | `SSLCert` | `""` (**secret** — env/file only, F-08) |
| `--ssl-key <F>` | `SSLKey` | `""` (**secret** — env/file only, F-08) |
| `--ssl-min-tls <V>` | `SSLMinTLS` | `1.2` (minimum TLS version) |
| `--control-tls-cert <F>` | `ControlTLSCert` | `""` (client cert for the control channel; the agents serve with their own cert and the controller pins it against `--control-tls-ca`) |
| `--control-tls-key <F>` | `ControlTLSKey` | `""` (**secret** — env/file only, F-08) |
| `--control-tls-ca <F>` | `ControlTLSCA` | `""` (CA bundle for the control channel) |

### Dry-run / planning (F-10)
| Flag | Config field | Default |
|------|--------------|---------|
| `--dry-run` | `DryRun` | `false` (plan only, do not execute) |
| `--timeout <S>` | `Timeout` | `120s` (bounds the plan-formation phase; F-10 §6) |

### Operation identity / state (v4 §8 "State")
| Flag | Config field | Default |
|------|--------------|---------|
| `--operation-id <id\|latest>` | `OperationID` | `""` (operation identity. `run`: a name, or omitted = a generated timestamp id `YYYYMMDDTHHMMSSZ` UTC, collision-suffixed `-2`…; the state dir is then a chronological history. `status`/`resume`/`cleanup`: a name or `latest` = the newest operation. `run` persists the plan JSON to `<state-dir>/<operation-id>/plan.json`; `dry-run` prints the plan without persisting it) |
| `--state-dir <dir>` | `StateDir` | `/var/lib/gpstream` (operation state lives at `<state-dir>/<operation-id>/`; `run` persists the plan JSON at `<state-dir>/<operation-id>/plan.json`) |

### Observability (F-04)
| Flag | Config field | Default |
|------|--------------|---------|
| `--metrics-addr <A>` | `MetricsAddr` | `""` (serve the Prometheus endpoint at `A/metrics` during `run`) |

### Operational
| Flag | Config field | Default |
|------|--------------|---------|
| `--config <file.toml\|file.yaml>` | `ConfigFile` | `""` (config file; file layer of precedence) |

---

## State commands and their flags

The state commands do **not** go through `parseConfig`; their flags are
bespoke and shared:

| Command | Flags |
|---------|-------|
| `status` | `--operation-id <id>` (required), `--state-dir <dir>` |
| `resume` | `--operation-id <id>` (required), `--state-dir <dir>`, `--auto-resume <N>` |
| `cleanup` | `--operation-id <id>` (or `--all-incomplete`), `--state-dir <dir>`, `--mode drop`, `--dest <connstring>`, `--dest-agent-ssh user@host[:port]`, `--dest-agent-binary <path>`, `--ssl-ca`, `--ssl-cert`, `--ssl-key`, `--ssl-min-tls`, **`--yes` (always required — cleanup is destructive)** |

`--operation-id <id>` names the operation directly; the state is resolved at
`<state-dir>/<operation-id>/`.

---

## Safety defaults

gpstream is deliberately conservative about overwriting destination data:

- **`ConflictAction` defaults to `stop`** — the run stops on the first
  collision; nothing is clobbered and nothing is silently skipped unless the
  operator explicitly chooses `skip`, `overwrite`, or `truncate`.
- **Data is never overwritten** unless the operator passes
  `--resolve-conflicts overwrite` (or the `--overwrite` shorthand) or
  `--resolve-conflicts truncate` (or the `--truncate` shorthand).
- **Secrets (`--ssl-cert`/`--ssl-key`/`--ssl-ca`) come from env or a `0600`
  file, never argv (F-08).** Although the parser accepts them as flags,
  supply them via `GPSTREAM_SSL_CERT` / `GPSTREAM_SSL_KEY` / `GPSTREAM_SSL_CA`
  or the config file.
- **The pg data plane is encrypted by default** (`sslmode=verify-full` is
  enforced unless the connstring sets an explicit `sslmode`).

## Destructive commands

Only **`cleanup`** modifies or destroys operation state. With `--mode drop`
it also removes destination-side objects, so use it deliberately. `run`,
`dry-run`, `status`, `resume`, `version`, `man` and
`help` are **not** destructive.

---

## Environment layer (summary)

Environment variables are read by `ApplyEnv()` (the env layer of
CLI > env > file > defaults). All are prefixed `GPSTREAM_`:

`RATE_LIMIT`, `WORKERS`, `MAX_SNAPSHOT_DELTA`, `COMPRESS`,
`SMALL_TABLE_THRESHOLD`, `AUTO_RESUME`, `ON_UNIT_FAILURE`, `SSL_CERT`,
`SSL_KEY`, `SSL_CA`, `SOURCE_AGENT_BINARY`, `DEST_AGENT_BINARY`,
`SEG_BINARY`, `SEG_LISTEN`, `PORT_RANGE`, `SEG_TLS_CERT`, `SEG_TLS_KEY`,
`SEG_TLS_CA`, `CONTROL_TLS_CERT`, `CONTROL_TLS_KEY`, `CONTROL_TLS_CA`,
`FRAME_ACK`, `ACK_TIMEOUT`, `MAX_RECONNECTS`, `RETRANSMIT_BUFFER_MAX`.

Boolean env vars accept `true`/`1`. See
[CONFIGURATION.md](CONFIGURATION.md) for the full env/TOML/YAML/precedence
reference.

> **Endpoints are not env-driven:** there is no `GPSTREAM_SOURCE`/
> `GPSTREAM_DEST`. The source/destination connstrings come from the
> `--source`/`--dest` flags or the config file.

---

*This reference is derived from `cmd/gpstream/main.go` and
`internal/config/config.go` (module `github.com/gpstream/gpstream`, Go 1.27).
Command names, flag names, defaults, and bindings above match the source
exactly.*
