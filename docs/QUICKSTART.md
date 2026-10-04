# gpstream — Quick Start

> NOTE (2026-10-04): the copy-mode surface is being simplified to TWO modes —
> coordinator and on_segment. The direct mode described in this document is
> redundant (its per-segment write is subsumed by on_segment's auto-selected
> per-table sub-paths: 1:1, routed, merge) and will be removed. This page
> still describes the CURRENT (shipped) behavior.

`gpstream` streams logical data between two Greenplum 6 clusters over a direct
high-speed network, **without any intermediate storage**. This guide is the
minimal path to a copy: set the endpoints, choose a scope, dry-run first, run,
monitor, and recover if needed.

> **Safety default:** conflicts **stop the run** (F-10). By default gpstream
> **never overwrites and never skips silently** — if a table already exists on
> the destination, the copy halts with a conflict and nothing is clobbered.
> To proceed you must opt in explicitly: `--resolve-conflicts skip` (skip
> conflicting units and continue), or `--resolve-conflicts overwrite` /
> `truncate` (replace / truncate+load).

> **Cluster requirement:** `--help` and `selftest` need no cluster. **`run`,
> `dry-run`, and `doctor` require a reachable source and destination cluster**
> (a dry-run plans against the live topology).
>
> **Where it installs:** the `gpstream` package goes on the **controller
> host** (the machine you run it from — it does not have to be a cluster
> host). For `--copy-mode on_segment` it also goes on **every source segment
> host** (it supplies `gpstream-segreceiver`). The **destination** never runs
> any gpstream code. Details: [INSTALL.md](INSTALL.md#63-deployment-model-what-goes-where).

---

## 1. The minimal path to a copy

A copy has six steps:

1. **Set the endpoints** — `--source` / `--dest` connstrings (or the `source` /
   `dest` keys in the config file).
2. **Select the scope** — `--dbs`, `--schemas`, `--tables`, or `--exclude`.
3. **Dry-run first** — `gpstream dry-run ...` to preview the deterministic plan.
4. **Run** — `gpstream run ...` to execute the copy.
5. **Monitor** — `gpstream status --operation-id <id>` and the `/metrics`
   endpoint (serve it with `--metrics-addr`).
6. **Recover** — `gpstream resume --operation-id <id>` (or `cleanup`).

---

## 2. Set the endpoints (source / dest)

Provide the source and destination connection strings. Use either CLI flags or
the `GPSTREAM_*` environment variables:

```bash
# Option A: CLI flags
gpstream run --source "host=prod-coordinator port=5432 user=gpstream dbname=postgres" \
              --dest   "host=preprod-coordinator port=5432 user=gpstream dbname=postgres"

# Option B: the config file (lowest precedence; a CLI flag still wins)
cat > /etc/gpstream/gpstream.toml <<'EOF'
source = "host=prod-coordinator port=5432 user=gpstream dbname=postgres"
dest   = "host=preprod-coordinator port=5432 user=gpstream dbname=postgres"
EOF
gpstream run --config /etc/gpstream/gpstream.toml
```

Precedence is **CLI > env (`GPSTREAM_*`) > `--config` file > defaults**, so a
flag always wins over an env var, which wins over a config file. The endpoints
themselves (`source`/`dest`) come from the CLI flags or the config file — there
is no `GPSTREAM_SOURCE`/`GPSTREAM_DEST` env var.

TLS: the pg data plane uses `sslmode=verify-full` by default (enforced unless
the connstring sets an explicit `sslmode`). Supply the CA bundle with `--ssl-ca
F` (or the `GPSTREAM_SSL_CA` env / a `0600` config file) — never as a raw secret
in argv.

---

## 3. Select the scope

Choose what to copy. Selection flags take **comma-separated lists**:

| Flag | Scope |
| --- | --- |
| `--dbs db1,db2` | One or more databases. |
| `--schemas db.schema[,...]` | One or more schemas. |
| `--tables db.schema.table[,...]` | One or more tables. |
| `--exclude <name>[,...]` | Exclude a database/schema/table. |

Additional metadata/data flags: `--metadata-only` (structure only, no data)
and `--data-only` (data only, into the existing structure — rows replaced, structure must already exist); they are mutually
exclusive, and with neither flag both are copied (the default).
You can also rename targets with `--map-db`, `--map-schema`, `--map-table`.

Example — copy the `orders` database, excluding an audit schema:

```bash
gpstream run --source "host=prod-coordinator port=5432 user=gpstream dbname=postgres" \
              --dest   "host=preprod-coordinator port=5432 user=gpstream dbname=postgres" \
              --dbs orders --exclude "public.audit"
```

---

## 4. Dry-run first

Always dry-run before running. `dry-run` builds and prints the **fully
deterministic plan** without executing the copy. It shows the units, volume,
ETA, conflicts (with the action that would apply), redistribution accounting,
and the plan hash:

```bash
gpstream dry-run \
  --source "host=prod-coordinator port=5432 user=gpstream dbname=postgres" \
  --dest   "host=preprod-coordinator port=5432 user=gpstream dbname=postgres" \
  --dbs orders
```

Every operation persists its deterministic plan to
`<state-dir>/<operation-id>/plan.json`. Two dry-runs of the same scope produce
the same **plan hash** (printed on screen and stored in the plan JSON) — use it
to confirm a plan has not changed between runs.

---

## 5. Run the copy

Once the dry-run looks right, run the copy:

```bash
gpstream run --source "host=prod-coordinator port=5432 user=gpstream dbname=postgres" \
              --dest   "host=preprod-coordinator port=5432 user=gpstream dbname=postgres" \
              --dbs orders
```

What happens:

- gpstream connects to **both** clusters (a real copy requires real streaming
  connections; it never falls back to a fake data plane).
- It builds the plan, runs the **transfer engine** (`source -> stream -> dest`)
  with **no intermediate storage**, and records every transition in the
  operation manifest — so the operation is crash-recoverable.
- Each unit lands (verify only with `--verify on`, off by default), indexes are rebuilt in the
  **background** (F-17), and the staging table is **atomically swapped** to its
  final name. A Prometheus `/metrics` endpoint is served when you pass
  `--metrics-addr`.

The operation identity is `--operation-id <id>`. When omitted, `run` generates
a **timestamp** id (UTC, `YYYYMMDDTHHMMSSZ`, e.g. `20261001T153000Z`) so the
state directory is a chronological history of runs — the newest operation is
the last entry. `status` / `resume` / `cleanup` accept a name or the magic
value `latest` (the newest operation). An explicit id names the operation and
is the way to resume an earlier run (a re-run with no explicit id is a *new*
operation).

---

## 6. Monitor

While (or after) a copy runs, use `status` and the `/metrics` endpoint.

### 6.1 `gpstream status`

Reports the state of an operation by its id; each unit is printed with its
state, kind, `db.schema.table`, and rows/bytes transferred:

```bash
gpstream status --operation-id <id>
```

### 6.2 The `/metrics` endpoint

There is no separate `metrics` subcommand — the Prometheus endpoint is served
**during a run** when you pass `--metrics-addr`:

```bash
gpstream run --dbs orders --metrics-addr http://localhost:9100
curl -s http://localhost:9100/metrics
```

`gpstream status --operation-id <id>` also prints a live table with rows,
throughput, and ETA.

---

## 7. Recover

If a copy is interrupted, resume it or clean it up by operation id.

### 7.1 Resume an interrupted operation

`resume` reopens the operation manifest and restarts every unit not in a
terminal state:

```bash
gpstream resume --operation-id <id>
gpstream resume --operation-id <id> --auto-resume 3
```

It reports how many units were re-queued. If all units are already terminal it
prints `resume: all units terminal — nothing to resume`. You can also set
`--auto-resume N` on the copy itself, so the operation auto-resumes up to `N`
times from the checkpoint if a unit does not reach a terminal state. Resume
re-read behaviour is fixed (a table is kept only if it was verified under one snapshot epoch)
(default `relaxed`).

### 7.2 Clean up

`cleanup` removes an operation's state and (optionally) its destination
objects:

```bash
gpstream cleanup --operation-id <id>
# remove all incomplete operations
gpstream cleanup --all-incomplete
# also drop the destination objects the operation created
gpstream cleanup --operation-id <id> --mode drop --dest "$DST"
```

---

## 8. Conflict handling (conflicts stop the run)

When a unit's target table **already exists** on the destination, gpstream must
decide what to do. **By default it refuses and stops the run** (F-10): the copy
halts with a conflict and **nothing is clobbered and nothing is silently
skipped**. You opt in to a policy with `--resolve-conflicts`:

| `--resolve-conflicts` | Behaviour |
| --- | --- |
| `stop` (default) | **A conflict stops the run.** Nothing is overwritten, nothing is skipped. |
| `skip` | Skip conflicting units and continue. |
| `overwrite` | Replace existing data (staging + atomic swap; verified before the swap). |
| `truncate` | Truncate the target, then load into it (in-place). |

`--overwrite` and `--truncate` are shorthand opt-ins that set the corresponding
policy. The dry-run lists any conflicts and the **verdict that would apply**
(stop / skip / overwrite / truncate), so you can see the impact before you run.

> **Choosing:** for a first copy of a fresh destination there are no conflicts
> and the run proceeds. When re-copying into a cluster that already holds data,
> decide deliberately: `--resolve-conflicts skip` to keep what's there, or
> `overwrite` / `truncate` to replace it.

---

## 9. First copy walkthrough

A realistic end-to-end first copy of a single database:

```bash
# 1. Point at the two clusters (flags or the config file; there is no
#    GPSTREAM_SOURCE/DEST env var)
SRC="host=prod-coordinator   port=5432 user=gpstream dbname=postgres"
DST="host=preprod-coordinator  port=5432 user=gpstream dbname=postgres"

# (Optional) TLS CA for the pg plane comes from env/file, never argv
# export GPSTREAM_SSL_CA=/etc/gpstream/tls/ca.crt

# 2. Dry-run first — preview the deterministic plan
gpstream dry-run --source "$SRC" --dest "$DST" --dbs orders

# 3. Inspect: units, volume, ETA, conflicts (default verdict = stop), redistribution, plan hash

# 4. Run the copy. On a fresh destination there are no conflicts, so the run
#    proceeds. If a target table already exists, the run STOPS by default —
#    re-run with --resolve-conflicts skip (keep what's there) or
#    --resolve-conflicts overwrite (replace it) to proceed deliberately.
gpstream run --source "$SRC" --dest "$DST" --dbs orders --operation-id first-copy --metrics-addr http://localhost:9100

# 5. Monitor
gpstream status --operation-id first-copy
curl -s http://localhost:9100/metrics

# 6. If interrupted, resume from the same operation id
gpstream resume --operation-id first-copy
```

The copy always reads the source under one run-wide REPEATABLE READ snapshot
per database (F-02), so the whole copy sees a single point in time.

---

## 10. Useful flags at a glance

| Category | Flags |
| --- | --- |
| Endpoints | `--source`, `--dest` |
| Scope | `--dbs`, `--schemas`, `--tables`, `--exclude` |
| Object class | `--metadata-only`, `--data-only` (default: both) |
| Rename | `--map-db`, `--map-schema`, `--map-table` |
| Conflict | `--resolve-conflicts stop\|skip\|overwrite\|truncate` (default `stop`), `--overwrite`, `--truncate` |
| Compression | `--compress none\|zlib\|zstd[:1-22]`, `--zstd-level N`, `--small-table-threshold SIZE` (F-12) |
| Copy mode | `--copy-mode coordinator\|on_segment\|direct`, `--seg-listen` (optional; auto when omitted) |
| Performance | `--workers N`, `--rate-limit <Gbps>` (adaptive AIMD always active) |
| Resilience | `--auto-resume N`, `--on-unit-failure continue\|abort`, `--verify on\|off`, `--frame-ack`, `--ack-timeout`, `--max-reconnects` |
| Consistency | always on: one run-wide REPEATABLE READ snapshot per database (F-02) |
| Indexes | `--index-workers N`, `--no-indexes`, `--index-priority size\|fifo` (default `size`; F-17, on by default) |
| Redistribution | (built into `dry-run`: raw skew + layout verdict; segment-fill balance is measured always, fixed 1.5 gate) |
| TLS | `--ssl-ca F` (pg plane; `sslmode=verify-full` default), `--seg-tls-cert/-key/-ca` (segment plane) |
| Planning | `--dry-run`, `--timeout S` |
| Observability | `--metrics-addr A` (serves `/metrics` during a run) |
| Operation | `--operation-id <id\|latest>` (run: a name, or omitted = timestamp id), `--state-dir DIR` |
| Config | `--config <file>` |

Precedence: **CLI > env (`GPSTREAM_*`) > file > defaults**.

---

## 11. Where to read more

- **`OPERATOR_GUIDE`** — running, monitoring, and operating copies in production.
- **`ADMIN_GUIDE`** — cluster-level setup, security/TLS, and administration.
- **`CLI_REFERENCE`** — the full flag surface for every subcommand.
- **`CONFIGURATION`** — the `gpstream.toml` config file, env variables, and precedence.
- **`docs/INSTALL.md`** — building, cross-compiling, packaging, systemd, and logrotate.
- **`docs/METRICS.md`** — the Prometheus metric names and semantics.
- **`man gpstream`** — the man page (installed to /usr/share/man/man8 by the package).
- **`gpstream --help`** — commands and flags.
