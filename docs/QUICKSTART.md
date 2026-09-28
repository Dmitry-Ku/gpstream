# gpstream — Quick Start

`gpstream` streams logical data between two Greenplum 6 clusters over a direct
high-speed network, **without any intermediate storage**. This guide is the
minimal path to a copy: configure the endpoints, choose a scope, dry-run first,
run, monitor, and recover if needed.

> **Safety default:** conflicts **stop the run** (FR-10 §8). By default gpstream
> **never overwrites and never skips silently** — if a table already exists on
> the destination, the copy halts with a conflict error and nothing is clobbered.
> To proceed you must opt in explicitly: `--skip-conflicts` (skip conflicting
> units), or `--overwrite` / `--truncate` / `--append` (replace / truncate+load /
> append).

> **Cluster requirement:** `--help`, `dry-run`, and `plan` run against a
> built-in fake and need no live cluster. **`copy` (and `doctor`) require a
> reachable source and destination cluster.**

---

## 1. The minimal path to a copy

A copy has six steps:

1. **Set the endpoints** — `--source` / `--dest` connstrings (or the
   `GPSTREAM_SOURCE` / `GPSTREAM_DEST` environment variables).
2. **Select the scope** — `--full`, `--db`, `--schema`, `--table`, or `--exclude-*`.
3. **Dry-run first** — `gpstream dry-run ...` to preview the deterministic plan.
4. **Run** — `gpstream run ...` to execute the copy.
5. **Monitor** — `gpstream status` and `gpstream metrics`.
6. **Recover** — `gpstream resume --mirror F` / `gpstream rollback --mirror F`.

---

## 2. Set the endpoints (source / dest)

Provide the source and destination connection strings. Use either CLI flags or
the `GPSTREAM_*` environment variables:

```bash
# Option A: CLI flags
gpstream run --source "host=prod-coordinator port=5432 user=gpstream dbname=postgres" \
              --dest   "host=preprod-coordinator port=5432 user=gpstream dbname=postgres"

# Option B: environment variables (GPSTREAM_SOURCE / GPSTREAM_DEST)
export GPSTREAM_SOURCE="host=prod-coordinator port=5432 user=gpstream dbname=postgres"
export GPSTREAM_DEST="host=preprod-coordinator port=5432 user=gpstream dbname=postgres"
gpstream run
```

Precedence is **CLI > env (`GPSTREAM_*`) > `--config` file > defaults**, so a
flag always wins over an env var, which wins over a config file.

TLS/secret material is never taken from argv: pass SSL cert / key / CA via
`GPSTREAM_SSL_CERT` / `GPSTREAM_SSL_KEY` / `GPSTREAM_SSL_CA` (or a `0600`
config file), not as command-line flags.

---

## 3. Select the scope

Choose what to copy. These are mutually consistent selection flags; pick the
granularity you need:

| Flag | Scope |
| --- | --- |
| `--full` | Copy the entire cluster (all databases). |
| `--db <name>` | One or more databases (repeatable). |
| `--schema <name>` | One or more schemas (repeatable). |
| `--table <name>` | One or more tables (repeatable). |
| `--exclude <name>` | Exclude a database/schema/table (repeatable). |

Additional metadata/data flags: `--metadata-only` and `--data-only`
(mutually exclusive). You can also rename targets with
`--rename-db src=dst` and `--rename-schema src=dst`.

Example — copy the `orders` database, excluding an audit schema:

```bash
gpstream run --source "$GPSTREAM_SOURCE" --dest "$GPSTREAM_DEST" \
              --db orders --exclude "public.audit"
```

---

## 4. Dry-run first

Always dry-run before running. `dry-run` builds and prints the **fully
deterministic plan** without executing anything (no cluster is required — it
uses a built-in fake when connstrings are absent). It shows the units, volume,
ETA, conflicts (with the action that would apply), redistribution accounting,
and the plan hash:

```bash
gpstream dry-run \
  --source "$GPSTREAM_SOURCE" --dest "$GPSTREAM_DEST" \
  --db orders
```

You can persist the plan to a file with `--plan-file` (e.g.
`gpstream dry-run --plan-file /tmp/plan.json --db orders`). `gpstream
plan` is the equivalent command that emits the plan as JSON (`--plan-file` or
stdout). The **plan hash is deterministic**, so two dry-runs of the same scope
produce the same hash — use it to confirm a plan has not changed between runs.

---

## 5. Run the copy

Once the dry-run looks right, run the copy:

```bash
gpstream run --source "$GPSTREAM_SOURCE" --dest "$GPSTREAM_DEST" --db orders
```

What happens:

- gpstream connects to **both** clusters (a real copy requires real streaming
  connections; it never falls back to a fake data plane).
- It builds the plan, runs the **Job Manager** (`source -> stream -> dest`)
  with **no intermediate storage**, and journals every transition (to the
  dest-DB journal plus a local file mirror) — so the job is crash-recoverable.
- Each unit is verified (default `verify: sample`), indexes are rebuilt in the
  **background**, and the staging table is **atomically swapped** to its final
  name. A metrics endpoint is served when you pass `--metrics-addr`.

Example output (a unit prints the **journal mirror path** if any unit is
skipped or failed, so you can resume or inspect it):

```
gpstream run
=============
units:          3
volume:         1.5 GB
conflicts:      none
plan hash:      <hash>

copy: 3 unit(s) completed, N rows transferred, 0 skipped, 0 failed
```

---

## 6. Monitor

While (or after) a copy runs, use `status` and `metrics`.

### 6.1 `gpstream status`

Reports the state of a job's journal. `--mirror` is required (point it at the
journal mirror produced by the copy); each unit is printed with its `unit id`,
`state`, `kind`, `db.schema.table`, and bytes transferred. Filter to a single
job with `--job <id>`:

```bash
gpstream status --mirror /tmp/gpstream_<hash>.mirror
```

### 6.2 `gpstream metrics`

Prints the live metrics snapshot (throughput, latency, error rate). Without an
endpoint it prints the current process counters; with one it points at the
Prometheus endpoint. Serve it during a copy with `--metrics-addr`:

```bash
gpstream metrics --metrics-addr http://localhost:9100
gpstream run --metrics-addr http://localhost:9100 --db orders
```

### 6.3 Other observability commands

- `gpstream logs --mirror F` — human-readable operation journal (one line per unit transition).
- `gpstream indexes --mirror F [--rebuild]` — report / rebuild indexes after a copy.

---

## 7. Recover

If a copy is interrupted, resume or roll back using the journal mirror (`F`).
Both `resume` and `rollback` (and `cancel`) are **destructive** and require
`--yes`; `resume` is the safe path to re-queue unfinished units.

### 7.1 Resume an interrupted job

`resume` reopens the operation journal (file mirror, optionally the live
dest-DB journal) and restarts every unit not in a terminal state:

```bash
gpstream resume --mirror /tmp/gpstream_<hash>.mirror
gpstream resume --mirror /tmp/gpstream_<hash>.mirror --auto-resume 3
```

It prints how many units were re-queued (and, with `--auto-resume N`, the
attempt count / max). If all units are already terminal it prints
`resume: all units terminal — nothing to resume`.

You can also set `--auto-resume N` on the copy itself, so the job auto-resumes up
to `N` times from the checkpoint if a unit does not reach a terminal state.

### 7.2 Roll back a job

`rollback` runs the crash-recovery sequence: any unit not in a terminal state
(i.e. not verified / swapped) is rolled back to `pending` so it is re-copied, and
completed-but-not-swapped staging objects are rolled back:

```bash
gpstream rollback --mirror /tmp/gpstream_<hash>.mirror --yes
```

### 7.3 Cancel / cleanup

- `gpstream cancel --mirror F --yes` — mark non-terminal units failed (terminal units preserved).
- `gpstream cleanup --mirror F --yes [--auto-cleanup]` — remove a job's journal and staging objects.

---

## 8. Conflict handling (conflicts stop the run)

When a unit's target table **already exists** on the destination, gpstream must
decide what to do. **By default it refuses and stops the run** (FR-10 §8): the
copy halts with a conflict error and **nothing is clobbered and nothing is
silently skipped**. You must opt in to a policy explicitly.

| Flag | Behaviour |
| --- | --- |
| *(default)* | **Strict** — a conflict **stops the run** (skip-existing with `--skip-conflicts` OFF). Nothing is overwritten, nothing is skipped. |
| `--skip-conflicts` | Skip conflicting units and continue (the old "safe skip" behaviour — now an explicit opt-in). |
| `--overwrite` | Replace existing data (staging + atomic rename; verified before the swap). |
| `--overwrite-only-conflicts` | Overwrite **only** the tables that have a conflict (v2 refinement; more conservative than `--overwrite`). |
| `--truncate` | Truncate the target, then load into it. |
| `--append` | Append to the existing target. |

The dry-run lists any conflicts and the **verdict that would apply** (stop /
skip / overwrite / truncate / append), so you can see the impact — and, for the
default, exactly which run would stop — before you run.

> **Choosing:** for a first copy of a fresh destination there are no conflicts
> and the run proceeds. When re-copying into a cluster that already holds data,
> decide deliberately: `--skip-conflicts` to keep what's there, or
> `--overwrite`/`--truncate`/`--append` to replace it.

---

## 9. First copy walkthrough

A realistic end-to-end first copy of a single database:

```bash
# 1. Point at the two clusters
export GPSTREAM_SOURCE="host=prod-coordinator   port=5432 user=gpstream dbname=postgres"
export GPSTREAM_DEST="host=preprod-coordinator  port=5432 user=gpstream dbname=postgres"

# (Optional) TLS secrets come from the environment, never argv
# export GPSTREAM_SSL_CERT=/etc/gpstream/tls/client.crt
# export GPSTREAM_SSL_KEY=/etc/gpstream/tls/client.key      # mode 0600
# export GPSTREAM_SSL_CA=/etc/gpstream/tls/ca.crt

# 2. Dry-run first — preview the deterministic plan
gpstream dry-run --dbs orders
#   (add --plan-file /tmp/plan.json to persist it)

# 3. Inspect: units, volume, ETA, conflicts (default verdict = stop), redistribution, plan hash

# 4. Run the copy. On a fresh destination there are no conflicts, so the run
#    proceeds. If a target table already exists, the run STOPS by default —
#    re-run with --skip-conflicts (keep what's there) or --overwrite
#    (replace it) to proceed deliberately.
gpstream run --db orders --metrics-addr http://localhost:9100

# 5. Monitor
gpstream status  --mirror /tmp/gpstream_<hash>.mirror
gpstream metrics --metrics-addr http://localhost:9100

# 6. If interrupted, resume (or rollback) from the journal mirror
gpstream resume  --mirror /tmp/gpstream_<hash>.mirror
gpstream rollback --mirror /tmp/gpstream_<hash>.mirror --yes
```

For a maintenance-window-wide, cluster-consistent copy, use `--consistent-cluster`
instead of the default `--consistent-db`.

---

## 10. Useful flags at a glance

| Category | Flags |
| --- | --- |
| Endpoints | `--source`, `--dest` |
| Scope | `--full`, `--db`, `--schema`, `--table`, `--exclude`, `--metadata-only`, `--data-only` |
| Conflict | *(default: conflicts stop the run)* — `--skip-conflicts` (alias `--skip-existing`) to skip, or `--overwrite` / `--overwrite-only-conflicts` / `--truncate` / `--append` to proceed |
| Performance | `--compress none|zlib|zstd[:1-19]`, `--rate-limit <Gbps>`, `--workers-per-pair N`, `--data-port-range a-b` |
| Resilience | `--auto-resume N`, `--verify full|sample|off` |
| Consistency | `--consistent-db`, `--consistent-cluster` |
| Indexes | `--index-workers N`, `--no-indexes`, `--index-priority size|manual`, `--background-indexes` |
| Redistribution | `--no-redistribute`, `--skew-threshold X`, `--redistribution-plan`, `--distribution-check` |
| TLS | `--ssl-cert`, `--ssl-key`, `--ssl-ca`, `--ssl-min-tls` (secrets from env/file, never argv) |
| Planning | `--dry-run`, `--timeout S`, `--plan-file F` |
| Observability | `--metrics`, `--metrics-addr A`, `--report-json/-csv/-text F` |
| Config | `--config <file>` |

Precedence: **CLI > env (`GPSTREAM_*`) > file > defaults**.

---

## 11. Where to read more

- **`OPERATOR_GUIDE`** — running, monitoring, and operating copies in production.
- **`ADMIN_GUIDE`** — cluster-level setup, security/TLS, and administration.
- **`CLI_REFERENCE`** — the full flag surface for every subcommand.
- **`CONFIGURATION`** — the `gpstream.toml` config file, env variables, and precedence.
- **`docs/INSTALL.md`** — building, cross-compiling, packaging, systemd, and logrotate.
- **`gpstream man`** / `man/gpstream.8` — the man page.
- **`gpstream --help`** — commands and flags.
