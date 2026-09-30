# gpstream — Troubleshooting

A working FAQ for `gpstream`: what a failed command means, why it failed,
and how to recover. Every symptom here maps to a **real** command, flag, or
behavior in the tool (the 9 subcommands: `doctor`, `dry-run`, `run`, `status`,
`resume`, `cleanup`, `selftest`, `version`, `man`); nothing is invented.

> If you are stuck, start with the diagnostics: `gpstream doctor`,
> `gpstream selftest`, and `gpstream dry-run --timeout 60`. They tell you
> almost everything without touching live data.

---

## 1. `doctor` says FAIL / reachability

**Symptom:** `gpstream doctor` prints a FAIL.

| Cause | Check | Fix |
|-------|-------|-----|
| Missing / bad connection string | `--source` / `--dest` (or the config file) | set a valid connstring |
| Cluster unreachable | network / host / port | fix network; re-run `doctor` |
| **0 up-segments** | segment count | a 0-up report is a **FAIL**, not a pass — bring segments up |
| TLS too old | `--ssl-min-tls` (default `1.2`) | TLS is **always** enforced; use >= 1.2 |

Key facts: a **missing connection string is a reachability FAIL** (not a
silent pass), and **0 up-segments is a FAIL**. `doctor` will never report a
healthy cluster that is actually down. There is **no `security-check`
subcommand** — TLS/certificate findings are part of `doctor` (`tls` check).

---

## 2. The copy stops / crashes

**Symptom:** a copy ends mid-way (kill -9, network drop, disk full).

**Recovery = resume, not restart.**

```
gpstream status  --operation-id <id>   # see where it stopped
gpstream resume  --operation-id <id>   # resume the operation
```

- A unit already in a **terminal state** (`completed`, `swapped`, or
  `failed`) is **skipped** on resume — it is not re-copied.
- `--auto-resume N` caps automatic resume attempts (default `0` = off).
- To discard a run's destination objects: `gpstream cleanup --operation-id
  <id> --mode drop --dest <conn> --yes`.
- Abandoned operation state: `gpstream cleanup --operation-id <id> --yes`.

There is **no `rollback` subcommand** — recovery is `resume` (re-queue
non-terminal units) and `cleanup --mode drop` (discard destination objects).
See `FAILOVER.md` for the full model. **Do not** silently restart a copy
that died — resume is the correct mechanism.

---

## 3. The copy is slow / a bottleneck

**Symptom:** throughput is lower than expected.

| Knob | Flag / env | What it does |
|------|-----------|--------------|
| Rate limit | `--rate-limit <Gbps>` (`GPSTREAM_RATE_LIMIT`) | caps throughput (accepts `bps`/`mbit`/`gbit`) |
| Workers | `--workers N` (default `2`) | parallelism per host-pair |
| Compression | `--compress` (default `zstd`, level 3) | reduces wire bytes |

The adaptive limiter (F-14) is **always active**: it samples source host
load and the destination's receive queue, reduces the rate on sustained
overload and recovers when load subsides. There is no `--adaptive-rate-limit`
flag to enable it — it is built in. There is no `--data-port-range` flag
(the stream manages its own data ports).

Diagnose with the metrics endpoint (see `METRICS.md`): `gpstream_bytes_total`,
`gpstream_bytes_decoded_total` (compression ratio), `gpstream_throughput_bytes`,
and `gpstream_errors_total`. A drop in throughput with `Errors` climbing points
to a network/CPU/disk bottleneck; the adaptive limiter backs off and recovers
automatically (each shift is logged to `events.jsonl` as `rate_limit_shift`).

---

## 4. Conflict / collision

**Symptom:** a target table already exists and the copy **stops**.

This is the **default** (`ConflictAction = stop`) — `gpstream` is
**non-destructive** and never overwrites existing data unless told to. A
conflict halts the run with a conflict error so the operator decides
deliberately.

| Flag | Effect |
|------|--------|
| `--resolve-conflicts stop` | **default** — a conflict stops the run |
| `--resolve-conflicts skip` | skip conflicting objects (reported in `conflicts.jsonl`) |
| `--resolve-conflicts overwrite` | replace the target (staging + atomic swap, verified) |
| `--resolve-conflicts truncate` | truncate then reload (in-place) |
| `--overwrite` / `--truncate` | shorthand opt-ins for the matching policy |

If you expected an overwrite, add `--resolve-conflicts overwrite` (or
`--overwrite`). If you expected a skip, pass `--resolve-conflicts skip` —
the default is to **stop**, not to skip.

---

## 5. Collation / locale / distribution mismatch

**Symptom:** a warning about collation, locale, or distribution in
`doctor` / `dry-run`.

| Check | Flag | Meaning |
|-------|------|---------|
| Collation | (built into `doctor` + `dry-run`) | `LC_COLLATE` / explicit column collation differences (warn) |
| Locale | `--locale-check` | locale differences (warn) |
| Redistribution | `--no-redistribute`, `--skew-threshold` (default `0.05`) | skip redistribution / skew tolerance |

A **distribution** difference may require redistribution on the destination.
`gpstream` accounts for it with `--no-redistribute` and `--skew-threshold`.
A warning is **not** a failure — review it in the `dry-run` output and decide
whether to proceed. The per-table × column collation detail lives in the
`dry-run` plan (`collation_diffs`) and the run's `events.jsonl`
(`collation_diff` events).

---

## 6. Indexes not created / slow

**Symptom:** indexes are missing or the copy is slow because of indexes.

Indexes are rebuilt **in the background** after data lands (F-17) by default,
so index builds do not contend with the bulk copy. There is **no separate
`indexes` subcommand** — index work is part of `run`.

| Knob | Flag | Effect |
|------|------|--------|
| Workers | `--index-workers N` | parallelism of background index builds |
| Skip | `--no-indexes` | do not create indexes for this run |
| Priority | `--index-priority` (`size` / `manual` / `background`) | index build order |

A `dry-run` shows the expected index work. If indexes are slow, raise
`--index-workers` or let the background path continue independently of the
data copy.

---

## 7. `dry-run` confidence is low

**Symptom:** `gpstream dry-run --timeout 60` reports low confidence or a
**partial plan**.

- A `--timeout` that is too small yields a **partial plan fragment** (the
  plan is incomplete, not wrong). Increase `--timeout`.
- Low **confidence** means some inputs (volume, collation, redistribution)
  could not be determined offline — the fields that cannot be predicted
  without a live copy are labelled as **predictions**, not fake numbers.
- Inspect the full plan: `gpstream dry-run --plan-file <F>`. There is **no
  `report` subcommand** — the final summary is printed on completion and the
  durable record is the journal under `<state-dir>/<operation-id>/`.

---

## 8. Metrics / logs

**Symptom:** you cannot see what a copy is doing.

| Need | Command / flag |
|------|----------------|
| Live counters | `--metrics-addr <A>` on `run` → Prometheus `/metrics` |
| Status | `gpstream status --operation-id <id>` |
| Event log | `<state-dir>/<operation-id>/events.jsonl` (diagnostic stream) |
| Action audit | `<state-dir>/<operation-id>/audit.log` |
| Conflicts | `<state-dir>/<operation-id>/conflicts.jsonl` |

There is **no `metrics` or `logs` subcommand** — the Prometheus endpoint is
served *during* `run` via `--metrics-addr`, and the logs are the per-operation
files under `<state-dir>/<operation-id>/`.

Key counters (see `METRICS.md`): `gpstream_bytes_total`,
`gpstream_bytes_decoded_total`, `gpstream_errors_total`,
`gpstream_frame_errors_total`, and `gpstream_throughput_bytes`. A rising
`Errors`/`FrameErrors` with falling `Throughput` is a bottleneck or integrity
problem.

---

## 9. Quick triage table

| Symptom | First command | Likely cause |
|---------|---------------|--------------|
| `doctor` FAIL | `gpstream doctor` | bad/missing connstring, 0 segments |
| copy stopped | `gpstream status` → `resume` | crash; resume, don't restart |
| slow | metrics endpoint + `events.jsonl` | CPU/disk/NIC bottleneck |
| stopped on a table | review `conflicts.jsonl` | non-destructive default `stop`; opt into a policy |
| collation/locale warn | `dry-run` | mismatch; review, not a fail |
| indexes slow | `run --index-workers N` | background rebuild |
| low confidence | `dry-run --timeout` | partial plan; raise timeout |

---

## 10. When to escalate

- If `doctor` FAILs on a cluster you expect to be up, that is a
  reachability/segment problem, not a `gpstream` bug.
- If a copy dies and `resume` does not continue it, inspect the journal
  (`journal.jsonl`) and the `events.jsonl` / `audit.log` files before
  assuming corruption.
- If a `dry-run` shows a redistribution requirement, decide explicitly
  (accept it, or avoid it with `--no-redistribute`) rather than guessing.

> Honesty note: the physical-scale and fault-injection behaviors
> (100 TB–1 PB, kill -9 / tc netem / disk-fill) are validated on a
> **live stand**. The fake connection used by tests has no data plane, so
> `run` never falls back to it.
