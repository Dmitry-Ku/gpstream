# gpstream — Troubleshooting

A working FAQ for `gpstream`: what a failed command means, why it failed,
and how to recover. Every symptom here maps to a **real** command, flag, or
behavior in the tool; nothing is invented.

> If you are stuck, start with the three diagnostics: `gpstream doctor`,
> `gpstream security-check`, and `gpstream dry-run --timeout 60`. They tell
> you almost everything without touching live data.

---

## 1. `doctor` says FAIL / reachability

**Symptom:** `gpstream doctor` (or `security-check`) prints a FAIL.

| Cause | Check | Fix |
|-------|-------|-----|
| Missing / bad connection string | `--source`/`--dest` or `GPSTREAM_SOURCE`/`GPSTREAM_DEST` | set a valid connstring |
| Cluster unreachable | network / host / port | fix network; re-run `doctor` |
| **0 up-segments** | segment count | a 0-up report is a **FAIL**, not a pass — bring segments up |
| TLS too old | `--ssl-min-tls` (default 1.2) | TLS is **always** enforced; use >= 1.2 |

Key facts: a **missing connection string is a reachability FAIL** (not a
silent pass), and **0 up-segments is a FAIL**. `doctor` will never report a
healthy cluster that is actually down.

---

## 2. The copy stops / crashes

**Symptom:** a copy ends mid-way (kill -9, network drop, disk full).

**Recovery = resume, not restart.**

```
gpstream resume --mirror <F>            # resume the job in <F>
gpstream status  --mirror <F>          # see where it stopped
```

- A unit already in **terminal success state (`StateSwapped`)** is
  **skipped** on resume — it is not re-copied.
- `--auto-resume N` caps automatic resume attempts (default `0` = off).
- If you need to undo a completed/swap: `gpstream rollback --mirror <F>
  --job <ID> --yes`.
- Abandoned data: `gpstream cleanup` (requires `--yes`).

See `FAILOVER.md` for the full model. **Do not** silently restart a copy
that died — resume is the correct mechanism.

---

## 3. The copy is slow / a bottleneck

**Symptom:** throughput is lower than expected.

| Knob | Flag / env | What it does |
|------|-----------|--------------|
| Rate limit | `--rate-limit <Gbps>` (`GPSTREAM_RATE_LIMIT`) | caps throughput (accepts `bps`/`mbit`/`gbit`) |
| Adaptive | `--adaptive-rate-limit` | **reduce** on a bottleneck, **recover** when it frees |
| Workers | `--workers-per-pair N` (default 2) | parallelism per segment pair |
| Ports | `--data-port-range a-b` | MPP parallel load ports |
| Compression | `--compress` (default `zstd`, level 3) | reduces wire bytes |

Diagnose with the metrics endpoint (see `METRICS.md`): `Bytes`,
`BytesDecoded` (compression ratio), `Throughput()`, and `Errors`. A drop in
throughput with `Errors` climbing points to a network/CPU/disk bottleneck;
enable `--adaptive-rate-limit` to let `gpstream` back off and recover.

---

## 4. Conflict / collision

**Symptom:** a target table already exists and the copy skips it.

This is the **default** (`ConflictAction = SkipExisting`) — `gpstream` is
**non-destructive** and never overwrites existing data unless told to.

| Flag | Effect |
|------|--------|
| `--overwrite` | overwrite the existing target |
| `--overwrite-only-conflicts` | overwrite **only** on conflict |
| `--truncate` | truncate before load |
| `--append` | append to the target |
| `--skip-conflicts` (default **off** = strict) | treat conflicts strictly |

If you expected an overwrite, add `--overwrite` (or
`--overwrite-only-conflicts`). If you expected a skip, the default is
already doing it.

---

## 5. Collation / locale / distribution mismatch

**Symptom:** a warning about collation, locale, or distribution in
`dry-run` / `plan`.

| Check | Flag | Meaning |
|-------|------|---------|
| Collation | `--collation-check` | `LC_COLLATE` / collation differences (warn) |
| Locale | `--locale-check` | locale differences (warn) |
| Distribution | `--distribution-check` | distribution-key / policy differences |
| Redistribution plan | `--redistribution-plan` | pre-computed redistribution plan |

A **distribution** difference may require redistribution on the destination.
`gpstream` accounts for it with `--no-redistribute` and `--skew-threshold`
(default `0.05`). A warning is **not** a failure — review it in the
`dry-run` output and decide whether to proceed.

---

## 6. Indexes not created / slow

**Symptom:** indexes are missing or the copy is slow because of indexes.

Indexes are rebuilt **in the background** after data lands (v2 FR-17).

| Knob | Flag | Effect |
|------|------|--------|
| Background rebuild | `--background-indexes` | rebuild after data lands |
| Workers | `--index-workers N` | parallelism |
| Skip | `--no-indexes` | do not rebuild |
| Priority | `--index-priority` (default `size`) | order |
| Inspect | `gpstream indexes --mirror <F> --rebuild` | status / rebuild |

A `dry-run` shows the expected **background index jobs**. If indexes are
slow, raise `--index-workers` or let the background path continue
independently of the data copy.

---

## 7. `dry-run` / `plan` confidence is low

**Symptom:** `gpstream dry-run --timeout 60` reports low confidence or a
**partial plan**.

- A `--timeout` that is too small yields a **partial plan fragment** (the
  plan is incomplete, not wrong). Increase `--timeout`.
- Low **confidence** means some inputs (volume, collation, redistribution)
  could not be determined offline — the fields that cannot be predicted
  without a live copy are labelled as **predictions**, not fake numbers.
- Inspect the full plan: `gpstream plan --plan-file <F>`; compare with
  `gpstream report --report-json` / `--report-csv` / `--report-text`.

---

## 8. Metrics / logs

**Symptom:** you cannot see what a copy is doing.

| Need | Command / flag |
|------|----------------|
| Live counters | `--metrics-addr <A>` → Prometheus `/metrics` + `/health` |
| Job log | `gpstream logs --mirror <F> --job <ID>` |
| Status | `gpstream status --mirror <F>` |
| Report | `gpstream report --report-json\|--report-csv\|--report-text` |

Key counters (see `METRICS.md`): `Bytes`, `BytesDecoded`, `Errors`,
`FrameErrors`, and `Throughput()`. A rising `Errors`/`FrameErrors` with
falling `Throughput` is a bottleneck or integrity problem.

---

## 9. Quick triage table

| Symptom | First command | Likely cause |
|---------|---------------|--------------|
| `doctor` FAIL | `gpstream doctor` | bad/missing connstring, 0 segments |
| copy stopped | `gpstream status` → `resume` | crash; resume, don't restart |
| slow | `metrics` + `--adaptive-rate-limit` | CPU/disk/NIC bottleneck |
| skipped a table | (default) `SkipExisting` | non-destructive default; add `--overwrite` |
| collation/locale warn | `dry-run` | mismatch; review, not a fail |
| indexes slow | `indexes` + `--index-workers` | background rebuild |
| low confidence | `dry-run --timeout` / `report` | partial plan; raise timeout |

---

## 10. When to escalate

- If `doctor` FAILs on a cluster you expect to be up, that is a
  reachability/segment problem, not a `gpstream` bug.
- If a copy dies and `resume` does not continue it, inspect the mirror
  journal and the `logs` output before assuming corruption.
- If a `dry-run` shows a redistribution requirement, decide explicitly
  (accept with `--redistribution-plan`, or avoid with
  `--no-redistribute`) rather than guessing.

> Honesty note: the physical-scale and fault-injection behaviors
> (100 TB–1 PB, kill -9 / tc netem / disk-fill) are validated on a
> **live stand**. The fake connection used by tests has no data plane, so
> `copy` never falls back to it.
