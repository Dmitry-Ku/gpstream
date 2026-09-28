# FEATURE AUDIT F-01..F-18 (TASK-SPEC-v4.md §6)

Honest, evidence-backed status of each feature against the actual code (read-only audit; no code changed).
Repo:  /opt/gpstream. Go 1.27.1. All `file:line` citations are to functions/lines actually read.

Status legend: **DONE** / **PARTIAL** (named gap) / **STUB** / **MISSING** / **BLOCKED-INFRA** (needs external GP6 clusters / real network / build host / 100TB→1PB pilot).

## Per-feature findings

### F-01 — Streaming copy, no intermediate storage — **DONE (mechanism)**
- Spec: segment→network→receiver in memory/buffers; no ~DB-sized files on either disk.
- Evidence: `internal/manifest/manager.go` `runUnit` (312+) streams each unit through an in-memory framed pipe (`net.Pipe()`); a TCP `NewNetPipe` variant exists but is **only used in tests**, not the production single-binary path. Data never touches disk beyond the journal/manifest.
- Gap: none in the mechanism.
- Test: `internal/transfer` (90.5%), `internal/manifest` (81.2%).
- **BLOCKED-INFRA**: the 100+ TB acceptance (free-space < 1 GB) needs real clusters.

### F-02 — Source-state consistency + fragment cleanup — **PARTIAL**
- Spec: per-DB `READ ONLY, REPEATABLE READ` snapshot; mandatory concurrent-write-during-copy test; per-table fallback flag; `cleanup --operation-id`/`--all-incomplete`.
- Evidence: `manager.go:51-55` `snapshotMode`/`snapshotTx` — a real long-running read-only REPEATABLE READ transaction on the source, all unit COPYs read from it. `manager.go:160-215` releases snapshot after all units. Tests: `manifest/manager_snapshot_test.go:38,67,94` (DB/cluster/no-snapshot). `cmd/gpstream/main.go:880+` `cmdCleanup` is real (`--operation-id`, `--all-incomplete`, `--mode drop`, `--yes`).
- Gaps: (a) **per-table fallback flag is missing** (`grep per-table` → none; only `ConsistentDB`/`ConsistentCluster` in `config.go:133-136`); (b) the **mandatory "concurrent source writes during copy are excluded" test EXISTS** (28.09 re-check): `integration/scale24_test.go TestScale24TablesConsistentSnapshot` — a writer loop hammers all 24 source tables during the copy, the destination must equal exactly the pre-copy rows, and the test asserts the writer DID write (live, M2, GREEN); (c) 2-DB concurrent-write + kill -9 acceptance needs real clusters (kill -9 on live GP6 done in E3).
- Test: snapshot tests exist; concurrent-write exclusion test absent.

### F-03 — Performance on small AO partitioned-table tail — **PARTIAL**
- Spec: pipeline (read N+1 while writing N), batching of housekeeping, leaf-partition parallelism with a cap, ANALYZE/VACUUM post-job only.
- Evidence: bounded worker pool `WorkersPerPair` (`manager.go:160-215`); `planner.go:35` `LeafPart []string` for routing.
- Gaps: leaf-partition **parallelism with an explicit concurrency cap** is not a distinct implemented mechanism (leaf parts are listed but I found no capped per-leaf-partition worker fan-out); housekeeping batching and the "<100 ms/table overhead" target are not implemented/measured.
- **BLOCKED-INFRA**: 100 000 × 1 MB ≤ 60 min acceptance.

### F-04 — Monitoring & metrics — **PARTIAL**
- Spec: Prometheus `/metrics`, human-readable `status`, mandatory metric set (throughput, rows, compression, queue depth, active streams, checksum errors, retries, per-pair rate-limit, **ETA**, status counters, auto-recovery events), JSON logs + **`events.jsonl`** for all diagnostic events.
- Evidence: `internal/monitor` (98.0%) renders metrics + a Prometheus endpoint; `cmdStatus` (`main.go:826+`) prints a per-unit table.
- **Closed (28.09):** `events.jsonl` is implemented — `monitor.EventLog` (`internal/monitor/events.go`, one JSON object per line `{ts,kind,...}`) + `Journal.EventsPath()`; the Manager emits `rate_limit_shift` (F-14), `conflict` (action=stop/skip), `unit_failed`, `unit_reread` (auto-recovery). No-op when the journal has no file mirror. Tests: `events_test.go` (format, no-op, lazy-open, concurrent) + `manager_events_test.go` (strict/skip conflict, unit failure, rate shift, live paths).
- Gaps: **live ETA is not a metric** (only a "prediction (timed out)" dry-run string at `main.go:588`); `status` shows no live throughput/queue depth.
- Test: `internal/monitor` 98.0%.

### F-05 — Delivery, self-diagnosis, docs — **PARTIAL**
- Spec: tar.gz+RPM+DEB, `/etc/gpstream/gpstream.toml`, `doctor` (all segments, versions, encoding match, collation, **extensions MADlib/PostGIS/orafce**, disk, TLS, ports, CPU), man pages, README/docs/LICENSE/CHANGELOG/SECURITY/CONTRIBUTING + **issue/PR templates**.
- Evidence: `packaging/{rpm,deb,examples,systemd,logrotate.conf}`; `man/gpstream.8`; `config.go` TOML+YAML; `doctor.go:122-247` checks reachability/version/segments/disk/permissions/collation/TLS; `docs/` has QUICKSTART, INSTALL, OPERATOR_GUIDE, TROUBLESHOOTING, PERFORMANCE, SECURITY, CLI_REFERENCE, CONFIGURATION, ADMIN_GUIDE, FAILOVER, METRICS, adr; `LICENSE/CHANGELOG.md/SECURITY.md/CONTRIBUTING.md` present.
- Gaps: **doctor has no extension check** (MADlib/PostGIS/orafce — `grep` → none); **no `.github/ISSUE_TEMPLATE` / `PULL_REQUEST_TEMPLATE`** (`.github/` has only `workflows/`); **`docs/design.md` is absent** (required by F-08 for dependency justification); man set is a single `gpstream.8`, not the full `gpstream-run/...` family.
- **BLOCKED-INFRA**: RPM/DEB build host + clean-host acceptance.

### F-06 — Resilience + `--auto-resume N` — **DONE** (flag + abort mode closed 28.09)
- Spec: auto-resume a failed chunk ≤ N times; `--on-unit-failure=continue|abort` (default continue); failed unit re-queued by `resume`; controller re-attach from manifest+state.
- Evidence: `--auto-resume` (`main.go`); `processUnitSafe` recovers panics, marks the unit `failed`, keeps the worker alive; `Run` loop continues past a failing unit by default (`continue`) or stops at the first failure (`--on-unit-failure=abort`, `stopOnFailure` gate in the worker pool — in-flight units finish, none new starts; `TestManagerOnUnitFailureAbort`/`TestManagerOnUnitFailureContinue`); re-attach via journal `LoadFromMirror` + `resume`. The `unit_failed` event now records `on_unit_failure` (the mode in effect).
- Gaps: the dead-worker's in-flight unit is marked failed for `resume` rather than re-queued mid-run (acceptable, not the literal re-queue).
- **BLOCKED-INFRA**: serial 50-fault acceptance test.

### F-07 — Integrity after failure, minimal rollback — **PARTIAL**
- Spec: after any crash, verify checksums of sent chunks, roll back no deeper than last verified chunk, no half tables, final row+checksum reconciliation.
- Evidence: `manifest/resume/resume.go:63` `CrashRecovery` rolls back incomplete + re-sends; staging-table + atomic swap in `runUnit` (`manager.go:312+`); resume tests `main_test.go:189,197`, `main_v4_coverage_test.go:91,97,108` (incl. `TestResumeRequeuesIncomplete`).
- Gaps: the literal "kill in the middle of a chunk ⇒ no half tables + full row+checksum reconciliation" acceptance is not an in-repo automated test (needs real clusters).
- Mechanism is real; acceptance is **BLOCKED-INFRA**.

### F-08 — Minimal dependencies, free components — **PARTIAL**
- Spec: static build; stdlib + small set of vetted free libs; <10 deps; each external dep justified in `docs/design.md`; no telemetry.
- Evidence: `go.mod` has 5 direct deps (BurntSushi/toml, embedded-postgres [test-only], jackc/pgx/v5, klauspost/compress, yaml.v3) — well under 10, all pure Go, no telemetry.
- Gaps: **`docs/design.md` (dependency justification) is missing**; static-build flag not confirmed in `Makefile`.
- **BLOCKED-INFRA**: two-distro/glibc run acceptance.

### F-09 — Automated tests — **PARTIAL**
- Spec: unit (manifest/checksum/limiter/planner/mapping/idempotency); integration on 2×GP6; fault injection; scale 100k + 100 Gbit soak; CI (lint, unit, x86_64/aarch64 build, govulncheck); **unit coverage ≥ 80%**.
- Evidence: strong unit suites — doctor 98.0%, monitor 98.0%, topology 98.4%, transfer 90.5%, ratelimit 92.3%, plan 88.2%, index 88.9%, resume 91.7%, source 92.1%, transfer/dest 86.3%, manifest 81.2%, compress 80.6%; `ci.yml` has an arch `matrix`.
- Gaps: **coverage is below 80% for `internal/config` (79.4%), `internal/transfer/pg` (72.4%), `cmd/gpstream` (73.7%)**; helper cmds `cmd/e2e`, `cmd/epg`, `cmd/pgboot` are 0%.
- **BLOCKED-INFRA**: 2×GP6 integration, iptables fault injection, 100k scale, 100 Gbit soak (self-hosted runner).

### F-10 — `--dry-run` full preflight — **DONE (M3, 28.09)**
- Spec: no-dest-change preflight: availability, size+duration estimate (timed probe or nominal, marked "prediction"), conflict diagnosis, collation/encoding warnings, action plan, `--timeout` (default 120 s), idempotent plan (hash), `--skip-conflicts` + `conflicts.jsonl` + manifest, `--overwrite` checksum-skip.
- Evidence: `cmdDryRun` (`main.go:~520-700`) — availability/size/conflicts/collation/plan/timeout are real; timeout→"prediction/forecast" path; plan idempotency via hash.
- **Closed in M3 (28.09, commit pending):** (1) **collation detection was buggy** — it compared only the FIRST `pg_database` row (the `postgres` service DB on a real cluster), so it mis-reported the DB and missed a real diff on the copied DB; now compares each source DB against its destination DB (post-rename), whole-cluster when no selection. Fixed + regression test (`TestDetectCollationsTargetsCopiedDB`, `...WholeClusterCoversAllDBs`) + **live acceptance** proves it reports the copied DB (`srcdb` C→C.utf8), not `postgres`. (2) **`conflicts.jsonl`** now written (`Journal.WriteConflicts`/`ConflictsPath`, wired in `cmdRun`; unit tests) — conflicts persisted alongside the journal, not just stdout. (3) **live F-10 acceptance test** `TestDryRunAcceptance` (+ `TestDryRunOverwriteAction`): exact conflict list + action, exact collation diff, byte-identical double dry-run, and the destination is left untouched.
- **Still open (honest):** (b) RESOLVED by variant B (spec F-10 §8, 28.09): `--skip-conflicts` now changes runtime behavior — the DEFAULT (skip-existing, `--skip-conflicts` OFF) is STRICT and **stops the run** on a conflict (`ConflictError`, worker loop halts, nothing clobbered); `--skip-conflicts`/`--skip-existing` skip; `--overwrite`/`--truncate`/`--append` proceed. Docs (QUICKSTART §8, OPERATOR_GUIDE §4, man) flipped to the strict default. (c) the duration estimate is nominal (spec-compliant: "расчёт от номинала — с пометкой «прогноз»"), not a timed probe.
- **Also closed in M3 (28.09):** `--overwrite` checksum-skip (spec §8: tables whose checksum already matches are skipped) — `dest.Checksum` (mirrors `source.Checksum`, order-independent digest) + a gate in `processUnit` (only `Overwrite` + dest exists + digests equal); `TestManagerOverwriteChecksumSkip` (the mismatch/re-copy case is covered by `TestManagerOverwriteSwap`).

### F-11 — Object selection, name mapping, metadata/data — **PARTIAL**
- Spec: `--dbs/--schemas/--tables` (+patterns); default no-overwrite; `--include metadata,data`, `--ddl-only`, `--data-only`; `--map-db/--map-schema/--map-table`; DDL + **sequences synced post-load**; extension check.
- Evidence: `main.go:165-176` `--include/--metadata-only/--ddl-only/--data-only`; rename/mapping flags present; planner builds per-table units.
- Gaps: **post-load `setval` for sequences is not implemented** (`grep create sequence/last_value` in the load path → none); **extension presence check missing** (same gap as F-05).
- **BLOCKED-INFRA**: "3 DB / 2 schemas / 1 renamed table, idempotent re-run" acceptance.

### F-12 — Stream compression — **PARTIAL**
- Spec: `--compression zstd|zlib|none` (default zstd), configurable level, between-hosts transparent, **auto-disable for very small tables**.
- Evidence: `transfer/compress/compress.go:1-36` zstd/zlib/none + level parsing (klauspost zstd); level degradation on CPU saturation driven by ratelimit.
- Gaps: **small-table auto-disable is missing** (only level auto-lower exists; no size threshold that turns compression off for tiny tables).
- Test: compress 80.6%.

### F-13 — Secure connections — **PARTIAL**
- Spec: TLS 1.2+ (prefer 1.3) sender↔receiver, CA + mTLS, config 0600; PG `sslmode=verify-full`; creds only env/`.pgpass`; **audit log of actions**.
- Evidence: `transfer/pg/pg.go:66` `buildTLS(cert,key,CA,minTLS)`; `pg.go:61 ensureVerifyFull` (28.09 re-check): when the DSN has no explicit `sslmode`, the connection is forced to `sslmode=verify-full` (an explicit value is respected); pgx reads `.pgpass` (pgpassfile dep); creds via `GPSTREAM_*` env (`config.go:225`); `doctor.go:240` TLS handshake check.
- Gaps: **production data path is the in-memory pipe, so TLS between sender/receiver is not actually exercised** (the TCP/TLS mode exists but is test-only); **`ServerName:""` (`pg.go:176`) means the verify-full hostname check is not enforced** (verify-full is ON as default sslmode, but the Go TLS layer still skips host-name verification); **audit log of actions is missing** (`grep auditlog` → none).
- **BLOCKED-INFRA**: wireshark encrypted-traffic acceptance.

### F-14 — Rate limiting + adaptive control — **DONE (28.09)**
- Spec: `--max-bw` global ceiling; per-host-pair token bucket; adaptive AIMD (detect receiver CPU/disk/queue bottleneck → step down ~25% floor ~1 Gbps, recover on stabilize); **each shift logged to `events.jsonl`**.
- Evidence: `ratelimit/limiter.go:1-36` per-host-pair adaptive limiter (step ~25%, floor ~1 Gbps); `ratelimit/pacer.go:9,66` pacer throttles the data path to the current rate; `manager.go:97-117` builds limiter+pacer and shares one pacer across the pool so total flow stays under the ceiling.
- **Closed (28.09):** every rate shift is now logged to `events.jsonl` as a `rate_limit_shift` event (old/new Gbps + reason) via `Manager.onRateChange` — the F-14 "каждый сдвиг лимита — диагностическое сообщение" requirement. Regression test `TestManagerOnRateChangeEmitsEvent`.
- Test: ratelimit 92.3%.

### F-15 — Data redistribution — **PARTIAL**
- Spec: plan flags redistribution (topologies differ) + coordinator load estimate; **indexes not created until after load**; segment-fill balance metric (max/avg) with skew warning.
- Evidence: `planner.go` `RedistributionPlan`/`Conflicts`/redistribution warnings + skew (numeric); `manager.go:434-438` index rebuild happens at step 7 (after copy, before swap) — index deferral is real.
- Gaps: **segment balance max/avg metric is missing from the monitor/report** (`grep balance/skew/max.avg` in `monitor/metrics.go` → none); skew is computed in the planner but not surfaced as a live per-segment balance metric.
- **BLOCKED-INFRA**: "copy time ≥30% higher without index deferral" demo + gp_segment_id distribution check.

### F-16 — Cross-OS compatibility — **PARTIAL**
- Spec: static binary; byte-for-byte data; **mandatory LC_COLLATE (DB and column) + encoding comparison** with warnings in doctor/dry-run/final report.
- Evidence: byte-for-byte COPY; `doctor.go:224-237` `checkCollation` compares `datcollate` (DB level); `planner.go:406,556` encoding comparison (`pg_encoding_to_char`) + `CollationDiff`.
- Gaps: **column-level collation (`attcollation`) comparison is missing** (only DB-level); static build not confirmed.
- **BLOCKED-INFRA**: two-distro/glibc run + `C`↔`en_US.UTF-8` data-correctness acceptance.

### F-17 — Background index re-creation — **DONE (core)**
- Spec: after data copy, a **background pool** building secondary indexes then PK/unique **last**, with `--index-parallel`, progress in status/metrics, retry of failed; GP6 has no `CREATE INDEX CONCURRENTLY` so "background" = async post-load pool.
- Evidence (re-verified 28.09, supersedes the earlier "near-STUB" finding): `index.Pool` (`index/indexer.go`) is a real bounded prioritized pool — N workers (`--index-workers`/`--index-parallel`), `--index-priority size|manual`, `onProgress` wired to `monitor.Metrics` (queue depth + done), `requeueIndexes` (`manager.go:796`) re-enqueues units killed after swap (idempotent `createOneIndex`: existing index → REINDEX by name), `IndexDone` journal flag prevents duplicate REINDEX on keep. Ordering **secondary-first, PK/unique last** is implemented in `source.Indexes` (`source.go:184-193`, `indisunique/indisprimary`). Jobs are enqueued after the atomic swap (post-load, read-only window) — the "background" semantics the spec asks for.
- Remaining gaps: the old synchronous `Builder.Rebuild` (single REINDEX) still exists as a separate path; the "data available read-only during index phase" acceptance is live-verifiable on the testbed but not yet run.
- Test: index 87.8%, requeue covered by t7.

### F-18 — Naming — **DONE**
- Binary, package, module, metrics, docs all use **gpstream** (`go.mod` module `github.com/gpstream/gpstream`, `cmd/gpstream`, `man/gpstream.8`). No gap.

## Coverage snapshot (28.09, `go test -cover`, non-race, HEAD 33c174e)
```
config        65.4%   transfer/pg   46.5%   cmd/gpstream  68.3%
doctor        88.7%   transfer      82.3%   cmd/{e2e,...} 0.0% (test harness)
index         87.8%   transfer/dest 80.4%
manifest      58.1%   source        75.0%   route         72.4%
resume        91.7%   segstream     77.1%
monitor       98.2%   compress      80.6%
plan          89.1%   selftest      88.8%
topology      91.5%   ratelimit     92.3%
```
"Unit coverage ≥ 80%" (F-09) is **not met** for: manifest (58.1%), config (65.4%),
cmd/gpstream (68.3%), route (72.4%), source (75.0%), segstream (77.1%),
transfer/pg (46.5%). The 0% entries are test-harness helpers, not product code.

**CI-FLAKE FOUND (28.09):** `internal/route TestPairMapBruteForceNonGolden`
fails under `-cover` (231–328 s > its hard 120 s wall-clock limit; plain run
36 s OK). `.github/workflows/ci.yml` runs `go test -cover` → CI will be red on
this test under load. Fix: raise the limit (e.g. 600 s) or move the test to a
`heavy` tag/schedule.

## Summary status table
| Feat | Status | One-line gap |
|------|--------|--------------|
| F-01 | DONE | (100TB acceptance infra-blocked) |
| F-02 | PARTIAL | no per-table fallback flag; (concurrent-write test EXISTS — scale24) |
| F-03 | PARTIAL | no capped leaf-partition parallelism; no overhead batching |
| F-04 | PARTIAL | events.jsonl DONE (28.09: rate-shift/conflict/failed/re-read); no live ETA/queue metrics in status |
| F-05 | PARTIAL | doctor no extension check; no issue/PR templates; no design.md |
| F-06 | DONE | --on-unit-failure continue/abort implemented (28.09); 50-fault serial acceptance infra-blocked |
| F-07 | PARTIAL | mechanism real + live E3 kill -9; full row+checksum acceptance infra-blocked |
| F-08 | PARTIAL | docs/design.md missing; static build unconfirmed |
| F-09 | PARTIAL | 7 packages <80%; CI flake (pairmap wall-clock); integration/scale infra-blocked |
| F-10 | DONE (M3 + variant B, 28.09) | conflicts stop the run by default (strict); `--skip-conflicts`/`--overwrite`/`--truncate`/`--append` proceed; conflicts.jsonl; probe nominal (spec-compliant) |
| F-11 | PARTIAL | no post-load sequence setval; no extension check |
| F-12 | PARTIAL | no small-table auto-disable |
| F-13 | PARTIAL | data path not TLS-exercised; verify-full ON by default but hostname check off (ServerName:""); no audit log |
| F-14 | DONE (28.09) | rate-limit shifts logged to events.jsonl (rate_limit_shift: old/new/reason) |
| F-15 | PARTIAL | segment balance max/avg metric missing |
| F-16 | PARTIAL | no column-level collation compare |
| F-17 | DONE (core) | index.Pool: workers, size priority, PK-last, idempotent, requeue, progress (28.09 re-verified) |
| F-18 | DONE | — |

## Closable in-env (no external cluster needed) — prioritized by impact (re-ordered 28.09)
0. **CI-FLAKE fix** — `TestPairMapBruteForceNonGolden` wall-clock limit (120 s)
   is exceeded under `-cover` (231–328 s) → CI red. Raise the limit or move the
   test to a `heavy` tag. (10 min, do first.) [CLOSED 28.09, heavy tag + schedule]
1. **F-04/F-14 `events.jsonl`** — [CLOSED 28.09] diagnostic event log
   implemented (`monitor.EventLog` + Manager hooks: rate-shift, conflict,
   unit-failed, unit-reread). Remaining from this item: F-13 audit log of
   actions (separate).
2. **F-06 `--on-unit-failure=continue|abort`** — DONE 28.09 (flag + abort mode
   + tests; `unit_failed` event carries the mode).
3. **F-02 per-table fallback flag** — explicit fallback mode (the mandatory
   concurrent-write exclusion test ALREADY EXISTS — scale24).
4. **F-12 small-table auto-disable** — compression-off threshold for tiny tables.
5. **F-13 verify-full hostname** — set TLS `ServerName` from the DSN host so
   verify-full actually validates the name (sslmode default is already
   verify-full via ensureVerifyFull).
6. **F-11 post-load sequence `setval`** sync.
7. **F-05/F-11 doctor extension check** — detect MADlib/PostGIS/orafce presence
   in the destination.
8. **F-15** — add the segment balance max/avg metric to the monitor/report.
9. **F-16** — column-level (`attcollation`) collation comparison.
10. **F-08/F-05 docs** — add `docs/design.md` (dependency justification) +
    `.github/ISSUE_TEMPLATE` + `PULL_REQUEST_TEMPLATE` + govulncheck in CI.
11. **F-09 coverage** — lift manifest (58%)/config (65%)/cmd/gpstream (68%)/
    source (75%)/route (72%)/segstream (77%)/transfer/pg (46%) to ≥80%.
(Closed since the original list: F-17 background index pool, F-10
conflicts.jsonl, F-10 strict default, F-09 live iptables/RST fault injection.)

## Blocked on infra (cannot finish in this environment)
- F-01: 100+ TB no-disk acceptance (real GP6 clusters).
- F-02: kill -9 + 2-DB concurrent-write acceptance (real clusters).
- F-03: 100 000 × 1 MB ≤ 60 min acceptance (real clusters + scale gen).
- F-09: 2×GP6 integration, iptables fault injection, 100k scale, 100 Gbit soak (self-hosted runner).
- F-13: wireshark encrypted-traffic verification (real network).
- F-15: index-deferral 30% time demo + gp_segment_id distribution (real clusters).
- F-16: two-distro/glibc static-binary run (build host).
- F-05: RPM/DEB build + clean-host acceptance (build host).
- Milestone M7: 100 TB → 1 PB pilot (≥50 Gbit/s).
