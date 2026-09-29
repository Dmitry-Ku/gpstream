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
- Gaps: (a) **per-table fallback flag — DONE 28.09** (`--consistent-tables`, `Config.ConsistentTables`, mutually exclusive with the run-wide modes; per-table short snapshot via `pg.Pool.AcquireSnapshotConn` + `Manager.unitSource`/`perTableSnapshotConn`; the data COPY and the checksum read of one table share the unit's short REPEATABLE READ snapshot; the per-table discrepancy is documented in CLI_REFERENCE/CONFIGURATION/OPERATOR_GUIDE; live test `integration/f02_per_table_test.go` asserts t_a excludes post-snapshot writer rows while t_b includes pre-snapshot writer rows); (b) the **mandatory "concurrent source writes during copy are excluded" test EXISTS** (28.09 re-check): `integration/scale24_test.go TestScale24TablesConsistentSnapshot` — a writer loop hammers all 24 source tables during the copy, the destination must equal exactly the pre-copy rows, and the test asserts the writer DID write (live, M2, GREEN); (c) 2-DB concurrent-write + kill -9 acceptance needs real clusters (kill -9 on live GP6 done in E3).
- Test: snapshot tests exist; concurrent-write exclusion test exists (scale24); per-table fallback live test exists (f02_per_table).

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
- Evidence: `packaging/{rpm,deb,examples,systemd,logrotate.conf}`; `man/gpstream.8`; `config.go` TOML+YAML; `doctor.go:122-247` checks reachability/version/segments/disk/permissions/collation/**extensions**/TLS; `docs/` has QUICKSTART, INSTALL, OPERATOR_GUIDE, TROUBLESHOOTING, PERFORMANCE, SECURITY, CLI_REFERENCE, CONFIGURATION, ADMIN_GUIDE, FAILOVER, METRICS, adr; `LICENSE/CHANGELOG.md/SECURITY.md/CONTRIBUTING.md` present.
- **Extension check DONE (29.09):** doctor diffs `pg_extension` source vs dest (WARN naming the missing extensions; never FAILs; query fault = WARN) — `checkExtensions` in `doctor.go`, tests `doctor_extensions_test.go` (missing/all-present/query-fault).
- Gaps: **no `.github/ISSUE_TEMPLATE` / `PULL_REQUEST_TEMPLATE`** (`.github/` has only `workflows/`); **`docs/design.md` is absent** (required by F-08 for dependency justification); man set is a single `gpstream.8`, not the full `gpstream-run/...` family.
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
- **Sequences synced post-load (29.09, this cron run):** serial-column detection via
  `pg_get_serial_sequence` (portable to PG 9.4 / GP6); the sequence is recreated on
  the destination BEFORE the table DDL (step 3a, `CREATE SEQUENCE` with the standard
  serial parameters — `IF NOT EXISTS` and `pg_get_sequencedef` don't exist on the 9.4
  kernel; idempotency via an explicit existence check + 42P07 swallow) and its value
  is resynchronised AFTER the swap (`setval($1::regclass, last_value, true)`; the
  qualified name is passed as a regclass parameter — a bare `schema.seq` is not a valid
  regclass argument). Non-fatal, logged to events.jsonl (`sequence_synced` /
  `sequence_sync_failed`). Also fixed a latent bug found while testing:
  `isAlreadyExists` matched SQLSTATE 42P01 (undefined_table) instead of 42P07
  (duplicate_object) — it swallowed real DDL failures. Live acceptance:
  `integration/f11_sequence_test.go` (TestSequencePostLoadSync: seq created,
  last_value == source 105, next nextval == 106, events recorded;
  TestSequenceSyncResumeIdempotent: re-run idempotent, no duplicates).
- **Extension presence check DONE (29.09)** — doctor `extensions` check (diff of `pg_extension` source vs dest; same implementation as F-05, see there).
- **BLOCKED-INFRA**: "3 DB / 2 schemas / 1 renamed table, idempotent re-run" acceptance.

### F-12 — Stream compression — **DONE (28.09)**
- Spec: `--compression zstd|zlib|none` (default zstd), configurable level, between-hosts transparent, **auto-disable for very small tables**.
- Evidence: `transfer/compress/compress.go:1-36` zstd/zlib/none + level parsing (klauspost zstd); level degradation on CPU saturation driven by ratelimit. **Small-table auto-disable (28.09):** `config.SmallTableThreshold` (default 1 MiB, `--small-table-threshold`, env `SMALL_TABLE_THRESHOLD`, file `small_table_threshold`); `Manager.compressionOffFor`/`unitCompressors` decide per unit (size = `pg_total_relation_size`); `framedPipe` now takes the per-unit compressor pair, so a small table rides the transport with compression off while the rest of the run stays on the operator's mode (each unit owns its own pipe). Decision is logged to events.jsonl as a `compression_off` event (F-04).
- Closed: `compression_off` event + live acceptance `integration/f12_small_table_test.go` (one run: tiny table below threshold = compression off, big table above = zstd on; both byte-correct under `--verify full`; exactly one `compression_off` event, for the tiny table only).
- Test: compress 80.6%; manifest F-12 unit tests (decision table, pair swap, event emit/absence) + live integration PASS.

### F-13 — Secure connections — **DONE (29.09)**
- Spec: TLS 1.2+ (prefer 1.3) sender↔receiver, CA + mTLS, config 0600; PG `sslmode=verify-full`; creds only env/`.pgpass`; **audit log of actions**.
- Evidence: `transfer/pg/pg.go` `buildTLS(host,cert,key,CA,minTLS)` (29.09): when a CA/client-cert is supplied the TLS config now sets **ServerName = the DSN host**, so `sslmode=verify-full` (the enforced default, `ensureVerifyFull`) actually validates the server's hostname — a wrong certificate is a hard, diagnosed failure (`x509.HostnameError`, F-13 acceptance «при неверном сертификате — отказ с диагностикой»); an IP-literal host degrades to verify-ca (no verifiable name). A `--ssl-ca` bundle is loaded into RootCAs for chain validation; an unreadable/CA-less bundle or a bad client keypair is a **hard error at connect** (no silent downgrade — the old code swallowed both). mTLS client cert, `--ssl-min-tls` (1.2 default) unchanged. pgx reads `.pgpass` (pgpassfile dep); creds via `GPSTREAM_*` env (`config.go:225`), never argv/config.
- **Audit log of actions (29.09):** `manifest.AuditLog` (job.go) + `Journal.AuditPath()` = `<state-dir>/<operation-id>/audit.log` (0600, alongside journal/events/conflicts). Recorded: `run` (operation-id, units, copy-mode, on-unit-failure, compression, consistency mode), `run-finished` (units/rows/skipped/failed + error), `resume` (requeued, attempt) and `cleanup` (mode, units) — logged BEFORE the state dir is removed.
- Test: `TestBuildTLSVerifyFullHostname` (ServerName set / IP exempt), `TestBuildTLSLoadsCA` (RootCAs; missing/CA-less file → error), `TestHostOf` (both DSN forms), `TestBuildTLSRejectsWrongHostname` (REAL handshake against a local TLS server: wrong DSN host → `certificate is valid for wrong.example.com, not right.example.com`; matching host → success; no CA → rejected), `TestAuditLogWritesJSONLines` (format + 0600), `TestJournalAuditPath`.
- **BLOCKED-INFRA**: wireshark encrypted-traffic acceptance (real network).

### F-14 — Rate limiting + adaptive control — **DONE (28.09)**
- Spec: `--max-bw` global ceiling; per-host-pair token bucket; adaptive AIMD (detect receiver CPU/disk/queue bottleneck → step down ~25% floor ~1 Gbps, recover on stabilize); **each shift logged to `events.jsonl`**.
- Evidence: `ratelimit/limiter.go:1-36` per-host-pair adaptive limiter (step ~25%, floor ~1 Gbps); `ratelimit/pacer.go:9,66` pacer throttles the data path to the current rate; `manager.go:97-117` builds limiter+pacer and shares one pacer across the pool so total flow stays under the ceiling.
- **Closed (28.09):** every rate shift is now logged to `events.jsonl` as a `rate_limit_shift` event (old/new Gbps + reason) via `Manager.onRateChange` — the F-14 "каждый сдвиг лимита — диагностическое сообщение" requirement. Regression test `TestManagerOnRateChangeEmitsEvent`.
- Test: ratelimit 92.3%.

### F-15 — Data redistribution — **DONE (29.09)**
- Spec: plan flags redistribution (topologies differ) + coordinator load estimate; **indexes not created until after load**; segment-fill balance metric (max/avg) with skew warning.
- Evidence: `planner.go` `RedistributionPlan`/`Conflicts`/redistribution warnings + skew (numeric); index rebuild deferred to the background pool AFTER the swap (FR-17). **Live segment balance (29.09):** `dest.BalanceTable`/`BalanceMaxAvg` (internal/transfer/dest/balance.go) measure the per-segment fill of each copied table (`gp_segment_id, count(*) GROUP BY`) after the swap; `manager.measureBalance` (step 9c) records it in `Stats.Balances` + a `segment_balance` events.jsonl event; the final report prints max/avg per table and a `WARNING` when `max/avg > --segment-balance-threshold` (default 1.5, negative = metric only). Non-fatal + Greenplum-only (plain PG degrades silently).
- Gaps: none in-env. **BLOCKED-INFRA**: "copy time ≥30% higher without index deferral" demo + gp_segment_id distribution check on real clusters (the live metric now feeds that acceptance).

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
| F-02 | PARTIAL | per-table fallback flag DONE 28.09 (`--consistent-tables`); (concurrent-write test EXISTS — scale24; per-table live test f02_per_table); 2-DB concurrent-write + kill -9 acceptance needs real clusters |
| F-03 | PARTIAL | no capped leaf-partition parallelism; no overhead batching |
| F-04 | PARTIAL | events.jsonl DONE (28.09: rate-shift/conflict/failed/re-read); no live ETA/queue metrics in status |
| F-05 | PARTIAL | doctor extension check DONE (29.09); no issue/PR templates; no design.md |
| F-06 | DONE | --on-unit-failure continue/abort implemented (28.09); 50-fault serial acceptance infra-blocked |
| F-07 | PARTIAL | mechanism real + live E3 kill -9; full row+checksum acceptance infra-blocked |
| F-08 | PARTIAL | docs/design.md missing; static build unconfirmed |
| F-09 | PARTIAL | 7 packages <80%; CI flake (pairmap wall-clock); integration/scale infra-blocked |
| F-10 | DONE (M3 + variant B, 28.09) | conflicts stop the run by default (strict); `--skip-conflicts`/`--overwrite`/`--truncate`/`--append` proceed; conflicts.jsonl; probe nominal (spec-compliant) |
| F-11 | PARTIAL | sequences synced post-load (29.09); extension check DONE (29.09) |
| F-12 | DONE (28.09) | small-table auto-disable (`--small-table-threshold`, default 1MiB; per-unit; `compression_off` event; live acceptance) |
| F-13 | DONE (29.09) | verify-full hostname enforced (ServerName=DSN host; wrong-cert = diagnosed refusal; CA loaded, no silent downgrade); audit.log of actions (run/run-finished/resume/cleanup); (wireshark traffic check infra-blocked) |
| F-14 | DONE (28.09) | rate-limit shifts logged to events.jsonl (rate_limit_shift: old/new/reason) |
| F-15 | DONE (29.09) | live segment-fill balance max/avg per copied table + warning above --segment-balance-threshold (final report + events.jsonl); (index-deferral 30% demo + live distribution check infra-blocked) |
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
3. **F-02 per-table fallback flag** — DONE 28.09 (`--consistent-tables`; per-table
   short snapshot + documented discrepancy + live test f02_per_table).
4. **F-12 small-table auto-disable** — DONE 28.09 (`--small-table-threshold`,
   default 1 MiB; per-unit decision in the Manager; `compression_off` event;
   live acceptance f12_small_table: small table off + big table zstd in one
   run, both byte-correct).
5. **F-13 verify-full hostname + audit log** — DONE 29.09 (buildTLS sets
   ServerName=DSN host → verify-full validates the name; wrong certificate
   = diagnosed handshake refusal (real-handshake regression test); CA
   bundle loaded into RootCAs, bad CA/keypair = hard error; audit.log of
   actions: run/run-finished/resume/cleanup at <state>/audit.log 0600).
6. **F-11 post-load sequence `setval`** — DONE 29.09 (serial detection via
   pg_get_serial_sequence; CREATE SEQUENCE before table DDL + setval after
   swap; 42P01/42P07 isAlreadyExists fix; events.jsonl sequence_synced /
   sequence_sync_failed; live acceptance f11_sequence_test.go).
7. **F-05/F-11 doctor extension check** — DONE 29.09 (doctor `extensions`
   check: `pg_extension` diff source vs dest, WARN naming missing
   extensions; tests doctor_extensions_test.go). (next)
8. **F-15** — [CLOSED 29.09] live segment balance max/avg metric:
   `dest.BalanceTable` + `manager.measureBalance` (step 9c) + final-report
   section + `segment_balance` event + `--segment-balance-threshold`
   (default 1.5, warning gate). Tests: balance_test.go (dest) +
   manager_balance_test.go (manifest, GP fake / plain-PG degrade).
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
