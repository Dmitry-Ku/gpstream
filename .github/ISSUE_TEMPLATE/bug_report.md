---
name: Bug report
about: Data-corruption, crash, or behavior bug in gpstream
labels: bug
---

<!--
gpstream is a 100 TB–1 PB Greenplum-to-Greenplum copy tool. Before posting,
check: `gpstream doctor` output, the run's journal.jsonl + audit.log under the
state dir, and events.jsonl (rate-shift/conflict/unit_failed/collation_diff
events). Attach those — a bug report without them will be closed as
incomplete.
-->

**Version / commit:** `git describe --tags` or commit SHA; how the binary was
built (CI artifact `gpstream-linux-amd64` or `make build`).

**What happened:** the exact command line, what gpstream did, what you
expected instead. For data-corruption claims: source/dest table, row counts
before/after, and the verification (checksum/SHA256) you ran.

**Environment (both sides):** GPDB version (`gpversion`), OS/glibc
(`ldd --version`), kernel (`uname -r`), network (100GbE/RoCE or testbed).

**Reproduction:** minimal `gpstream run` command with `--operation-id`,
`--state-dir`, and the source/dest DSNs (credentials redacted — they must not
appear in env or .pgpass in the report). Smallest failing table set.

**Logs:** paste or attach (0600-safe, no credentials):
- `journal.jsonl` (the durable unit state mirror)
- `audit.log` (run/run-finished/resume/cleanup)
- `events.jsonl` (diagnostic events)
- `gpstream doctor` output, `gpstream status` output
- full stderr of the failing command

**State-dir snapshot** (crash/resume bugs): `tar czf state.tar.gz <state-dir>/<operation-id>` (journal, audit, events, conflicts.jsonl).
