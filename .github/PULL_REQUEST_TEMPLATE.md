---
name: Pull request
about: A change to gpstream
labels: ''
---

<!--
Gate (must all be green locally before review):
  go build ./...
  go vet ./... && go vet -tags integration ./...
  gofmt -l <touched files>   # must print nothing
  go test -race -count=1 ./...
  go test -race -count=1 -tags integration ./...   # slow; run at least the
  # packages you touched plus ./tests/integration/ if you touched data paths
CI also runs govulncheck (TASK-SPEC §9) — do not add a dependency without
recording it in docs/design.md (F-08: fixed version + justification per
external package).
-->

**What changes and why:** the problem, the chosen approach, and the alternative
you rejected (one line each). Link the issue/spec item (F-xx / NFR / §8 CLI).

**CLI / audit / events surface:** if the PR touches `cmd/gpstream`, state
whether the CLI surface, `journal.jsonl`/`audit.log`/`events.jsonl` schema,
or Prometheus metrics changed, and update `docs/CLI_REFERENCE.md`,
`docs/OPERATOR_GUIDE.md`, `man/gpstream.8`, and `FEATURE-AUDIT-F01-F18.md`
in the same PR.

**Data-path impact:** if the PR touches source→dest streaming, snapshot
model, swap, or checksums: state explicitly which F-xx guarantees
(snapshot consistency, byte-for-byte data, idempotent resume, atomic swap)
are affected and add or update the live test that proves them
(`tests/integration/*_test.go`).

**Tests added / changed:** file list. New data-path behavior needs a live
test, not only a unit test.

**Rollback / compatibility:** journal schema changes must stay backward
compatible with already-written journals (resume must work across the change)
— state how.

**Checklist**
- [ ] `go build ./...` / `go vet ./...` / `go vet -tags integration ./...` green
- [ ] `gofmt -l` clean on touched files
- [ ] unit + (touched) integration tests pass with `-race`
- [ ] docs/CLI_REFERENCE + OPERATOR_GUIDE + man + FEATURE-AUDIT updated (if surface changed)
- [ ] no new dependency without a `docs/design.md` entry (F-08)
- [ ] no credentials, hostnames of real clusters, or 100 Tb-scale data in the diff
