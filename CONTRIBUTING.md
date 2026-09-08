# Contributing to gpstream

Thanks for your interest! gpstream is a large-data migration tool for
Greenplum clusters; contributions are reviewed against
[TASK-SPEC-v4](TASK-SPEC-v4.md), which is the single source of truth for
feature requirements (F-01…F-18) and non-functional constraints (§7).

## Process

1. Branch from `main`: `feat/<milestone>-<short-name>` or
   `fix/<short-name>`.
2. Commit with [Conventional Commits](https://www.conventionalcommits.org/)
   (`feat: …`, `fix: …`, `docs: …`, `test: …`, `chore: …`).
3. Before opening a PR:
   ```sh
   go build ./...
   go vet ./...
   go test ./...        # all packages green
   go test ./... -cover # no package below 70% (see NFR §7)
   ```
4. Open a PR with: what changed, which F-/NFR item it serves, and how you
   verified it (test names, bench numbers for perf work).
5. CI must be green (lint, unit, build matrix x86_64/aarch64; integration on
   the self-hosted lab runner) and the PR needs one approving review.
6. Releases are cut from tagged `vX.Y.Z` via goreleaser (binaries + RPM/DEB +
   man pages); update `CHANGELOG.md` in the same PR for user-visible changes.

## Code Conventions

- Go 1.22+, stdlib-first; new dependencies need a justification in the PR.
- Package layout follows the monorepo structure in §9 (`internal/…`,
  `cmd/gpstream`, `tests/…`).
- Tests: table-driven where practical; fault-injection scenarios live under
  `tests/chaos` and use the `pg.FakeConn` harness — never require a live
  cluster for unit tests.
- No secrets in code, tests, fixtures, or CI logs.

## Milestones

Work is organized by the §11 milestones (M0–M7). Check the project board for
open issues labeled by milestone before starting something new.
