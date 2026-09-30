---
name: Feature request
about: A capability not covered by TASK-SPEC-v4.md, or an extension of an F-xx
labels: enhancement
---

<!--
Before requesting: check TASK-SPEC-v4.md (F-01..F-18, NFR §7, CLI §8) — most
"missing" features are already specified or explicitly rejected there (e.g.
Web UI, telemetry, C libraries). If this conflicts with the spec, say so —
the spec is the source of truth and this issue will be routed to a spec
change, not a PR.
-->

**Feature / spec item:** F-xx if it maps to one, otherwise "out of spec".

**Problem it solves:** what operator pain, and why the existing flags
(`--rate-limit`, `--consistent-tables`, `--small-table-threshold`,
`--on-unit-failure`, …) don't cover it.

**Proposed behavior:** the exact CLI surface (command + flags), what appears
in the journal/audit/events, and how it degrades on failure. One paragraph,
concrete.

**Acceptance:** how you would verify it (command + expected output), in the
style of the F-xx acceptance criteria lines.

**Out of scope (explicit):** what you are NOT asking for.
