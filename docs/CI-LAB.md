# CI on the lab bench (integration-gp6)

The `integration-gp6` job runs the live GP6 integration tests (data-path +
`-tags gp6lab`, including the two-agent crash matrix) on a **self-hosted
runner labeled `gp6`** — the lab bench is the only place with a real
PG 9.4.26 toolchain and the 4-cluster topology.

The bench is **not powered on permanently**, so the job must not sit
queued forever while it is off. That is solved by a file-based heartbeat
(no GitHub API involved — the repo's workflow token cannot query the
runners endpoint, and the bench cannot reach github.com:22).

## How the gate works

1. A background loop on the **always-on build host** runs
   `/opt/data/gp6-hb/bin/hb.sh` every 5 minutes:
   - probes bench vm1 over SSH (`gp@192.168.50.85:2222`) for the runner
     marker file `/opt/gp6-runner/online`;
   - publishes `"<state> <unix-ts>"` to `bench.status` on the
     **`runner-heartbeat`** branch (pushes only when the state changes,
     via the repo deploy key).
2. The CI `integration-gate` job (ubuntu-latest) checks out that branch
   and sets `runner_online=true` only when `state=online` and the
   timestamp is fresh (age ≤ 15 min).
3. `integration-gp6` runs only when `runner_online == 'true'`; otherwise
   it is **skipped** (the run stays green).

Gate verdict matrix (all verified locally before shipping):

| heartbeat file            | age      | result        |
|---------------------------|----------|---------------|
| `online <ts>`             | ≤ 15 min | job runs      |
| `online <ts>`             | > 15 min | job skipped   |
| `offline <ts>`            | any      | job skipped   |
| missing / malformed / CRLF| any      | job skipped   |

The gate step is fail-proof by construction (`exit 0` at the end): a
broken heartbeat can only ever skip the lab job, never turn the run red.

### Restarting the heartbeat loop

The loop is a tracked background process on the build host
(`while :; do /opt/data/gp6-hb/bin/hb.sh; sleep 300; done`, started with
`persist_on_release`). After a host reboot, re-launch it the same way.
`/opt/data/gp6-hb/loop.log` collects its output.

## Registering the lab runner (one-time)

1. GitHub → repo `Dmitry-Ku/gpstream` → **Settings → Actions → Runners →
   New self-hosted runner** → linux / amd64 → add label **`gp6`** →
   create.
2. On **vm1** (the bench coordinator host), run the two commands GitHub
   shows (download the runner package, `config.sh --url ... --token
   ... --labels gp6 --name gp6-vm1`, then start it as a service).
3. Create the marker so the heartbeat flips to `online`:
   ```
   sudo mkdir -p /opt/gp6-runner && sudo touch /opt/gp6-runner/online
   ```
   and wire it to the runner service (`ExecStartPost=/bin/touch
   /opt/gp6-runner/online`, `ExecStopPost=/bin/rm -f
   /opt/gp6-runner/online`) so a dead runner also de-flips the gate.
4. vm1 prerequisites the job needs: Go 1.27.4 and the PG 9.4.26
   toolchain discoverable by the job's locator
   (`/opt/pg9426-vm/bin` or `/opt/toolchain/pg9426/bin`).

Until the runner exists, every push shows `integration-gate: success`
+ `integration-gp6: skipped` — that is the correct state, not a failure.

## Old queued runs

Runs triggered before the gate existed can stay `queued` indefinitely
(no runner to pick them up). Cancel them in the UI (run → ⋯ → Cancel
run); they are harmless but noisy.
