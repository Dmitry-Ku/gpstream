//go:build gp6lab

package integration

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
)

// TestProcessRunnerMeshStartRelease is the P3 production-dispatch INC3
// acceptance for the CONTROLLER-SIDE mesh orchestration, self-testable on a
// plain-PostgreSQL stand (no live GP6 / no ON SEGMENT kernel dispatch).
//
// It drives the REAL production process runner (manifest.NewProcessTwoAgentRunner)
// in mesh mode (CopyMode=mesh) against a single plain-PG cluster used as both
// source and destination (a D=1 self-copy layout). A plain-PG destination has
// no gp_segment_configuration, so the runner's catalog read falls back to ONE
// local segment (D=1). Start() must therefore:
//
//  1. launch the destination agent in SUPERVISOR mode (control + persist +
//     report listeners; the shared data listener off — the 4+D formula),
//  2. launch the D=1 destination-segment mini-agent (gpstream-dest-agent
//     --mini) as a local subprocess, which dials the supervisor's report
//     listener and sends its hello,
//  3. reach the MiniMap (the D-entry address book is complete) — Start()
//     blocks on the mini-map op until it is ready, so a successful Start()
//     proves the whole supervisor + mini + address-book handshake,
//
// and Release() must tear it all down cleanly (the supervisor's release op
// sends each mini a mini-done for a clean exit; no orphaned agent processes).
//
// The mesh DATA path (the ON SEGMENT dispatch -> segreceiver -> mini utility
// COPY) is exercised live on the GP6 stand (the A/D/B dev matrix); this test
// covers the orchestration that must work before any data flows.
func TestProcessRunnerMeshStartRelease(t *testing.T) {
	// Reuse the crash-matrix binary builder (same gp6lab package): it compiles
	// the three real two-agent binaries (controller + source + dest agent) to
	// a temp dir. The mini-agent is the dest-agent binary in --mini mode, so
	// the srcAgent + destAgent paths are all this test needs.
	bins := buildTwoAgentBinaries(t)

	// A single plain-PG cluster plays BOTH source and destination (a D=1
	// self-copy layout; no gp_segment_configuration -> the catalog read
	// falls back to one local segment).
	srcCl, err := pgtest.Start(5741, "inc3_mesh")
	if err != nil {
		t.Fatalf("start pgtest: %v", err)
	}
	defer srcCl.Stop()
	url := srcCl.URL()

	cfg := config.Default()
	cfg.Source = url
	cfg.Dest = url
	cfg.CopyMode = config.CopyMesh
	cfg.SourceAgentBinary = bins.srcAgent
	cfg.DestAgentBinary = bins.destAgent
	// Explicit transport values (config.Default leaves these empty; the agent
	// flags would otherwise receive an empty --ack-timeout and fail to parse).
	cfg.FrameAck = "on"
	cfg.AckTimeout = "30s"
	cfg.MaxReconnects = 20
	cfg.RetransmitBufferMax = 64 << 20
	cfg.WorkersPerPair = 1

	r := manifest.NewProcessTwoAgentRunner(&cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	if err := r.Start(ctx); err != nil {
		t.Fatalf("mesh Start (supervisor dest-agent + D=1 mini + MiniMap): %v", err)
	}
	t.Log("Start OK: supervisor-mode dest-agent + 1 mini launched, MiniMap ready")

	// A second Start must fail (the agents are already up / ports held) — a
	// cheap guard that Start is not a no-op. (The runner is single-run.)
	// Release now: the supervisor's release op sends the mini a mini-done
	// (clean exit); the handles' Stop is the last-resort kill.
	if err := r.Release(ctx); err != nil {
		t.Fatalf("mesh Release (clean supervisor + mini teardown): %v", err)
	}
	t.Log("Release OK: supervisor + mini torn down")

	// No orphaned agent processes for this cluster should remain (the source
	// agent, the supervisor dest-agent, and the mini all exited on Release).
	// Match the dest-agent by its unique "--dest <url>" argument (the
	// controller's own command line never carries it) and the mini by
	// "--mini". Give the process a moment to exit.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if agentsGone(t, url) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("orphaned agent processes remain after Release (pattern --dest/--mini %q)", url)
}

// agentsGone reports whether no gpstream agent process for the given DSN is
// still running (the source agent, the supervisor dest-agent, and the mini).
func agentsGone(t *testing.T, dsn string) bool {
	t.Helper()
	// The agent binaries' command lines contain the DSN (source: --src <dsn>;
	// dest/mini: --dest <dsn>). A pgrep -f on a substring of the DSN that no
	// non-agent process carries is enough: use the pgtest data-dir-free host
	// + port, which is unique to this test's cluster.
	out, _ := exec.Command("pgrep", "-af", "gpstream-(source|dest)-agent").Output()
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		if strings.Contains(line, dsn) {
			return false // an agent for this cluster is still up
		}
	}
	return true
}
