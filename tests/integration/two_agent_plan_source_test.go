//go:build gp6lab

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestSourceAgentBuildsPlan is the 2A (Phase 2) acceptance: the copy plan is
// built ON THE SOURCE AGENT, not the controller. It drives the REAL production
// process runner in COORDINATOR mode against two plain-PG clusters:
//
//  1. a controller-side plan (plan.Build with local src/dst pools) is the
//     REFERENCE (the old architecture) — its hash is what the source-agent
//     plan must reproduce;
//  2. mgr.Plan launches the two agents and asks the SOURCE agent to run
//     plan.Build (its LOCAL source pool + a destination querier that proxies
//     read-only destination-catalog queries to the DESTINATION agent over the
//     persistent source->dest peer channel). The controller opens NO direct
//     destination connection for planning (architecture 08.10);
//  3. the source-agent plan must carry the SAME units + the SAME deterministic
//     hash as the reference — proving the proxy round-trip is type-faithful
//     (the destination catalog rows the source reconstructs are identical to
//     what a local pool would have produced).
func TestSourceAgentBuildsPlan(t *testing.T) {
	const (
		srcPort = 5751
		dstPort = 5752
	)
	ctx := context.Background()

	bins := buildTwoAgentBinaries(t)

	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { dst.Close() })

	// Seed two tables (the plan must include both).
	for _, name := range []string{"orders", "customers"} {
		if _, err := src.Exec(ctx, "CREATE TABLE "+name+" (id integer, note text)"); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		if _, err := src.Exec(ctx, "INSERT INTO "+name+" SELECT g, 'row' || g FROM generate_series(1, 500) g"); err != nil {
			t.Fatalf("seed %s rows: %v", name, err)
		}
	}

	// A config with the agent binaries (the process runner launches them) +
	// coordinator mode (CopyMode empty) — the common layout, and the one that
	// exercises the coordinator-mode peer channel (the mesh path is covered by
	// TestProcessRunnerMeshStartRelease).
	cfg := config.Default()
	cfg.Source = srcCl.URL()
	cfg.Dest = dstCl.URL()
	cfg.ConflictAction = config.Overwrite
	cfg.WorkersPerPair = 1
	cfg.SourceAgentBinary = bins.srcAgent
	cfg.DestAgentBinary = bins.destAgent
	cfg.FrameAck = "on"
	cfg.AckTimeout = "30s"
	cfg.MaxReconnects = 20
	cfg.RetransmitBufferMax = 64 << 20

	// 1. The REFERENCE plan (controller-side, the old architecture).
	refCtx, refCancel := context.WithTimeout(ctx, 60*time.Second)
	ref, err := plan.Build(refCtx, &cfg, src, dst)
	refCancel()
	if err != nil {
		t.Fatalf("reference plan.Build: %v", err)
	}
	if len(ref.Units) != 2 {
		t.Fatalf("reference plan has %d units, want 2", len(ref.Units))
	}
	t.Logf("reference plan (controller-side): %d units, hash=%s", len(ref.Units), ref.Hash)

	// 2. The SOURCE-AGENT plan (2A): mgr.Plan launches the agents and asks the
	//    source agent to build the plan over the peer channel.
	journalPath := filepath.Join(t.TempDir(), "journal.jsonl")
	mgr := manifest.NewManager(&cfg, manifest.NewJournal(dst, journalPath))
	planCtx, planCancel := context.WithTimeout(ctx, 120*time.Second)
	defer planCancel()
	p, err := mgr.Plan(planCtx)
	if err != nil {
		t.Fatalf("mgr.Plan (source-agent plan over the peer channel): %v", err)
	}
	// The agents are left running on success. If the hash check below fails
	// (Fatal), Run is not reached — release the agents explicitly. On success
	// Run's deferred Release is the teardown (the agents are reused, Start is
	// idempotent); ReleaseAgents is a no-op safety net (idempotent).
	defer mgr.ReleaseAgents()

	// 3. The source-agent plan must match the reference: same units + the same
	//    deterministic hash (type-faithful proxy).
	if len(p.Units) != len(ref.Units) {
		t.Fatalf("source-agent plan has %d units, want %d (the reference)", len(p.Units), len(ref.Units))
	}
	gotNames := map[string]bool{}
	for _, u := range p.Units {
		gotNames[u.Schema+"."+u.Name] = true
	}
	for _, u := range ref.Units {
		if !gotNames[u.Schema+"."+u.Name] {
			t.Errorf("source-agent plan missing unit %s.%s", u.Schema, u.Name)
		}
	}
	if p.Hash != ref.Hash {
		t.Fatalf("source-agent plan hash %q != reference hash %q (the peer-channel proxy is not type-faithful)", p.Hash, ref.Hash)
	}
	t.Logf("source-agent plan: %d units, hash=%s (== reference)", len(p.Units), p.Hash)

	// 4. The PRODUCTION FLOW: mgr.Run(ctx, p) with the SOURCE-AGENT plan. The
	//    agents started by Plan are reused (Start is idempotent) and the copy
	//    must actually move the data. This exercises the full run() restructure
	//    (journal + manager exist before the plan; the plan comes from the
	//    source agent; Run reuses the agents).
	runCtx, runCancel := context.WithTimeout(ctx, 180*time.Second)
	defer runCancel()
	if err := mgr.Run(runCtx, p); err != nil {
		t.Fatalf("mgr.Run (source-agent plan): %v", err)
	}
	// The data must have landed on the destination (both tables, full row
	// counts) — the source-agent plan drove a real copy.
	for _, name := range []string{"orders", "customers"} {
		rows, err := dst.Query(ctx, "SELECT count(*) FROM "+name)
		if err != nil {
			t.Fatalf("dest %s: %v", name, err)
		}
		var n int64
		if len(rows) > 0 && len(rows[0]) > 0 {
			switch v := rows[0][0].(type) {
			case int64:
				n = v
			case int:
				n = int64(v)
			case float64:
				n = int64(v)
			}
		}
		if n != 500 {
			t.Fatalf("dest %s has %d rows, want 500 (the copy did not run)", name, n)
		}
	}
	t.Logf("run completed: both tables copied (500 rows each) with the source-agent plan")
}

// TestSourceAgentPlanDestUnreachable: if the DESTINATION agent is down (its
// peer listener not reachable), the source agent's plan-build fails (the
// destination catalog cannot be read) — the run aborts rather than planning
// against a half-connected destination. The controller never falls back to a
// direct destination connection (there is none).
func TestSourceAgentPlanDestUnreachable(t *testing.T) {
	const srcPort = 5753
	ctx := context.Background()
	bins := buildTwoAgentBinaries(t)

	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	if _, err := src.Exec(ctx, "CREATE TABLE orders (id integer)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A dest DSN that is valid-shaped but points at a port nothing listens on
	// (the dest agent cannot come up / its peer listener is unreachable).
	cfg := config.Default()
	cfg.Source = srcCl.URL()
	cfg.Dest = "postgres://gpadmin@127.0.0.1:59999/dstdb"
	cfg.WorkersPerPair = 1
	cfg.SourceAgentBinary = bins.srcAgent
	cfg.DestAgentBinary = bins.destAgent
	cfg.FrameAck = "on"
	cfg.AckTimeout = "30s"
	cfg.MaxReconnects = 20
	cfg.RetransmitBufferMax = 64 << 20

	journalPath := filepath.Join(t.TempDir(), "journal.jsonl")
	mgr := manifest.NewManager(&cfg, manifest.NewJournal(nil, journalPath))
	planCtx, planCancel := context.WithTimeout(ctx, 60*time.Second)
	defer planCancel()
	if _, err := mgr.Plan(planCtx); err == nil {
		t.Fatal("mgr.Plan succeeded with an unreachable destination; want a hard error (no direct-destination fallback)")
	} else {
		t.Logf("mgr.Plan failed as expected with an unreachable destination: %v", err)
	}
}
