package planner

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/pg"
)

// TestBuildTimeout verifies that a --timeout (FR-10) actually bounds the
// plan-formation phase: when a cluster's availability check (Ping) hangs
// longer than the context deadline, Build returns the deadline error instead
// of blocking. This is the regression lock for the timeout wiring in
// cmdCopy/cmdDryRun/cmdPlan.
func TestBuildTimeout(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout = 30 * time.Millisecond

	src := pg.NewFakeConn()
	src.SetPingDelay(2 * time.Second) // hung segment: blocks the availability check
	dst := pg.NewFakeConn()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	start := time.Now()
	_, err := Build(ctx, &cfg, src, dst)
	if err == nil {
		t.Fatal("Build should fail when the source hangs past the timeout")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("Build did not honour the timeout (took too long)")
	}
}

// TestBuildHealthyWithinTimeout verifies the normal path: a healthy cluster
// answers before the deadline, so Build succeeds and is not killed by the
// timeout.
func TestBuildHealthyWithinTimeout(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout = 2 * time.Second

	src := pg.NewFakeConn()
	src.On("pg_class", [][]any{
		{"public", "t1", "r", int64(10), int64(0), int64(0)},
	})
	src.On("gp_segment_configuration", [][]any{{int64(4)}})
	src.On("has_privilege", [][]any{{int64(1)}})
	src.On("pg_database", [][]any{{"C", "C"}})
	src.On("SELECT 1", [][]any{{int64(1)}})
	dst := pg.NewFakeConn()
	dst.On("SELECT 1", [][]any{{int64(1)}})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	if _, err := Build(ctx, &cfg, src, dst); err != nil {
		t.Fatalf("Build should succeed within the timeout: %v", err)
	}
}
