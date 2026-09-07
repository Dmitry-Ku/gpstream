package plan

import (
	"context"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestBuildTimeoutPartialFragment verifies (FR-10 §6.6) that on a --timeout the
// plan-formation phase returns the partial fragment already collected (Partial
// true, no deterministic hash) rather than a hard nil error, so the CLI can
// print the fragment with a prediction/forecast marker.
func TestBuildTimeoutPartialFragment(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout = 10 * time.Millisecond

	// A source that hangs its availability check past the deadline.
	src := pg.NewFakeConn()
	src.SetPingDelay(5 * time.Second)
	dst := pg.NewFakeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	p, err := Build(ctx, &cfg, src, dst)
	// The fragment is returned even though formation timed out.
	if p == nil {
		t.Fatalf("expected a partial fragment on timeout, got nil")
	}
	if !p.Partial {
		t.Fatalf("expected p.Partial=true on timeout")
	}
	// A partial plan carries no deterministic hash.
	if p.Hash != "" {
		t.Errorf("partial plan should have no hash, got %q", p.Hash)
	}
	// The timeout error is still surfaced so the CLI can mark it.
	if err == nil {
		t.Fatalf("expected a timeout error alongside the partial fragment")
	}
}

// TestBuildTimeoutGenuineFailureIsNil verifies that a non-timeout failure
// (context not cancelled) still returns nil, not a partial fragment — a real
// failure must not be masked as a completed fragment (CР-7).
func TestBuildTimeoutGenuineFailureIsNil(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout = 5 * time.Second

	// A source whose Ping fails immediately (not a hang) with a cancelled-but-
	// alive context: the error is a genuine reachability failure, not a timeout.
	src := pg.NewFakeConn()
	src.SetPingError(context.DeadlineExceeded)
	// Use a non-cancelled context so ctx.Err() is nil -> genuine failure.
	dst := pg.NewFakeConn()

	p, err := Build(context.Background(), &cfg, src, dst)
	if err == nil {
		t.Fatalf("expected a genuine failure error")
	}
	if p != nil {
		t.Errorf("a genuine (non-timeout) failure should return nil plan, got %+v", p)
	}
}
