package limiter

import (
	"context"
	"testing"
	"time"
)

// fakeProv is an injected Metrics provider (FR-14) so the limiter is
// testable without a live host.
type fakeProv struct{ m Metrics }

func (f *fakeProv) Snapshot() Metrics { return f.m }

// TestPacerUnlimitedWhenNoBaseline verifies the pacer is a no-op when the
// limiter has no baseline (FR-14: --rate-limit unset = unlimited).
func TestPacerUnlimitedWhenNoBaseline(t *testing.T) {
	lim := New(Options{}) // baseline 0
	p := NewPacer(lim)
	start := time.Now()
	if err := p.Wait(context.Background(), 1<<20); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if time.Since(start) > 20*time.Millisecond {
		t.Fatalf("unlimited pacer should not block, took %v", time.Since(start))
	}
}

// TestPacerThrottles verifies the pacer blocks to the current rate when a
// baseline is set (FR-14). ~3 MiB at a 1 MiB/s effective rate (minus the 1 MiB
// burst) must take a measurable amount of time.
func TestPacerThrottles(t *testing.T) {
	lim := New(Options{Baseline: 0.008}) // 0.008 Gbps = 1 MiB/s
	p := NewPacer(lim)
	start := time.Now()
	// Send ~3 MiB; at ~1 MiB/s (1 MiB burst already available) this takes ~2s.
	if err := p.Wait(context.Background(), 3*1024*1024); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if d := time.Since(start); d < 500*time.Millisecond {
		t.Fatalf("pacer did not throttle enough: 3 MiB at 1 MiB/s took %v", d)
	}
}

// TestPacerCtxCancel verifies ctx cancellation unblocks the pacer (FR-14/FR-01
// back-pressure).
func TestPacerCtxCancel(t *testing.T) {
	lim := New(Options{Baseline: 0.0001}) // 0.0001 Gbps = 10 KiB/s
	p := NewPacer(lim)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.Wait(ctx, 10*1024*1024)
	if err == nil {
		t.Fatalf("expected ctx cancellation")
	}
	if d := time.Since(start); d > 1*time.Second {
		t.Fatalf("pacer did not honour ctx cancel: %v", d)
	}
}

// TestLimiterReduceRecover verifies the adaptive limiter reduces on overload and
// recovers when load is normal (FR-14), and that a reduction fires the
// diagnostic callback (FR-04).
func TestLimiterReduceRecover(t *testing.T) {
	fp := &fakeProv{}
	lim := New(Options{Baseline: 10, Floor: 1, Step: 0.25, Provider: fp})
	var changes int
	lim.SetOnChange(func(_, _ Gbps, _ string) { changes++ })

	fp.m = Metrics{IOWaitPercent: 50}
	lim.Update()
	afterReduce := lim.Current()
	if afterReduce >= 10 {
		t.Fatalf("expected reduction, got %v", afterReduce)
	}
	if changes == 0 {
		t.Fatalf("expected a diagnostic callback on reduction")
	}

	fp.m = Metrics{IOWaitPercent: 0} // healthy
	lim.Update()
	if lim.Current() < afterReduce {
		t.Fatalf("expected recovery, got %v", lim.Current())
	}
}
