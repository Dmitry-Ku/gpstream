package ratelimit

import (
	"testing"
	"time"
)

// fakeProvider returns a fixed Metrics snapshot (for tests).
type fakeProvider struct {
	m Metrics
}

func (f *fakeProvider) Snapshot() Metrics { return f.m }

func TestReduceOnIOWait(t *testing.T) {
	p := &fakeProvider{m: Metrics{IOWaitPercent: 35}} // > 30% threshold
	l := New(Options{Baseline: 100, Floor: 1, Provider: p})
	changes := make(chan Gbps, 16)
	l.SetOnChange(func(from, to Gbps, cause string) {
		changes <- to
	})
	l.Update()
	select {
	case to := <-changes:
		if to >= 100 {
			t.Errorf("expected rate to drop below 100, got %.2f", to)
		}
		// A single 25% step from 100 -> 75.
		if to < 70 || to > 80 {
			t.Errorf("rate = %.2f, want ~75", to)
		}
	case <-time.After(time.Second):
		t.Fatal("no rate change emitted on overload")
	}
}

func TestRecoverWhenHealthy(t *testing.T) {
	p := &fakeProvider{m: Metrics{IOWaitPercent: 5}} // healthy
	l := New(Options{Baseline: 100, Floor: 1, Provider: p})
	// Start below baseline to simulate a prior reduction.
	l.mu.Lock()
	l.current = 75
	l.mu.Unlock()
	changes := make(chan Gbps, 16)
	l.SetOnChange(func(from, to Gbps, cause string) {
		changes <- to
	})
	l.Update()
	select {
	case to := <-changes:
		if to <= 75 {
			t.Errorf("expected recovery above 75, got %.2f", to)
		}
		if to > 100 {
			t.Errorf("recovery must not exceed baseline 100, got %.2f", to)
		}
	case <-time.After(time.Second):
		t.Fatal("no recovery emitted")
	}
}

func TestFloorHolds(t *testing.T) {
	p := &fakeProvider{m: Metrics{IOWaitPercent: 60}}
	l := New(Options{Baseline: 100, Floor: 1, Provider: p})
	// Many overloads must not drop below the floor.
	for i := 0; i < 100; i++ {
		l.Update()
	}
	if got := l.Current(); got < 1 || got > 1.01 {
		t.Errorf("floor: current = %.3f, want ~1.0", got)
	}
}

func TestNoProviderNoPanic(t *testing.T) {
	l := New(Options{Baseline: 100, Provider: nil})
	l.Update() // must not panic
	if l.Current() != 100 {
		t.Errorf("no provider: current = %.2f, want 100", l.Current())
	}
}

func TestDiskUtilAndRetransmit(t *testing.T) {
	// Disk util > 90 triggers reduction.
	p := &fakeProvider{m: Metrics{DiskUtil: 95}}
	l := New(Options{Baseline: 100, Provider: p})
	changed := false
	l.SetOnChange(func(_, _ Gbps, _ string) { changed = true })
	l.Update()
	if !changed {
		t.Error("disk-util overload should reduce rate")
	}
	// TCP retransmits > 100 triggers reduction.
	p2 := &fakeProvider{m: Metrics{Retransmits: 500}}
	l2 := New(Options{Baseline: 100, Provider: p2})
	changed = false
	l2.SetOnChange(func(_, _ Gbps, _ string) { changed = true })
	l2.Update()
	if !changed {
		t.Error("tcp-retransmit overload should reduce rate")
	}
}
