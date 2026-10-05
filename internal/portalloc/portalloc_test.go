package portalloc

import (
	"fmt"
	"net"
	"testing"
)

// TestDisabledAllocatorPicksFreePort: with no range the allocator hands back
// a kernel-picked free port (the legacy behavior).
func TestDisabledAllocatorPicksFreePort(t *testing.T) {
	a := New(0, 0)
	if a.Enabled() {
		t.Fatal("disabled allocator reported enabled")
	}
	port, err := a.Next("127.0.0.1")
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if port < 1 || port > 65535 {
		t.Fatalf("port out of range: %d", port)
	}
	// The returned port must actually bind (it was probed free).
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("rebind %d: %v (allocator returned a taken port)", port, err)
	}
	l.Close()
}

// TestAllocatorStaysInRange: every allocated port is within [lo, hi].
func TestAllocatorStaysInRange(t *testing.T) {
	// Use a wide range that is very unlikely to be half-full.
	lo, hi := 30000, 31000
	a := New(lo, hi)
	for i := 0; i < 5; i++ {
		port, err := a.Next("127.0.0.1")
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		if port < lo || port > hi {
			t.Fatalf("port %d outside [%d,%d]", port, lo, hi)
		}
	}
}

// TestAllocatorDoesNotRepeat: successive Next calls never return the same
// port (the reserved set prevents re-probing a handed-out port).
func TestAllocatorDoesNotRepeat(t *testing.T) {
	a := New(30000, 31000)
	seen := map[int]bool{}
	for i := 0; i < 8; i++ {
		port, err := a.Next("127.0.0.1")
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		if seen[port] {
			t.Fatalf("allocator repeated port %d", port)
		}
		seen[port] = true
	}
}

// TestAllocatorSkipsBusyPort: a port already held by this test's own listener
// is skipped, not handed out.
func TestAllocatorSkipsBusyPort(t *testing.T) {
	// Bind a specific port first so the allocator must go around it.
	lo := 32100
	held, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", lo))
	if err != nil {
		t.Fatalf("bind %d: %v", lo, err)
	}
	defer held.Close()

	a := New(lo, lo+2)
	port, err := a.Next("127.0.0.1")
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if port == lo {
		t.Fatalf("allocator handed out the busy port %d", lo)
	}
	if port < lo || port > lo+2 {
		t.Fatalf("port %d outside [%d,%d]", port, lo, lo+2)
	}
}

// TestAllocatorExhaustion: a range fully occupied by reserved+busy ports
// yields a clear "widen the range" error, not a wrong port.
func TestAllocatorExhaustion(t *testing.T) {
	lo := 33100
	held, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", lo))
	if err != nil {
		t.Fatalf("bind %d: %v", lo, err)
	}
	defer held.Close()

	a := New(lo, lo) // a one-port range, and that port is busy
	if _, err := a.Next("127.0.0.1"); err == nil {
		t.Fatal("expected exhaustion error, got nil")
	} else if got := err.Error(); len(got) == 0 {
		t.Fatal("empty error")
	}
}

// TestListenBindsInRange: Listen returns an OPEN listener whose port is in
// the range, and the port is not repeated for a second Listen.
func TestListenBindsInRange(t *testing.T) {
	lo, hi := 34000, 34100
	a := New(lo, hi)
	l1, err := a.Listen("127.0.0.1")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l1.Close()
	p1 := l1.Addr().(*net.TCPAddr).Port
	if p1 < lo || p1 > hi {
		t.Fatalf("listener port %d outside [%d,%d]", p1, lo, hi)
	}
	l2, err := a.Listen("127.0.0.1")
	if err != nil {
		t.Fatalf("Listen #2: %v", err)
	}
	defer l2.Close()
	p2 := l2.Addr().(*net.TCPAddr).Port
	if p1 == p2 {
		t.Fatalf("Listen repeated port %d", p1)
	}
}

// TestListenDisabled: with no range, Listen binds a kernel-picked port.
func TestListenDisabled(t *testing.T) {
	a := New(0, 0)
	l, err := a.Listen("127.0.0.1")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()
	if p := l.Addr().(*net.TCPAddr).Port; p < 1 {
		t.Fatalf("bad port %d", p)
	}
}

// TestSetExclude: an excluded port is never picked even if it is free.
func TestSetExclude(t *testing.T) {
	lo, hi := 35000, 35002
	a := New(lo, hi)
	// Exclude the first port of the range.
	a.SetExclude(lo)
	port, err := a.Next("127.0.0.1")
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if port == lo {
		t.Fatalf("allocator handed out the excluded port %d", lo)
	}
}

// TestBadRangeDisables: hi < lo disables the allocator (config.Validate is
// the real gate; this is the defensive fallback).
func TestBadRangeDisables(t *testing.T) {
	a := New(5000, 4000)
	if a.Enabled() {
		t.Fatal("hi<lo should disable the allocator")
	}
}
