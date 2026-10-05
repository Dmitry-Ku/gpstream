package portalloc

import (
	"fmt"
	"net"
	"testing"
)

// TestParseRange: empty -> disabled; "LO-HI" -> the range; single "LO" -> a
// one-port range; malformed / inverted ranges error.
func TestParseRange(t *testing.T) {
	if a, err := ParseRange(""); err != nil || a.Enabled() {
		t.Fatalf("empty range: err=%v enabled=%v (want disabled, no err)", err, a.Enabled())
	}
	a, err := ParseRange("9000-9007")
	if err != nil {
		t.Fatal(err)
	}
	if lo, hi := a.Range(); lo != 9000 || hi != 9007 {
		t.Fatalf("9000-9007 -> %d-%d", lo, hi)
	}
	a, err = ParseRange("9500")
	if err != nil {
		t.Fatal(err)
	}
	if lo, hi := a.Range(); lo != 9500 || hi != 9500 {
		t.Fatalf("single 9500 -> %d-%d (want 9500-9500)", lo, hi)
	}
	for _, bad := range []string{"9010-9000", "abc", "9000-", "-9000", "0-100", "1-70000"} {
		if _, err := ParseRange(bad); err == nil {
			t.Errorf("ParseRange(%q): want error, got nil", bad)
		}
	}
}

// TestListenAddrFixedPort: a non-zero port is bound verbatim (the range does
// not override an explicit port).
func TestListenAddrFixedPort(t *testing.T) {
	a, _ := ParseRange("9000-9007")
	// Pick a port inside the range and bind it directly to confirm it's free,
	// then have ListenAddr bind it again after closing.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	free := probe.Addr().(*net.TCPAddr).Port
	probe.Close()

	l, err := a.ListenAddr(fmt.Sprintf("127.0.0.1:%d", free))
	if err != nil {
		t.Fatalf("ListenAddr fixed port: %v", err)
	}
	defer l.Close()
	if got := l.Addr().(*net.TCPAddr).Port; got != free {
		t.Fatalf("bound %d, want the explicit port %d", got, free)
	}
}

// TestListenAddrZeroPicksInRange: "host:0" with an active range binds a port
// from the range (not an arbitrary one).
func TestListenAddrZeroPicksInRange(t *testing.T) {
	lo, hi := 36000, 36100
	a, _ := ParseRange(fmt.Sprintf("%d-%d", lo, hi))
	l, err := a.ListenAddr("127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenAddr :0: %v", err)
	}
	defer l.Close()
	p := l.Addr().(*net.TCPAddr).Port
	if p < lo || p > hi {
		t.Fatalf("picked %d outside [%d,%d]", p, lo, hi)
	}
}

// TestListenAddrDisabled: with no range, "host:0" degrades to the legacy
// kernel-picked behavior.
func TestListenAddrDisabled(t *testing.T) {
	a, _ := ParseRange("")
	l, err := a.ListenAddr("127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenAddr disabled :0: %v", err)
	}
	defer l.Close()
}

// TestReleaseAllowsReuse: a one-port range is exhausted after the first
// Next unless the port is released — the in-proc runner's per-unit data
// ports are bound serially and must be reused across units.
func TestReleaseAllowsReuse(t *testing.T) {
	a := New(22000, 22000)
	p1, err := a.Next("127.0.0.1")
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if p1 != 22000 {
		t.Fatalf("got %d, want 22000", p1)
	}
	// Without release the second Next must fail (range exhausted).
	if _, err := a.Next("127.0.0.1"); err == nil {
		t.Fatal("expected exhaustion without release")
	}
	a.Release(p1)
	p2, err := a.Next("127.0.0.1")
	if err != nil {
		t.Fatalf("Next after release: %v", err)
	}
	if p2 != 22000 {
		t.Fatalf("got %d after release, want 22000", p2)
	}
}

// TestReleaseManyUnits: the in-proc pattern — many sequential bind/release
// cycles stay within a small range (the reserved set must not grow).
func TestReleaseManyUnits(t *testing.T) {
	a := New(22100, 22101) // 2 ports
	for unit := 0; unit < 50; unit++ {
		p, err := a.Next("127.0.0.1")
		if err != nil {
			t.Fatalf("unit %d: %v", unit, err)
		}
		if p < 22100 || p > 22101 {
			t.Fatalf("unit %d: port %d out of range", unit, p)
		}
		a.Release(p)
	}
}
