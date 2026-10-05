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
