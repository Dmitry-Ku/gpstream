package agent

import (
	"net"
	"strconv"
	"testing"

	"github.com/gpstream/gpstream/internal/portalloc"
)

// Strict firewall: with --port-range set, the source agent's auto-picked
// (":0") control port must come from the range (the agent now self-picks its
// control port in the two-agent remote layout, so the range has to be
// enforced on the AGENT host, not by the controller).
func TestBindControlRespectsPortRange(t *testing.T) {
	alloc, err := portalloc.ParseRange("45000-45999")
	if err != nil {
		t.Fatalf("ParseRange: %v", err)
	}
	s := NewSourceServer(ServerOptions{Listen: "0.0.0.0:0", PortAlloc: alloc})
	l, err := s.bindControl()
	if err != nil {
		t.Fatalf("bindControl: %v", err)
	}
	defer l.Close()
	p, perr := strconv.Atoi(l.Addr().String()[len(l.Addr().String())-5:])
	if perr != nil {
		// parse host:port properly
		_, portStr, e2 := net.SplitHostPort(l.Addr().String())
		if e2 != nil {
			t.Fatalf("split %q: %v", l.Addr().String(), e2)
		}
		p, perr = strconv.Atoi(portStr)
		if perr != nil {
			t.Fatalf("port of %q: %v", l.Addr().String(), perr)
		}
	}
	if p < 45000 || p > 45999 {
		t.Fatalf("control port %d outside the requested range 45000-45999", p)
	}
}

// No range (nil allocator) = kernel pick, as before.
func TestBindControlKernelPickWithoutRange(t *testing.T) {
	s := NewSourceServer(ServerOptions{Listen: "127.0.0.1:0"})
	l, err := s.bindControl()
	if err != nil {
		t.Fatalf("bindControl: %v", err)
	}
	defer l.Close()
	if l.Addr().String() == "" {
		t.Fatal("no bound address")
	}
}

// A pinned (non-auto) control address is bound as-is even with a range set.
func TestBindControlPinnedAddressBypassesRange(t *testing.T) {
	alloc, err := portalloc.ParseRange("45000-45999")
	if err != nil {
		t.Fatalf("ParseRange: %v", err)
	}
	// Find a free port to pin (avoid collisions with other tests).
	probe, perr := net.Listen("tcp", "127.0.0.1:0")
	if perr != nil {
		t.Fatalf("probe: %v", perr)
	}
	portStr := probe.Addr().String()[len(probe.Addr().String())-5:]
	probe.Close()
	s := NewSourceServer(ServerOptions{Listen: "127.0.0.1:" + portStr, PortAlloc: alloc})
	l, err := s.bindControl()
	if err != nil {
		t.Fatalf("bindControl: %v", err)
	}
	defer l.Close()
	if l.Addr().String() != "127.0.0.1:"+portStr {
		t.Fatalf("pinned address not honored: %q", l.Addr().String())
	}
}
