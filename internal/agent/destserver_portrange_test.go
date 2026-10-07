package agent

import (
	"net"
	"strconv"
	"testing"

	"github.com/gpstream/gpstream/internal/portalloc"
)

// TestDestServerPortRange: with --port-range active and auto-pick
// (":0") listener addresses, NewDestServer must bind BOTH listeners inside
// the range (the strict-firewall case), and the two must not collide.
func TestDestServerPortRange(t *testing.T) {
	alloc, err := portalloc.ParseRange("21000-21005")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewDestServer(DestServerOptions{
		DataListen:    "127.0.0.1:0",
		ControlListen: "127.0.0.1:0",
		PortAlloc:     alloc,
		DestConn:      "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable",
	})
	if err != nil {
		t.Fatalf("NewDestServer: %v", err)
	}
	defer srv.Close()

	for name, addr := range map[string]string{"data": srv.DataAddr(), "control": srv.ControlAddr()} {
		_, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("%s addr %q: %v", name, addr, err)
		}
		p, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatalf("%s port %q: %v", name, portStr, err)
		}
		if p < 21000 || p > 21005 {
			t.Fatalf("%s port %d outside range 21000-21005 (addr %q)", name, p, addr)
		}
	}
	if srv.DataAddr() == srv.ControlAddr() {
		t.Fatalf("data and control landed on the same address: %s", srv.DataAddr())
	}
}

// TestDestServerPortRangeExhausted: a range fully occupied by foreign
// listeners must fail with a clear error (not bind outside the range).
func TestDestServerPortRangeExhausted(t *testing.T) {
	// Occupy the whole 2-port range.
	l1, err := net.Listen("tcp", "127.0.0.1:21100")
	if err != nil {
		t.Fatalf("occupy 21100: %v", err)
	}
	defer l1.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:21101")
	if err != nil {
		t.Fatalf("occupy 21101: %v", err)
	}
	defer l2.Close()

	alloc, err := portalloc.ParseRange("21100-21101")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDestServer(DestServerOptions{
		DataListen:    "127.0.0.1:0",
		ControlListen: "127.0.0.1:0",
		PortAlloc:     alloc,
		DestConn:      "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable",
	})
	if err == nil {
		t.Fatal("expected an error: the whole range is occupied")
	}
}

// TestDestServerFixedPortIgnoresRange: an explicit (non-:0) address binds
// verbatim even when a range is active (operator-fixed ports win).
func TestDestServerFixedPortIgnoresRange(t *testing.T) {
	alloc, err := portalloc.ParseRange("21200-21205")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewDestServer(DestServerOptions{
		DataListen:    "127.0.0.1:21250",
		ControlListen: "127.0.0.1:21251",
		PortAlloc:     alloc,
		DestConn:      "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable",
	})
	if err != nil {
		t.Fatalf("NewDestServer: %v", err)
	}
	defer srv.Close()
	// 21250 is OUTSIDE the 21200-21205 range and must still bind.
	if _, portStr, _ := net.SplitHostPort(srv.DataAddr()); portStr != "21250" {
		t.Fatalf("DataAddr = %q, want port 21250 (fixed ports ignore the range)", srv.DataAddr())
	}
}
