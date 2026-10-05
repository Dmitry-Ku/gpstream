package agent

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestDestServerProtocol exercises the long-lived dest agent's control
// protocol on loopback: hello (agent speaks first), serve-unit (immediate
// ack + background serve + unit-done report), release. No pg is involved:
// the coordinator serve fails at the pg connect (no database in unit
// tests), which is exactly what the unit-done report must carry — the
// protocol mechanics (ordering, ack-then-report) are what is under test.
func TestDestServerProtocol(t *testing.T) {
	// Bind free ports first so the agent's real addresses are known.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dataAddr := l.Addr().String()
	l.Close()
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctrlAddr := l2.Addr().String()
	l2.Close()

	srv, err := NewDestServer(DestServerOptions{
		DataListen:    dataAddr,
		ControlListen: ctrlAddr,
		DestConn:      "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable", // unreachable on purpose
		AcceptTimeout: 2 * time.Second,
		StreamTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDestServer: %v", err)
	}
	defer srv.Close()
	if srv.DataAddr() != dataAddr {
		t.Fatalf("DataAddr = %q, want %q", srv.DataAddr(), dataAddr)
	}

	client, err := DialDestControl(context.Background(), ctrlAddr)
	if err != nil {
		t.Fatalf("DialDestControl: %v", err)
	}
	defer client.Close()
	// The hello carries the real bound addresses (auto :0 mode).
	if client.Hello.Op != "hello" || client.Hello.Data != dataAddr || client.Hello.Control != ctrlAddr {
		t.Fatalf("hello = %+v (data %q ctrl %q)", client.Hello, dataAddr, ctrlAddr)
	}

	// serve-unit: the ack must arrive immediately (the serve is running in
	// the background); the unit-done report follows once the serve fails at
	// the (unreachable) pg connect.
	spec := &UnitSpec{Schema: "s", Table: "t", Staging: "t_gpstream_stg"}
	ack, err := client.BeginServe(DestServeReq{Kind: "coordinator", Spec: spec})
	if err != nil {
		t.Fatalf("BeginServe: %v", err)
	}
	if !ack.Ok {
		t.Fatalf("ack = %+v", ack)
	}
	done, err := client.WaitUnitDone()
	if err != nil {
		t.Fatalf("WaitUnitDone: %v", err)
	}
	if done.Op != "unit-done" || done.Ok {
		t.Fatalf("unit-done = %+v (want op=unit-done ok=false: pg unreachable)", done)
	}
	if done.Error == "" {
		t.Fatalf("unit-done: want a non-empty error (pg connect failed), got %+v", done)
	}

	// An unknown kind is reported as a failed unit (the ack succeeds — the
	// serve started; the unit-done carries the error).
	ack2, err := client.BeginServe(DestServeReq{Kind: "bogus"})
	if err != nil {
		t.Fatalf("BeginServe(bogus): %v", err)
	}
	if !ack2.Ok {
		t.Fatalf("ack for bogus kind = %+v (want ok=true; the error rides unit-done)", ack2)
	}
	done2, err := client.WaitUnitDone()
	if err != nil {
		t.Fatalf("WaitUnitDone(bogus): %v", err)
	}
	if done2.Ok || done2.Error == "" {
		t.Fatalf("unit-done for bogus kind = %+v (want ok=false + error)", done2)
	}

	// release: the control loop exits (Done closes).
	if _, err := client.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case <-srv.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("server did not exit after release")
	}
}

// TestDestServerWildcardDataAddr is the remote-launch (SSH) data-path bug:
// the controller computes --data-listen 0.0.0.0:0 (it does not know the
// agent host's IP), so the bound listener is [::]:port. DataAddr must report
// a ROUTABLE address (the host's primary non-loopback IPv4), not the raw
// wildcard bind — otherwise the source agent dials localhost and the data
// connection is refused.
func TestDestServerWildcardDataAddr(t *testing.T) {
	srv, err := NewDestServer(DestServerOptions{
		DataListen:    "0.0.0.0:0",
		ControlListen: "127.0.0.1:0",
		DestConn:      "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable",
		AcceptTimeout: 2 * time.Second,
		StreamTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDestServer: %v", err)
	}
	defer srv.Close()

	got := srv.DataAddr()
	host, port, err := net.SplitHostPort(got)
	if err != nil {
		t.Fatalf("DataAddr %q not host:port: %v", got, err)
	}
	if host == "0.0.0.0" || host == "::" || host == "[::]" || host == "127.0.0.1" {
		t.Fatalf("DataAddr = %q: wildcard/loopback host is not routable", got)
	}
	if ip := net.ParseIP(host); ip == nil || ip.IsLoopback() || !ip.To4().IsGlobalUnicast() {
		t.Fatalf("DataAddr host %q is not a global unicast IPv4", host)
	}
	// The port must be the real bound port (non-zero).
	if port == "0" {
		t.Fatalf("DataAddr port is 0: %q", got)
	}
}

// TestDestServerBadKind reports the error as unit-done (a kind the server
// accepts but cannot build a launch for).
func TestDestServerBadKind(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dataAddr := l.Addr().String()
	l.Close()
	l2, _ := net.Listen("tcp", "127.0.0.1:0")
	ctrlAddr := l2.Addr().String()
	l2.Close()

	srv, err := NewDestServer(DestServerOptions{
		DataListen:    dataAddr,
		ControlListen: ctrlAddr,
		DestConn:      "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable",
	})
	if err != nil {
		t.Fatalf("NewDestServer: %v", err)
	}
	defer srv.Close()
	client, err := DialDestControl(context.Background(), ctrlAddr)
	if err != nil {
		t.Fatalf("DialDestControl: %v", err)
	}
	defer client.Close()

	// kind=direct without a launch payload: the serve starts (ack ok) and
	// the unit-done carries the error.
	ack, err := client.BeginServe(DestServeReq{Kind: "direct"})
	if err != nil {
		t.Fatalf("BeginServe: %v", err)
	}
	if !ack.Ok {
		t.Fatalf("ack = %+v (want ok=true; the error rides unit-done)", ack)
	}
	done, err := client.WaitUnitDone()
	if err != nil {
		t.Fatalf("WaitUnitDone: %v", err)
	}
	if done.Ok || done.Error == "" {
		t.Fatalf("unit-done = %+v (want ok=false + error: missing launch)", done)
	}
	if _, rerr := client.Release(); rerr != nil {
		t.Logf("Release (best effort): %v", rerr)
	}
}
