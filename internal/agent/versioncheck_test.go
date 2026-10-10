package agent

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/version"
)

func TestCheckVersions(t *testing.T) {
	const own = "0.2.0 (abc1234)"
	cases := []struct {
		name    string
		sv      SourceVersion
		minis   map[int]string
		wantErr bool
		errPart string
	}{
		{"all equal", SourceVersion{Version: own, S: 2, SegReceivers: []SegVersion{{Host: "h0", Version: own}, {Host: "h1", Version: own}}}, map[int]string{0: own, 1: own}, false, ""},
		{"no segs no minis", SourceVersion{Version: own}, map[int]string{}, false, ""},
		{"source mismatch", SourceVersion{Version: "9.9 (zzz)"}, nil, true, "source-agent="},
		{"segreceiver mismatch", SourceVersion{Version: own, S: 1, SegReceivers: []SegVersion{{Host: "h0", Version: "0.1.0 (old)"}}}, nil, true, "segreceiver@h0="},
		{"mini mismatch", SourceVersion{Version: own}, map[int]string{0: "0.1.0 (old)"}, true, "mini[seg0]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckVersions(own, tc.sv, tc.minis)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil")
				}
				if !strings.Contains(err.Error(), tc.errPart) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errPart)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestPeerExchange drives the source side of the version exchange against a
// fake destination peer listener (the cross-check is exercised on that fake:
// the destination replies Ok on a match, a hard error on a mismatch). The
// source has no pg pool (S=0: a plain-PostgreSQL source reports no
// segreceivers), so no SSH is involved.
func TestPeerExchange(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got := make(chan SourceVersion, 1)
	go func() {
		conn, aerr := l.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		var sv SourceVersion
		if rerr := recvLine(bufio.NewReader(conn), &sv); rerr != nil {
			return
		}
		got <- sv
		// The fake destination: accept only an exact match with its own
		// identity.
		if err := CheckVersions(version.String(), sv, nil); err != nil {
			_ = SendLine(conn, PeerAck{Op: "peer-ack", Ok: false, Error: err.Error()})
			return
		}
		_ = SendLine(conn, PeerAck{Op: "peer-ack", Ok: true})
	}()

	s := NewSourceServer(ServerOptions{})
	req := OnSegmentReq{Op: "onsegment", PeerAddr: l.Addr().String(), SegBinary: "/nonexistent/segreceiver"}
	if perr := s.peerExchange(t.Context(), req); perr != nil {
		t.Fatalf("peerExchange (match): %v", perr)
	}
	var sv SourceVersion
	select {
	case sv = <-got:
	default:
		t.Fatal("the fake peer did not receive a SourceVersion")
	}
	if sv.Op != "source-version" || sv.Version == "" {
		t.Fatalf("bad SourceVersion: %+v", sv)
	}
	if sv.S != 0 || len(sv.SegReceivers) != 0 {
		t.Fatalf("no-pool source must report S=0 (got S=%d segs=%v)", sv.S, sv.SegReceivers)
	}

	// A second exchange is a no-op (peerOnce): the listener is still there,
	// but no new connection is made (the once ran).
	if perr := s.peerExchange(t.Context(), req); perr != nil {
		t.Fatalf("peerExchange (re-run): %v", perr)
	}
}

// TestPeerExchangeMismatch: a destination that rejects the source's version
// fails the in-flight unit (the caller surfaces the error) before any data.
func TestPeerExchangeMismatch(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		conn, aerr := l.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		var sv SourceVersion
		if rerr := recvLine(bufio.NewReader(conn), &sv); rerr != nil {
			return
		}
		_ = SendLine(conn, PeerAck{Op: "peer-ack", Ok: false, Error: "version mismatch before the first unit: source-agent=9.9 (want 0.2.0)"})
	}()

	s := NewSourceServer(ServerOptions{})
	req := OnSegmentReq{Op: "onsegment", PeerAddr: l.Addr().String(), SegBinary: "/nonexistent/segreceiver"}
	perr := s.peerExchange(t.Context(), req)
	if perr == nil {
		t.Fatal("want a hard error (mismatch), got nil")
	}
	if !strings.Contains(perr.Error(), "version mismatch") {
		t.Fatalf("error %q does not carry the destination's verdict", perr.Error())
	}
}

// TestPeerVersionVerdict drives the DESTINATION side of the version exchange
// (the real process path): the supervisor's mini-map is ready with a matching
// mini version -> PeerAck{Ok}; a source or mini version mismatch -> a hard
// PeerAck with the discrepancy list (no data flows). It drives the persistent
// peer session (servePeerSession): the first message is the SourceVersion, the
// ack is the verdict; a mismatch returns before the dst-query loop.
func TestPeerVersionVerdict(t *testing.T) {
	const mismatch = "9.9 (old)"
	cases := []struct {
		name    string
		sv      SourceVersion
		miniVer string
		wantOk  bool
		errPart string
	}{
		{"match", SourceVersion{Op: "source-version", Version: version.String()}, version.String(), true, ""},
		{"source mismatch", SourceVersion{Op: "source-version", Version: mismatch}, version.String(), false, "source-agent="},
		{"mini mismatch", SourceVersion{Op: "source-version", Version: version.String()}, mismatch, false, "mini[seg0]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sup, err := NewSupervisor("127.0.0.1:0", nil, "", "", "", "")
			if err != nil {
				t.Fatal(err)
			}
			defer sup.Close()
			mcConn, mcPeer := net.Pipe()
			defer mcConn.Close()
			defer mcPeer.Close()
			sup.minis[0] = &miniConn{seg: 0, conn: mcConn, version: tc.miniVer}
			s := &DestServer{
				opts:         DestServerOptions{AcceptTimeout: 2 * time.Second},
				supervisor:   sup,
				miniMapReady: true,
			}
			l, lerr := net.Listen("tcp", "127.0.0.1:0")
			if lerr != nil {
				t.Fatal(lerr)
			}
			defer l.Close()
			go func() {
				conn, aerr := l.Accept()
				if aerr != nil {
					return
				}
				defer conn.Close()
				s.servePeerSession(conn)
			}()
			client, derr := net.Dial("tcp", l.Addr().String())
			if derr != nil {
				t.Fatal(derr)
			}
			defer client.Close()
			_ = client.SetDeadline(time.Now().Add(10 * time.Second))
			if werr := SendLine(client, tc.sv); werr != nil {
				t.Fatal(werr)
			}
			var ack PeerAck
			if rerr := recvLine(bufio.NewReader(client), &ack); rerr != nil {
				t.Fatalf("read peer-ack: %v", rerr)
			}
			if ack.Op != "peer-ack" {
				t.Fatalf("bad op %q", ack.Op)
			}
			if ack.Ok != tc.wantOk {
				t.Fatalf("Ok=%v want %v (error=%q)", ack.Ok, tc.wantOk, ack.Error)
			}
			if tc.errPart != "" && !strings.Contains(ack.Error, tc.errPart) {
				t.Fatalf("error %q does not contain %q", ack.Error, tc.errPart)
			}
		})
	}
}
