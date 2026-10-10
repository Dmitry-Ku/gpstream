package agent

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
)

// TestDialPeerOverDataTLS (BUG #12 regression): the destination wraps the
// peer listener in the data-plane TLS (mTLS: it requires the source's client
// cert), so the source's dialPeer must be the TLS client. Before the fix
// dialPeer used a plain net.Dial and sent its version report in plaintext:
// the destination's TLS handshake failed, the connection was closed, and the
// source failed the build-plan with "peer: read verdict: EOF" (found on the
// first data-TLS live run, INC8 D->A). Here the version exchange must
// complete over TLS: the server receives the SourceVersion and the client
// receives the PeerAck verdict.
func TestDialPeerOverDataTLS(t *testing.T) {
	cert, key, caPEM := genReportTLSCerts(t)

	// Destination side: the TLS-wrapped peer listener (what NewDestServer
	// builds when data TLS is on) + the version-exchange first-message read.
	scfg, err := transfer.ServerTLSConfig(cert, key, caPEM, "")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := tls.NewListener(plain, scfg)
	peerAddr := l.Addr().String()

	type exchange struct {
		sv  SourceVersion
		err error
	}
	exCh := make(chan exchange, 1)
	go func() {
		conn, aerr := l.Accept()
		if aerr != nil {
			exCh <- exchange{err: aerr}
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		var sv SourceVersion
		if rerr := recvLine(r, &sv); rerr != nil {
			exCh <- exchange{err: rerr}
			return
		}
		// The server's verdict (servePeerSession sends the ack after the
		// cross-check; the cross-check is out of scope for this test).
		ack := PeerAck{Op: "peer-ack", Ok: true}
		b, _ := json.Marshal(ack)
		b = append(b, '\n')
		if _, werr := conn.Write(b); werr != nil {
			exCh <- exchange{sv: sv, err: werr}
			return
		}
		exCh <- exchange{sv: sv}
	}()

	sv := SourceVersion{Op: "source-version", Version: "test", S: 3, SegReceivers: []SegVersion{{Host: "127.0.0.1", Version: "test"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pc, derr := dialPeer(ctx, peerAddr, sv, cert, key, caPEM)
	if derr != nil {
		t.Fatalf("dialPeer over data-TLS failed (BUG #12: the source dials the peer listener in plaintext against the destination's TLS server): %v", derr)
	}
	defer pc.close()

	select {
	case ex := <-exCh:
		if ex.err != nil {
			t.Fatalf("server side of the peer version exchange failed: %v", ex.err)
		}
		if ex.sv.Version != "test" {
			t.Fatalf("server received wrong version report: %+v", ex.sv)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the version report never arrived (the source's plaintext version did not cross the TLS boundary)")
	}
}
