package pg

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// TestNewConnExplicitDisableWinsOverSSLFlags (BUG #10 regression): an
// explicit sslmode=disable in the conn string must stay plaintext even when
// the --ssl-* flags are set (they configure the data plane / a different
// cluster's CA, not this connection). Without the fix the flags overrode the
// explicit sslmode and the client sent an SSLRequest to an ssl=off server
// ("server refused TLS connection"). The probe listener reads the client's
// first packet: an SSLRequest is 8 bytes (len=8, code=80877103); a plaintext
// startup message is longer (len>8) and its first 4 bytes are the length.
func TestNewConnExplicitDisableWinsOverSSLFlags(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port

	type probe struct{ sslRequest bool }
	probeCh := make(chan probe, 1)
	go func() {
		c, aerr := l.Accept()
		if aerr != nil {
			probeCh <- probe{false}
			return
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		head := make([]byte, 8)
		n := 0
		for n < len(head) {
			m, rerr := c.Read(head[n:])
			n += m
			if rerr != nil {
				probeCh <- probe{false}
				return
			}
		}
		// SSLRequest: [0,0,0,8, 0,0,C1,39] — len word 8. Plaintext startup:
		// len word > 8 (a startup message is at least ~40 bytes).
		sslRequest := head[0] == 0 && head[1] == 0 && head[2] == 0 && head[3] == 8
		probeCh <- probe{sslRequest}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	dsn := "host=127.0.0.1 port=" + strconv.Itoa(port) + " dbname=x user=x sslmode=disable"
	// The connect fails (the probe server answers nothing) — expected; the
	// probe only needs the client's first packet.
	_, _ = NewConn(ctx, dsn, "/nonexistent/cert.pem", "/nonexistent/key.pem", "/nonexistent/ca.pem", "")

	p := <-probeCh
	if p.sslRequest {
		t.Fatal("BUG #10: sslmode=disable was overridden by --ssl-* — the client sent an SSLRequest to a plaintext server")
	}
}
