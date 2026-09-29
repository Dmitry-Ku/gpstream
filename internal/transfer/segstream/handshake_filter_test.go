package segstream

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"
)

// TestHandshakeFilter (F-13): a rejected/noisy connection on the TLS port
// (a probe, a monitor, a client that fails certificate verification) must be
// dropped and must NOT abort the wait for the n legitimate segment streams.
// Without the filter, tls.Listener.Accept surfaces a handshake failure as an
// Accept error and Receive treats it as fatal — a stray probe would kill an
// in-flight copy.
func TestHandshakeFilter(t *testing.T) {
	// A TLS listener with no server certificate: every client connection
	// fails the handshake, so each Accept returns an error. The filter must
	// swallow those and keep waiting; the wait ends only when the listener
	// is closed (the clean, expected error path).
	scfg := &tls.Config{MinVersion: tls.VersionTLS12}
	plain, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := HandshakeFilter(tls.NewListener(plain, scfg))

	go func() {
		// Fire stray connections (all fail the handshake); the filtered
		// listener must not return an error for them. Then close the
		// listener to end the wait with a clean error.
		for i := 0; i < 3; i++ {
			c, _ := net.Dial("tcp", plain.Addr().String())
			if c != nil {
				c.Close()
			}
			time.Sleep(50 * time.Millisecond)
		}
		l.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, rerr := Receive(ctx, l, 1, nil)
	// The listener closed before any stream arrived, so Receive errors — but
	// the error must be the closed/partial-count path, NOT a leaked
	// handshake failure (which would mean the filter did not work).
	if rerr != nil && (strings.Contains(rerr.Error(), "handshake") ||
		strings.Contains(rerr.Error(), "certificate") ||
		strings.Contains(rerr.Error(), "tls:")) {
		t.Fatalf("HandshakeFilter leaked a handshake error to Receive: %v", rerr)
	}
}
