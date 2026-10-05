package transfer

import (
	"net"
	"sync"
	"testing"
	"time"
)

// tcpCfg is a fast config for the TCP round-trip tests (small window, tight
// ACK cadence, short blackhole timeout) so the tests finish in milliseconds.
func tcpCfg() ReliablePipeConfig {
	c := DefaultReliablePipeConfig()
	c.AckTimeout = 500 * time.Millisecond
	c.AckTimeInterval = 5 * time.Millisecond
	c.MaxReconnects = 20
	return c
}

func TestReliablePipeTCPRoundTrip(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	s, r, err := NewReliablePipeTCPPair(tcpCfg(), l)
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	defer s.Close()
	defer r.Close()

	const n, size = 100, 4096
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pumpSender(t, s, n, size)
	}()
	drainReceiver(t, r, n, size)
	wg.Wait()

	if got := r.Stats().LastConsumed; got != uint64(n-1) {
		t.Fatalf("lastConsumed = %d, want %d", got, n-1)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.Stats().LastAck == uint64(n-1) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.Stats().LastAck; got != uint64(n-1) {
		t.Fatalf("lastAck = %d, want %d", got, n-1)
	}
	if got := s.Stats().RingBytes; got != 0 {
		t.Fatalf("ring not drained: %d bytes", got)
	}
}

// TestReliablePipeTCPBreakResume is the Inc3 core: a mid-stream transport break
// (RST) is transparent to the consumer — the sender re-dials the same address,
// the receiver re-accepts and dedups the replayed ring, and the receiver's Read
// yields every frame exactly once, in order.
func TestReliablePipeTCPBreakResume(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	s, r, err := NewReliablePipeTCPPair(tcpCfg(), l)
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	defer s.Close()
	defer r.Close()

	const n, size = 60, 2048
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			// Break the transport a few frames in (after some are in flight and
			// ACKed) so the replay ring is non-trivial on resume.
			if i == 25 {
				r.BreakConn()
				s.CloseTransport()
			}
			if _, err := s.Write(testFrame(1, 1, uint64(i), bytesRepeat(byte(i%251), size)).Encode()); err != nil {
				t.Errorf("write frame %d: %v", i, err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	drainReceiver(t, r, n, size)
	wg.Wait()

	st := s.Stats()
	if st.Reconnects < 1 {
		t.Errorf("expected >=1 reconnect after the break, got %d", st.Reconnects)
	}
	if got := r.Stats().LastConsumed; got != uint64(n-1) {
		t.Fatalf("lastConsumed = %d, want %d", got, n-1)
	}
}

// TestReliablePipeTCPBlackhole verifies the ACK watchdog (not a visible RST)
// detects a silent break and re-resumes over TCP.
func TestReliablePipeTCPBlackhole(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	c := tcpCfg()
	c.AckTimeout = 200 * time.Millisecond
	s, r, err := NewReliablePipeTCPPair(c, l)
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	defer s.Close()
	defer r.Close()

	const n, size = 40, 1024
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if i == 15 {
				// Silent break: drop the receiver's conn WITHOUT the sender
				// knowing (no FIN from the sender's side); the sender's ACK
				// watchdog must notice and re-dial.
				r.BreakConn()
			}
			if _, err := s.Write(testFrame(1, 1, uint64(i), bytesRepeat(byte(i%251), size)).Encode()); err != nil {
				t.Errorf("write frame %d: %v", i, err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	drainReceiver(t, r, n, size)
	wg.Wait()

	if got := r.Stats().LastConsumed; got != uint64(n-1) {
		t.Fatalf("lastConsumed = %d, want %d", got, n-1)
	}
}

// bytesRepeat is a tiny local helper (avoids importing bytes just for one call
// in the break test's inline writer).
func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
