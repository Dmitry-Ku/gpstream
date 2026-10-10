package transfer

import (
	"net"
	"testing"
	"time"
)

// TestReliablePipeTCPCloseDrainsRing is the BUG #9 regression test (INC7
// scale run: the end-of-stream trailer of a ~10 GB stream was lost).
//
// The production failure: the sender (segreceiver) finishes the stream and
// closes immediately (SendCompressed's defer conn.Close()). The last frame
// (the trailer) is still un-ACKed in the retransmit ring, and the peer's
// ACKs are still in the sender's socket receive buffer. A TCP close with
// un-ACKed send data and unread receive data yields an RST, not a FIN: the
// kernel discards the un-ACKed tail, the receiver gets a reset instead of
// the clean end-of-stream, waits ResumeTimeout for a re-dial that never
// comes, and the stream ends WITHOUT its trailer (ErrTruncatedStream).
//
// The fix's contract: the sender's Close() must DRAIN the ring — wait
// (bounded by CloseTimeout) for the peer to ACK every written frame —
// before tearing the transport down. A drained ring means every frame is
// already in the receiver's output buffer (Read serves it before EOF), so
// the close is a clean FIN and no stream tail can be lost.
//
// Deterministic RED without the fix: the sender writes n frames and calls
// Close() immediately after the last Write (no sleep, exactly like
// SendCompressed). The ACK for the last frame (AckTimeInterval = 5 ms
// cadence) cannot have arrived yet, so Close tears the transport down with
// a non-empty ring; the late ACK is then lost (the sender's conn is closed)
// and the ring is observably non-empty after Close returns.
func TestReliablePipeTCPCloseDrainsRing(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	c := tcpCfg()
	c.ResumeTimeout = 400 * time.Millisecond
	c.CloseTimeout = 5 * time.Second

	s, err := NewReliablePipeSender(c, l.Addr().String())
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	r, err := NewReliablePipeReceiver(c, l)
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer r.Close()

	const n, size = 50, 4096
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if _, err := s.Write(testFrame(1, 1, uint64(i), bytesRepeat(byte(i%251), size)).Encode()); err != nil {
				t.Errorf("write frame %d: %v", i, err)
				return
			}
		}
		// No sleep: Close immediately after the last Write, exactly like
		// SendCompressed's defer conn.Close(). The ACK for the last frame
		// (5 ms cadence) cannot have arrived yet.
		_ = s.Close()
	}()

	// The receiver must get every frame, then the stream must end.
	drainReceiver(t, r, n, size)
	<-done

	// The invariant: after Close returns, the ring is drained (every frame
	// ACKed) — that is what makes the close a clean FIN instead of an RST.
	st := s.Stats()
	if st.RingBytes != 0 {
		t.Fatalf("close with undrained ring: ringBytes=%d (lastAck=%d, lastSent=%d) — the stream tail can be RST-discarded (BUG #9)",
			st.RingBytes, st.LastAck, st.LastSent)
	}
	if st.LastAck != uint64(n-1) {
		t.Fatalf("lastAck=%d, want %d (ring not fully ACKed before close)", st.LastAck, n-1)
	}
}
