package transfer

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testFrame builds a v2 data frame with the given seq and payload.
func testFrame(jobID, unitID uint32, seq uint64, data []byte) *Frame {
	return &Frame{JobID: jobID, UnitID: unitID, Seq: seq, ContentID: ContentCoordinator, Data: data}
}

// pumpSender writes frames 0..n-1 (payload = repeated byte i%251, size bytes)
// into the ReliablePipe sender end.
func pumpSender(t *testing.T, s *ReliablePipe, n, size int) {
	t.Helper()
	for i := 0; i < n; i++ {
		data := bytes.Repeat([]byte{byte(i % 251)}, size)
		if _, err := s.Write(testFrame(1, 1, uint64(i), data).Encode()); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
	}
}

// drainReceiver reads exactly n encoded frames from the receiver end and
// verifies seq + payload integrity.
func drainReceiver(t *testing.T, r *ReliablePipe, n, size int) {
	t.Helper()
	for i := 0; i < n; i++ {
		f, err := ReadFrame(context.Background(), r)
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		want := bytes.Repeat([]byte{byte(i % 251)}, size)
		if !bytes.Equal(f.Data, want) {
			t.Fatalf("frame %d: payload mismatch (got %d bytes, want %d)", i, len(f.Data), size)
		}
		if f.Seq != uint64(i) {
			t.Fatalf("frame %d: seq %d", i, f.Seq)
		}
	}
}

func TestReliablePipeRoundTrip(t *testing.T) {
	s, r := NewReliablePipePair(DefaultReliablePipeConfig())
	defer s.Close()
	defer r.Close()

	const n, size = 200, 4096
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
	// The trailing ACK is throttled (AckTimeInterval); poll for it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := s.Stats().LastAck; got == uint64(n-1) {
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

// TestReliablePipeReconnectRST severs the transport mid-stream and verifies
// the sender reconnects, replays the un-ACKed ring, and the receiver gets
// every frame exactly once (DESIGN §4.6.3: RST = seconds, no table re-read).
func TestReliablePipeReconnectRST(t *testing.T) {
	cfg := DefaultReliablePipeConfig()
	cfg.AckTimeInterval = 1 * time.Millisecond
	cfg.AckByteInterval = 64 * 1024
	s, r := NewReliablePipePair(cfg)
	defer s.Close()
	defer r.Close()

	const n, size = 500, 8192
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			data := bytes.Repeat([]byte{byte(i % 251)}, size)
			if _, err := s.Write(testFrame(1, 1, uint64(i), data).Encode()); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
			if i == n/2 {
				s.Break() // RST at the halfway point
			}
		}
	}()
	drainReceiver(t, r, n, size)
	wg.Wait()

	if st := s.Stats(); st.Reconnects < 1 {
		t.Fatalf("expected >=1 reconnect, got %d", st.Reconnects)
	}
	if st := s.Stats(); st.Retransmitted == 0 {
		t.Fatalf("expected retransmissions after replay, got 0")
	}
	if got := r.Stats().LastConsumed; got != uint64(n-1) {
		t.Fatalf("lastConsumed = %d, want %d", got, n-1)
	}
}

// TestReliablePipeBlackhole catches a silent blackhole via the ACK watchdog:
// the transport dies without a clean EOF, so the sender relies on "no ACK for
// the window" to reconnect and replay.
func TestReliablePipeBlackhole(t *testing.T) {
	cfg := DefaultReliablePipeConfig()
	cfg.AckTimeout = 150 * time.Millisecond
	cfg.AckTimeInterval = 1 * time.Millisecond
	cfg.AckByteInterval = 64 * 1024
	s, r := NewReliablePipePair(cfg)
	defer s.Close()
	defer r.Close()

	const n, size = 300, 8192
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		drainReceiver(t, r, n, size)
	}()
	for i := 0; i < 60; i++ {
		data := bytes.Repeat([]byte{byte(i % 251)}, size)
		if _, err := s.Write(testFrame(1, 1, uint64(i), data).Encode()); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	s.Break() // blackhole
	for i := 60; i < n; i++ {
		data := bytes.Repeat([]byte{byte(i % 251)}, size)
		if _, err := s.Write(testFrame(1, 1, uint64(i), data).Encode()); err != nil {
			t.Fatalf("write %d after blackhole: %v", i, err)
		}
	}
	<-receiverDone

	if st := s.Stats(); st.Reconnects < 1 {
		t.Fatalf("expected >=1 reconnect (blackhole), got %d", st.Reconnects)
	}
	if got := r.Stats().LastConsumed; got != uint64(n-1) {
		t.Fatalf("lastConsumed = %d, want %d", got, n-1)
	}
}

// TestReliablePipeCorruptRetransmit corrupts one in-flight frame and verifies
// the receiver NACKs it and the sender retransmits: no ErrCorrupt leaks to the
// consumer, no unit death (DESIGN §4.6.5).
func TestReliablePipeCorruptRetransmit(t *testing.T) {
	cfg := DefaultReliablePipeConfig()
	cfg.AckTimeInterval = 1 * time.Millisecond
	cfg.AckByteInterval = 64 * 1024
	s, r := NewReliablePipePair(cfg)
	defer s.Close()
	defer r.Close()

	const n, size = 100, 4096
	var corrupted int32
	s.SetCorruptor(func(b []byte) []byte {
		if len(b) < 28 || binary.LittleEndian.Uint32(b[0:4]) != magicV2 {
			return b
		}
		seq := binary.LittleEndian.Uint64(b[20:28])
		if seq == 40 && atomic.CompareAndSwapInt32(&corrupted, 0, 1) {
			out := append([]byte(nil), b...)
			out[28] ^= 0xFF // flip the first payload byte
			return out
		}
		return b
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pumpSender(t, s, n, size)
	}()
	drainReceiver(t, r, n, size)
	wg.Wait()

	if st := s.Stats(); st.Retransmitted < 1 {
		t.Fatalf("expected the corrupt frame to be retransmitted, retransmitted=%d", st.Retransmitted)
	}
	if got := r.Stats().LastConsumed; got != uint64(n-1) {
		t.Fatalf("lastConsumed = %d, want %d", got, n-1)
	}
}

// TestReliablePipeMaxReconnects verifies the reconnect budget is enforced.
func TestReliablePipeMaxReconnects(t *testing.T) {
	cfg := DefaultReliablePipeConfig()
	cfg.MaxReconnects = 3
	cfg.AckTimeout = 100 * time.Millisecond
	cfg.AckTimeInterval = 1 * time.Millisecond
	cfg.AckByteInterval = 64 * 1024
	s, r := NewReliablePipePair(cfg)
	defer s.Close()
	defer r.Close()

	// Keep breaking before the receiver drains: each break needs a
	// reconnect; the budget must cap them and eventually fail the pipe.
	for i := 0; i < 30 && !s.dead.Load(); i++ {
		data := bytes.Repeat([]byte{byte(i % 251)}, 32*1024)
		_, _ = s.Write(testFrame(1, 1, uint64(i), data).Encode())
		s.Break()
		time.Sleep(120 * time.Millisecond)
	}
	if st := s.Stats(); st.Reconnects > cfg.MaxReconnects {
		t.Fatalf("reconnects %d exceed budget %d", st.Reconnects, cfg.MaxReconnects)
	}
	_ = r
	_ = io.EOF
}
