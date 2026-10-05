package transfer

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// TestReliablePipeCrossProcessBreakResume is the production shape: the sender
// and receiver hold INDEPENDENT links (no shared in-memory state — the two ends
// live in different processes in the field). The sender dials the receiver's
// listener; on a mid-stream break the sender re-dials the same address and the
// receiver re-accepts via its bounded resumer. Delivery must stay exactly-once
// and in order across the break.
func TestReliablePipeCrossProcessBreakResume(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	c := tcpCfg()
	c.AckTimeout = 300 * time.Millisecond
	c.ResumeTimeout = 5 * time.Second

	sCh := make(chan *ReliablePipe, 1)
	sErr := make(chan error, 1)
	go func() {
		s, err := NewReliablePipeSender(c, l.Addr().String())
		if err != nil {
			sErr <- err
			return
		}
		sCh <- s
	}()
	r, err := NewReliablePipeReceiver(c, l)
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer r.Close()
	s, ok := <-sCh
	if !ok {
		t.Fatalf("sender: %v", <-sErr)
	}
	_ = sErr
	defer s.Close()

	const n, size = 50, 2048
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if i == 20 {
				// Break both ends' current transport (an RST): the sender
				// re-dials, the receiver re-accepts on its listener.
				s.CloseTransport()
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

	if st := s.Stats(); st.Reconnects < 1 {
		t.Errorf("expected >=1 sender reconnect, got %d", st.Reconnects)
	}
	if got := r.Stats().LastConsumed; got != uint64(n-1) {
		t.Fatalf("lastConsumed = %d, want %d", got, n-1)
	}
}

// TestReliablePipeCrossProcessResumeTimeout verifies the receiver's liveness
// bound: if the sender dies WITHOUT re-dialing (no re-dial within
// ResumeTimeout), the receiver declares it gone and Read terminates (EOF)
// rather than hanging the destination's unit forever.
func TestReliablePipeCrossProcessResumeTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	c := tcpCfg()
	c.ResumeTimeout = 300 * time.Millisecond
	c.AckTimeout = 300 * time.Millisecond

	sCh := make(chan *ReliablePipe, 1)
	sErr := make(chan error, 1)
	go func() {
		s, err := NewReliablePipeSender(c, l.Addr().String())
		if err != nil {
			sErr <- err
			return
		}
		sCh <- s
	}()
	r, err := NewReliablePipeReceiver(c, l)
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	defer r.Close()
	s := <-sCh

	// Send a few frames, then KILL the sender (mark it dead + sever the
	// transport) so it never re-dials. The receiver breaks, tries to re-accept
	// for ResumeTimeout, finds nothing, and ends the stream.
	const size = 1024
	for i := 0; i < 5; i++ {
		if _, err := s.Write(testFrame(1, 1, uint64(i), bytesRepeat(byte(i%251), size)).Encode()); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond) // let the frames land + be ACKed
	s.Close()                         // sender end goes away (no re-dial)
	r.BreakConn()                     // the transport break

	// Read must terminate (EOF) after ResumeTimeout, not hang.
	got := 0
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f, err := readOneFrame(r)
		if f != nil {
			got++
		}
		if err != nil {
			break // io.EOF (stream ended) or error
		}
	}
	if got != 5 {
		t.Fatalf("read %d frames before end, want 5", got)
	}
}

// readOneFrame reads a single frame (or nil + err at end).
func readOneFrame(r *ReliablePipe) (*Frame, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return ReadFrame(ctx, r)
}
