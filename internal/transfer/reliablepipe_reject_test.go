package transfer

import (
	"net"
	"testing"
	"time"
)

// TestReliablePipeSenderResumeRejection verifies the fast-death: a destination
// that keeps its listener open (the long-lived agent between units) but no
// longer serves the stream (the serve ended) accepts the re-dials into its
// backlog but never greets them (no HELLO, no ACK). The sender must give up
// after 3 consecutive no-progress deaths instead of burning the
// MaxReconnects budget and piling zombie re-dials into the backlog (which
// would poison the next unit's accept).
func TestReliablePipeSenderResumeRejection(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	c := tcpCfg()
	c.AckTimeout = 150 * time.Millisecond
	c.AckTimeInterval = 1 * time.Millisecond
	c.RetransmitBufferMax = 128 * 1024 // small window: back-pressure fast
	c.MaxReconnects = 20

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
	defer l.Close()
	s := <-sCh

	// Let a few frames land + be ACKed (the transport is alive at first).
	for i := 0; i < 4; i++ {
		if _, err := s.Write(testFrame(1, 1, uint64(i), bytesRepeat(byte(i), 512)).Encode()); err != nil {
			t.Fatalf("initial write %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// The destination's serve ends (the agent stops the stream but KEEPS the
	// shared data listener for the next unit): the receiver app goes away, the
	// listener stays open and accepts re-dials into the backlog without
	// reading (no HELLO, no ACK).
	r.Close()

	// The sender keeps writing: it must fail (resume rejection) within a few
	// ACK timeouts — NOT after MaxReconnects (20) reconnects.
	deadline := time.Now().Add(5 * time.Second)
	var writeErr error
	for time.Now().Before(deadline) {
		if _, writeErr = s.Write(testFrame(1, 1, 100, bytesRepeat(0, 512)).Encode()); writeErr != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if writeErr == nil {
		t.Fatal("expected the sender to fail after repeated resume rejections")
	}
	// Fast death: the rejection (3 consecutive no-progress deaths) ends the
	// pipe far before the MaxReconnects (20) budget, and well before the
	// 5s deadline. (The Write surfaces the last transport error — the write
	// races the reconnect's close — so the rejection itself is observed via
	// the small reconnect count.)
	if st := s.Stats(); st.Reconnects > 6 {
		t.Fatalf("expected fast death (<=6 reconnects), got %d", st.Reconnects)
	}
}
