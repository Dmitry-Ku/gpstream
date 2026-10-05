package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestReliablePipeRouterPoolBinding is the PRODUCTION on_segment topology:
// the destination agent knows only the slice COUNT (S), not the source
// segment content ids (the kernel substitutes <SEGID> on each source segment).
// So it pre-creates S pool slots, and the router BINDS a StreamID to a free
// slot on first sight of that StreamID's connection. A resume re-dial for the
// bound StreamID must reach the same slot (no free slots left to mis-route
// to). Two slices, one shared listener, slice A breaks mid-stream: both must
// finish exactly-once, in order.
func TestReliablePipeRouterPoolBinding(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	router := NewResumeRouter(l)
	go router.Run()
	defer router.Close()

	const n = 200
	const size = 4096

	// The destination pre-creates S=2 pool slots (it does not know the
	// content ids). Each is fed to a receiver pipe (in a goroutine: the
	// receiver blocks on its slot until the router hands over the conn, as
	// the production dest agent does).
	slot1 := router.NewPoolSlot()
	slot2 := router.NewPoolSlot()

	cfg := DefaultReliablePipeConfig()
	cfg.ResumeTimeout = 5 * time.Second

	type recRes struct {
		rp  *ReliablePipe
		err error
	}
	recCh1 := make(chan recRes, 1)
	recCh2 := make(chan recRes, 1)
	go func() {
		rp, e := NewReliablePipeReceiverOnSlot(cfg, slot1, 15*time.Second)
		recCh1 <- recRes{rp, e}
	}()
	go func() {
		rp, e := NewReliablePipeReceiverOnSlot(cfg, slot2, 15*time.Second)
		recCh2 <- recRes{rp, e}
	}()

	// Two segreceivers (distinct source content ids) dial the shared
	// listener. The first to be routed binds its StreamID to a free slot.
	idA := StreamID{JobID: 7, UnitID: 3, Content: 10}
	idB := StreamID{JobID: 7, UnitID: 3, Content: 11}
	dial := func() (dupConn, error) {
		return net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
	}
	cA, err := dial()
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	sndA, err := NewReliablePipeSenderOnConnID(cfg, cA, dial, idA)
	if err != nil {
		t.Fatalf("sender A: %v", err)
	}
	cB, err := dial()
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	sndB, err := NewReliablePipeSenderOnConnID(cfg, cB, dial, idB)
	if err != nil {
		t.Fatalf("sender B: %v", err)
	}

	// Both receivers must come up (dynamic binding routed the two inits).
	r1 := <-recCh1
	if r1.err != nil {
		t.Fatalf("receiver 1: %v", r1.err)
	}
	r2 := <-recCh2
	if r2.err != nil {
		t.Fatalf("receiver 2: %v", r2.err)
	}

	// Pump A (break mid-stream: its re-dial must be routed to the ALREADY
	// BOUND slot — there are no free slots left, so a routing mistake fails
	// the resume) and B (no break).
	var aPumped, bPumped int64
	go func() {
		for i := 0; i < n; i++ {
			if _, werr := sndA.Write(testFrame(7, 3, uint64(i), bytesRepeatA(i, size)).Encode()); werr != nil {
				return
			}
			atomic.AddInt64(&aPumped, 1)
			if i == n/2-1 {
				sndA.CloseTransport()
			}
		}
	}()
	go func() {
		for i := 0; i < n; i++ {
			if _, werr := sndB.Write(testFrame(7, 3, uint64(i), bytesRepeatB(i, size)).Encode()); werr != nil {
				return
			}
			atomic.AddInt64(&bPumped, 1)
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&aPumped) == n && atomic.LoadInt64(&bPumped) == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt64(&aPumped) != n {
		t.Fatalf("slice A pumped %d/%d (resume may have failed)", atomic.LoadInt64(&aPumped), n)
	}
	if atomic.LoadInt64(&bPumped) != n {
		t.Fatalf("slice B pumped %d/%d", atomic.LoadInt64(&bPumped), n)
	}

	_ = sndA.Close()
	_ = sndB.Close()

	// Which receiver got which slice is up to the binding order: identify by
	// the first frame's payload (A = byte 0, B = byte 128) and verify each.
	k1, err1 := verifySliceByFirstByte(t, r1.rp, n, size)
	if err1 != nil {
		t.Fatalf("receiver 1: %v", err1)
	}
	k2, err2 := verifySliceByFirstByte(t, r2.rp, n, size)
	if err2 != nil {
		t.Fatalf("receiver 2: %v", err2)
	}
	if k1 == k2 {
		t.Fatalf("both receivers drained the same slice (k1=%d k2=%d) — cross-slice leak", k1, k2)
	}
	if k1 != 0 && k1 != 1 {
		t.Fatalf("receiver 1 drained unknown slice k=%d", k1)
	}
}

// verifySliceByFirstByte drains rec to EOF, identifies the slice by the
// first frame's payload byte (0 = A / bytesRepeatA, 128 = B / bytesRepeatB)
// and asserts the frames are exactly 0..n-1 in order with that payload.
// Returns 0 for slice A, 1 for slice B.
func verifySliceByFirstByte(t *testing.T, rec *ReliablePipe, n, size int) (int, error) {
	t.Helper()
	first, err := ReadFrame(context.Background(), rec)
	if err != nil {
		return -1, fmt.Errorf("first frame: %w", err)
	}
	if len(first.Data) != size {
		return -1, errors.New("first frame wrong size")
	}
	var slice int
	var repeat func(i, size int) []byte
	switch first.Data[0] {
	case 0:
		slice, repeat = 0, bytesRepeatA
	case 128:
		slice, repeat = 1, bytesRepeatB
	default:
		return -1, fmt.Errorf("unknown slice (first byte %d)", first.Data[0])
	}
	// Frame 0 must be seq 0; frames 1..n-1 follow in order.
	if first.Seq != 0 {
		return slice, errors.New("first frame seq != 0")
	}
	for i := 1; i < n; i++ {
		f, err := ReadFrame(context.Background(), rec)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return slice, fmt.Errorf("truncated: %d/%d frames", i, n)
			}
			return slice, err
		}
		if f.Seq != uint64(i) {
			return slice, fmt.Errorf("out-of-order/gap at pos %d: seq %d", i, f.Seq)
		}
		want := repeat(i, size)
		if len(f.Data) != size {
			return slice, fmt.Errorf("frame %d wrong size", i)
		}
		for k := range f.Data {
			if f.Data[k] != want[k] {
				return slice, fmt.Errorf("frame %d payload mismatch", i)
			}
		}
	}
	return slice, nil
}
