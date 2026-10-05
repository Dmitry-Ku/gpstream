package transfer

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestReliablePipeRouterMultiTenantBreakResume is the on_segment core: TWO
// slices (two segreceivers) dial ONE shared data listener, each over its own
// ReliablePipe (identity-carrying HELLO). The destination's ResumeRouter
// routes each connection (initial AND resume) to the matching receiver-pipe.
// When slice A's transport breaks mid-stream, A re-dials the SAME listener;
// the router must route that re-dial to A's pipe (not B's), and A must resume
// exactly-once in order while B is untouched.
func TestReliablePipeRouterMultiTenantBreakResume(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	router := NewResumeRouter(l)
	go router.Run()
	defer router.Close()

	const n = 200
	const size = 4096

	idA := StreamID{JobID: 1, UnitID: 1, Content: 0}
	idB := StreamID{JobID: 1, UnitID: 1, Content: 1}

	// Register both receiver slots (the router hands conns to these).
	slotA, err := router.Register(idA)
	if err != nil {
		t.Fatalf("register A: %v", err)
	}
	slotB, err := router.Register(idB)
	if err != nil {
		t.Fatalf("register B: %v", err)
	}

	cfg := DefaultReliablePipeConfig()
	cfg.ResumeTimeout = 5 * time.Second

	dial := func() (dupConn, error) {
		return net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
	}

	// The two destination receiver-pipes (their transport comes from the
	// router slots, not a listener they own). Each blocks in its slot until
	// the matching segreceiver dials — so construct them in goroutines, as
	// the production dest agent does (it creates the receivers, then the
	// kernel launches the segreceivers which dial).
	type recRes struct {
		rp  *ReliablePipe
		err error
	}
	recACh := make(chan recRes, 1)
	recBCh := make(chan recRes, 1)
	go func() {
		rp, e := NewReliablePipeReceiverOnSlot(cfg, slotA, 15*time.Second)
		recACh <- recRes{rp, e}
	}()
	go func() {
		rp, e := NewReliablePipeReceiverOnSlot(cfg, slotB, 15*time.Second)
		recBCh <- recRes{rp, e}
	}()

	// Two source senders (segreceivers), each dialing the shared listener and
	// greeting with its own identity.
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

	// Both receivers must come up (each routed its slice's initial conn).
	ra := <-recACh
	if ra.err != nil {
		t.Fatalf("receiver A: %v", ra.err)
	}
	recA := ra.rp
	rb := <-recBCh
	if rb.err != nil {
		t.Fatalf("receiver B: %v", rb.err)
	}
	recB := rb.rp

	// Pump slice A's frames in a goroutine; break A's transport mid-stream.
	var aPumped int64
	go func() {
		for i := 0; i < n; i++ {
			if _, werr := sndA.Write(testFrame(1, 1, uint64(i), bytesRepeatA(i, size)).Encode()); werr != nil {
				return
			}
			atomic.AddInt64(&aPumped, 1)
			// Break after half the frames (mid-stream, un-ACKed ring non-empty).
			if i == n/2-1 {
				sndA.CloseTransport()
			}
		}
	}()

	// Pump slice B's frames (no break) concurrently.
	var bPumped int64
	go func() {
		for i := 0; i < n; i++ {
			if _, werr := sndB.Write(testFrame(1, 1, uint64(i), bytesRepeatB(i, size)).Encode()); werr != nil {
				return
			}
			atomic.AddInt64(&bPumped, 1)
		}
	}()

	// Wait for both senders to finish pumping (or time out).
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&aPumped) == n && atomic.LoadInt64(&bPumped) == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt64(&aPumped) != n {
		t.Fatalf("slice A pumped %d/%d (break may have failed the sender)", atomic.LoadInt64(&aPumped), n)
	}
	if atomic.LoadInt64(&bPumped) != n {
		t.Fatalf("slice B pumped %d/%d", atomic.LoadInt64(&bPumped), n)
	}

	// Clean end-of-stream: close both senders (FIN). The receivers then drain
	// to io.EOF.
	_ = sndA.Close()
	_ = sndB.Close()

	// Read each receiver to EOF and verify exactly-once, in order.
	// Slice A's payload is bytesRepeatA(i, size) = byte(i%251) x size; its seq
	// must be 0..n-1 with no dup/loss. Slice B is independent.
	if verr := verifySlice(t, "A", recA, n, size, bytesRepeatA); verr != nil {
		t.Fatal(verr)
	}
	if verr := verifySlice(t, "B", recB, n, size, bytesRepeatB); verr != nil {
		t.Fatal(verr)
	}
}

// verifySlice reads rec to EOF and asserts the frames are exactly 0..n-1,
// in order, each payload = repeat(i%251, size) (the in-order, exactly-once
// contract across the resume).
func verifySlice(t *testing.T, name string, rec *ReliablePipe, n, size int, repeat func(i, size int) []byte) error {
	t.Helper()
	var got []byte
	for i := 0; ; i++ {
		f, err := ReadFrame(context.Background(), rec)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if i >= n {
			return errors.New("more frames than expected (duplicate?)")
		}
		if f.Seq != uint64(i) {
			return errors.New("out-of-order or gap: frame at pos " + itoa(int64(i)) + " has seq " + itoa(int64(f.Seq)))
		}
		want := repeat(i, size)
		if len(f.Data) != size {
			return errors.New("frame " + itoa(int64(i)) + " wrong size " + itoa(int64(len(f.Data))))
		}
		for k := 0; k < size; k++ {
			if f.Data[k] != want[k] {
				return errors.New("frame " + itoa(int64(i)) + " payload mismatch")
			}
		}
		got = append(got, f.Data...)
	}
	if len(got) != n*size {
		return errors.New("total bytes " + itoa(int64(len(got))) + " != " + itoa(int64(n*size)))
	}
	return nil
}

// bytesRepeatA / bytesRepeatB produce distinct per-slice payloads so a
// cross-slice leak (A's frame read by B) is detected.
func bytesRepeatA(i, size int) []byte { return repeatByte(byte(i%251), size) }
func bytesRepeatB(i, size int) []byte { return repeatByte(byte((i%251)+128), size) }
func repeatByte(b byte, size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = b
	}
	return out
}
