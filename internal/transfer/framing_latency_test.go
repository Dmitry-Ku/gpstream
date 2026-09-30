package transfer

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// TestFramedPipeWriteLatencyHook verifies the F-14 receive-queue hook:
// each source-side transport write reports its duration, and when the
// destination stops draining (a slow reader on the unbuffered pipe) the
// recorded write durations grow — the receive-queue signal the adaptive
// limiter consumes.
func TestFramedPipeWriteLatencyHook(t *testing.T) {
	comp := compress.New(compress.ModeNone, 0)
	dec := compress.NewDec(compress.ModeNone)
	w, r := net.Pipe()
	fp := NewFramedPipe(w, r, 1, 1, comp, dec)

	type lat struct {
		n   int
		sum time.Duration
	}
	var mu sync.Mutex
	var cur lat

	fp.SetWriteLatencyHook(func(d time.Duration) {
		mu.Lock()
		cur.n++
		cur.sum += d
		mu.Unlock()
	})

	// A slow reader: sleeps 5ms per chunk, so the source's 256KB frame
	// writes block until it drains (unbuffered pipe back-pressure).
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		buf := make([]byte, 4096)
		for {
			if _, err := r.Read(buf); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	payload := make([]byte, 600*1024) // > frameBufLimit (256KB) → ≥2 writes
	if _, err := fp.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := fp.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	<-slowDone

	mu.Lock()
	n, sum := cur.n, cur.sum
	mu.Unlock()
	if n == 0 {
		t.Fatal("no write latencies recorded")
	}
	avgSlow := sum / time.Duration(n)
	if avgSlow < 2*time.Millisecond {
		t.Errorf("slow-reader avg write latency = %v, want >= 2ms against a 5ms-sleep reader (receive queue must show)", avgSlow)
	}

	// A second pipe with a fast reader: writes drain instantly.
	w2, r2 := net.Pipe()
	fp2 := NewFramedPipe(w2, r2, 1, 2, comp, dec)
	var n2 int
	var sum2 time.Duration
	var mu2 sync.Mutex
	fp2.SetWriteLatencyHook(func(d time.Duration) {
		mu2.Lock()
		n2++
		sum2 += d
		mu2.Unlock()
	})
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		// Drain to EOF (the Close() tail write) so the source never blocks
		// on a gone reader.
		io.Copy(discard{}, r2)
	}()
	if _, err := fp2.Write(payload); err != nil {
		t.Fatalf("write2: %v", err)
	}
	_ = fp2.Close()
	<-fastDone
	mu2.Lock()
	fn, fsum := n2, sum2
	mu2.Unlock()
	if fn == 0 {
		t.Fatal("no write latencies recorded on the fast path")
	}
	avgFast := fsum / time.Duration(fn)
	// The fast reader drains instantly, so its write latency must be a small
	// fraction of the slow reader's (the receive queue shows up as the
	// difference). Use a relative bound so the assertion is robust to the
	// race detector's memcpy overhead.
	if avgFast >= avgSlow/4 {
		t.Errorf("fast-reader avg write latency = %v (slow = %v), want < slow/4 (drains freely)", avgFast, avgSlow)
	}
}

// discard is an io.Writer that swallows bytes.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
