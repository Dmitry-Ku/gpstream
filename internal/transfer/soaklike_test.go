package transfer

import (
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// benchSoakLike mirrors the T7 soak data path: 4 parallel stream workers,
// zstd, small rows batched into 256 KiB frames, 600 MB total, fast consumer.
// It reports elapsed + peak heap + total allocations to distinguish a CPU
// regression from a memory/GC regression.
func benchSoakLike(t *testing.T, reliable bool) (time.Duration, uint64, uint64) {
	const (
		workers = 4
		totalMB = 600
	)
	perStream := totalMB * 1024 * 1024 / workers
	comp := compress.New(compress.ModeZstd, 3)

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(unitID uint32) {
			defer wg.Done()
			dec := compress.NewDec(compress.ModeZstd)
			var wPipe, rPipe Pipe
			if reliable {
				s, r := NewReliablePipePair(DefaultReliablePipeConfig())
				wPipe, rPipe = s, r
			} else {
				pw, pr := netPipe()
				wPipe, rPipe = pw, pr
			}
			fp := NewFramedPipeOver(wPipe, rPipe, 1, unitID, comp, dec)
			// Fast consumer: drain as fast as possible (models a quick dest).
			go func() { _, _ = drainAll(fp) }()
			// Small rows batched into 256 KiB frames.
			chunk := make([]byte, 1<<10) // 1 KiB of "rows"
			for i := range chunk {
				chunk[i] = byte(i % 253)
			}
			for written := 0; written < perStream; written += len(chunk) {
				if _, err := fp.Write(chunk); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
			_ = fp.Close()
		}(uint32(w))
	}
	wg.Wait()
	elapsed := time.Since(start)
	runtime.ReadMemStats(&m1)
	return elapsed, m1.TotalAlloc - m0.TotalAlloc, m1.HeapAlloc
}

func drainAll(fp *FramedPipe) (int64, error) {
	buf := make([]byte, 1<<20)
	var n int64
	for {
		k, err := fp.Read(buf)
		n += int64(k)
		if err != nil {
			return n, err
		}
	}
}

func netPipe() (Pipe, Pipe) {
	a, b := net.Pipe()
	return a, b
}

func TestSoakLikeCompare(t *testing.T) {
	// warmup
	_, _, _ = benchSoakLike(t, false)
	eNP, allocNP, heapNP := benchSoakLike(t, false)
	eRP, allocRP, heapRP := benchSoakLike(t, true)
	t.Logf("net.Pipe:    %.1f ms, totalAlloc=%.0f MB, peakHeap=%.0f MB",
		float64(eNP.Nanoseconds())/1e6, float64(allocNP)/1e6, float64(heapNP)/1e6)
	t.Logf("ReliablePipe: %.1f ms, totalAlloc=%.0f MB, peakHeap=%.0f MB",
		float64(eRP.Nanoseconds())/1e6, float64(allocRP)/1e6, float64(heapRP)/1e6)
}
