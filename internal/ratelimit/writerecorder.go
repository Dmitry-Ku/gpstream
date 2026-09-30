package ratelimit

import (
	"sync"
	"time"
)

// WriteRecorder records the durations of data-path writes so the limiter can
// observe the destination's receive queue (F-14: "receive-queue growth —
// receiver worker data from /proc and COPY metrics").
//
// The data path is an unbuffered pipe (net.Pipe / ReliablePipe ring): a
// write only completes once the destination has drained the bytes. So the
// per-write duration IS the receive-queue depth as the controller observes
// it — the in-process "COPY metrics" the spec names, without needing the
// destination host's /proc (which the controller cannot read).
//
// The recorder keeps the samples of the last few seconds and reports the
// average write duration over a window (1s, matching the limiter sampler
// tick). It is safe for concurrent use: every in-flight unit records into
// the one run-wide recorder, and the limiter reads it from the sampler
// goroutine.
type WriteRecorder struct {
	mu      sync.Mutex
	samples []writeSample
}

type writeSample struct {
	at time.Time
	d  time.Duration
}

// maxSamples bounds memory: at 256KB frames and 50GB/s that is far more
// writes than possible per window; the cap is a safety net only.
const maxSamples = 8192

// window is the default averaging window for AvgMs (the limiter sampler
// ticks once per second).
const window = time.Second

// Record adds one write duration. Cheap (one append + occasional prune);
// it is called from every data-path write, so it must not allocate on the
// common path.
func (r *WriteRecorder) Record(d time.Duration) {
	now := time.Now()
	r.mu.Lock()
	r.samples = append(r.samples, writeSample{at: now, d: d})
	if len(r.samples) > maxSamples {
		// Drop the oldest half rather than scanning; the window is 1s, so
		// 4096 samples still hold far more than a second of writes.
		drop := len(r.samples) - maxSamples/2
		r.samples = r.samples[drop:]
	}
	r.pruneLocked(now)
	r.mu.Unlock()
}

// AvgMs returns the average write duration (ms) over the trailing window,
// or 0 when no write happened in the window (the limiter treats 0 as "the
// destination drained freely / no data moved").
func (r *WriteRecorder) AvgMs(w time.Duration) float64 {
	if w <= 0 {
		w = window
	}
	now := time.Now()
	cutoff := now.Add(-w)
	r.mu.Lock()
	r.pruneLocked(now)
	var sum, n float64
	for _, s := range r.samples {
		if s.at.Before(cutoff) {
			continue
		}
		sum += float64(s.d)
		n++
	}
	r.mu.Unlock()
	if n == 0 {
		return 0
	}
	return sum / n / float64(time.Millisecond)
}

// pruneLocked drops samples older than 2x the window (they are stale for any
// caller). Must be called with r.mu held.
func (r *WriteRecorder) pruneLocked(now time.Time) {
	cutoff := now.Add(-2 * window)
	i := 0
	for i < len(r.samples) && r.samples[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		r.samples = r.samples[i:]
	}
}
