package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Pacer throttles a byte flow to the limiter's *current* adaptive rate (FR-14).
// It is a token bucket that refills at the current Gbps (from Limiter.Current),
// so when the limiter reduces the rate on host overload the pacer slows the
// data flow, and when the load subsides the pacer recovers. When the baseline
// is 0 the pacer is a no-op (unlimited, FR-14).
type Pacer struct {
	l    *Limiter
	mu   sync.Mutex
	tok  float64 // available bytes to send now
	last time.Time
}

// NewPacer creates a pacer backed by l. It starts full (tok at the current
// burst) so the first writes are not throttled before the first sample.
func NewPacer(l *Limiter) *Pacer {
	burst := float64(l.Current()) * 1e9 / 8 * 1.0 // 1s of the current rate
	if burst <= 0 {
		burst = 1 << 20 // 1 MiB default burst when rate unknown
	}
	return &Pacer{l: l, tok: burst, last: time.Now()}
}

// Wait blocks until up to n bytes may be sent at the current rate, honouring
// ctx cancellation. It is a no-op when the limiter has no baseline (unlimited).
func (p *Pacer) Wait(ctx context.Context, n int) error {
	if p.l == nil || p.l.base() <= 0 {
		return nil // unlimited
	}
	p.mu.Lock()
	// Refill the bucket for the elapsed time at the *current* rate.
	now := time.Now()
	dt := now.Sub(p.last)
	p.last = now
	bps := p.bytesPerSecond()
	p.tok += dt.Seconds() * bps
	if p.tok > p.burst() {
		p.tok = p.burst()
	}
	need := float64(n)
	if p.tok >= need {
		p.tok -= need
		p.mu.Unlock()
		return nil
	}
	// How long until enough tokens accrue to send n bytes at the current rate.
	remaining := need - p.tok
	var waitTime time.Duration
	if bps > 0 {
		waitTime = time.Duration(remaining / bps * float64(time.Second))
	}
	p.mu.Unlock()
	if waitTime <= 0 {
		return nil
	}
	// Sleep the precise time to accrue the remaining tokens at the current
	// rate, honouring ctx cancellation. Advancing p.last past the wait (below)
	// prevents the next call from double-counting it. The limiter stays
	// adaptive between calls: each Wait refills at ratelimit.Current(), which the
	// sampler updates as host load changes (FR-14).
	t := time.NewTimer(waitTime)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	p.mu.Lock()
	// The wait already consumed the remaining tokens; reset the bucket baseline
	// so the next refill is measured from "now", not from before the sleep.
	p.tok = 0
	p.last = time.Now()
	p.mu.Unlock()
	return nil
}

// bytesPerSecond is the current throughput in bytes/sec (Gbps * 1e9 / 8).
func (p *Pacer) bytesPerSecond() float64 {
	bps := p.l.Current() * 1e9 / 8
	if bps < 1 { // never throttle to 0 (avoids a permanent stall); floor ~1 byte/s
		return 1
	}
	return bps
}

// burst is the token bucket capacity (~1s of the current rate).
func (p *Pacer) burst() float64 {
	b := p.bytesPerSecond()
	if b < 1<<20 {
		return 1 << 20
	}
	return b
}

// base returns the limiter's baseline (0 = unlimited).
func (l *Limiter) base() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.baseline
}
