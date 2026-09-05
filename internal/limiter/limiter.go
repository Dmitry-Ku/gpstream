// Package limiter implements the adaptive rate limiter (FR-14). One limiter
// runs per host-agent; it reads host load metrics (CPU iowait from
// /proc/stat, disk await/util from /proc/diskstats, TCP retransmits) and
// reduces the *rate of a specific host-pair* by a step (~25%, floor ~1 Gbps)
// on sustained overload, then recovers when load subsides.
//
// Each change is a diagnostic message carrying the metric values (FR-14), so
// the degradation is observable (FR-04). The limiter is decoupled from
// /proc: it takes a Metrics provider so it is unit-testable without a live
// host (CР-2/FR-14).
package limiter

import (
	"sync"
)

// Metrics is a snapshot of host load metrics (FR-14). In production these come
// from /proc/stat, /proc/diskstats and the netstat counters; in tests they
// are injected.
type Metrics struct {
	// IOWaitPercent is the CPU iowait percentage (0-100).
	IOWaitPercent float64
	// DiskAwait is the average per-disk await (ms); 0 if unknown.
	DiskAwait float64
	// DiskUtil is the average per-disk utilisation (0-100+); 0 if unknown.
	DiskUtil float64
	// Retransmits is the TCP retransmit rate (per second); 0 if unknown.
	Retransmits float64
}

// Provider supplies Metrics snapshots (so the limiter is testable).
type Provider interface {
	Snapshot() Metrics
}

// Limiter is the adaptive rate limiter for one host-pair (FR-14).
type Limiter struct {
	mu          sync.Mutex
	baseline    float64 // Gbps ceiling (from --rate-limit)
	current     float64 // current Gbps (starts at baseline)
	floor       float64 // minimum Gbps (~1 Gbps)
	step        float64 // reduction fraction (~0.25)
	recoverStep float64 // recovery fraction per healthy interval
	overload    float64 // sustained overload score (iowait% etc.)
	provider    Provider
	onChange    func(Gbps, Gbps, string) // callback for diagnostic logging
}

// Options configure a Limiter.
type Options struct {
	Baseline float64 // Gbps ceiling
	Floor    float64 // minimum Gbps (default 1)
	Step     float64 // reduction fraction (default 0.25)
	Provider Provider
}

// New creates a limiter. Zero options fall back to sane defaults (FR-14).
func New(o Options) *Limiter {
	floor := o.Floor
	if floor <= 0 {
		floor = 1
	}
	step := o.Step
	if step <= 0 {
		step = 0.25
	}
	return &Limiter{
		baseline:    o.Baseline,
		current:     o.Baseline,
		floor:       floor,
		step:        step,
		recoverStep: 0.10,
		provider:    o.Provider,
	}
}

// SetOnChange registers a callback invoked on every rate change (FR-14
// diagnostic logging, FR-04).
func (l *Limiter) SetOnChange(fn func(from, to Gbps, reason string)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onChange = fn
}

// Current returns the current rate in Gbps.
func (l *Limiter) Current() Gbps {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

// Update samples the host metrics once and adjusts the rate. It is called
// periodically (e.g. every second) by the host-agent.
func (l *Limiter) Update() {
	if l.provider == nil {
		return
	}
	m := l.provider.Snapshot()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.overload = m.IOWaitPercent
	switch {
	case m.IOWaitPercent > 30: // sustained iowait > 30% (FR-14 example)
		l.reduceLocked("iowait", m)
	case m.DiskUtil > 90:
		l.reduceLocked("disk-util", m)
	case m.Retransmits > 100:
		l.reduceLocked("tcp-retransmits", m)
	default:
		// Load is healthy: recover towards baseline (FR-14).
		if l.current < l.baseline {
			before := l.current
			l.current += l.current * l.recoverStep
			if l.current > l.baseline {
				l.current = l.baseline
			}
			if l.current > before {
				l.emit(before, l.current, "recover: load normal")
			}
		}
	}
}

// reduceLocked reduces the current rate by the step (floor-limited). It must
// be called with l.mu held.
func (l *Limiter) reduceLocked(cause string, m Metrics) string {
	if l.current <= l.floor {
		return "" // already at the floor
	}
	before := l.current
	l.current -= l.current * l.step
	if l.current < l.floor {
		l.current = l.floor
	}
	return l.emit(before, l.current, cause)
}

// emit records the change and fires the diagnostic callback (FR-14/FR-04).
func (l *Limiter) emit(from, to Gbps, cause string) string {
	if l.onChange != nil {
		l.onChange(from, to, cause)
	}
	return cause
}

// Gbps is a rate in gigabits per second.
type Gbps = float64
