package agent

// mini_idle.go — the L2 data-idle watchdog (DESIGN: "таймаут передачи данных").
//
// A unit's data (sub-)stream that receives NO byte for `--data-idle-timeout`
// (default 5m) is declared dead: the mini closes the slice connection, the
// receiver's blocked Read unblocks, the stream errors out, and the unit FAILS
// with a diagnostic instead of hanging the run (and, transitively, the
// controller's WaitUnitDone holding streamMu and deadlocking every worker —
// the BUG #13 class of hang).
//
// Why idle-based (not a hard total-timeout): a hard timeout cannot be
// calibrated against table size / link width (a 100 GB table over a narrow
// link legitimately takes hours). "No data transferred for N minutes" is
// size- and link-independent and is the metric the operator agreed on.
//
// The watchdog is PER (sub-)STREAM: in a routed unit (S source segments x W
// workers) every sub-stream is wrapped independently, so one dead source
// segment's sub-stream times out even while the others still flow.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
)

// idlePipe wraps a unit's data (sub-)stream pipe with the data-idle watchdog.
type idlePipe struct {
	inner   transfer.Pipe
	closeFn func() error // closes the underlying net.Conn (unblocks a blocked Read)
	timeout time.Duration

	last atomic.Int64 // unix-nano of the last data byte (seeded at accept)
	stop atomic.Bool
	once sync.Once
	done chan struct{}
}

// newIdlePipe wraps inner with the data-idle watchdog (timeout must be > 0)
// and starts the watchdog goroutine. The "last byte" marker is seeded to now
// (the accept instant), so "no data since accept" counts as idle — a
// segreceiver that connects but never sends a byte is caught, not only one
// that stalls mid-stream.
func newIdlePipe(inner transfer.Pipe, closeFn func() error, timeout time.Duration) *idlePipe {
	ip := &idlePipe{inner: inner, closeFn: closeFn, timeout: timeout, done: make(chan struct{})}
	ip.last.Store(time.Now().UnixNano())
	go ip.watch()
	return ip
}

// Read forwards to the inner pipe and refreshes the last-byte marker on any
// forward progress (n > 0).
func (ip *idlePipe) Read(p []byte) (int, error) {
	n, err := ip.inner.Read(p)
	if n > 0 {
		ip.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func (ip *idlePipe) Write(p []byte) (int, error) { return ip.inner.Write(p) }

// Close stops the watchdog and closes the inner pipe. Safe to call more than
// once (the underlying double-close returns an error that callers ignore).
func (ip *idlePipe) Close() error {
	ip.stop.Store(true)
	ip.once.Do(func() { close(ip.done) })
	return ip.inner.Close()
}

// watch ticks at timeout/2 and closes the connection once the stream has been
// idle for the full timeout.
func (ip *idlePipe) watch() {
	tick := time.NewTicker(ip.timeout / 2)
	defer tick.Stop()
	for range tick.C {
		if ip.stop.Load() {
			return
		}
		last := ip.last.Load()
		if last != 0 && time.Since(time.Unix(0, last)) >= ip.timeout {
			ip.stop.Store(true)
			_ = ip.closeFn() // unblock the blocked Read
			return
		}
		select {
		case <-ip.done:
			return
		default:
		}
	}
}
