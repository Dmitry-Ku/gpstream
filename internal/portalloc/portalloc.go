// Package portalloc hands out TCP ports for the gpstream data/control
// listeners, optionally restricted to an operator-supplied range
// (--data-port-range LO-HI). It is the single place that knows how to pick a
// free port within a range, so every bind site (the controller's local
// prebind, the in-proc test runner, and the remote agents' self-bind) shares
// the same allocation policy.
//
// When no range is set (lo == 0) every helper degrades to the kernel-picks
// behavior ("host:0"), so a run without --data-port-range is byte-for-byte
// unchanged from before the flag existed.
package portalloc

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// isAddrInUse reports whether the bind failure means "this port is taken —
// try the next one". The kernel surfaces it as syscall.EADDRINUSE wrapped in
// *net.OpError (sometimes *net.AddrError on older runtimes); anything else
// (bad address, permission) is a hard error.
func (a *Allocator) isAddrInUse(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.EADDRINUSE)
}

// Allocator hands out distinct ports within [lo, hi]. lo == 0 disables the
// range (kernel picks). The reserved set guarantees the allocator never hands
// the same port to two callers in the same process (successive Next calls do
// not re-probe a port it already gave out).
type Allocator struct {
	lo, hi     int
	reserved   map[int]bool
	exclude    map[int]bool // caller-provided "do not pick" (e.g. a port this process already bound)
	mu         sync.Mutex
}

// New builds an Allocator. hi < lo is a misconfiguration and yields a
// disabled allocator (the caller validates the range in config.Validate).
func New(lo, hi int) *Allocator {
	if lo > 0 && hi < lo {
		lo, hi = 0, 0
	}
	return &Allocator{lo: lo, hi: hi, reserved: map[int]bool{}, exclude: map[int]bool{}}
}

// ParseRange parses a "LO-HI" (or single "LO") port range into an Allocator.
// An empty string yields a disabled allocator (kernel picks). A malformed
// range is an error (the agent / controller surfaces it at startup).
func ParseRange(s string) (*Allocator, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return New(0, 0), nil
	}
	loStr, hiStr := s, s
	if i := strings.Index(s, "-"); i >= 0 {
		loStr, hiStr = s[:i], s[i+1:]
	}
	lo, err := strconv.Atoi(strings.TrimSpace(loStr))
	if err != nil || lo < 1 || lo > 65535 {
		return nil, fmt.Errorf("portalloc: low port %q must be 1-65535", loStr)
	}
	hi, err := strconv.Atoi(strings.TrimSpace(hiStr))
	if err != nil || hi < 1 || hi > 65535 {
		return nil, fmt.Errorf("portalloc: high port %q must be 1-65535", hiStr)
	}
	if hi < lo {
		return nil, fmt.Errorf("portalloc: high port %d < low port %d", hi, lo)
	}
	return New(lo, hi), nil
}

// Enabled reports whether a range is active.
func (a *Allocator) Enabled() bool { return a.lo > 0 }

// Range returns the active [lo, hi] (0,0 when disabled).
func (a *Allocator) Range() (lo, hi int) { return a.lo, a.hi }

// SetExclude marks ports the allocator must never pick (used when a process
// has already bound one and now binds a second listener from the same range).
func (a *Allocator) SetExclude(ports ...int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range ports {
		a.exclude[p] = true
	}
}

func (a *Allocator) taken(p int) bool {
	return a.reserved[p] || a.exclude[p]
}

// Release frees a port previously handed out by Next/Listen so it can be
// handed out again. Callers that reuse ports (the in-proc runner binds a
// fresh data port per unit, serially) release it once the unit's listener is
// closed; otherwise the reserved set would grow one port per unit and a small
// range would run out. Releasing a port that is not reserved is a no-op.
func (a *Allocator) Release(p int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.reserved, p)
}

// Next returns a free port for host, reserving it (so a later Next call does
// not re-probe it) and closing the probe listener. The port is handed to a
// separate process (a local agent) which re-binds it; the tiny close->rebind
// window is the same pre-existing race as the old :0 prebind and is surfaced
// as a clear bind error if it ever loses, not silent corruption.
func (a *Allocator) Next(host string) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.Enabled() {
		l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			return 0, err
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		return port, nil
	}
	for p := a.lo; p <= a.hi; p++ {
		if a.taken(p) {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort(host, itoa(p)))
		if err != nil {
			if a.isAddrInUse(err) {
				continue // a foreign process holds it; try the next
			}
			return 0, fmt.Errorf("portalloc: probe %s:%d: %w", host, p, err)
		}
		_ = l.Close()
		a.reserved[p] = true
		return p, nil
	}
	return 0, fmt.Errorf("portalloc: no free port in range %d-%d on %s (widen --data-port-range)", a.lo, a.hi, host)
}

// Listen binds a listener on host within the active range and returns it
// OPEN (the caller owns it). This is the no-race path: the agent binds the
// port it will actually serve on, so nothing can steal it between pick and
// bind. With no range it binds host:0 (kernel picks).
func (a *Allocator) Listen(host string) (net.Listener, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.Enabled() {
		return net.Listen("tcp", net.JoinHostPort(host, "0"))
	}
	for p := a.lo; p <= a.hi; p++ {
		if a.taken(p) {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort(host, itoa(p)))
		if err != nil {
			if a.isAddrInUse(err) {
				continue
			}
			return nil, fmt.Errorf("portalloc: listen %s:%d: %w", host, p, err)
		}
		a.reserved[p] = true
		return l, nil
	}
	return nil, fmt.Errorf("portalloc: no free port in range %d-%d on %s (widen --data-port-range)", a.lo, a.hi, host)
}

func itoa(p int) string { return strconv.Itoa(p) }

// ListenAddr binds the listener for a "host:port" address. When the port is
// 0 (the "pick a free port" sentinel) and a range is active, the port is
// chosen from the range on that host (no close->rebind race); otherwise the
// address is bound verbatim (a fixed port, or a disabled range = legacy :0).
// This is the single entry point the agents use, so a remote agent that the
// controller launches with "host:0" still honors --data-port-range.
func (a *Allocator) ListenAddr(addr string) (net.Listener, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return net.Listen("tcp", addr)
	}
	p, aerr := strconv.Atoi(portStr)
	if aerr != nil || p != 0 || !a.Enabled() {
		return net.Listen("tcp", addr)
	}
	return a.Listen(host)
}
