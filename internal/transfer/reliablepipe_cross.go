// reliablepipe_cross.go — cross-process ReliablePipe constructors (Inc3).
//
// NewReliablePipePair / NewReliablePipeTCPPair wire BOTH ends of one pipe into a
// single process, coordinating the re-accept through a shared *rpLink (the
// sender's dialer literally creates the receiver's fresh transport). That is
// correct for the in-process validation tests, but the production data path
// (source-agent / segreceiver process <-> destination-agent process) needs the
// two ends in DIFFERENT processes: there is no shared link, the sender re-dials
// the receiver's listener over TCP, and the receiver re-accepts on its own.
//
// These constructors build each end with its OWN link (no shared state):
//   - NewReliablePipeSender: the sender end. It dials l.Addr() once up front and
//     re-dials the SAME address on every break (the RST-resume); its link only
//     tracks its own reconnect budget / abort flag.
//   - NewReliablePipeReceiver: the receiver end. It accepts the initial
//     connection and, on each break, performs ONE bounded re-accept of the
//     sender's re-dial (cfg.ResumeTimeout); if none arrives in time it declares
//     the sender gone (the stream ends -> the unit fails -> the controller
//     resumes). Accepting only on a break (not a continuous accept loop) keeps
//     a shared data listener free for the next unit between breaks.
//
// The receiver owns the listener's lifetime is left to the caller (the
// destination agent closes it on release); Close() here tears down the pipe end
// (and, for the receiver, the listener is NOT closed so the agent can reuse it
// for the next unit).
package transfer

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// NewReliablePipeSender builds the SENDER end of a cross-process frame-ACK'd
// pipe. It dials addr once (bounded) and starts the sender's receiveLoop +
// watchdog; on a transport break it re-dials addr and replays the un-ACKed
// ring. addr must be a listening receiver (NewReliablePipeReceiver).
func NewReliablePipeSender(cfg ReliablePipeConfig, addr string) (*ReliablePipe, error) {
	dialer := func() (dupConn, error) {
		return net.DialTimeout("tcp", addr, 10*time.Second)
	}
	c, err := dialer()
	if err != nil {
		return nil, &tcpPairError{op: "dial", err: err}
	}
	return NewReliablePipeSenderOnConn(cfg, c, dialer)
}

// NewReliablePipeSenderOnConn builds the SENDER end using an already-
// established initial transport (firstConn — e.g. a TLS conn the caller dialed
// and wrote a pre-stream header such as the UnitSpec on). On a transport break
// the pipe re-establishes the transport via dialer (which must reproduce the
// full connection setup, e.g. dial+TLS handshake) and replays the un-ACKed
// ring. dialer is NOT called for the initial connection (firstConn is used);
// it is only the re-dial on a break.
func NewReliablePipeSenderOnConn(cfg ReliablePipeConfig, firstConn dupConn, dialer func() (dupConn, error)) (*ReliablePipe, error) {
	if cfg.MaxReconnects <= 0 {
		cfg.MaxReconnects = 20
	}
	if cfg.RetransmitBufferMax <= 0 {
		cfg.RetransmitBufferMax = 1 << 30
	}
	if cfg.AckByteInterval <= 0 {
		cfg.AckByteInterval = 1 << 20
	}
	if cfg.AckTimeInterval <= 0 {
		cfg.AckTimeInterval = 100 * time.Millisecond
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	link := newLink()
	rp := newReliablePipe(rpSender, cfg, link, firstConn)
	link.s = rp
	rp.dialer = dialer
	rp.wg.Add(2)
	go rp.receiveLoop()
	go rp.watchdogLoop()
	return rp, nil
}

// NewReliablePipeReceiver builds the RECEIVER end of a cross-process frame-
// ACK'd pipe. It accepts the initial connection from l (bounded) and starts
// the receiver's receiveLoop + ackLoop. On a transport break it performs one
// bounded re-accept (cfg.ResumeTimeout) of the sender's re-dial.
func NewReliablePipeReceiver(cfg ReliablePipeConfig, l net.Listener) (*ReliablePipe, error) {
	first, err := boundedAccept(l, 30*time.Second)
	if err != nil {
		return nil, &tcpPairError{op: "accept", err: err}
	}
	return NewReliablePipeReceiverOnConn(cfg, first, l)
}

// NewReliablePipeReceiverOnConn builds the RECEIVER end using an already-
// accepted initial transport (firstConn — e.g. the conn the caller read a
// pre-stream header such as the UnitSpec from). On a transport break it
// re-accepts ONE fresh connection from l (the shared data listener) for the
// sender's re-dial, bounded by cfg.ResumeTimeout; if none arrives in time the
// sender is declared gone. l must stay open for the life of the stream (the
// destination agent reuses it for the next unit between breaks).
func NewReliablePipeReceiverOnConn(cfg ReliablePipeConfig, firstConn dupConn, l net.Listener) (*ReliablePipe, error) {
	if cfg.MaxReconnects <= 0 {
		cfg.MaxReconnects = 20
	}
	if cfg.RetransmitBufferMax <= 0 {
		cfg.RetransmitBufferMax = 1 << 30
	}
	if cfg.AckByteInterval <= 0 {
		cfg.AckByteInterval = 1 << 20
	}
	if cfg.AckTimeInterval <= 0 {
		cfg.AckTimeInterval = 100 * time.Millisecond
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}
	if cfg.ResumeTimeout <= 0 {
		cfg.ResumeTimeout = 30 * time.Second
	}
	link := newLink()
	rp := newReliablePipe(rpReceiver, cfg, link, firstConn)
	link.r = rp
	// The per-break re-accept: the sender's RST-resume re-dial. Bounded so a
	// dead sender cannot hang the destination's unit forever (ResumeTimeout).
	rp.resumer = func() dupConn {
		deadline := time.Now().Add(cfg.ResumeTimeout)
		for {
			if rp.dead.Load() || rp.link.aborted.Load() {
				return nil
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil
			}
			c, _ := boundedAccept(l, remaining)
			if c == nil {
				return nil
			}
			// Only a resume conn (ctrl HELLO) belongs to this pipe. A fresh
			// conn (a UnitSpec header) would be the next unit's — under the
			// sequential control loop it cannot arrive mid-resume, so if it
			// does, close it and keep waiting within the budget.
			rc, isResume := PeekStreamPrefix(c)
			if !isResume {
				_ = rc.Close()
				continue
			}
			c = rc
			// If the receiver died (app Close / abort) while we were
			// accepting, drop the stray connection rather than swapping it
			// into a dead end.
			if rp.dead.Load() || rp.link.aborted.Load() {
				_ = c.Close()
				return nil
			}
			return c
		}
	}
	rp.wg.Add(2)
	go rp.receiveLoop()
	go rp.ackLoop()
	return rp, nil
}

// DupConn is the transport contract a ReliablePipe end reads/writes on (an
// alias of the internal dupConn): any io.ReadWriteCloser — a net.Conn, a
// *tls.Conn, or an in-memory pipe. Exported so agent packages can name the
// re-dial function type in NewReliablePipeSenderOnConn.
type DupConn = dupConn

// PeekStreamPrefix peeks the first 4 bytes of an accepted data connection and
// classifies it WITHOUT consuming the stream: it returns a conn that replays
// those 4 bytes before the live bytes (net.Conn has no multi-byte Unread, so
// they are wrapped, not un-read) plus a flag telling which kind of stream it
// is:
//
//	isResume == true  — the first 4 bytes are the control-frame magic
//	(magicCtrl): a frame-ACK resume connection (the sender greets every
//	re-dial with a ctrl HELLO). The destination's per-break re-accept wants
//	these.
//	isResume == false — the first 4 bytes are a pre-stream header (the
//	UnitSpec: 1 version byte + 4-byte little-endian length): a FRESH
//	connection for a unit. The destination's initial accept wants these.
//
// The caller decides what to do with the conn (use it, or close it and
// re-accept); this helper NEVER closes a live conn on its own — it only
// closes one that died before yielding 4 bytes (the read error case). The
// destination agent calls it on every accept: the initial accept (skip a
// STALE resume from a previous, already-ended unit, take the fresh UnitSpec
// conn) and each re-accept (take only resumes, close a fresh one — which,
// under the sequential control loop, can only be the next unit's conn and is
// therefore never expected mid-resume).
func PeekStreamPrefix(c DupConn) (DupConn, bool) {
	type deadlineConn interface {
		SetReadDeadline(time.Time) error
	}
	if d, ok := c.(deadlineConn); ok {
		_ = d.SetReadDeadline(time.Now().Add(10 * time.Second))
	}
	var magic [4]byte
	_, err := io.ReadFull(c, magic[:])
	if d, ok := c.(deadlineConn); ok {
		_ = d.SetReadDeadline(time.Time{})
	}
	if err != nil {
		_ = c.Close()
		return nil, false
	}
	isResume := binary.LittleEndian.Uint32(magic[:]) == magicCtrl
	return &prefixConn{prefix: append([]byte(nil), magic[:]...), inner: c}, isResume
}

// prefixConn prepends a byte prefix to a conn's read stream (the peeked ctrl
// HELLO magic, so the pipe's receiveLoop sees the connection exactly as the
// sender wrote it).
type prefixConn struct {
	prefix []byte
	inner  dupConn
}

func (p *prefixConn) Read(b []byte) (int, error) {
	if len(p.prefix) > 0 {
		n := copy(b, p.prefix)
		p.prefix = p.prefix[n:]
		return n, nil
	}
	return p.inner.Read(b)
}
func (p *prefixConn) Write(b []byte) (int, error) { return p.inner.Write(b) }
func (p *prefixConn) Close() error                { return p.inner.Close() }

// newLink builds a fresh, independent link (no in-memory a/b, a done channel).
func newLink() *rpLink {
	l := &rpLink{}
	l.swapCv = sync.NewCond(&l.mu)
	l.doneOnce.Do(func() { l.done = make(chan struct{}) })
	return l
}

// boundedAccept accepts one connection from l, or nil after d elapses / the
// listener is closed.
func boundedAccept(l net.Listener, d time.Duration) (dupConn, error) {
	type res struct {
		c   dupConn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := l.Accept()
		ch <- res{c: c, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return r.c, nil
	case <-time.After(d):
		return nil, errors.New("accept timeout")
	}
}
