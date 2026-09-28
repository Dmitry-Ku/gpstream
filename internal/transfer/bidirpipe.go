package transfer

import (
	"errors"
	"io"
	"sync"
)

// bufPipe is a bounded, full-duplex-capable byte pipe with a socket-like
// buffer: a Write returns once the bytes fit in the buffer (it does NOT block
// until the peer reads), and a Read blocks until bytes are available. This is
// the in-process stand-in for the TCP connection "B" of DESIGN §4.6 — the
// socket buffer is exactly what lets the two-way HELLO handshake complete
// (each side can send its half without waiting for the other to read).
type bufPipe struct {
	cap int

	mu      sync.Mutex
	buf     []byte
	closed  bool
	readers int
	writers int
	dataCv  *sync.Cond // data available (for Read)
	spaceCv *sync.Cond // space available (for Write)
}

func newBufPipe(cap int) *bufPipe {
	if cap <= 0 {
		cap = 1 << 20
	}
	p := &bufPipe{cap: cap, readers: 1, writers: 1}
	p.dataCv = sync.NewCond(&p.mu)
	p.spaceCv = sync.NewCond(&p.mu)
	return p
}

// Write sends b to the peer. Blocks only while the buffer is full.
func (p *bufPipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := len(b)
	if p.writers == 0 {
		return 0, errClosed
	}
	// Copy into the buffer in chunks, waiting for space as needed.
	for len(b) > 0 {
		space := p.cap - len(p.buf)
		if space <= 0 {
			if p.readers == 0 {
				return 0, errClosed
			}
			p.spaceCv.Wait()
			continue
		}
		n := len(b)
		if n > space {
			n = space
		}
		p.buf = append(p.buf, b[:n]...)
		b = b[n:]
		p.dataCv.Signal()
	}
	return total, nil
}

func (p *bufPipe) bufLen() int { return len(p.buf) }

// Read reads bytes the peer sent. Blocks until data is available or the pipe
// is closed and drained.
func (p *bufPipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.buf) == 0 {
		if p.readers == 0 || (p.writers == 0) {
			return 0, io.EOF
		}
		p.dataCv.Wait()
	}
	n := copy(b, p.buf)
	p.buf = p.buf[n:]
	p.spaceCv.Signal()
	return n, nil
}

// Close shuts the pipe down. Idempotent. A subsequent Read drains any buffered
// data and then returns io.EOF; a Write returns an error.
func (p *bufPipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writers == 0 && p.readers == 0 {
		return nil
	}
	p.writers = 0
	p.readers = 0
	p.dataCv.Broadcast()
	p.spaceCv.Broadcast()
	return nil
}

var errClosed = errors.New("bufPipe: closed")

// bidirPipe is an in-memory, full-duplex, connected byte channel: two
// endpoints wired together so a write on one side is readable on the other, in
// both directions. Built from two buffered bufPipes (a->b and b->a) so it
// inherits socket-like buffering (a write does not block until the peer
// reads). A net.Conn satisfies the same io.ReadWriteCloser contract, so the
// ReliablePipe built on it is transport-agnostic: in-process today, over the
// wire in a distributed deployment.
type bidirPipe struct {
	out *bufPipe // this side writes here; the peer reads it
	in  *bufPipe // this side reads here; the peer wrote it
}

// newBidirPipe returns two connected bidirectional endpoints (a and b). A byte
// written to a is readable from b and vice versa.
func newBidirPipe() (a, b *bidirPipe) {
	ab := newBufPipe(1 << 20) // a -> b
	ba := newBufPipe(1 << 20) // b -> a
	return &bidirPipe{out: ab, in: ba}, &bidirPipe{out: ba, in: ab}
}

// Read implements io.Reader (reads bytes the peer wrote to us).
func (bp *bidirPipe) Read(p []byte) (int, error) { return bp.in.Read(p) }

// Write implements io.Writer (sends bytes to the peer).
func (bp *bidirPipe) Write(p []byte) (int, error) { return bp.out.Write(p) }

// Close shuts both directions down. Idempotent.
func (bp *bidirPipe) Close() error {
	_ = bp.out.Close()
	_ = bp.in.Close()
	return nil
}

// dupConn is the minimal read/write/close contract the ReliablePipe needs from
// a transport endpoint. A net.Conn and a *bidirPipe both satisfy it.
type dupConn interface {
	io.ReadWriteCloser
}
