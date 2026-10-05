// reliablepipe_router.go — multi-tenant RST-resume routing (Inc3, on_segment).
//
// The on_segment data path has S senders (one segreceiver per source segment)
// all dialing ONE shared data listener on the destination agent. Each slice is
// its own ReliablePipe (sender=segreceiver, receiver=one destination slot).
// When one slice's transport breaks, its segreceiver re-dials the SAME shared
// listener (RST-resume); that re-dial must reach the receiver-pipe of the
// matching slice, not a sibling's.
//
// This file adds the routing primitive:
//
//   - StreamID identifies one slice (job, unit, source-content) and rides the
//     HELLO, so a resume re-dial carries enough to be routed.
//   - ResumeRouter owns the shared listener, reads each accepted connection's
//     HELLO, and hands the connection to the pipe registered under that
//     StreamID. Initial and resume connections are routed identically.
//   - NewReliablePipeReceiverOnSlot builds a receiver whose transport (initial
//     AND resume) is supplied by a router handoff channel, not its own
//     listener. The HELLO is replayed on each connection so the pipe's
//     existing HELLO handling (version check + progress) runs unchanged.
//
// The coordinator leg (single sender -> single receiver, one pipe) does NOT
// use the router: it re-accepts on its own listener (NewReliablePipeReceiver-
// OnConn), which is safe because only one sender can re-dial it.
package transfer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// StreamID identifies one data slice (one sender->receiver pipe) on a shared
// listener. It rides the HELLO so a resume re-dial can be routed to the
// correct pipe. It covers all three on_segment sub-paths:
//
//	merge (c):  Content=source segment, DstContent=0, Worker=0
//	routed (b): Content=source segment, DstContent=destination segment,
//	           Worker=the (s,d) worker index
//	direct (a): Content=0 (no source-segment fan-in), DstContent=destination
//	           segment, Worker=0
//	coordinator (unused — the coordinator leg does not route): all zero.
type StreamID struct {
	JobID  uint32
	UnitID uint32
	// Content is the SOURCE segment content (merge/routed); 0 for direct.
	Content int16
	// DstContent is the DESTINATION segment content (routed/direct); 0 for
	// merge.
	DstContent int16
	// Worker is the per-pair worker index (routed); 0 otherwise.
	Worker int32
}

func (s StreamID) String() string {
	return fmt.Sprintf("job=%d unit=%d src=%d dst=%d worker=%d",
		s.JobID, s.UnitID, s.Content, s.DstContent, s.Worker)
}

// helloVerID is the HELLO version that carries a StreamID (the on_segment
// multi-tenant resume). A HELLO body of version helloVerID is
// version(4)|seq(8)|jobID(4)|unitID(4)|contentID(2)|dstContentID(2)|worker(4)
// = 28 bytes; version helloVer (2) is version(4)|seq(8) = 12 bytes (no
// identity, coordinator).
const helloVerID = 3

const helloBodyID = 28

// encodeHelloID builds a StreamID-carrying HELLO body (version helloVerID).
func encodeHelloID(seq uint64, id StreamID) []byte {
	b := make([]byte, helloBodyID)
	binary.LittleEndian.PutUint32(b[0:4], helloVerID)
	binary.LittleEndian.PutUint64(b[4:12], seq)
	binary.LittleEndian.PutUint32(b[12:16], id.JobID)
	binary.LittleEndian.PutUint32(b[16:20], id.UnitID)
	binary.LittleEndian.PutUint16(b[20:22], uint16(id.Content))
	binary.LittleEndian.PutUint16(b[22:24], uint16(id.DstContent))
	binary.LittleEndian.PutUint32(b[24:28], uint32(id.Worker))
	return b
}

// decodeHelloID parses a StreamID from a HELLO body (version helloVerID).
func decodeHelloID(body []byte) (StreamID, bool) {
	if len(body) < helloBodyID {
		return StreamID{}, false
	}
	if v := binary.LittleEndian.Uint32(body[0:4]); v != helloVerID {
		return StreamID{}, false
	}
	return StreamID{
		JobID:      binary.LittleEndian.Uint32(body[12:16]),
		UnitID:     binary.LittleEndian.Uint32(body[16:20]),
		Content:    int16(binary.LittleEndian.Uint16(body[20:22])),
		DstContent: int16(binary.LittleEndian.Uint16(body[22:24])),
		Worker:     int32(binary.LittleEndian.Uint32(body[24:28])),
	}, true
}

// ResumeRouter accepts connections on a SHARED data listener, reads each
// connection's HELLO (its StreamID), and hands the connection to the pipe
// registered under that StreamID. Both initial and resume (re-dial)
// connections are routed the same way, so a broken slice's re-dial reaches
// its own receiver-pipe. The router replays the HELLO bytes on the handed-off
// connection (prefixConn) so the receiving pipe sees the connection exactly
// as the sender wrote it (its HELLO handler + data frames).
//
// A connection whose StreamID has no registered pipe (a stale resume from a
// previous unit, or a sibling's re-dial after this unit ended) is closed.
type ResumeRouter struct {
	l net.Listener

	mu     sync.Mutex
	slots  map[StreamID]chan dupConn
	closed bool
	done   chan struct{}
	once   sync.Once
}

// NewResumeRouter builds a router over the shared data listener l. The caller
// runs Run in a goroutine for the unit's lifetime and Close on unit end.
func NewResumeRouter(l net.Listener) *ResumeRouter {
	return &ResumeRouter{
		l:     l,
		slots: make(map[StreamID]chan dupConn),
		done:  make(chan struct{}),
	}
}

// Register reserves a handoff channel for id. The returned channel carries
// the connections (initial + resume) routed to that slice; the receiver pipe
// (NewReliablePipeReceiverOnSlot) reads from it. Registering the same id
// twice is an error (a slice has one pipe).
func (r *ResumeRouter) Register(id StreamID) (<-chan dupConn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("resume router: closed")
	}
	if _, ok := r.slots[id]; ok {
		return nil, fmt.Errorf("resume router: duplicate registration %s", id)
	}
	ch := make(chan dupConn, 1)
	r.slots[id] = ch
	return ch, nil
}

// Run is the accept loop: accept a connection, read its HELLO, parse the
// StreamID, and hand the connection (HELLO replayed) to the matching slot.
// It runs until the listener is closed or Close is called.
func (r *ResumeRouter) Run() {
	for {
		c, err := r.l.Accept()
		if err != nil {
			return // listener closed (unit end)
		}
		r.route(c)
	}
}

// route reads the first ctrl frame (the HELLO) of c and hands c to the slot
// for its StreamID (or closes c if unregistered).
func (r *ResumeRouter) route(c dupConn) {
	// The HELLO is the first frame the sender writes. Read it (bounded: a
	// conn that yields no HELLO is a stray — close it).
	type deadlineConn interface {
		SetReadDeadline(time.Time) error
	}
	if d, ok := c.(deadlineConn); ok {
		_ = d.SetReadDeadline(time.Now().Add(15 * time.Second))
	}
	kind, body, err := readCtrlOrFrame(c)
	if d, ok := c.(deadlineConn); ok {
		_ = d.SetReadDeadline(time.Time{})
	}
	if err != nil || kind != ctrlHello {
		_ = c.Close()
		return
	}
	id, ok := decodeHelloID(body)
	if !ok {
		_ = c.Close()
		return
	}
	r.mu.Lock()
	ch, reg := r.slots[id]
	r.mu.Unlock()
	if !reg {
		_ = c.Close() // stale/foreign: no pipe for this slice
		return
	}
	// Replay the HELLO so the pipe's receiveLoop sees it (version check +
	// progress), then the live data frames. Rebuild the exact ctrl frame the
	// sender wrote (magic|kind|len|body|crc) so the replayed bytes are
	// byte-identical.
	full := make([]byte, 4+5+len(body)+4)
	binary.LittleEndian.PutUint32(full[0:4], magicCtrl)
	full[4] = ctrlHello
	binary.LittleEndian.PutUint32(full[5:9], uint32(len(body)))
	copy(full[9:], body)
	binary.LittleEndian.PutUint32(full[9+len(body):], checksum(full[:9+len(body)]))
	select {
	case ch <- &prefixConn{prefix: full, inner: c}:
	case <-r.done:
		_ = c.Close()
	}
}

// Close stops the router (the accept loop exits on the listener close) and
// drains any pending handoffs.
func (r *ResumeRouter) Close() {
	r.once.Do(func() { close(r.done) })
	r.mu.Lock()
	r.closed = true
	r.slots = make(map[StreamID]chan dupConn)
	r.mu.Unlock()
	_ = r.l.Close()
}

// prefixConn is defined in reliablepipe_cross.go (reused for the HELLO replay).

// boundedSlotAccept waits up to d for a connection on ch, or nil on timeout /
// the router closing.
func boundedSlotAccept(ch <-chan dupConn, d time.Duration) dupConn {
	select {
	case c := <-ch:
		return c
	case <-time.After(d):
		return nil
	}
}
