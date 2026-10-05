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
	"os"
	"sync"
	"sync/atomic"
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

	mu         sync.Mutex
	slots      map[StreamID]chan dupConn // bound: StreamID -> handoff channel
	free       []chan dupConn            // unbound pool slots (dynamic binding)
	wire       atomic.Uint64             // raw network bytes read from every routed conn (FR-04)
	closed     bool
	done       chan struct{}
	stop       atomic.Bool // set by Stop: the Run accept loop exits on the next wake
	acceptPoll time.Duration
	once       sync.Once
}

// NewResumeRouter builds a router over the shared data listener l. The caller
// runs Run in a goroutine for the unit's lifetime and Stop (or Close, when
// the serve owns the listener) on unit end.
func NewResumeRouter(l net.Listener) *ResumeRouter {
	return &ResumeRouter{
		l:          l,
		slots:      make(map[StreamID]chan dupConn),
		done:       make(chan struct{}),
		acceptPoll: time.Second,
	}
}

// NewPoolSlot creates one handoff channel and adds it to the unbound pool. The
// destination agent does NOT know the source segment content ids in advance
// (the kernel substitutes <SEGID> on each source segment; the agent is told
// only the count), so it pre-creates one pool slot per expected slice (S in
// merge mode) and feeds each to a NewReliablePipeReceiverOnSlot. On the first
// connection carrying a given StreamID the router binds that StreamID to a
// free pool slot; a resume re-dial for the same StreamID is routed to the
// already-bound slot. Which slice lands on which receiver-pipe is immaterial
// (the SliceMux routes by the per-frame content tag), so any free slot is a
// valid binding.
func (r *ResumeRouter) NewPoolSlot() <-chan DupConn {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	ch := make(chan dupConn, 1)
	r.free = append(r.free, ch)
	return ch
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

// Run is the accept loop: accept a connection and route it (read its HELLO,
// parse the StreamID, hand it to the matching slot) in a per-conn goroutine,
// so one slow/stale conn (a 15s HELLO read bound) cannot head-of-line-block
// the accepts of the other segreceivers dialing the shared listener. It runs
// until the listener is closed, or until Stop: the loop bounds each Accept
// with a 1s read deadline (the listener must support SetReadDeadline — a
// TCP listener and the HandshakeFilter/tls.Listener wrappers do), so a Stop
// is honored within a second WITHOUT closing the listener. This matters for
// the long-lived shared listener (one data port per agent, units served
// SEQUENTIALLY by serveOne): the unit's router must release the listener
// before the next unit's router takes it over, or it would steal (and close)
// the next unit's segreceiver connections.
func (r *ResumeRouter) Run() {
	type deadlineListener interface {
		SetReadDeadline(time.Time) error
	}
	setDeadline := func() {
		if d, ok := r.l.(deadlineListener); ok {
			_ = d.SetReadDeadline(time.Now().Add(r.acceptPoll))
		}
	}
	for {
		setDeadline()
		c, err := r.l.Accept()
		if err != nil {
			if r.stop.Load() {
				return
			}
			if os.IsTimeout(err) {
				continue // the poll deadline fired; re-check stop
			}
			return // listener closed (unit end)
		}
		if r.stop.Load() {
			_ = c.Close() // a conn that raced the Stop — drop it
			return
		}
		go r.route(c)
	}
}

// WireBytes returns the raw network bytes the router has read from its
// routed connections (initial + resume, all slices): the segment<->agent
// links' on-wire volume (FR-04 / ADR-002 acceptance #1). The frame-ack
// data path carries the bytes inside the ReliablePipe transport, so this is
// the authoritative "bytes read from the network" counter for the serve.
func (r *ResumeRouter) WireBytes() uint64 { return r.wire.Load() }

// route reads the first ctrl frame (the HELLO) of c and hands c to the slot
// for its StreamID (or closes c if unregistered).
func (r *ResumeRouter) route(c dupConn) {
	if r.stop.Load() {
		_ = c.Close() // the router was stopped (unit end) while this conn was in flight
		return
	}
	// Count every raw byte read from this connection (the HELLO + the data
	// frames) — the on-wire volume the serve reports (FR-04).
	var cc dupConn = &countingDup{c: c, n: &r.wire}
	// The HELLO is the first frame the sender writes. Read it (bounded: a
	// conn that yields no HELLO is a stray — close it).
	type deadlineConn interface {
		SetReadDeadline(time.Time) error
	}
	if d, ok := cc.(deadlineConn); ok {
		_ = d.SetReadDeadline(time.Now().Add(15 * time.Second))
	}
	kind, body, err := readCtrlOrFrame(cc)
	if d, ok := cc.(deadlineConn); ok {
		_ = d.SetReadDeadline(time.Time{})
	}
	if err != nil || kind != ctrlHello {
		_ = cc.Close()
		return
	}
	id, ok := decodeHelloID(body)
	if !ok {
		_ = cc.Close()
		return
	}
	r.mu.Lock()
	ch, reg := r.slots[id]
	if !reg && len(r.free) > 0 {
		// First sight of this StreamID: bind it to a free pool slot. The
		// resume re-dials for the same StreamID will find it in slots now.
		ch = r.free[len(r.free)-1]
		r.free = r.free[:len(r.free)-1]
		r.slots[id] = ch
		reg = true
	}
	r.mu.Unlock()
	if !reg {
		_ = cc.Close() // no pool slot left: this slice is unexpected — drop it
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
	case ch <- &prefixConn{prefix: full, inner: cc}:
	case <-r.done:
		_ = cc.Close()
	}
}

// Stop stops the router (the accept loop exits; pending handoffs are
// dropped) WITHOUT closing the listener. The production serve uses this when
// the listener is long-lived (SharedListen: one data port per agent, owned by
// the dest server, not the serve).
func (r *ResumeRouter) Stop() {
	r.stop.Store(true) // the Run() accept loop exits within one poll
	r.once.Do(func() { close(r.done) })
	r.mu.Lock()
	r.closed = true
	r.slots = make(map[StreamID]chan dupConn)
	r.free = nil
	r.mu.Unlock()
}

// Close stops the router AND closes the listener it was built over (the
// serve owns the listener).
func (r *ResumeRouter) Close() {
	r.Stop()
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

// countingDup wraps a conn and counts every raw byte read from it into n
// (the on-wire volume, FR-04). It satisfies dupConn; it forwards
// SetReadDeadline (the router's HELLO bound) when the inner conn supports it.
type countingDup struct {
	c dupConn
	n *atomic.Uint64
}

func (cd *countingDup) Read(p []byte) (int, error) {
	n, err := cd.c.Read(p)
	if n > 0 {
		cd.n.Add(uint64(n))
	}
	return n, err
}
func (cd *countingDup) Write(p []byte) (int, error) { return cd.c.Write(p) }
func (cd *countingDup) Close() error                { return cd.c.Close() }
func (cd *countingDup) SetReadDeadline(t time.Time) error {
	if d, ok := cd.c.(interface{ SetReadDeadline(time.Time) error }); ok {
		return d.SetReadDeadline(t)
	}
	return nil
}
func (cd *countingDup) SetWriteDeadline(t time.Time) error {
	if d, ok := cd.c.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(t)
	}
	return nil
}
func (cd *countingDup) SetDeadline(t time.Time) error {
	if d, ok := cd.c.(interface{ SetDeadline(time.Time) error }); ok {
		return d.SetDeadline(t)
	}
	return nil
}
