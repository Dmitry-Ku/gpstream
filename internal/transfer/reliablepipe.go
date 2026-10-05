// reliablepipe.go — frame-level ACK and RST-resume (DESIGN §4.6, Tier-1,
// approved 2026-09-16, decision R2).
//
// ReliablePipe is the frame-aware, reliable transport over data-path "B"
// (agent<->agent). Today B is an in-memory full-duplex pipe (the coordinator
// path is one process: the source and destination goroutines are connected in
// RAM); a real TCP connection satisfies the same contract, so the module is
// transport-agnostic.
//
// It operates at FRAME granularity. The v2 wire frames already carry Seq, so
// the frame byte stream the FramedWriter emits (one Write per encoded frame)
// is exactly the retransmit unit:
//
//   - Sender: every frame is kept in a retransmission ring until a cumulative
//     ACK covers it. A transport break (RST, or a silent blackhole caught by
//     the ACK watchdog) triggers a reconnect: close the old transport, dial a
//     fresh pair, exchange HELLOs (sender: version + firstSeq = lastACK+1;
//     receiver: version + lastConsumed), then the sender replays the un-ACKed
//     ring in seq order. The PG scan position is preserved: the source was
//     blocked in Write (back-pressure), so no table re-read (§4.6.3).
//
//   - Receiver: frames are accepted in order; duplicates (seq <= lastConsumed,
//     i.e. the replayed ring) are dropped; a CRC-corrupt frame in the
//     in-flight window is NACKed and retransmitted — the unit does not die on
//     a bad frame (§4.6.5). Cumulative ACKs are throttled (~1 MiB or 100 ms).
//
// Correctness: cumulative ACK + in-order delivery => no gaps; dedup by seq =>
// no duplicates. The destination COPY session is not interrupted by a B break
// (the PG connections block; statement_timeout is never set by the code).
//
// Concurrency model (race-free):
//   - One reader per end: the receiveLoop is the only goroutine that reads the
//     transport, so framing reads never interleave.
//   - Writes are serialized by wmu (data frames from Write, control frames,
//     and the ring replay all share it) so byte streams never interleave.
//   - Reconnect is serialized by reconn and guarded by the connection
//     generation: a stale error from an already-replaced transport is a no-op.
//   - The HELLO handshake is coordinated through the receiveLoop (the sender
//     waits on a channel the receiveLoop closes when it reads the peer HELLO),
//     so the reconnect path never reads the transport directly.
//
// Control frames use a distinct magic ("GPSA") so they never collide with the
// data-frame magics ("GPST"/"GPST"+1). HELLO body: version(4)|seq(8). A peer
// advertising version < helloVer is rejected (the caller falls back to the
// plain FramedPipe, §4.6 version negotiation).

package transfer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// helloVer is the ReliablePipe HELLO version: 2 = frame-ACK capable.
const helloVer = 2

// magicCtrl is the control-frame magic ("GPSA"), distinct from the data-frame
// magics (GPST v1/v2).
const magicCtrl uint32 = 0x4750_5341

// CtrlMagic exposes the control-frame magic so a peer can tell a resume
// connection (first frame = a ctrl HELLO) from a fresh one (first bytes = a
// pre-stream header such as the UnitSpec) on a shared listener.
func CtrlMagic() uint32 { return magicCtrl }

// Control-frame kinds.
const (
	ctrlHello byte = 1 // body: version(4) | seq(8)
	ctrlAck   byte = 2 // body: lastConsumed(8)
	ctrlNack  byte = 3 // body: seq(8) — corrupt frame, retransmit
	ctrlFin   byte = 4 // empty body — clean end-of-stream (sender done)
)

// frameData is a pseudo-kind returned for a data (GPST) frame.
const frameData byte = 0

// ReliablePipeConfig carries the R2-approved parameters.
type ReliablePipeConfig struct {
	// AckTimeout is the silent-blackhole detector: if the sender has
	// un-ACKed frames and no ACK arrives within this window, the transport is
	// broken and reconnected. With AdaptiveAckTimeout it is the fallback until
	// enough RTT samples exist, then 3x median RTT (min 1s).
	AckTimeout time.Duration
	// AdaptiveAckTimeout selects adaptive = 3x median RTT.
	AdaptiveAckTimeout bool
	// MaxReconnects bounds reconnect attempts; exhausted => the pipe fails
	// (unit-retry remains the reserve).
	MaxReconnects int
	// RetransmitBufferMax caps both windows (the sender's un-ACKed ring and
	// the receiver's not-yet-read buffer); overflow => back-pressure to the
	// source (the PG scan blocks), §4.6.5.
	RetransmitBufferMax int64
	// AckByteInterval / AckTimeInterval throttle cumulative ACKs.
	AckByteInterval int
	AckTimeInterval time.Duration
	// ResumeTimeout bounds the receiver's wait for the sender's RST-resume
	// re-dial after a transport break: if no fresh connection arrives within
	// this window the sender is declared gone (the stream ends, the unit
	// fails and the controller resumes). It must comfortably exceed the
	// sender's re-dial latency (the sender reconnects immediately, no
	// backoff); 30s is very generous. It is the receiver-side twin of the
	// sender's MaxReconnects budget (RESILIENCE: budget exhausted -> unit
	// failed -> resume).
	ResumeTimeout time.Duration
}

// DefaultReliablePipeConfig is the R2-approved default set: --frame-ack on,
// --ack-timeout 30s, --max-reconnects 20, --retransmit-buffer-max 1 GiB.
//
// NOTE: this is the internal fallback (unit tests, direct construction). The
// CLI default is 64 MiB (config.Validate), and in production the controller
// always passes its value to the agents / segreceivers via argv, so this
// fallback is overridden on the production data path.
//
// The 1 GiB in-flight window is the §4.6 WAN-BDP figure (50 Gbps x 100 ms).
// A T7 soak A/B (28.09, non-race) confirmed frame-ack is throughput-neutral
// locally (42-43 MB/s for off, on+1G, and on+64M alike); the window is only
// material on a high-BDP network, where ~1 GiB is the right size — raise it
// with --retransmit-buffer-max for long-haul WAN.
func DefaultReliablePipeConfig() ReliablePipeConfig {
	return ReliablePipeConfig{
		AckTimeout:          30 * time.Second,
		MaxReconnects:       20,
		RetransmitBufferMax: 1 << 30,
		AckByteInterval:     1 << 20,
		AckTimeInterval:     100 * time.Millisecond,
		ResumeTimeout:       30 * time.Second,
	}
}

// NewReliablePipeConfig builds a ReliablePipeConfig from the raw operator
// parameters (the agent launch args: --ack-timeout / --max-reconnects /
// --retransmit-buffer-max), starting from the defaults. ackTimeout is
// "adaptive" (3x median RTT) or a duration (e.g. "30s"); empty keeps the
// default. This mirrors config.ReliablePipeConfig so the agent processes
// (which receive the params as argv, not a *config.Config) produce the same
// transport the controller computes.
func NewReliablePipeConfig(ackTimeout string, maxReconnects int, retransmitMax int64) ReliablePipeConfig {
	cfg := DefaultReliablePipeConfig()
	if maxReconnects > 0 {
		cfg.MaxReconnects = maxReconnects
	}
	if retransmitMax > 0 {
		cfg.RetransmitBufferMax = retransmitMax
	}
	switch {
	case ackTimeout == "adaptive":
		cfg.AckTimeout = 30 * time.Second
		cfg.AdaptiveAckTimeout = true
	case ackTimeout != "":
		if d, err := time.ParseDuration(ackTimeout); err == nil && d > 0 {
			cfg.AckTimeout = d
		}
	}
	return cfg
}

type rpRole int

const (
	rpSender rpRole = iota
	rpReceiver
)

// Stats is the observable state of a ReliablePipe end (FR-04 metrics:
// frames_retransmitted, reconnects_total, ack_lag).
type Stats struct {
	Retransmitted uint64
	Reconnects    int
	AckLag        time.Duration
	RingBytes     int64
	LastConsumed  uint64 // receiver
	LastSent      uint64 // sender
	LastAck       uint64 // sender
	OutBufBytes   int64  // receiver
	OutFrames     int    // receiver
}

// ReliablePipe is one end of a frame-ACK'd transport. It exposes the same
// Read/Write/Close surface the FramedPipe needs: the FramedWriter hands each
// encoded frame to Write as one call, the FramedReader consumes the in-order
// frame byte stream from Read.
type ReliablePipe struct {
	role rpRole
	cfg  ReliablePipeConfig
	link *rpLink

	connMu  sync.Mutex
	conn    dupConn
	connGen int        // bumped on every swapConn
	wmu     sync.Mutex // serializes all transport writes

	dialer rpDialer // sender-only (reconnect initiator)
	reconn sync.Mutex
	// pendHello is closed by the receiveLoop when it reads the peer HELLO
	// during a reconnect handshake (sender only).
	helloMu   sync.Mutex
	pendHello chan struct{}

	// Sender: retransmit ring.
	ringMu    sync.Mutex
	ring      map[uint64][]byte
	ringBytes int64
	lastSent  uint64
	lastAck   uint64
	lastAckAt time.Time
	spaceCv   *sync.Cond // ring space available

	// Receiver (network mode): resumer performs one bounded re-accept of a
	// fresh connection after a transport break (the sender's RST-resume
	// re-dial). It is nil in in-memory mode, where the shared link's dialer
	// creates the fresh pair and the receiver simply waits for the swap.
	resumer func() dupConn

	// identity, when set, rides every HELLO (the on_segment multi-tenant
	// resume routing). The sender greets with it on the initial connection
	// and every re-dial; the destination's ResumeRouter reads it to route a
	// connection to the matching receiver-pipe. The coordinator leg (one
	// sender per listener) leaves it unset and uses the plain 12-byte HELLO.
	identity    StreamID
	hasIdentity bool

	// Receiver: in-order output buffer.
	outMu    sync.Mutex
	outBuf   [][]byte
	outBytes int64
	outCv    *sync.Cond // outBuf has data
	lastCons uint64
	// consumedAny distinguishes "nothing consumed yet" (lastCons==0) from
	// "consumed frame 0": the sender's first frame has seq 0, which would
	// otherwise be mistaken for a duplicate of the initial lastCons.
	consumedAny uint64
	ooo         map[uint64][]byte

	// Receiver: ACK throttle state.
	ackMu    sync.Mutex
	ackDirty bool

	// Sender: RTT samples for the adaptive timeout.
	rttMu  sync.Mutex
	rtt    []time.Duration
	lastWr time.Time

	// Sender: resume-rejection fast death. A fresh transport that dies
	// WITHOUT any progress (no ACK, no peer HELLO since the swap) means the
	// destination rejected the resume (the unit ended: a failed serve closed
	// the conn / drained the backlog). A live receiver always greets a re-dial
	// with a HELLO (and ACKs within the ACK cadence), so "no progress" is an
	// unambiguous rejection. Three in a row => give up immediately instead of
	// burning the MaxReconnects budget (and piling zombie re-dials into the
	// destination's listener backlog, which would poison the next unit's
	// accept).
	progMu       sync.Mutex
	connProgress bool // any ACK/HELLO seen on the current transport
	rejects      int  // consecutive no-progress transport deaths

	// Shared.
	reconnects    int
	retransmitted uint64
	dead          atomic.Bool
	// finished (receiver) is set when a clean FIN arrives: every frame has
	// been delivered in order, so Read may return io.EOF once the output
	// buffer drains. Distinct from dead (abort/error), which must NOT yield a
	// clean EOF (that would drop in-flight frames).
	finished atomic.Bool
	done     chan struct{}
	doneOnce sync.Once
	wg       sync.WaitGroup
}

type rpDialer func() (dupConn, error)

// rpLink is the shared state between the two in-memory ends. It serializes
// reconnect (new-pair creation) and lets tests break the transport (fault
// injection) or install an in-flight corruptor.
type rpLink struct {
	mu        sync.Mutex
	a, b      *bidirPipe
	s, r      *ReliablePipe
	swapCv    *sync.Cond
	swapGen   int
	corruptor func(b []byte) []byte
	// gone is set (under mu) when either end dies; it wakes the peer's
	// waitSwap so a passive receiver does not wait forever for a swap that
	// will never come (the sender is dead).
	gone bool
	// aborted is set by the receiver on a destination failure (Abort): the
	// sender's blocked Write must then fail fast instead of reconnecting
	// (the peer is gone; replaying to a dead destination would hang).
	aborted atomic.Bool
	// done is closed by the network accept loop (NewReliablePipeTCPPair) when
	// the server side stops re-accepting (an app-level Close). The receiver's
	// waitSwap then yields "sender gone" so Read can terminate; it is distinct
	// from a transport break, where the accept loop keeps running so the
	// sender's RST-resume re-dial lands (the pipe's whole point).
	done     chan struct{}
	doneOnce sync.Once
}

// NewReliablePipePair builds a connected in-memory pair: the sender end S and
// the receiver end R over a fresh bidirectional pipe. It is the in-process
// stand-in for the agent<->agent connection (the production TCP variant plugs
// the same ReliablePipe onto a network dialer).
func NewReliablePipePair(cfg ReliablePipeConfig) (s, r *ReliablePipe) {
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
	a, b := newBidirPipe()
	link := &rpLink{}
	link.swapCv = sync.NewCond(&link.mu)
	link.a, link.b = a, b
	s = newReliablePipe(rpSender, cfg, link, a)
	r = newReliablePipe(rpReceiver, cfg, link, b)
	link.s, link.r = s, r
	s.dialer = func() (dupConn, error) {
		link.mu.Lock()
		defer link.mu.Unlock()
		na, nb := newBidirPipe()
		link.a, link.b = na, nb
		link.swapGen++
		link.swapCv.Broadcast()
		r.swapConn(nb)
		return na, nil
	}
	s.wg.Add(2)
	r.wg.Add(2)
	go s.receiveLoop()
	go s.watchdogLoop()
	go r.receiveLoop()
	go r.ackLoop()
	return s, r
}

func newReliablePipe(role rpRole, cfg ReliablePipeConfig, link *rpLink, conn dupConn) *ReliablePipe {
	link.doneOnce.Do(func() { link.done = make(chan struct{}) })
	rp := &ReliablePipe{
		role:      role,
		cfg:       cfg,
		link:      link,
		conn:      conn,
		ring:      make(map[uint64][]byte),
		ooo:       make(map[uint64][]byte),
		done:      make(chan struct{}),
		lastAckAt: time.Now(),
	}
	rp.outCv = sync.NewCond(&rp.outMu)
	rp.spaceCv = sync.NewCond(&rp.ringMu)
	return rp
}

func (rp *ReliablePipe) swapConn(c dupConn) {
	rp.connMu.Lock()
	rp.conn = c
	rp.connGen++
	rp.connMu.Unlock()
}

func (rp *ReliablePipe) currentConn() (dupConn, int) {
	rp.connMu.Lock()
	defer rp.connMu.Unlock()
	return rp.conn, rp.connGen
}

func (rp *ReliablePipe) connGeneration() int {
	rp.connMu.Lock()
	defer rp.connMu.Unlock()
	return rp.connGen
}

func (rp *ReliablePipe) markDead() {
	rp.doneOnce.Do(func() {
		rp.dead.Store(true)
		close(rp.done)
		// Wake any Read blocked on an empty output buffer: dead means it may
		// now return io.EOF (clean finish or abort).
		rp.outMu.Lock()
		rp.outCv.Broadcast()
		rp.outMu.Unlock()
		// Wake a SENDER blocked in ringAdd back-pressure (the ring is full
		// because the peer stopped ACKing and is now dead): without this the
		// sender's data-COPY goroutine waits on spaceCv forever and the unit
		// never fails. The re-check of dead/aborted in the ringAdd loop makes
		// it return the "closed while back-pressured" error.
		rp.ringMu.Lock()
		rp.spaceCv.Broadcast()
		rp.ringMu.Unlock()
		// Wake the peer: a dead end means no swap will ever come, so a passive
		// receiver blocked in waitSwap must not wait forever.
		l := rp.link
		l.mu.Lock()
		l.gone = true
		l.swapCv.Broadcast()
		l.mu.Unlock()
	})
}

// Abort terminates the pipe because the DESTINATION failed (the receiver side
// calls it). It marks the link aborted so the sender's blocked Write fails
// fast (instead of reconnecting to a dead destination) and severs the
// transport. Idempotent.
func (rp *ReliablePipe) Abort() error {
	l := rp.link
	l.aborted.Store(true)
	l.mu.Lock()
	l.gone = true
	l.swapCv.Broadcast()
	l.mu.Unlock()
	a, b := l.a, l.b
	if a != nil {
		_ = a.Close()
	}
	if b != nil {
		_ = b.Close()
	}
	rp.CloseTransport() // TCP mode: sever the current transport (a/b are nil)
	return nil
}

// Break severs the current transport (both ends), simulating an RST/blackhole
// (§8 fault-injection). Blocked reads/writes on the old pipe error, which
// drives the sender's reconnect.
func (rp *ReliablePipe) Break() {
	l := rp.link
	l.mu.Lock()
	a, b := l.a, l.b
	l.mu.Unlock()
	if a != nil {
		_ = a.Close()
	}
	if b != nil {
		_ = b.Close()
	}
	rp.CloseTransport() // TCP mode: sever the current transport (a/b are nil)
}

// SetCorruptor installs an in-flight corruptor (tests): every data frame the
// sender writes through the current transport is passed through fn. A nil fn
// clears it.
func (rp *ReliablePipe) SetCorruptor(fn func(b []byte) []byte) {
	l := rp.link
	l.mu.Lock()
	l.corruptor = fn
	l.mu.Unlock()
}

// Close finishes the pipe (idempotent). The SENDER end first sends a clean
// FIN (in order after every data frame on the wire) so the receiver gets a
// clean end-of-stream (io.EOF) rather than a transport error; if the FIN
// cannot be sent the transport close below still yields EOF on the receiver.
// The RECEIVER end just tears down.
func (rp *ReliablePipe) Close() error {
	if rp.role == rpSender {
		_ = rp.sendCtrl(ctrlFin, nil) // best effort; order on the wire is kept
	}
	l := rp.link
	l.doneOnce.Do(func() { close(l.done) }) // the network accept loop stops re-accepting
	rp.markDead()
	c, _ := rp.currentConn()
	_ = c.Close()
	return nil
}

// linkGone marks the link permanently gone (the accept loop stopped, or the
// in-memory peer's app closed): the receiver's waitSwap then yields "sender
// gone". Idempotent.
func (rp *ReliablePipe) linkGone() {
	l := rp.link
	l.mu.Lock()
	l.gone = true
	l.swapCv.Broadcast()
	l.mu.Unlock()
}

// CloseTransport severs the CURRENT transport without finishing the pipe:
// the sender keeps its ring and will RST-resume (re-dial + replay) on the next
// Write; the receiver keeps its dedup state and re-accepts. This is the
// test/operational analogue of an RST. (Close, by contrast, is the app-level
// clean end-of-stream.)
func (rp *ReliablePipe) CloseTransport() {
	c, _ := rp.currentConn()
	_ = c.Close()
}

// BreakConn severs the receiver's current transport (the sender re-dials; the
// re-accept loop swaps the fresh conn in). In-memory tests use Break instead;
// this is the network variant.
func (rp *ReliablePipe) BreakConn() {
	c, _ := rp.currentConn()
	_ = c.Close()
}

// ---- sender: Write ----

// Write (sender) accepts one encoded frame, records it in the retransmit ring
// (back-pressuring while the window is full) and sends it. On a transport
// error it reconnects; the ring replay re-sends the frame, so the caller
// never retries.
func (rp *ReliablePipe) Write(p []byte) (int, error) {
	if rp.role != rpSender {
		return 0, errors.New("reliablepipe: Write on the receiver end")
	}
	if len(p) == 0 {
		return 0, nil
	}
	seq := frameSeq(p)
	if err := rp.ringAdd(p); err != nil {
		return 0, err
	}
	rp.ringMu.Lock()
	rp.lastSent = seq
	rp.ringMu.Unlock()
	for {
		if rp.link.aborted.Load() {
			return 0, errors.New("reliablepipe: aborted by destination")
		}
		c, gen := rp.currentConn()
		if n, err := rp.sendData(c, p); err == nil {
			rp.rttMu.Lock()
			rp.lastWr = time.Now()
			rp.rttMu.Unlock()
			return n, nil
		} else if rp.dead.Load() || rp.link.aborted.Load() {
			return 0, fmt.Errorf("reliablepipe: write: %w", err)
		} else if rp.connGeneration() == gen {
			// The failing conn is still current: reconnect (serialized; the
			// ring replay re-sends this frame if un-ACKed).
			if err := rp.reconnect(gen); err != nil {
				return 0, err
			}
		}
		// Loop to re-validate the (possibly fresh) conn.
	}
}

func (rp *ReliablePipe) sendData(c dupConn, p []byte) (int, error) {
	l := rp.link
	l.mu.Lock()
	corr := l.corruptor
	l.mu.Unlock()
	if corr != nil && len(p) >= 4 && binary.LittleEndian.Uint32(p[0:4]) != magicCtrl {
		p = corr(p)
	}
	rp.wmu.Lock()
	defer rp.wmu.Unlock()
	_, err := writeFull(c, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// ringAdd records the frame in the retransmit ring, back-pressuring while the
// window is over the cap (§4.6.5: the PG scan blocks).
func (rp *ReliablePipe) ringAdd(p []byte) error {
	cp := make([]byte, len(p))
	copy(cp, p)
	seq := frameSeq(cp)
	rp.ringMu.Lock()
	defer rp.ringMu.Unlock()
	for rp.ringBytes+int64(len(cp)) > rp.cfg.RetransmitBufferMax {
		if rp.dead.Load() || rp.link.aborted.Load() {
			return errors.New("reliablepipe: closed while back-pressured")
		}
		rp.spaceCv.Wait()
	}
	if _, dup := rp.ring[seq]; !dup {
		rp.ring[seq] = cp
		rp.ringBytes += int64(len(cp))
	}
	return nil
}

// ringDropUpTo frees ring entries covered by the cumulative ACK.
func (rp *ReliablePipe) ringDropUpTo(ack uint64) {
	rp.ringMu.Lock()
	for seq, b := range rp.ring {
		if seq <= ack {
			rp.ringBytes -= int64(len(b))
			delete(rp.ring, seq)
		}
	}
	space := rp.ringBytes < rp.cfg.RetransmitBufferMax
	if space {
		rp.spaceCv.Broadcast()
	}
	rp.ringMu.Unlock()
}

// applyAck advances lastAck (and the RTT sample) and frees the ring.
func (rp *ReliablePipe) applyAck(ack uint64) {
	rp.ringMu.Lock()
	if ack > rp.lastAck {
		now := time.Now()
		// Sample the RTT under rttMu: lastWr is written by Write under the
		// same lock.
		rp.rttMu.Lock()
		if !rp.lastWr.IsZero() && now.After(rp.lastWr) {
			rp.rtt = append(rp.rtt, now.Sub(rp.lastWr))
			if len(rp.rtt) > 32 {
				rp.rtt = rp.rtt[len(rp.rtt)-32:]
			}
		}
		rp.rttMu.Unlock()
		rp.lastAck = ack
		rp.lastAckAt = now
	}
	rp.ringMu.Unlock()
	rp.ringDropUpTo(ack)
}

// ---- receiver: Read ----

// Read (receiver) serves the in-order, de-duplicated, CRC-verified frame byte
// stream to the FramedReader above it. It returns io.EOF when the sender sent
// a clean FIN (finished) or the pipe died (abort/error) — in both cases after
// the output buffer has drained, so no delivered frame is lost.
func (rp *ReliablePipe) Read(p []byte) (int, error) {
	if rp.role != rpReceiver {
		return 0, errors.New("reliablepipe: Read on the sender end")
	}
	for {
		rp.outMu.Lock()
		for len(rp.outBuf) == 0 && !rp.dead.Load() {
			rp.outCv.Wait()
		}
		if len(rp.outBuf) == 0 {
			rp.outMu.Unlock()
			return 0, io.EOF
		}
		n := copy(p, rp.outBuf[0])
		if n == len(rp.outBuf[0]) {
			rp.outBuf = rp.outBuf[1:]
		} else {
			rp.outBuf[0] = rp.outBuf[0][n:]
		}
		rp.outBytes -= int64(n)
		rp.outCv.Broadcast() // wake a blocked enqueue (buffer freed up)
		rp.outMu.Unlock()
		return n, nil
	}
}

// ---- control/data receive loop (the sole reader, both ends) ----

func (rp *ReliablePipe) receiveLoop() {
	defer rp.wg.Done()
	for {
		c, gen := rp.currentConn()
		kind, body, err := readCtrlOrFrame(c)
		if err != nil {
			if rp.dead.Load() {
				return
			}
			if rp.role == rpSender {
				// The transport broke (a real break is an error; a clean EOF is
				// the peer closing, which is also a break to resume from — the
				// re-dial + rejection fast-death bound a dead/done peer).
				// Serialized, stale-gen guarded so a stale error is a no-op.
				if rp.connGeneration() == gen {
					if rerr := rp.reconnect(gen); rerr != nil {
						return
					}
				}
				continue
			}
			// The receiver is the reconnect passive side. A clean EOF is NOT
			// "the stream ended": a FIN (from a transport break) is
			// indistinguishable from a clean close, and a resuming sender WILL
			// re-dial. The re-accept below handles both: a finished/gone sender
			// never re-dials (the bounded wait times out -> stream ends), a
			// resuming one does (the wait swaps it in).
			// The receiver is the reconnect passive side. Two modes:
			//  - network (resumer set): accept ONE fresh connection on the
			//    listener (bounded by ResumeTimeout) — the sender's re-dial.
			//    Accepting on break (not continuously) keeps a shared listener
			//    free for the next unit between breaks.
			//  - in-memory: wait for the sender's dialer to swap a fresh pair
			//    in through the shared link.
			if rp.resumer != nil {
				nc := rp.resumer()
				if nc == nil {
					// ResumeTimeout elapsed: the sender is gone. End the
					// receiver so Read terminates once the buffer drains; the
					// destination's row-count verification catches any
					// shortfall (unit failed -> resume).
					rp.linkGone()
					rp.markDead()
					return
				}
				rp.swapConn(nc)
				l := rp.link
				l.mu.Lock()
				l.swapGen++
				l.swapCv.Broadcast()
				l.mu.Unlock()
			} else {
				l := rp.link
				l.mu.Lock()
				for l.swapGen == gen && !l.gone {
					l.swapCv.Wait()
				}
				swapped := l.swapGen != gen
				l.mu.Unlock()
				if !swapped {
					// The sender is gone: no more frames can arrive. End the
					// receiver so Read terminates once the buffer drains; the
					// destination's row-count verification catches any
					// shortfall.
					rp.markDead()
					return
				}
			}
			_ = rp.sendHello(atomic.LoadUint64(&rp.lastCons))
			continue
		}
		switch kind {
		case ctrlHello:
			if len(body) < 4 {
				continue
			}
			if v := binary.LittleEndian.Uint32(body[0:4]); v < helloVer {
				rp.markDead()
				return
			}
			// The HELLO completes the sender's reconnect handshake.
			rp.helloMu.Lock()
			if rp.pendHello != nil {
				close(rp.pendHello)
				rp.pendHello = nil
			}
			rp.helloMu.Unlock()
			if rp.role == rpSender {
				rp.noteProgress() // the receiver greeted the (re-)dial: it is alive
			}
		case ctrlAck:
			if rp.role == rpSender && len(body) == 8 {
				rp.applyAck(binary.LittleEndian.Uint64(body))
				rp.noteProgress()
			}
		case ctrlNack:
			if rp.role == rpSender && len(body) == 8 {
				rp.retransmitOne(binary.LittleEndian.Uint64(body))
				rp.noteProgress()
			}
		case ctrlFin:
			// Clean end-of-stream: the sender has sent every frame. The FIN
			// arrives in order after all data frames on the same byte stream,
			// so every frame is already in outBuf — mark the stream finished.
			// Read returns io.EOF once the buffer drains. This is NOT dead:
			// the pipe ended normally, no reconnect is needed.
			if rp.role == rpReceiver {
				rp.finished.Store(true)
				rp.outMu.Lock()
				rp.outCv.Broadcast()
				rp.outMu.Unlock()
				rp.markDead()
				return
			}
		case frameData:
			if rp.role == rpReceiver {
				rp.handleData(body)
			}
		}
	}
}

// waitSwap blocks until the link's transport generation advances (the sender
// dialed a fresh pair), the peer died (gone), or the pipe is closed.
func (rp *ReliablePipe) waitSwap(gen int) bool {
	l := rp.link
	l.mu.Lock()
	for l.swapGen == gen && !l.gone {
		l.swapCv.Wait()
	}
	l.mu.Unlock()
	return !rp.dead.Load()
}

// retransmitOne re-sends a single NACKed frame from the ring.
func (rp *ReliablePipe) retransmitOne(seq uint64) {
	rp.ringMu.Lock()
	frame, ok := rp.ring[seq]
	if ok {
		frame = append([]byte(nil), frame...)
	}
	rp.ringMu.Unlock()
	if !ok {
		return // already ACKed and dropped; the NACK is stale
	}
	c, _ := rp.currentConn()
	rp.wmu.Lock()
	_, err := c.Write(frame)
	rp.wmu.Unlock()
	if err == nil {
		atomic.AddUint64(&rp.retransmitted, 1)
	}
}

// ---- receiver: handleData ----

func (rp *ReliablePipe) handleData(enc []byte) {
	seq := frameSeq(enc)
	last := atomic.LoadUint64(&rp.lastCons)
	any := atomic.LoadUint64(&rp.consumedAny)
	if any == 1 && seq <= last {
		return // duplicate (the replayed ring after a reconnect)
	}
	// Verify the CRC at this layer so a corrupt in-flight frame is NACKed and
	// retransmitted instead of killing the unit (§4.6.5).
	if f, err := Decode(enc); err != nil || f == nil {
		rp.sendNack(seq)
		return
	}
	// Expected next seq: 0 for the very first frame, else last+1.
	expected := last
	if any == 1 {
		expected = last + 1
	}
	if seq != expected {
		// Out of order (only possible across a replay boundary). Buffer until
		// the gap fills.
		rp.outMu.Lock()
		rp.ooo[seq] = enc
		rp.outMu.Unlock()
		rp.flushOOO()
		return
	}
	atomic.StoreUint64(&rp.lastCons, seq)
	atomic.StoreUint64(&rp.consumedAny, 1)
	rp.enqueue(enc)
	rp.signalAck()
}

// enqueue appends a frame to the output buffer, back-pressuring while the
// buffer is over the cap (a slow destination drains before the sender's
// window fills).
func (rp *ReliablePipe) enqueue(enc []byte) {
	rp.outMu.Lock()
	for rp.outBytes+int64(len(enc)) > rp.cfg.RetransmitBufferMax && !rp.dead.Load() {
		rp.outCv.Wait()
	}
	if rp.dead.Load() {
		rp.outMu.Unlock()
		return
	}
	rp.outBuf = append(rp.outBuf, enc)
	rp.outBytes += int64(len(enc))
	rp.outCv.Broadcast() // wake a blocked Read
	rp.outMu.Unlock()
}

// flushOOO releases buffered out-of-order frames once the gap fills.
func (rp *ReliablePipe) flushOOO() {
	for {
		next := atomic.LoadUint64(&rp.lastCons) + 1
		rp.outMu.Lock()
		enc, ok := rp.ooo[next]
		if ok {
			delete(rp.ooo, next)
			rp.outMu.Unlock()
			atomic.StoreUint64(&rp.lastCons, next)
			rp.enqueue(enc)
			continue
		}
		rp.outMu.Unlock()
		return
	}
}

// ---- receiver: throttled cumulative ACKs ----

func (rp *ReliablePipe) ackLoop() {
	defer rp.wg.Done()
	t := time.NewTicker(rp.cfg.AckTimeInterval)
	defer t.Stop()
	for {
		select {
		case <-rp.done:
			return
		case <-t.C:
		}
		rp.ackMu.Lock()
		if !rp.ackDirty {
			rp.ackMu.Unlock()
			continue
		}
		rp.ackMu.Unlock()
		ack := atomic.LoadUint64(&rp.lastCons)
		if err := rp.sendCtrl(ctrlAck, encodeU64(ack)); err != nil {
			// Transport broke; the receiveLoop drives the reconnect.
			continue
		}
		rp.ackMu.Lock()
		rp.ackDirty = false
		rp.ackMu.Unlock()
	}
}

func (rp *ReliablePipe) signalAck() {
	rp.ackMu.Lock()
	rp.ackDirty = true
	rp.ackMu.Unlock()
}

// ---- sender: watchdog (silent-blackhole detector) ----

func (rp *ReliablePipe) watchdogLoop() {
	defer rp.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-rp.done:
			return
		case <-t.C:
		}
		rp.ringMu.Lock()
		pending := rp.lastSent > rp.lastAck
		since := time.Since(rp.lastAckAt)
		rp.ringMu.Unlock()
		if pending && since > rp.effectiveAckTimeout() {
			// Silent blackhole: no ACK for the window while data is in
			// flight. Sever the transport and drive the reconnect ourselves:
			// a blocked Write (in the ring back-pressure) would otherwise
			// never see a transport error and never reconnect. An aborted
			// destination is NOT reconnected (the peer is gone).
			if rp.link.aborted.Load() {
				rp.Break()
				continue
			}
			rp.Break()
			_ = rp.reconnect(rp.connGeneration())
		}
	}
}

func (rp *ReliablePipe) effectiveAckTimeout() time.Duration {
	if !rp.cfg.AdaptiveAckTimeout {
		return rp.cfg.AckTimeout
	}
	rp.rttMu.Lock()
	samples := make([]time.Duration, len(rp.rtt))
	copy(samples, rp.rtt)
	rp.rttMu.Unlock()
	if len(samples) < 10 {
		return rp.cfg.AckTimeout
	}
	sortDur(samples)
	to := 3 * samples[len(samples)/2]
	if to < time.Second {
		to = time.Second
	}
	return to
}

// ---- reconnect (sender-initiated, serialized) ----

// reconnect severs the failing transport, dials a fresh pair, exchanges HELLOs
// and replays the un-ACKed ring. failGen is the generation of the conn that
// errored; if the conn already advanced under us (a concurrent reconnect), it
// is a no-op.
func (rp *ReliablePipe) reconnect(failGen int) error {
	rp.reconn.Lock()
	defer rp.reconn.Unlock()

	// Stale-gen guard: if the conn already advanced, a concurrent reconnect
	// handled it.
	if rp.connGeneration() != failGen {
		return nil
	}

	// The previous transport died without any progress (no ACK/HELLO since
	// the swap): the destination rejected the resume (the unit is over).
	// Cap consecutive rejections so a dead unit fails fast instead of
	// re-dialing the destination MaxReconnects times.
	if rp.noProgressDeaths() >= 3 {
		rp.markDead()
		return fmt.Errorf("reliablepipe: destination rejected 3 consecutive resumes (unit over?)")
	}

	rp.ringMu.Lock()
	if rp.reconnects >= rp.cfg.MaxReconnects {
		rp.ringMu.Unlock()
		rp.markDead()
		return fmt.Errorf("reliablepipe: max reconnects (%d) exhausted", rp.cfg.MaxReconnects)
	}
	rp.reconnects++
	rp.ringMu.Unlock()

	// Sever the old transport (unblocks both ends' blocked IO).
	old, _ := rp.currentConn()
	_ = old.Close()

	c, err := rp.dialer()
	if err != nil {
		rp.markDead()
		return fmt.Errorf("reliablepipe: dial: %w", err)
	}
	rp.swapConn(c)
	// Fresh transport: progress tracking restarts (the receiver greets a
	// resume with a HELLO; a rejected one dies before any progress).
	rp.progMu.Lock()
	rp.connProgress = false
	rp.progMu.Unlock()

	// The dialer has already swapped the receiver onto the fresh pipe, so the
	// replay below lands in the receiver's new read buffer — no synchronous
	// handshake is required for correctness (cumulative ACK + ring replay +
	// receiver dedup guarantee exactly-once). HELLO is informational only:
	// version + our resume point, sent fire-and-forget so a slow/absent peer
	// greeting can never hold `reconn` and deadlock the receiveLoop.
	rp.ringMu.Lock()
	mySeq := rp.lastAck + 1
	rp.ringMu.Unlock()
	_ = rp.sendHello(mySeq)

	// Replay the un-ACKed ring in seq order (§4.6.3).
	return rp.replayRing(c)
}

// noProgressDeaths reports the consecutive no-progress transport deaths and
// records THIS one (called from the sender's reconnect path, which only runs
// on a transport death). A transport that saw an ACK or a peer HELLO since
// its swap made progress: the death resets the rejection count.
func (rp *ReliablePipe) noProgressDeaths() int {
	rp.progMu.Lock()
	defer rp.progMu.Unlock()
	if rp.connProgress {
		rp.connProgress = false
		rp.rejects = 0
		return 0
	}
	rp.rejects++
	return rp.rejects
}

// noteProgress records that the current transport made progress (an ACK or a
// peer HELLO arrived): a subsequent death is a real break, not a rejection.
func (rp *ReliablePipe) noteProgress() {
	rp.progMu.Lock()
	rp.connProgress = true
	rp.progMu.Unlock()
}

func (rp *ReliablePipe) replayRing(c dupConn) error {
	rp.ringMu.Lock()
	var seqs []uint64
	for seq := range rp.ring {
		seqs = append(seqs, seq)
	}
	sortU64(seqs)
	var payload [][]byte
	for _, s := range seqs {
		if s <= rp.lastAck {
			continue
		}
		payload = append(payload, rp.ring[s])
	}
	rp.ringMu.Unlock()
	for _, p := range payload {
		rp.wmu.Lock()
		_, err := writeFull(c, p)
		rp.wmu.Unlock()
		if err != nil {
			return err
		}
		atomic.AddUint64(&rp.retransmitted, 1)
	}
	return nil
}

// ---- control frames ----

// readCtrlOrFrame reads the 4-byte magic and dispatches: for a control frame
// it returns (kind, body); for a data frame it returns (frameData, full
// encoded frame) so the receiver can CRC-verify and serve it in order.
func readCtrlOrFrame(c dupConn) (kind byte, body []byte, err error) {
	var magic [4]byte
	if _, err = io.ReadFull(c, magic[:]); err != nil {
		return 0, nil, err
	}
	m := binary.LittleEndian.Uint32(magic[:])
	switch m {
	case magicCtrl:
		var kl [5]byte
		if _, err = io.ReadFull(c, kl[:]); err != nil {
			return 0, nil, err
		}
		l := int(binary.LittleEndian.Uint32(kl[1:5]))
		tail := make([]byte, l+4)
		if _, err = io.ReadFull(c, tail); err != nil {
			return 0, nil, err
		}
		full := make([]byte, 4+5+l+4)
		copy(full, magic[:])
		copy(full[4:], kl[:])
		copy(full[9:], tail)
		if want, got := checksum(full[:9+l]), binary.LittleEndian.Uint32(full[9+l:]); got != want {
			return 0, nil, errors.New("reliablepipe: control crc mismatch")
		}
		return kl[0], full[9 : 9+l], nil
	case magicV1, magicV2:
		var lenb [4]byte
		if _, err = io.ReadFull(c, lenb[:]); err != nil {
			return 0, nil, err
		}
		total := int(binary.LittleEndian.Uint32(lenb[:])) // header+payload (excl. crc)
		if total < 8 {
			return 0, nil, fmt.Errorf("reliablepipe: bad frame len %d", total)
		}
		rest := make([]byte, total-8+4)
		if _, err = io.ReadFull(c, rest); err != nil {
			return 0, nil, err
		}
		enc := make([]byte, total+4)
		copy(enc, magic[:])
		copy(enc[4:8], lenb[:])
		copy(enc[8:], rest)
		return frameData, enc, nil
	default:
		return 0, nil, fmt.Errorf("reliablepipe: bad magic 0x%08x", m)
	}
}

func (rp *ReliablePipe) sendCtrl(kind byte, body []byte) error {
	c, _ := rp.currentConn()
	full := make([]byte, 4+5+len(body)+4)
	binary.LittleEndian.PutUint32(full[0:4], magicCtrl)
	full[4] = kind
	binary.LittleEndian.PutUint32(full[5:9], uint32(len(body)))
	copy(full[9:], body)
	binary.LittleEndian.PutUint32(full[9+len(body):], checksum(full[:9+len(body)]))
	rp.wmu.Lock()
	_, err := c.Write(full)
	rp.wmu.Unlock()
	return err
}

func (rp *ReliablePipe) sendNack(seq uint64) {
	_ = rp.sendCtrl(ctrlNack, encodeU64(seq))
}

func (rp *ReliablePipe) sendHello(seq uint64) error {
	if rp.hasIdentity {
		return rp.sendCtrl(ctrlHello, encodeHelloID(seq, rp.identity))
	}
	body := make([]byte, 12)
	binary.LittleEndian.PutUint32(body[0:4], helloVer)
	binary.LittleEndian.PutUint64(body[4:12], seq)
	return rp.sendCtrl(ctrlHello, body)
}

// ---- Stats ----

// Stats snapshots the observable counters (FR-04).
func (rp *ReliablePipe) Stats() Stats {
	st := Stats{
		Retransmitted: atomic.LoadUint64(&rp.retransmitted),
		Reconnects:    rp.reconnects,
	}
	if rp.role == rpSender {
		rp.ringMu.Lock()
		st.RingBytes = rp.ringBytes
		st.LastSent = rp.lastSent
		st.LastAck = rp.lastAck
		st.AckLag = time.Since(rp.lastAckAt)
		rp.ringMu.Unlock()
	} else {
		st.LastConsumed = atomic.LoadUint64(&rp.lastCons)
		rp.outMu.Lock()
		st.OutBufBytes = rp.outBytes
		st.OutFrames = len(rp.outBuf)
		rp.outMu.Unlock()
	}
	return st
}

// ---- helpers ----

// frameSeq extracts the seq from an encoded frame (v1 or v2 by magic).
func frameSeq(enc []byte) uint64 {
	if len(enc) < 8 {
		return 0
	}
	switch binary.LittleEndian.Uint32(enc[0:4]) {
	case magicV1:
		if len(enc) < 24 {
			return 0
		}
		return binary.LittleEndian.Uint64(enc[16:24])
	case magicV2:
		if len(enc) < 28 {
			return 0
		}
		return binary.LittleEndian.Uint64(enc[20:28])
	}
	return 0
}

// writeFull writes all of p to c (a TCP conn may return a partial write;
// the frame byte stream must not be split mid-write). It returns the first
// error, if any.
func writeFull(c dupConn, p []byte) (int, error) {
	for len(p) > 0 {
		n, err := c.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func encodeU64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func sortU64(x []uint64) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j] < x[j-1]; j-- {
			x[j], x[j-1] = x[j-1], x[j]
		}
	}
}

func sortDur(x []time.Duration) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j] < x[j-1]; j-- {
			x[j], x[j-1] = x[j-1], x[j]
		}
	}
}
