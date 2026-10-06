package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// The persistent coordinator data channel (P1c).
//
// One connection for the whole run, carrying many units. Each unit is
// delimited by a UnitBegin frame (carrying the UnitSpec) and a Trailer frame
// (the end-of-unit marker, 72 bytes after P0). The connection is NOT closed
// between units — the next unit begins with a fresh UnitBegin frame. An idle
// Ping/Pong watchdog (~30s) keeps a NAT/firewall idle-timeout from reaping the
// channel and detects a dead peer early.
//
// This saves, per unit, the TCP 3-way + TLS handshake + the UnitSpec transfer
// (≈4-5 RTT) that the legacy per-unit connection paid.
//
// The channel is transport-generic over a transfer.Pipe (a net.Conn in
// production, optionally TLS; a net.Pipe end in tests). The UnitSpec payload
// is the agent's own JSON (spec.go's shape), carried opaquely in the frame.

// pingInterval is the idle-watchdog period (P1c). ~30s is well under typical
// NAT idle-timeouts (60-300s) and cheap (a 28-byte frame).
const pingInterval = 30 * time.Second

// marshalSpecPayload / unmarshalSpecPayload serialize a UnitSpec to the JSON
// payload the UnitBegin frame carries (the same JSON WriteUnitSpec uses, minus
// the 5-byte length header — the frame provides its own length).
func marshalSpecPayload(s UnitSpec) ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("agent: marshal unit spec: %w", err)
	}
	return b, nil
}

func unmarshalSpecPayload(b []byte) (UnitSpec, error) {
	var s UnitSpec
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("agent: unmarshal unit spec: %w", err)
	}
	return s, nil
}

// PersistentSender is the source side of the persistent coordinator channel.
// It holds the connection and sends one unit at a time (UnitBegin + framed
// compressed data + Trailer), keeping the connection open between units.
type PersistentSender struct {
	conn  transfer.Pipe
	jobID uint32

	mu       sync.Mutex // serializes unit sends + ping writes (one writer at a time)
	seq      uint64     // monotonically increasing frame seq (shared by units + pings)
	closed   bool
	pingCtx  context.Context
	pingDone chan struct{} // non-nil once StartPing has launched the watchdog
	pingEndCancel context.CancelFunc
}

// NewPersistentSender wraps an established connection as the source side of
// the persistent channel. The caller owns the connection's lifetime (and the
// re-dial on a break); the sender only writes frames to it.
func NewPersistentSender(conn transfer.Pipe, jobID uint32) *PersistentSender {
	pctx, pcancel := context.WithCancel(context.Background())
	return &PersistentSender{conn: conn, jobID: jobID, pingCtx: pctx, pingEndCancel: pcancel}
}

// StartPing launches the idle-watchdog goroutine (a Ping every pingInterval).
// Starting it more than once is a no-op.
func (s *PersistentSender) StartPing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.pingDone != nil {
		return
	}
	s.pingDone = make(chan struct{})
	go s.pingLoop()
}

func (s *PersistentSender) pingLoop() {
	defer close(s.pingDone)
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-s.pingCtx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return
			}
			_ = transfer.WriteFrame(s.conn, transfer.NewPingFrame(s.jobID, 0, s.seq))
			s.seq++
			s.mu.Unlock()
		}
	}
}

// SendUnit sends one unit over the persistent channel: the UnitBegin frame
// (the spec), the framed + compressed data (the logical rows, FR-12), and the
// Trailer frame (the end-of-unit marker, carrying the source's raw per-row
// digest accumulator + row count). The connection is NOT closed. It returns
// the wire (framed, compressed) and logical (uncompressed) byte volumes for
// FR-04.
func (s *PersistentSender) SendUnit(ctx context.Context, spec UnitSpec, data io.Reader, comp *compress.Compressor, acc [32]byte, rows int64) (wire, logical uint64, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, 0, errors.New("persistent sender: closed")
	}
	s.mu.Unlock()

	// 1. UnitBegin frame: the spec opens the unit on the persistent channel.
	specBytes, merr := marshalSpecPayload(spec)
	if merr != nil {
		return 0, 0, merr
	}
	s.mu.Lock()
	if werr := transfer.WriteFrame(s.conn, transfer.NewUnitBeginFrame(s.jobID, spec.UnitID, s.seq, specBytes)); werr != nil {
		s.mu.Unlock()
		return 0, 0, fmt.Errorf("persistent sender: unit-begin %d: %w", spec.UnitID, werr)
	}
	s.seq++
	s.mu.Unlock()

	// 2. Framed + compressed data (FR-12). A fresh FramedWriter per unit (its
	// seq restarts at 0 within the unit — the destination's per-unit reader is
	// also fresh). We Flush (not CloseFlush) at the end so the connection
	// stays open for the next unit.
	fw := transfer.NewFramedWriter(s.conn, s.jobID, spec.UnitID, comp)
	counted := &logicalCounterU64{}
	if _, cerr := io.Copy(fw, &countingReader{r: data, n: &counted.n}); cerr != nil {
		return 0, 0, fmt.Errorf("persistent sender: stream unit %d: %w", spec.UnitID, cerr)
	}
	if flerr := fw.Flush(); flerr != nil {
		return 0, 0, fmt.Errorf("persistent sender: flush unit %d: %w", spec.UnitID, flerr)
	}
	logical = uint64(counted.n)
	wire = fw.Wire()

	// 3. Trailer frame: the end-of-unit marker (the source's raw digest acc +
	// rows, P0's 72-byte form).
	tr := transfer.Trailer{Digest: acc, Rows: rows}
	s.mu.Lock()
	if twerr := transfer.WriteFrame(s.conn, transfer.NewTrailerFrame(s.jobID, spec.UnitID, s.seq, transfer.ContentCoordinator, tr)); twerr != nil {
		s.mu.Unlock()
		return 0, 0, fmt.Errorf("persistent sender: trailer unit %d: %w", spec.UnitID, twerr)
	}
	s.seq++
	s.mu.Unlock()
	return wire, logical, nil
}

// Close tears the channel down (stops the ping loop and closes the
// connection). Safe to call more than once.
func (s *PersistentSender) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.pingEndCancel != nil {
		s.pingEndCancel()
	}
	_ = s.conn.Close()
}

// PersistentReceiver is the destination side of the persistent coordinator
// channel. It blocks on the connection, answering Pings with Pongs, until a
// UnitBegin frame arrives; it then yields the spec + a data reader that
// streams the unit's decompressed rows until the Trailer frame (io.EOF), plus
// the Trailer (the source's raw digest acc + row count). The connection stays
// open after each unit for the next.
type PersistentReceiver struct {
	conn  transfer.Pipe
	jobID uint32
}

// NewPersistentReceiver wraps an accepted connection as the destination side
// of the persistent channel.
func NewPersistentReceiver(conn transfer.Pipe, jobID uint32) *PersistentReceiver {
	return &PersistentReceiver{conn: conn, jobID: jobID}
}

// ReceiveUnit blocks until the next unit begins (a UnitBegin frame, skipping
// any idle Pings) and returns its spec and a reader that streams the unit's
// decompressed rows (ending at the Trailer frame with io.EOF). The connection
// is not closed. The reader's Trailer() (the source's raw digest acc + row
// count) and Logical() are valid once the reader reaches io.EOF.
//
// The reader must be fully consumed (to io.EOF) before the next call to
// ReceiveUnit: the reader and the next ReceiveUnit share the connection's read
// stream, and the reader stops exactly at the Trailer frame.
func (p *PersistentReceiver) ReceiveUnit(ctx context.Context) (spec UnitSpec, data *unitDataReader, err error) {
	for {
		f, rerr := transfer.ReadFrame(ctx, p.conn)
		if rerr != nil {
			return spec, nil, rerr
		}
		switch {
		case f.IsPing():
			_ = transfer.WriteFrame(p.conn, transfer.NewPongFrame(p.jobID, 0, f.Seq))
		case f.IsUnitBegin():
			spec, err = unmarshalSpecPayload(f.Data)
			if err != nil {
				return spec, nil, fmt.Errorf("persistent receiver: unit-begin: %w", err)
			}
			mode, _, perr := compress.Parse(spec.Compress)
			if perr != nil {
				return spec, nil, fmt.Errorf("persistent receiver: compress %q: %w", spec.Compress, perr)
			}
			ud := &unitDataReader{conn: p.conn, dec: compress.NewDec(mode)}
			return spec, ud, nil
		default:
			return spec, nil,
				fmt.Errorf("persistent receiver: expected UnitBegin, got frame flags=%d (unit %d)", f.Flags, f.UnitID)
		}
	}
}

// unitDataReader streams one unit's decompressed rows off the persistent
// channel, ending at the Trailer frame (which it stashes and surfaces as
// io.EOF to the caller's COPY). It owns a fresh per-unit Decompressor (a
// zstd/zlib stream is stateful across a unit's frames).
type unitDataReader struct {
	conn    transfer.Pipe
	dec     *compress.Decompressor
	buf     []byte
	trailer transfer.Trailer
	logical uint64
}

// Trailer returns the unit's Trailer (valid after the reader reaches io.EOF).
func (u *unitDataReader) Trailer() transfer.Trailer { return u.trailer }

// Logical returns the decompressed byte volume served (FR-04).
func (u *unitDataReader) Logical() uint64 { return u.logical }

func (u *unitDataReader) Read(p []byte) (int, error) {
	if len(u.buf) > 0 {
		n := copy(p, u.buf)
		u.buf = u.buf[n:]
		u.logical += uint64(n)
		return n, nil
	}
	for {
		f, err := transfer.ReadFrame(context.Background(), u.conn)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, err
		}
		if f.Flags&transfer.FlagTrailer != 0 {
			u.trailer, err = f.Trailer()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		if f.IsPing() {
			// A ping should not arrive mid-unit (the source pings only when
			// idle), but tolerate one: answer and keep reading the unit.
			_ = transfer.WriteFrame(u.conn, transfer.NewPongFrame(0, 0, f.Seq))
			continue
		}
		u.buf = u.dec.Decode(f.Data)
		if len(u.buf) == 0 {
			continue // an empty compressed frame (keep draining)
		}
		break
	}
	n := copy(p, u.buf)
	u.buf = u.buf[n:]
	u.logical += uint64(n)
	return n, nil
}

// countingReader counts the bytes it serves (the logical, uncompressed volume
// the source read, for FR-04).
type countingReader struct {
	r io.Reader
	n *uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 && c.n != nil {
		*c.n += uint64(n)
	}
	return n, err
}

// logicalCounterU64 is the uint64 the countingReader accumulates into.
type logicalCounterU64 struct{ n uint64 }
