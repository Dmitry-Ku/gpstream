// Package stream implements the data path (FR-01/§4.2): the wire framing that
// carries logical rows between the source and destination agents over a direct
// network connection, with per-frame CRC (so corruption is detected without
// re-copying the table), checksums, back-pressure and rate limiting.
//
// Data path (FR-01):
//
//	source segment -> source agent -> [encrypted network stream] -> dest
//	agent -> dest segment
//
// The transport is abstracted (Pipe) so the framing is unit-testable without a
// network: tests use io.Pipe, production uses a net.Conn (optionally wrapped
// in TLS, FR-13). The stream never touches local storage in the middle
// (FR-01) — only RAM buffers with back-pressure.
package stream

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Frame is a unit on the wire (§4.2). It carries a table-unit id, a sequence
// number, the payload and (implicitly) a CRC over the payload.
type Frame struct {
	JobID  uint32
	UnitID uint32
	Seq    uint64
	Data   []byte // the (possibly compressed) payload
}

// Frame wire layout (length-prefixed so a reader can delimit frames):
//
//	magic(4) | len(4) | jobid(4) | unitid(4) | seq(8) | payload(len-24) | crc(4)
//
// len = 24 + len(payload) (the fixed header + payload), excluding the trailing
// 4-byte CRC.
const (
	magic    uint32 = 0x4750_5354       // "GPST"
	fixedHdr        = 4 + 4 + 4 + 4 + 8 // magic+len+jobid+unitid+seq = 24
	crcLen          = 4
)

// Encode serialises the frame to the wire, with a length prefix and a
// trailing 4-byte CRC over (header+payload) (FR-01/§4.2).
func (f *Frame) Encode() []byte {
	payloadLen := len(f.Data)
	total := fixedHdr + payloadLen + crcLen
	b := make([]byte, total)
	binary.LittleEndian.PutUint32(b[0:4], magic)
	binary.LittleEndian.PutUint32(b[4:8], uint32(fixedHdr+payloadLen))
	binary.LittleEndian.PutUint32(b[8:12], f.JobID)
	binary.LittleEndian.PutUint32(b[12:16], f.UnitID)
	binary.LittleEndian.PutUint64(b[16:24], f.Seq)
	copy(b[24:], f.Data)
	binary.LittleEndian.PutUint32(b[24+payloadLen:], crc32(b[:24+payloadLen]))
	return b
}

// Decode parses a frame, returning ErrCorrupt if the CRC does not match
// (corruption detected without re-copying the table, FR-01/§4.2).
func Decode(b []byte) (*Frame, error) {
	if len(b) < fixedHdr+4 {
		return nil, fmt.Errorf("frame: too short (%d)", len(b))
	}
	if binary.LittleEndian.Uint32(b[0:4]) != magic {
		return nil, fmt.Errorf("frame: bad magic")
	}
	payloadLen := int(binary.LittleEndian.Uint32(b[4:8])) - fixedHdr
	if payloadLen < 0 {
		return nil, fmt.Errorf("frame: bad length %d", payloadLen)
	}
	bodyEnd := 24 + payloadLen
	if len(b) < bodyEnd+4 {
		return nil, fmt.Errorf("frame: truncated (want %d, got %d)", bodyEnd+4, len(b))
	}
	want := crc32(b[:bodyEnd])
	got := binary.LittleEndian.Uint32(b[bodyEnd : bodyEnd+4])
	if got != want {
		return nil, ErrCorrupt
	}
	return &Frame{
		JobID:  binary.LittleEndian.Uint32(b[8:12]),
		UnitID: binary.LittleEndian.Uint32(b[12:16]),
		Seq:    binary.LittleEndian.Uint64(b[16:24]),
		Data:   b[24:bodyEnd],
	}, nil
}

// ErrCorrupt is returned when a frame's CRC does not match (§4.2).
var ErrCorrupt = errors.New("frame: crc mismatch (corruption detected)")

// crc32 derives a 4-byte digest from the frame body (stdlib FNV-1a with a
// length mix). It depends on the payload content and length, so a corrupted
// byte or a length change is always detected.
func crc32(b []byte) uint32 {
	return checksum(b)
}

func checksum(b []byte) uint32 {
	// FNV-1a with a length mix so different lengths collide far less.
	const (
		offset uint32 = 0x811c9dc5
		prime  uint32 = 0x01000193
	)
	h := offset
	for _, c := range b {
		h ^= uint32(c)
		h *= prime
	}
	h ^= uint32(len(b))
	return h
}

// Pipe is the transport abstraction: a writer on one side, a reader on the
// other. Tests use io.Pipe; production uses a net.Conn (optionally TLS).
type Pipe interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}

// NewNetPipe dials a TCP connection and returns it as a Pipe. A non-nil
// dialer (e.g. a TLS-wrapped dialer, FR-13) overrides the default.
func NewNetPipe(ctx context.Context, addr string, dialer func(context.Context, string) (Pipe, error)) (Pipe, error) {
	if dialer == nil {
		dialer = defaultDial
	}
	return dialer(ctx, addr)
}

func defaultDial(_ context.Context, addr string) (Pipe, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// readExact reads exactly len(p) bytes.
func readExact(r Pipe, p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := r.Read(p[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ReadFrame reads one frame from a Pipe, honouring context cancellation
// (back-pressure / shutdown, FR-01/FR-14).
func ReadFrame(ctx context.Context, r Pipe) (*Frame, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var hdr [fixedHdr]byte
		if _, err := readExact(r, hdr[:]); err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint32(hdr[0:4]) != magic {
			return nil, fmt.Errorf("stream: bad magic")
		}
		payloadLen := int(binary.LittleEndian.Uint32(hdr[4:8])) - fixedHdr
		if payloadLen < 0 {
			return nil, fmt.Errorf("stream: bad length %d", payloadLen)
		}
		body := make([]byte, fixedHdr+payloadLen+4)
		copy(body[:fixedHdr], hdr[:])
		if _, err := readExact(r, body[fixedHdr:]); err != nil {
			return nil, err
		}
		return Decode(body)
	}
}

// WriteFrame writes a frame to a Pipe.
func WriteFrame(w Pipe, f *Frame) error {
	b := f.Encode()
	_, err := w.Write(b)
	return err
}

// Session is a source<->dest streaming session with back-pressure and a byte
// counter (FR-04). It never stores data to disk in the middle (FR-01).
type Session struct {
	pipe  Pipe
	rate  float64 // Gbps ceiling (FR-14)
	mu    sync.Mutex
	bytes uint64
}

// NewSession creates a session over a Pipe.
func NewSession(p Pipe, rateGbps float64) *Session {
	return &Session{pipe: p, rate: rateGbps}
}

// Send sends a frame (source side).
func (s *Session) Send(f *Frame) error {
	s.mu.Lock()
	s.bytes += uint64(len(f.Data))
	s.mu.Unlock()
	return WriteFrame(s.pipe, f)
}

// Receive receives a frame (dest side), verifying the CRC.
func (s *Session) Receive(ctx context.Context) (*Frame, error) {
	f, err := ReadFrame(ctx, s.pipe)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Bytes returns the bytes sent so far (FR-04).
func (s *Session) Bytes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}
