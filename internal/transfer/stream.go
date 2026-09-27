// Package stream implements the data path (FR-01/§4.2): the wire framing that
// carries logical rows between the source and destination agents over a direct
// network connection, with per-frame CRC (so corruption is detected without
// re-copying the table), checksums, back-pressure and rate limiting.
//
// Data path (FR-01):
//
//	source segment -> source agent -> [network stream] -> dest agent -> dest
//
// The transport is abstracted (Pipe) so the framing is unit-testable without a
// network: tests use io.Pipe, production uses a net.Conn (optionally wrapped in
// TLS, FR-13). The stream never touches local storage in the middle (FR-01) —
// only RAM buffers with back-pressure.
package transfer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// ContentCoordinator marks a frame's content id as "the coordinator path"
// (no per-segment slice). Used on the v0.1 coordinator-only data path.
const ContentCoordinator int16 = -1

// Frame is a unit on the wire (§4.2). v2 frames carry a version byte, a
// source-segment content id (which slice the bytes belong to) and a flags byte
// (e.g. a TRAILER control frame carrying sha256 + row count). v1 frames remain
// decodable (backward-compat reader) so an old agent can interoperate.
type Frame struct {
	JobID     uint32
	UnitID    uint32
	Seq       uint64
	ContentID int16  // v2: source segment content; ContentCoordinator on the coordinator path
	Flags     uint8  // v2: bit flags (FlagTrailer, ...)
	Ver       int    // decoded version (1 or 2); set by Decode, ignored by Encode
	Data      []byte // the (possibly compressed) payload; for a Trailer frame, a Trailer payload
}

// Frame flags (v2 Flags byte).
const (
	// FlagTrailer marks a control frame whose Data is a Trailer payload
	// (sha256 + row count) rather than table bytes (§4.3).
	FlagTrailer uint8 = 1 << 0
)

// Trailer carries the per-slice verification digest and row count (§4.3/§4.4).
// The source computes it over the (decompressed) binary-COPY payload of the
// slice; the destination compares its own computed values against the Trailer
// to decide verified vs corrupt.
type Trailer struct {
	SHA256 [32]byte
	Rows   int64
}

// TrailerPayloadLen is the on-wire size of a Trailer payload (32 + 8).
const TrailerPayloadLen = 40

// EncodePayload serialises a Trailer to its 40-byte wire form (little-endian).
func (t Trailer) EncodePayload() []byte {
	b := make([]byte, TrailerPayloadLen)
	copy(b[:32], t.SHA256[:])
	binary.LittleEndian.PutUint64(b[32:40], uint64(t.Rows))
	return b
}

// DecodeTrailerPayload parses a 40-byte Trailer payload.
func DecodeTrailerPayload(b []byte) (Trailer, error) {
	var t Trailer
	if len(b) != TrailerPayloadLen {
		return t, fmt.Errorf("trailer: bad payload len %d (want %d)", len(b), TrailerPayloadLen)
	}
	copy(t.SHA256[:], b[:32])
	t.Rows = int64(binary.LittleEndian.Uint64(b[32:40]))
	return t, nil
}

// NewTrailerFrame builds a v2 control frame carrying a Trailer for a slice.
func NewTrailerFrame(jobID, unitID uint32, seq uint64, contentID int16, t Trailer) *Frame {
	return &Frame{
		JobID:     jobID,
		UnitID:    unitID,
		Seq:       seq,
		ContentID: contentID,
		Flags:     FlagTrailer,
		Ver:       frameVer2,
		Data:      t.EncodePayload(),
	}
}

// Trailer returns the Trailer carried by a Trailer frame, or an error if the
// frame is not a Trailer (or its payload is malformed).
func (f *Frame) Trailer() (Trailer, error) {
	if f.Flags&FlagTrailer == 0 {
		return Trailer{}, errors.New("trailer: frame is not a Trailer (FlagTrailer not set)")
	}
	return DecodeTrailerPayload(f.Data)
}

// Wire magic. v1 is unchanged ("GPST"); v2 is "GPST"+1 so the first 4 bytes
// unambiguously select the layout (the backward-compat reader sniffs this).
const (
	magicV1 uint32 = 0x4750_5354 // "GPST" — legacy v1 frame
	magicV2 uint32 = 0x4750_5355 // "GPST"+1 — v2 frame (ver/flags/content_id/trailer)
)

const (
	frameVer1 = 1
	frameVer2 = 2

	// fixedHdrV1 is the v1 header size: magic+len+jobid+unitid+seq.
	fixedHdrV1 = 4 + 4 + 4 + 4 + 8 // = 24
	// fixedHdrV2 adds ver(1)+flags(1)+content_id(2) after len:
	// magic(4)|len(4)|ver(1)|flags(1)|content_id(2)|jobid(4)|unitid(4)|seq(8)
	fixedHdrV2 = 4 + 4 + 1 + 1 + 2 + 4 + 4 + 8 // = 28
)

// Wire layouts (length-prefixed so a reader can delimit frames; trailing 4-byte
// CRC over header+payload):
//
//	v1: magic(4)|len(4)|jobid(4)|unitid(4)|seq(8)|payload(len-24)|crc(4)
//	v2: magic(4)|len(4)|ver(1)|flags(1)|content_id(2)|jobid(4)|unitid(4)|seq(8)|payload(len-28)|crc(4)
//
// len always counts the fixed header + payload (excluding the trailing CRC).
// The first 4 bytes (magic) unambiguously select the layout, so a single
// reader handles both (backward-compat, §4.2).

// Encode serialises the frame to the wire as a v2 frame (the current default),
// with a length prefix and a trailing 4-byte CRC over (header+payload)
// (FR-01/§4.2).
func (f *Frame) Encode() []byte {
	payloadLen := len(f.Data)
	total := fixedHdrV2 + payloadLen + 4
	b := make([]byte, total)
	binary.LittleEndian.PutUint32(b[0:4], magicV2)
	binary.LittleEndian.PutUint32(b[4:8], uint32(fixedHdrV2+payloadLen))
	b[8] = frameVer2
	b[9] = f.Flags
	binary.LittleEndian.PutUint16(b[10:12], uint16(f.ContentID))
	binary.LittleEndian.PutUint32(b[12:16], f.JobID)
	binary.LittleEndian.PutUint32(b[16:20], f.UnitID)
	binary.LittleEndian.PutUint64(b[20:28], f.Seq)
	copy(b[fixedHdrV2:], f.Data)
	binary.LittleEndian.PutUint32(b[fixedHdrV2+payloadLen:], crc32(b[:fixedHdrV2+payloadLen]))
	return b
}

// EncodeV1 serialises the frame in the legacy v1 layout (no ver/flags/
// content_id) for backward-compat writers and tests.
func (f *Frame) EncodeV1() []byte {
	payloadLen := len(f.Data)
	total := fixedHdrV1 + payloadLen + 4
	b := make([]byte, total)
	binary.LittleEndian.PutUint32(b[0:4], magicV1)
	binary.LittleEndian.PutUint32(b[4:8], uint32(fixedHdrV1+payloadLen))
	binary.LittleEndian.PutUint32(b[8:12], f.JobID)
	binary.LittleEndian.PutUint32(b[12:16], f.UnitID)
	binary.LittleEndian.PutUint64(b[16:24], f.Seq)
	copy(b[fixedHdrV1:], f.Data)
	binary.LittleEndian.PutUint32(b[fixedHdrV1+payloadLen:], crc32(b[:fixedHdrV1+payloadLen]))
	return b
}

// Decode parses a full frame (v1 or v2, selected by magic), returning
// ErrCorrupt if the CRC does not match (corruption detected without
// re-copying the table, FR-01/§4.2).
func Decode(b []byte) (*Frame, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("frame: too short (%d)", len(b))
	}
	switch binary.LittleEndian.Uint32(b[0:4]) {
	case magicV1:
		return decodeV1(b)
	case magicV2:
		return decodeV2(b)
	default:
		return nil, fmt.Errorf("frame: bad magic 0x%08x", binary.LittleEndian.Uint32(b[0:4]))
	}
}

// decodeV1 parses the legacy v1 layout.
func decodeV1(b []byte) (*Frame, error) {
	_, bodyEnd, err := checkFrameLen(b, fixedHdrV1)
	if err != nil {
		return nil, err
	}
	if want, got := crc32(b[:bodyEnd]), binary.LittleEndian.Uint32(b[bodyEnd:bodyEnd+4]); got != want {
		return nil, ErrCorrupt
	}
	return &Frame{
		JobID:  binary.LittleEndian.Uint32(b[8:12]),
		UnitID: binary.LittleEndian.Uint32(b[12:16]),
		Seq:    binary.LittleEndian.Uint64(b[16:24]),
		Ver:    frameVer1,
		Data:   b[fixedHdrV1 : bodyEnd],
	}, nil
}

// decodeV2 parses the v2 layout.
func decodeV2(b []byte) (*Frame, error) {
	_, bodyEnd, err := checkFrameLen(b, fixedHdrV2)
	if err != nil {
		return nil, err
	}
	if want, got := crc32(b[:bodyEnd]), binary.LittleEndian.Uint32(b[bodyEnd:bodyEnd+4]); got != want {
		return nil, ErrCorrupt
	}
	return &Frame{
		JobID:     binary.LittleEndian.Uint32(b[12:16]),
		UnitID:    binary.LittleEndian.Uint32(b[16:20]),
		Seq:       binary.LittleEndian.Uint64(b[20:28]),
		ContentID: int16(binary.LittleEndian.Uint16(b[10:12])),
		Flags:     b[9],
		Ver:       frameVer2,
		Data:      b[fixedHdrV2 : bodyEnd],
	}, nil
}

// checkFrameLen validates the length prefix and bounds, returning the payload
// length and the end offset of the body (header+payload, before the CRC).
func checkFrameLen(b []byte, hdrLen int) (int, int, error) {
	if len(b) < hdrLen+4 {
		return 0, 0, fmt.Errorf("frame: too short (%d)", len(b))
	}
	payloadLen := int(binary.LittleEndian.Uint32(b[4:8])) - hdrLen
	if payloadLen < 0 {
		return 0, 0, fmt.Errorf("frame: bad length %d", payloadLen)
	}
	bodyEnd := hdrLen + payloadLen
	if len(b) < bodyEnd+4 {
		return 0, 0, fmt.Errorf("frame: truncated (want %d, got %d)", bodyEnd+4, len(b))
	}
	return payloadLen, bodyEnd, nil
}

// ErrCorrupt is returned when a frame's CRC does not match (§4.2).
var ErrCorrupt = errors.New("frame: crc mismatch (corruption detected)")

// crc32 derives a 4-byte digest from the frame body (FNV-1a with a length
// mix). It depends on the payload content and length, so a corrupted byte or a
// length change is always detected.
func crc32(b []byte) uint32 {
	return checksum(b)
}

func checksum(b []byte) uint32 {
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
// (back-pressure / shutdown, FR-01/FR-14). It auto-detects v1 vs v2 by magic.
func ReadFrame(ctx context.Context, r Pipe) (*Frame, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var hdr [8]byte // magic + len (v1 and v2 agree on these first 8 bytes)
		if _, err := readExact(r, hdr[:]); err != nil {
			return nil, err
		}
		var ver, hdrLen int
		switch binary.LittleEndian.Uint32(hdr[0:4]) {
		case magicV1:
			ver, hdrLen = frameVer1, fixedHdrV1
		case magicV2:
			ver, hdrLen = frameVer2, fixedHdrV2
		default:
			return nil, fmt.Errorf("stream: bad magic 0x%08x", binary.LittleEndian.Uint32(hdr[0:4]))
		}
		payloadLen := int(binary.LittleEndian.Uint32(hdr[4:8])) - hdrLen
		if payloadLen < 0 {
			return nil, fmt.Errorf("stream: bad length %d", payloadLen)
		}
		body := make([]byte, hdrLen+payloadLen+4)
		copy(body[:8], hdr[:8])
		if _, err := readExact(r, body[8:]); err != nil {
			return nil, err
		}
		// Decode the assembled body with the version-specific parser (the
		// magic already selected ver/hdrLen above).
		if ver == frameVer1 {
			return decodeV1(body)
		}
		return decodeV2(body)
	}
}

// WriteFrame writes a frame to a Pipe (v2, the current default).
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

// Send sends a frame (source side, v2).
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
