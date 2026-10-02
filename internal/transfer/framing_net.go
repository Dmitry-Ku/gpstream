package transfer

import (
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// This file exposes the two halves of the framed transport as independently
// constructible objects, so the source and destination AGENTS (separate
// processes, ADR-001/ADR-002 remediation) can each build their half over a
// real network connection (net.Conn, optionally TLS). The in-process
// FramedPipe (NewFramedPipe / NewFramedPipeOver) keeps both halves in one
// process over a net.Pipe for the single-controller build and for tests.
//
// The network data path (two-agent design):
//
//	source agent:  NewFramedWriter(srcConn, ...)  -- compress + frame -->
//	                                                      |  real TCP (TLS)
//	dest agent:    NewFramedReader(dstConn, ...)  <-- decompress + verify --
//
// The bytes between the agents travel COMPRESSED over the socket (FR-12
// "compression between hosts"), which the single-controller build does not do
// (its pipe is in-memory). The byte counters (Wire/Logical) let each agent
// report its side of the volume to the controller (FR-04).

// NewFramedWriter builds the source-side (write) half of the framed transport
// over a connected Pipe (a net.Conn in production, a net.Pipe end in tests).
// The writer compresses (comp, FR-12) and CRC-frames the bytes it is given;
// CloseFlush sends the tail and closes the connection (end-of-copy for the
// destination). comp must be non-nil (use compress.New with a mode; a
// ModeNone compressor is a pass-through).
func NewFramedWriter(conn Pipe, jobID, unitID uint32, comp *compress.Compressor) *FramedWriter {
	return &FramedWriter{conn: conn, jobID: jobID, unitID: unitID, comp: comp}
}

// NewFramedReader builds the destination-side (read) half of the framed
// transport over a connected Pipe. The reader frame-verifies (ErrCorrupt on a
// bad CRC), decompresses (dec), and serves the logical bytes; it returns
// io.EOF when the source closes its end. dec must be non-nil.
func NewFramedReader(conn Pipe, dec *compress.Decompressor) *FramedReader {
	return &FramedReader{conn: conn, comp: dec}
}

// Wire returns the source-side byte count pushed onto the transport
// (compressed, framed) — the bytes actually on the wire between the agents.
// Exposed so the source agent can report FR-04 volume to the controller.
func (fw *FramedWriter) Wire() uint64 { return atomic.LoadUint64(&fw.wire) }

// SetWriteLatencyHook installs the source-side write-duration hook (F-14
// receive-queue metric) on a standalone writer (the FramedPipe variant lives
// on the pipe).
func (fw *FramedWriter) SetWriteLatencyHook(fn func(d time.Duration)) {
	fw.latHook = fn
}

// Logical returns the destination-side byte count served after decompression
// (the logical table bytes the destination wrote). Exposed so the dest agent
// can report FR-04 volume to the controller.
func (fr *FramedReader) Logical() uint64 { return atomic.LoadUint64(&fr.logical) }

// Abort closes the destination-side connection without reading to EOF. It is
// the destination agent's escape hatch when its COPY FROM fails mid-stream:
// without it the source agent would block forever writing into a full socket
// (FR-06). After Abort the source's next write returns an error so it
// unwinds. Safe to call more than once.
func (fr *FramedReader) Abort() error { return fr.conn.Close() }
