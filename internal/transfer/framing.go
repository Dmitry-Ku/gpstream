package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// FramedPipe is the real data-path transport (FR-01/§4.2): it carries the
// logical bytes of a table between the source and destination agents over a
// net.Pipe (or, in production, a net.Conn, optionally TLS-wrapped, FR-13),
// with CRC framing so corruption is detected without re-copying the table.
//
// It is used by the job manager as the bridge between the source agent's
// `COPY ... TO STDOUT` (write side) and the destination agent's
// `COPY ... FROM STDIN` (read side). It never touches local storage in the
// middle (FR-01) — only RAM with back-pressure.
//
//	source agent --(FramedWriter)--> FramedPipe --(FramedReader)--> dest agent
//
// The write side compresses (FR-12) and frames; the read side frames-verify
// and decompresses. A CRC mismatch (ErrCorrupt) is surfaced, not swallowed.
type FramedPipe struct {
	write *FramedWriter
	read  *FramedReader
}

// NewFramedPipe builds a framed transport over a connected pair of
// connections (net.Pipe). The source agent writes to writeConn; the
// destination agent reads from readConn. comp/dec select the compression mode
// (FR-12).
func NewFramedPipe(writeConn, readConn net.Conn, jobID, unitID uint32, comp *compress.Compressor, dec *compress.Decompressor) *FramedPipe {
	return &FramedPipe{
		write: &FramedWriter{conn: writeConn, jobID: jobID, unitID: unitID, comp: comp},
		read:  &FramedReader{conn: readConn, comp: dec},
	}
}

// Read is the destination side (reads framed, compressed bytes from the pipe).
func (fp *FramedPipe) Read(p []byte) (int, error) { return fp.read.Read(p) }

// Write is the source side (writes framed, compressed bytes to the pipe).
func (fp *FramedPipe) Write(p []byte) (int, error) { return fp.write.Write(p) }

// Close flushes the write side's tail (the batched frame buffer), then
// closes both ends of the transport (signals end-of-copy to the
// destination, FR-01). Safe to call more than once.
func (fp *FramedPipe) Close() error {
	err1 := fp.write.CloseFlush()
	err2 := fp.read.conn.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// frameBufLimit is the source-side accumulator size before a frame is
// encoded and written. One frame per ~256 KiB keeps per-frame overhead
// (compress init, header, CRC) at ~0.1% and gives the in-memory net.Pipe
// back-pressure granularity that the coordinator path already relies on.
// The segment-direct path feeds the mux in per-field chunks (6 B..4 KiB),
// so without this batching every field became a frame — ~15 MB/s on the
// 4-vCPU vm1 controller (E2 e2e, 27.09).
const frameBufLimit = 256 * 1024

// FramedWriter compresses and frames the source-side bytes.
type FramedWriter struct {
	conn   net.Conn
	jobID  uint32
	unitID uint32
	comp   *compress.Compressor
	seq    uint64
	buf    []byte
	closed bool
}

// Write buffers p and flushes a compressed, CRC-framed frame once the
// accumulator reaches frameBufLimit (or on Flush/Close). It returns len(p)
// once the bytes are accepted into the transport (io.Writer contract).
func (fw *FramedWriter) Write(p []byte) (int, error) {
	if fw.closed {
		return 0, fmt.Errorf("framed writer: write after close")
	}
	if len(p) == 0 {
		return 0, nil
	}
	fw.buf = append(fw.buf, p...)
	if len(fw.buf) >= frameBufLimit {
		if err := fw.Flush(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Flush encodes and frames any buffered bytes. It is idempotent when the
// buffer is empty and is called by Close so the tail is never lost.
func (fw *FramedWriter) Flush() error {
	if len(fw.buf) == 0 {
		return nil
	}
	// Compress on the source (FR-12); the destination decompresses.
	data := fw.comp.Encode(fw.buf)
	f := &Frame{JobID: fw.jobID, UnitID: fw.unitID, Seq: fw.seq, Data: data}
	fw.seq++
	err := WriteFrame(fw.conn, f)
	fw.buf = fw.buf[:0]
	return err
}

// CloseFlush flushes the tail and closes the write side of the transport
// (end-of-copy for the destination, FR-01).
func (fw *FramedWriter) CloseFlush() error {
	if fw.closed {
		return nil
	}
	fw.closed = true
	err := fw.Flush()
	if cerr := fw.conn.Close(); err == nil {
		err = cerr
	}
	return err
}

// FramedReader frames-verifies and decompresses the destination-side bytes.
type FramedReader struct {
	conn net.Conn
	comp *compress.Decompressor
	buf  []byte
}

// Read pulls one frame at a time, verifies its CRC (ErrCorrupt on mismatch,
// §4.2), decompresses it (FR-12) and serves bytes. A clean end of the
// connection yields io.EOF; a CRC mismatch yields ErrCorrupt so a corrupted
// frame is not silently accepted (FR-01/§4.2).
func (fr *FramedReader) Read(p []byte) (int, error) {
	for len(fr.buf) == 0 {
		f, err := ReadFrame(context.Background(), fr.conn)
		if err != nil {
			if errors.Is(err, ErrCorrupt) {
				return 0, ErrCorrupt
			}
			return 0, io.EOF
		}
		fr.buf = fr.comp.Decode(f.Data)
	}
	n := copy(p, fr.buf)
	fr.buf = fr.buf[n:]
	return n, nil
}
