package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// FramedPipe is the real data-path transport (FR-01/§4.2): it carries the
// logical bytes of a table between the source and destination agents over a
// net.Pipe (or, in production, a net.Conn, optionally TLS-wrapped, FR-13),
// with CRC framing so corruption is detected without re-copying the table.
// With --frame-ack (DESIGN §4.6) the transport is a ReliablePipe pair instead
// of a raw pipe: frames ride a retransmit ring with cumulative ACKs, so a
// transport reset costs seconds (replay from RAM) and never a table re-read.
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
	// rpS is the ReliablePipe on the write side (frame-ack on); nil for a
	// plain net.Pipe transport. Exposed so the manager can surface the
	// retransmit/reconnect counters (FR-04/§4.6).
	rpS *ReliablePipe
}

// NewFramedPipe builds a framed transport over a connected pair of
// connections (net.Pipe). The source agent writes to writeConn; the
// destination agent reads from readConn. comp/dec select the compression mode
// (FR-12).
func NewFramedPipe(writeConn, readConn net.Conn, jobID, unitID uint32, comp *compress.Compressor, dec *compress.Decompressor) *FramedPipe {
	return NewFramedPipeOver(writeConn, readConn, jobID, unitID, comp, dec)
}

// NewFramedPipeOver builds a framed transport over an arbitrary connected
// Pipe pair. The write side may be a ReliablePipe sender and the read side
// its receiver (--frame-ack, §4.6); a ReliablePipe on the write side is
// detected and exposed via Reliable().
func NewFramedPipeOver(writeConn, readConn Pipe, jobID, unitID uint32, comp *compress.Compressor, dec *compress.Decompressor) *FramedPipe {
	fp := &FramedPipe{
		write: &FramedWriter{conn: writeConn, jobID: jobID, unitID: unitID, comp: comp},
		read:  &FramedReader{conn: readConn, comp: dec},
	}
	if rp, ok := writeConn.(*ReliablePipe); ok {
		fp.rpS = rp
	}
	return fp
}

// Reliable returns the frame-ack transport on the write side, or nil when the
// pipe is a plain (non-reliable) transport.
func (fp *FramedPipe) Reliable() *ReliablePipe { return fp.rpS }

// Stats returns the pipe's byte counters (FR-04 volume/speed metrics):
// wire bytes the write side actually pushed onto the transport (compressed,
// framed) and logical bytes the read side served after decompression. The
// manager aggregates them per unit so the /metrics endpoint reports real
// transferred volume and compression in/out — not row counts (F-04:
// «объём и скорость», «сжатие (вход/выход)»).
func (fp *FramedPipe) Stats() (wire, logical uint64) {
	return atomic.LoadUint64(&fp.write.wire), atomic.LoadUint64(&fp.read.logical)
}

// Read is the destination side (reads framed, compressed bytes from the pipe).
func (fp *FramedPipe) Read(p []byte) (int, error) { return fp.read.Read(p) }

// Write is the source side (writes framed, compressed bytes to the pipe).
func (fp *FramedPipe) Write(p []byte) (int, error) { return fp.write.Write(p) }

// Close flushes the write side's tail (the batched frame buffer), then
// closes both ends of the transport (signals end-of-copy to the
// destination, FR-01). Safe to call more than once. With frame-ack the write
// side is a ReliablePipe: its Close sends a clean FIN after the tail frame,
// so the destination gets a proper io.EOF (and no spurious transport error).
func (fp *FramedPipe) Close() error {
	err1 := fp.write.CloseFlush()
	var err2 error
	if fp.rpS != nil {
		// Reliable path: CloseFlush already closed the sender, which sent an
		// in-order FIN after the tail frame. The receiver ends on its own when
		// its receiveLoop reads that FIN (all frames are already in the output
		// buffer by then) — so we must NOT close the receiver from here; doing
		// so would race the receiveLoop and could drop in-flight frames. The
		// destination's Read returns io.EOF once the buffer drains.
		err2 = err1
	} else {
		err2 = fp.read.conn.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// Abort closes the READ end of the transport without flushing the write
// side. It is the destination's escape hatch: when the destination copy
// fails mid-stream (e.g. the staged COPY FROM is rejected), it must stop
// consuming the pipe; without an Abort the source would block forever
// writing into the pipe (a full pipe with no reader), hanging the whole unit
// (FR-06: a worker failure must not abort the batch — the unit must fail, not
// hang). After Abort the source's blocked write returns an error, so the
// source goroutine unwinds and the run records the unit as failed (a resume
// can retry it). Safe to call more than once. With frame-ack the destination
// abort also tells the ReliablePipe sender to stop (no reconnect to a dead
// destination).
func (fp *FramedPipe) Abort() error {
	err := fp.read.conn.Close()
	if fp.rpS != nil {
		if err == nil {
			err = fp.rpS.Abort()
		}
	}
	return err
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
	conn   Pipe // net.Conn or a ReliablePipe sender (frame-ack, §4.6)
	jobID  uint32
	unitID uint32
	comp   *compress.Compressor
	seq    uint64
	buf    []byte
	closed bool
	// wire is the compressed, framed byte count pushed onto the transport
	// (FR-04 volume). Atomic: the manager reads it via Stats() while the
	// source goroutine is still writing.
	wire uint64
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
	err := WriteFrameCounted(fw.conn, f, &fw.wire)
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
	conn Pipe // net.Conn or a ReliablePipe receiver (frame-ack, §4.6)
	comp *compress.Decompressor
	buf  []byte
	// logical is the decompressed byte count served to the destination
	// (FR-04: compression in/out). Atomic: the manager reads it via Stats()
	// while the destination goroutine is still reading.
	logical uint64
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
	if n > 0 {
		atomic.AddUint64(&fr.logical, uint64(n))
	}
	return n, nil
}
