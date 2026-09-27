package transfer

// RowCounter (DESIGN §3.3) is a streaming tee over a PostgreSQL binary COPY
// stream: it passes every byte through unmodified (so it can sit between the
// COPY source and the compressor, or between the decompressor and
// `COPY FROM STDIN`) while counting complete rows.
//
// Wire format (verified against live PostgreSQL 9.4.26, the GP6 kernel):
//
//	11-byte signature  "PGCOPY\n\xff\r\n\x00"
//	int32  remainingHeaderSize   (extension-block length, 0 when none)
//	int32  flags
//	{ int16 fieldCount; (int32 len; bytes[len]) * fieldCount } * rows
//	int16  -1                    (end-of-stream marker)
//
// A field with len == -1 is NULL (no data bytes). Rows are counted as complete
// units; NULL fields and arbitrarily long fields are skipped O(1) via their
// length prefixes — the parser does a constant amount of work per structural
// field, not per data byte. The only per-byte work is the pass-through copy,
// so throughput is bounded by memory bandwidth (target ≥ 2 GB/s/core, §3.3).
//
// NOTE: DESIGN §3.3 sketches the trailer as "int32 0xFFFFFFFF"; the real
// PostgreSQL 9.4.26 end-of-stream marker is int16 (-1 = 0xFFFF). RowCounter
// implements the real format.
//
// Invariants (what makes the incremental parser correct):
//   - parsePos  = how far we have structurally parsed (may sit mid-row).
//   - emitEnd   = end of the LAST COMPLETE row (≤ parsePos). The caller is
//     served [off, emitEnd); bytes [emitEnd, len(buf)) are the partial tail.
//   - We compact (shift the buffer) only up to emitEnd, and only when
//     off == emitEnd. Because the partial tail is never compacted, the parser
//     never re-reads a length prefix it has not fully consumed.
//
// RowCounter is used on both ends (source and destination) and in both roles
// (counting the sent stream on the source, the received stream on the dest,
// for the TRAILER cross-check, §4.3).
import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	copySignature = "PGCOPY\n\xff\r\n\x00"
	copySigLen    = len(copySignature) // 11
	copyHdrLen    = 8                  // int32 remainingHeaderSize + int32 flags
)

// parser modes
const (
	modeSig = iota
	modeHeader
	modeExt
	modeRows
	modeFields
	modeDone
)

// ErrBadCopyStream is returned when the stream is not a valid PostgreSQL
// binary COPY stream (bad signature, bad field count/length, or the stream
// ended without the end-of-stream marker).
var ErrBadCopyStream = errors.New("rowcount: not a valid PostgreSQL binary COPY stream")

// RowCounter counts rows in a binary COPY stream while passing bytes through.
// Not goroutine-safe: a single reader per counter (one COPY stream).
type RowCounter struct {
	r         io.Reader
	buf       []byte
	off       int // bytes already handed to the caller (off <= emitEnd)
	emitEnd   int // end of last complete row (served boundary)
	parsePos  int // how far we have structurally parsed (>= emitEnd)
	mode      int
	fieldLeft int16 // fields remaining in the current row
	extLeft   int   // extension-block bytes remaining to skip
	rows      int64
	err       error // first stream error (sticky)
	eofSeen   bool  // underlying reader returned io.EOF
}

// NewRowCounter wraps r (a binary COPY byte source) with row counting.
func NewRowCounter(r io.Reader) *RowCounter {
	return &RowCounter{r: r, buf: make([]byte, 0, 512*1024)}
}

// Rows returns the number of complete rows seen so far.
func (rc *RowCounter) Rows() int64 { return rc.rows }

// Err returns the first stream error (bad signature/header/length, or
// truncation), if any.
func (rc *RowCounter) Err() error { return rc.err }

// Valid reports whether the stream began with a valid binary COPY signature
// and header (i.e. it was a binary COPY stream at all).
func (rc *RowCounter) Valid() bool {
	return rc.mode == modeRows || rc.mode == modeFields || rc.mode == modeDone
}

// Read implements io.Reader: it serves the binary COPY bytes (complete rows
// only) and counts rows as they are parsed. The first error returned is the
// stream error (ErrBadCopyStream or the underlying reader error) or io.EOF on
// a cleanly consumed stream.
func (rc *RowCounter) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		// Serve validated (complete-row) bytes.
		if rc.off < rc.emitEnd {
			n := copy(p, rc.buf[rc.off:rc.emitEnd])
			rc.off += n
			return n, nil
		}
		// Nothing to serve; compact the fully-served prefix (off == emitEnd).
		if rc.off > 0 {
			rest := copy(rc.buf, rc.buf[rc.off:])
			rc.buf = rc.buf[:rest]
			rc.emitEnd -= rc.off
			rc.parsePos -= rc.off
			rc.off = 0
		}
		if rc.err != nil {
			return 0, rc.err
		}
		if rc.mode == modeDone {
			// Stream fully consumed (marker seen); confirm the underlying
			// reader is at EOF (reject trailing garbage).
			if rc.eofSeen {
				return 0, io.EOF
			}
			if err := rc.readChunk(); err != nil {
				if err == io.EOF {
					rc.eofSeen = true
					return 0, io.EOF
				}
				rc.err = err
				return 0, err
			}
			rc.err = fmt.Errorf("%w: %d unexpected bytes after end-of-stream marker", ErrBadCopyStream, len(rc.buf))
			return 0, rc.err
		}
		// Pull more bytes from the underlying reader.
		if err := rc.readChunk(); err != nil {
			rc.parse() // parse whatever arrived
			if err == io.EOF {
				rc.eofSeen = true
				if rc.mode != modeDone {
					rc.err = fmt.Errorf("%w: stream ended without end-of-stream marker (truncated)", ErrBadCopyStream)
				}
			} else {
				rc.err = err
			}
			continue // serve any newly-validated bytes, then surface the error
		}
		rc.parse()
	}
}

// readChunk appends one read from the underlying reader to rc.buf, growing the
// buffer when it is nearly full (a single field can exceed the buffer size).
// It returns the underlying read error, or nil on progress.
func (rc *RowCounter) readChunk() error {
	space := cap(rc.buf) - len(rc.buf)
	if space < 64*1024 {
		nb := make([]byte, len(rc.buf), 2*cap(rc.buf))
		copy(nb, rc.buf)
		rc.buf = nb
		space = cap(rc.buf) - len(rc.buf)
	}
	n, err := rc.r.Read(rc.buf[len(rc.buf) : len(rc.buf)+space])
	if n > 0 {
		rc.buf = rc.buf[:len(rc.buf)+n]
		return nil
	}
	return err
}

// parse advances parsePos (and emitEnd at row boundaries) over as many
// complete structures as possible. It only advances parsePos over a field once
// the field's full data is present, so parsePos never points into a
// half-consumed field. emitEnd is updated to the end of each complete row.
func (rc *RowCounter) parse() {
	b := rc.buf
	for rc.parsePos < len(b) {
		avail := len(b) - rc.parsePos
		switch rc.mode {
		case modeSig:
			if avail < copySigLen {
				return
			}
			if string(b[rc.parsePos:rc.parsePos+copySigLen]) != copySignature {
				rc.err = fmt.Errorf("%w: bad signature at byte %d", ErrBadCopyStream, rc.parsePos)
				return
			}
			rc.parsePos += copySigLen
			rc.mode = modeHeader
		case modeHeader:
			if avail < copyHdrLen {
				return
			}
			rem := int32(binary.BigEndian.Uint32(b[rc.parsePos : rc.parsePos+4]))
			// flags := int32(binary.BigEndian.Uint32(b[rc.parsePos+4:rc.parsePos+8]))
			rc.parsePos += copyHdrLen
			if rem < 0 {
				rc.err = fmt.Errorf("%w: negative remainingHeaderSize %d", ErrBadCopyStream, rem)
				return
			}
			if rem > 0 {
				rc.extLeft = int(rem)
				rc.mode = modeExt
				continue
			}
			rc.mode = modeRows
		case modeExt:
			take := rc.extLeft
			if take > avail {
				take = avail
			}
			rc.parsePos += take
			rc.extLeft -= take
			if rc.extLeft == 0 {
				rc.mode = modeRows
			}
		case modeRows:
			if avail < 2 {
				return
			}
			fc := int16(binary.BigEndian.Uint16(b[rc.parsePos:]))
			rc.parsePos += 2
			if fc == -1 {
				rc.mode = modeDone
				rc.emitEnd = rc.parsePos // the marker is the end of the stream
				return
			}
			if fc < 0 {
				rc.err = fmt.Errorf("%w: bad fieldCount %d", ErrBadCopyStream, fc)
				return
			}
			rc.fieldLeft = fc
			rc.mode = modeFields
		case modeFields:
			if rc.fieldLeft == 0 {
				// Row with zero fields (e.g. `SELECT FROM t`): count it.
				rc.rows++
				rc.emitEnd = rc.parsePos
				rc.mode = modeRows
				continue
			}
			if avail < 4 {
				return
			}
			ln := int32(binary.BigEndian.Uint32(b[rc.parsePos:]))
			if ln < -1 {
				rc.err = fmt.Errorf("%w: bad field length %d", ErrBadCopyStream, ln)
				return
			}
			dataLen := 0
			if ln >= 0 {
				dataLen = int(ln)
			}
			// Only commit the field once its full data is present, so parsePos
			// never sits mid-field (which would make compaction unsafe).
			if avail < 4+dataLen {
				return
			}
			rc.parsePos += 4 + dataLen
			rc.fieldLeft--
			if rc.fieldLeft == 0 {
				rc.rows++
				rc.emitEnd = rc.parsePos
				rc.mode = modeRows
			}
		}
	}
}

// CountRows drains r, returning the row count. A well-formed stream yields
// (count, nil); a malformed one yields (count-so-far, ErrBadCopyStream or the
// underlying reader error).
func CountRows(r io.Reader) (int64, error) {
	rc := NewRowCounter(r)
	buf := make([]byte, 256*1024)
	for {
		if _, err := rc.Read(buf); err != nil {
			if err == io.EOF {
				if s := rc.Err(); s != nil {
					return rc.rows, s
				}
				return rc.rows, nil
			}
			return rc.rows, err
		}
	}
}
