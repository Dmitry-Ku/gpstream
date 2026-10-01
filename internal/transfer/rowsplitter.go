package transfer

import (
	"encoding/binary"
	"fmt"
	"io"
)

// RowSplitter (R5) is an incremental parser over a PostgreSQL binary-COPY
// stream that yields COMPLETE ROWS one at a time. It is the source side of the
// R5 direct (segment-direct-dest) path: the manager streams
// `COPY t TO STDOUT (FORMAT binary)` into a RowSplitter, which skips the
// source's signature+header (+ optional extension block), then returns each
// row's wire bytes (int16 fieldCount followed by its fields) so the caller can
// (a) hash the distribution-key fields and (b) forward the row to the
// destination segment it belongs to.
//
// Wire format (verified live PostgreSQL 9.4.26, the GP6 kernel — see
// RowCounter for the full rationale):
//
//	11-byte signature  "PGCOPY\n\xff\r\n\x00"
//	int32  remainingHeaderSize  (extension-block length, 0 when none)
//	int32  flags
//	{ int16 fieldCount; (int32 len; bytes[len]) * fieldCount } * rows
//	int16  -1               (end-of-stream marker)
//
// A field with len == -1 is NULL (no data bytes). Each returned row is a copy
// (safe to retain); the splitter compacts its buffer after each returned row.
//
// Not goroutine-safe: a single reader per splitter (one COPY stream).
type RowSplitter struct {
	r         io.Reader
	buf       []byte
	parsePos  int  // how far we have structurally parsed (>= rowEnd)
	rowStart  int  // start of the current row (>= rowEnd)
	rowEnd    int  // end of the last COMMITTED row (== rowStart until a row completes)
	rowCommit bool // a complete row is available in [rowStart, rowEnd)
	mode      int
	fieldLeft int16 // fields remaining in the current row
	extLeft   int   // extension-block bytes remaining to skip
	eofSeen   bool
	err       error // first stream error (sticky)
}

// NewRowSplitter wraps r (a binary-COPY byte source) to yield complete rows.
func NewRowSplitter(r io.Reader) *RowSplitter {
	return &RowSplitter{r: r, buf: make([]byte, 0, 512*1024), mode: modeSig}
}

// NextRow returns the next complete row's wire bytes (int16 fieldCount +
// fields), or io.EOF after the end-of-stream marker. A malformed stream
// returns ErrBadCopyStream (or the underlying read error).
//
// It parses from the already-buffered data before pulling more from the
// reader. This is what keeps the buffer bounded: readChunk reads up to the
// whole free capacity each time it is called, so reading on every NextRow
// would leave the buffer nearly full after each single-row commit and trip
// the "nearly full → double capacity" growth on every call, ratcheting the
// allocation up (512KB → 1MB → … → 512MB) until the runtime OOMs on a
// multi-GB direct copy (R1-8, 01.10: a 3.2GB table OOM'd a 7.6G host). By
// only reading when the buffered data no longer holds a complete row, the
// buffer stays at ~one read chunk plus the in-flight row; the readChunk
// growth path is then exercised only for a genuinely oversized single row.
func (rs *RowSplitter) NextRow() ([]byte, error) {
	for {
		if rs.err != nil {
			return nil, rs.err
		}
		if rs.mode == modeDone {
			// Marker consumed; confirm the reader is at EOF (reject trailing
			// garbage) and finish.
			if rs.eofSeen {
				return nil, io.EOF
			}
			if err := rs.readChunk(); err != nil {
				if err == io.EOF {
					rs.eofSeen = true
					return nil, io.EOF
				}
				rs.err = err
				return nil, err
			}
			rs.err = fmt.Errorf("%w: unexpected bytes after end-of-stream marker", ErrBadCopyStream)
			return nil, rs.err
		}
		// Parse a row from what is already buffered before pulling more.
		rs.parse()
		if rs.rowCommit {
			return rs.takeRow(), nil
		}
		// No complete row buffered: the stream is exhausted (no marker seen)
		// or we must pull more data.
		if rs.eofSeen {
			// EOF without the end-of-stream marker: truncated (a well-formed
			// stream always ends with the marker).
			rs.err = fmt.Errorf("%w: stream ended without end-of-stream marker (truncated)", ErrBadCopyStream)
			return nil, rs.err
		}
		if err := rs.readChunk(); err != nil {
			if err == io.EOF {
				rs.eofSeen = true
				continue
			}
			rs.err = err
			return nil, err
		}
	}
}

// takeRow copies out the committed row and resets the row window so the buffer
// can be compacted on the next read.
func (rs *RowSplitter) takeRow() []byte {
	row := make([]byte, rs.rowEnd-rs.rowStart)
	copy(row, rs.buf[rs.rowStart:rs.rowEnd])
	rs.rowStart = rs.rowEnd
	rs.rowCommit = false
	return row
}

// readChunk appends one read from the underlying reader to rs.buf, growing the
// buffer when it is nearly full (a single field can exceed the buffer size).
func (rs *RowSplitter) readChunk() error {
	// Compact the fully-consumed prefix (everything before rowStart) so the
	// buffer does not grow without bound. rowStart is safe to drop because
	// takeRow has already copied out every committed row.
	if rs.rowStart > 0 {
		rest := copy(rs.buf, rs.buf[rs.rowStart:])
		rs.buf = rs.buf[:rest]
		rs.parsePos -= rs.rowStart
		rs.rowStart = 0
	}
	space := cap(rs.buf) - len(rs.buf)
	if space < 64*1024 {
		nb := make([]byte, len(rs.buf), 2*cap(rs.buf))
		copy(nb, rs.buf)
		rs.buf = nb
		space = cap(rs.buf) - len(rs.buf)
	}
	n, err := rs.r.Read(rs.buf[len(rs.buf) : len(rs.buf)+space])
	if n > 0 {
		rs.buf = rs.buf[:len(rs.buf)+n]
		return nil
	}
	return err
}

// parse advances parsePos over as many complete structures as possible,
// committing a row (rowCommit, rowEnd) when a row's fields are all present.
func (rs *RowSplitter) parse() {
	b := rs.buf
	for rs.parsePos < len(b) {
		avail := len(b) - rs.parsePos
		switch rs.mode {
		case modeSig:
			if avail < copySigLen {
				return
			}
			if string(b[rs.parsePos:rs.parsePos+copySigLen]) != copySignature {
				rs.err = fmt.Errorf("%w: bad signature at byte %d", ErrBadCopyStream, rs.parsePos)
				return
			}
			rs.parsePos += copySigLen
			rs.mode = modeHeader
		case modeHeader:
			if avail < copyHdrLen {
				return
			}
			rem := int32(binary.BigEndian.Uint32(b[rs.parsePos : rs.parsePos+4]))
			rs.parsePos += copyHdrLen
			if rem < 0 {
				rs.err = fmt.Errorf("%w: negative remainingHeaderSize %d", ErrBadCopyStream, rem)
				return
			}
			if rem > 0 {
				rs.extLeft = int(rem)
				rs.mode = modeExt
				continue
			}
			rs.mode = modeRows
		case modeExt:
			take := rs.extLeft
			if take > avail {
				take = avail
			}
			rs.parsePos += take
			rs.extLeft -= take
			if rs.extLeft == 0 {
				rs.mode = modeRows
			}
		case modeRows:
			if avail < 2 {
				return
			}
			fc := int16(binary.BigEndian.Uint16(b[rs.parsePos:]))
			if fc == -1 {
				rs.mode = modeDone
				return
			}
			if fc < 0 {
				rs.err = fmt.Errorf("%w: bad fieldCount %d", ErrBadCopyStream, fc)
				return
			}
			// A row begins at its fieldCount.
			rs.rowStart = rs.parsePos
			rs.parsePos += 2
			rs.fieldLeft = fc
			rs.mode = modeFields
			if fc == 0 {
				// Zero-field row: complete immediately.
				rs.rowEnd = rs.parsePos
				rs.rowCommit = true
				rs.mode = modeRows
				return
			}
		case modeFields:
			if avail < 4 {
				return
			}
			ln := int32(binary.BigEndian.Uint32(b[rs.parsePos:]))
			if ln < -1 {
				rs.err = fmt.Errorf("%w: bad field length %d", ErrBadCopyStream, ln)
				return
			}
			dataLen := 0
			if ln >= 0 {
				dataLen = int(ln)
			}
			if avail < 4+dataLen {
				return
			}
			rs.parsePos += 4 + dataLen
			rs.fieldLeft--
			if rs.fieldLeft == 0 {
				// Row complete: commit it and stop — one row per NextRow call
				// (rowCommit is single-valued; the next call parses the next row).
				rs.rowEnd = rs.parsePos
				rs.rowCommit = true
				rs.mode = modeRows
				return
			}
		}
	}
}

// SplitRowFields parses a complete row's wire bytes (as returned by NextRow)
// and returns, for each requested column index, the field's data bytes and
// NULL flag. keyIdx are 0-based column positions within the row (the
// distribution-key columns, in distkey order); the row must have at least
// max(keyIdx)+1 fields. It returns data (nil for NULL) and isNull per key.
//
// The data slices are backed by row (do not mutate row while using them).
func SplitRowFields(row []byte, keyIdx []int) ([][]byte, []bool, error) {
	if len(row) < 2 {
		return nil, nil, fmt.Errorf("%w: row shorter than fieldCount", ErrBadCopyStream)
	}
	fc := int(int16(binary.BigEndian.Uint16(row[0:2])))
	if fc < 0 {
		return nil, nil, fmt.Errorf("%w: bad fieldCount %d", ErrBadCopyStream, fc)
	}
	if len(keyIdx) == 0 {
		return nil, nil, nil
	}
	maxIdx := -1
	for _, i := range keyIdx {
		if i > maxIdx {
			maxIdx = i
		}
	}
	if maxIdx >= fc {
		return nil, nil, fmt.Errorf("%w: key column %d out of range (fieldCount %d)", ErrBadCopyStream, maxIdx, fc)
	}
	data := make([][]byte, len(keyIdx))
	isNull := make([]bool, len(keyIdx))
	pos := 2
	for fi := 0; fi < fc; fi++ {
		if pos+4 > len(row) {
			return nil, nil, fmt.Errorf("%w: truncated field length", ErrBadCopyStream)
		}
		ln := int(int32(binary.BigEndian.Uint32(row[pos : pos+4])))
		pos += 4
		if ln == -1 {
			// NULL: no data bytes.
			if isKey(keyIdx, fi) {
				setKey(keyIdx, fi, &data, &isNull, nil, true)
			}
			continue
		}
		if ln < 0 || pos+ln > len(row) {
			return nil, nil, fmt.Errorf("%w: truncated field data", ErrBadCopyStream)
		}
		fd := row[pos : pos+ln]
		pos += ln
		if isKey(keyIdx, fi) {
			setKey(keyIdx, fi, &data, &isNull, fd, false)
		}
	}
	return data, isNull, nil
}

// isKey reports whether field position fi is one of the requested key
// columns, and setKey records it. (Key columns are distinct, so at most one
// keyIdx entry matches a given fi.)
func isKey(keyIdx []int, fi int) bool {
	for _, i := range keyIdx {
		if i == fi {
			return true
		}
	}
	return false
}

func setKey(keyIdx []int, fi int, data *[][]byte, isNull *[]bool, fd []byte, null bool) {
	for k, i := range keyIdx {
		if i == fi {
			(*data)[k] = fd
			(*isNull)[k] = null
			return
		}
	}
}
