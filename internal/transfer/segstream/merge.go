package segstream

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// The destination reads ONE binary-COPY stream (a single COPY FROM STDIN),
// but segment-direct produces N parallel slice streams (one per primary
// segment), each a self-contained binary-COPY stream (header + rows + EOF).
// SliceMux merges the N slices into a single valid binary-COPY stream on w:
// one header, then all rows from all slices (in arrival order — row order is
// irrelevant, the destination redistributes by its own distribution key), then
// one EOF. It strips each slice's own header and EOF.
//
// The mux is driven by the per-slice writer callback of Receive (one goroutine
// per slice stream, concurrent), so all writes to w are serialised by a mutex.
//
// Binary-COPY slice layout (PG 9.4.26, verified):
//
//	header: 11-byte sig "PGCOPY\n\xff\r\n\0" | int32 rem | int32 flags [| rem ext bytes]
//	rows:   { int16 fc; (int32 len; bytes[len]) * fc } * rows
//	eof:    int16 -1
const bcopySig = "PGCOPY\n\xff\r\n\x00"

// slice parser phases.
const (
	spSig = iota
	spRem
	spFlags
	spExt
	spRows
	spDone
)

// row sub-states.
const (
	rsRowHdr = iota // reading the int16 (fieldCount, or the -1 EOF)
	rsFieldLen      // reading the int32 field length
	rsFieldData     // reading the field's data bytes
)

// sliceParser strips one slice's header and EOF, forwarding only the row bytes.
type sliceParser struct {
	phase    int
	acc      []byte // accumulator for the current fixed-size read
	rem      int
	extLeft  int
	state    int
	need     int // bytes to accumulate (2 for int16, 4 for int32)
	fc       int16
	dataLeft int
	done     bool
	counted  bool
	// rowBuf accumulates the CURRENT row's bytes (header + field lengths +
	// data) so that a row is written to the sink as ONE atomic unit. Without
	// this, a row that straddles a chunk boundary is emitted across two
	// feed() calls, and a concurrent sibling slice's complete rows can be
	// interleaved in the MIDDLE of the row, corrupting the merged stream
	// (E2 on_segment live bug, 28.09). Bounded to one row's size.
	rowBuf []byte
}

// newSliceParser returns a parser ready to consume a slice from its start.
func newSliceParser() *sliceParser {
	return &sliceParser{phase: spSig, acc: make([]byte, 0, 16), state: rsRowHdr, need: 2, rowBuf: make([]byte, 0, 256)}
}

// flushRow writes the buffered complete row to w and resets rowBuf. It is a
// no-op when no row is buffered. Row emission is atomic: a row is only ever
// handed to the sink as a single Write, so concurrent sibling slices cannot
// interleave their rows inside it.
func (sp *sliceParser) flushRow(w io.Writer) error {
	if len(sp.rowBuf) == 0 {
		return nil
	}
	if _, err := w.Write(sp.rowBuf); err != nil {
		return err
	}
	sp.rowBuf = sp.rowBuf[:0]
	return nil
}

// take appends up to want bytes from p to sp.acc and returns the count taken.
func (sp *sliceParser) take(p []byte, want int) int {
	if want > len(p) {
		want = len(p)
	}
	sp.acc = append(sp.acc, p[:want]...)
	return want
}

// fieldDone decrements the current row's field counter and advances the
// sub-state (next field, or next row header). When the last field of a row is
// consumed it flushes the buffered row to w, so a row is written as one atomic
// unit (see rowBuf) and a concurrent sibling slice cannot interleave into it.
func (sp *sliceParser) fieldDone(w io.Writer) error {
	if sp.fc > 1 {
		sp.fc--
		sp.state = rsFieldLen
		sp.need = 4
		return nil
	}
	sp.fc = 0
	sp.state = rsRowHdr
	sp.need = 2
	return sp.flushRow(w)
}

// feed consumes p, forwarding the row-region bytes to w. It returns the number
// of bytes of p consumed. It is safe to call repeatedly with successive chunks.
func (sp *sliceParser) feed(p []byte, w io.Writer) (int, error) {
	off := 0
	for off < len(p) && !sp.done {
		advance := off
		switch sp.phase {
		case spSig:
			n := sp.take(p[off:], 11-len(sp.acc))
			off += n
			if len(sp.acc) == 11 {
				if string(sp.acc) != bcopySig {
					return off, fmt.Errorf("segstream: bad slice signature %q", sp.acc)
				}
				sp.acc = sp.acc[:0]
				sp.phase = spRem
			}
		case spRem:
			n := sp.take(p[off:], 4-len(sp.acc))
			off += n
			if len(sp.acc) == 4 {
				rem := int(int32(binary.BigEndian.Uint32(sp.acc)))
				if rem < 0 {
					return off, fmt.Errorf("segstream: bad header rem %d", rem)
				}
				sp.rem = rem
				sp.acc = sp.acc[:0]
				sp.phase = spFlags
			}
		case spFlags:
			n := sp.take(p[off:], 4-len(sp.acc))
			off += n
			if len(sp.acc) == 4 {
				sp.acc = sp.acc[:0]
				if sp.rem > 0 {
					sp.extLeft = sp.rem
					sp.phase = spExt
				} else {
					sp.phase = spRows
				}
			}
		case spExt:
			n := len(p) - off
			if n > sp.extLeft {
				n = sp.extLeft
			}
			off += n
			sp.extLeft -= n
			if sp.extLeft == 0 {
				sp.phase = spRows
			}
		case spRows:
			if sp.state == rsFieldData {
				n := len(p) - off
				if n > sp.dataLeft {
					n = sp.dataLeft
				}
				sp.rowBuf = append(sp.rowBuf, p[off:off+n]...)
				off += n
				sp.dataLeft -= n
				if sp.dataLeft == 0 {
					if err := sp.fieldDone(w); err != nil {
						return off, err
					}
				}
				continue
			}
			// Accumulating a fixed-size read (int16 row header or int32 length).
			if len(sp.acc) < sp.need {
				n := sp.take(p[off:], sp.need-len(sp.acc))
				off += n
				if len(sp.acc) < sp.need {
					continue
				}
			}
			if sp.state == rsRowHdr {
				v := int16(binary.BigEndian.Uint16(sp.acc))
				sp.acc = sp.acc[:0]
				if v == -1 {
					// EOF marker: consume, stop. Not forwarded.
					sp.done = true
					continue
				}
				if v < 0 {
					return off, fmt.Errorf("segstream: bad fieldCount %d", v)
				}
				// Buffer the 2-byte row header; it is emitted atomically with
				// the row's fields when the row completes (see rowBuf).
				var b2 [2]byte
				binary.BigEndian.PutUint16(b2[:], uint16(v))
				sp.rowBuf = append(sp.rowBuf, b2[:]...)
				sp.fc = v
				if sp.fc == 0 {
					// Zero-field row: complete immediately.
					sp.state = rsRowHdr
					sp.need = 2
					if err := sp.flushRow(w); err != nil {
						return off, err
					}
				} else {
					sp.state = rsFieldLen
					sp.need = 4
				}
			} else { // rsFieldLen
				ln := int32(binary.BigEndian.Uint32(sp.acc))
				// Buffer the 4-byte length.
				var b4 [4]byte
				binary.BigEndian.PutUint32(b4[:], uint32(ln))
				sp.rowBuf = append(sp.rowBuf, b4[:]...)
				sp.acc = sp.acc[:0]
				if ln < -1 {
					return off, fmt.Errorf("segstream: bad field length %d", ln)
				}
				if ln == -1 { // NULL
					if err := sp.fieldDone(w); err != nil {
						return off, err
					}
				} else {
					sp.dataLeft = int(ln)
					if sp.dataLeft == 0 {
						if err := sp.fieldDone(w); err != nil {
							return off, err
						}
					} else {
						sp.state = rsFieldData
					}
				}
			}
		}
		if off == advance {
			// No progress (should not happen, but guard against a spin).
			break
		}
	}
	return off, nil
}

// SliceMux merges N slice streams into a single binary-COPY stream on w.
type SliceMux struct {
	w      io.Writer
	n      int
	mu     sync.Mutex
	exts   map[int16]*sliceParser
	done   int
	header bool
	closed bool
}

// NewSliceMux creates a mux for n slices, writing the merged stream to w.
// Call Begin once before feeding, Write per chunk, and End when all slices are
// complete (AllDone).
func NewSliceMux(w io.Writer, n int) *SliceMux {
	return &SliceMux{w: w, n: n, exts: make(map[int16]*sliceParser, n)}
}

// Begin writes the single merged header. It must be called once, before any
// Write. The header is the fixed 19-byte binary-COPY header (no extension
// block); the per-row fieldCount carries the column structure.
func (m *SliceMux) Begin() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.header {
		return nil
	}
	h := make([]byte, 19)
	copy(h, bcopySig)
	// bytes 11-14 rem=0, 15-18 flags=0 already zero.
	if _, err := m.w.Write(h); err != nil {
		return err
	}
	m.header = true
	return nil
}

// Write feeds a payload chunk belonging to slice contentID. It forwards the
// chunk's row bytes (header/EOF stripped) to the merged stream.
func (m *SliceMux) Write(contentID int16, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sp, ok := m.exts[contentID]
	if !ok {
		sp = newSliceParser()
		m.exts[contentID] = sp
	}
	_, err := sp.feed(p, m.w)
	if err == nil && sp.done && !sp.counted {
		sp.counted = true
		m.done++
	}
	return err
}

// AllDone reports whether every slice's stream has reached its EOF.
func (m *SliceMux) AllDone() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done >= m.n
}

// End writes the single merged EOF. It is safe to call once AllDone is true.
func (m *SliceMux) End() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	var eof [2]byte
	binary.BigEndian.PutUint16(eof[:], 0xFFFF) // int16 -1
	if _, err := m.w.Write(eof[:]); err != nil {
		return err
	}
	m.closed = true
	return nil
}
