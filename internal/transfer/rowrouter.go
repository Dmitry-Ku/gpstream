package transfer

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gpstream/gpstream/internal/route"
)

// BinaryCopyStream (R5) writes a PostgreSQL binary-COPY stream to w: a
// 19-byte header (11B signature + int32 remainingHeaderSize=4 + int32 flags=0)
// on Begin, each row's wire bytes as written, and the int16 (-1) end-of-stream
// marker on End. It is the per-segment writer of the R5 direct path: each
// destination segment receives its own well-formed binary-COPY stream so a
// `COPY t FROM STDIN (FORMAT binary)` on that segment accepts it.
//
// Integer datums inside the rows are already in network (big-endian) order in
// the source stream and are forwarded verbatim, so no re-encoding is needed.
type BinaryCopyStream struct {
	w       io.Writer
	started bool
	ended   bool
}

// NewBinaryCopyStream wraps w. The caller must call Begin before writing rows
// and End after the last row.
func NewBinaryCopyStream(w io.Writer) *BinaryCopyStream {
	return &BinaryCopyStream{w: w}
}

// Begin writes the binary-COPY header. It matches the server's own
// `COPY ... TO BINARY` output exactly (verified live, PG 9.4.26/GP6):
// 11B signature + int32 extraHdrSize(0) + int32 flags(0).
func (b *BinaryCopyStream) Begin() error {
	if b.started {
		return nil
	}
	hdr := make([]byte, 19)
	copy(hdr, copySignature)                  // 11B
	binary.BigEndian.PutUint32(hdr[11:15], 0) // extraHdrSize = 0 (no extension block)
	binary.BigEndian.PutUint32(hdr[15:19], 0) // flags = 0
	if _, err := b.w.Write(hdr); err != nil {
		return err
	}
	b.started = true
	return nil
}

// WriteRow writes one row's wire bytes (int16 fieldCount + fields) verbatim.
func (b *BinaryCopyStream) WriteRow(row []byte) (int, error) {
	if !b.started {
		if err := b.Begin(); err != nil {
			return 0, err
		}
	}
	return b.w.Write(row)
}

// End writes the end-of-stream marker (int16 -1). It ensures the header was
// written first, so a segment that received zero rows still gets a well-formed
// (empty) binary-COPY stream: header + trailer.
func (b *BinaryCopyStream) End() error {
	if b.ended {
		return nil
	}
	if err := b.Begin(); err != nil {
		return err
	}
	m := make([]byte, 2)
	binary.BigEndian.PutUint16(m, 0xFFFF) // -1
	if _, err := b.w.Write(m); err != nil {
		return err
	}
	b.ended = true
	return nil
}

// RowRouter (R5) is the destination-side router of the direct (segment-direct-
// dest) path. It reads a source binary-COPY stream (one table, all columns),
// and for each row:
//
//  1. parses the row's distribution-key fields (keyIdx, in distkey order),
//  2. computes the kernel's segment for the destination cluster:
//     h = 0; for each key attr: h = rotl1(h) ^ hashfunc(datum)  (NULL: rotl1);
//     seg = JumpConsistentHash(h, numSegments),
//  3. forwards the row's wire bytes verbatim to segWriters[seg] (a
//     BinaryCopyStream), so the row lands on the segment the destination
//     kernel would place it on.
//
// The row bytes are forwarded unchanged (only the key is read), so the
// destination receives byte-identical data to the source — the same
// order-independent content digest verifies it (FR-02).
//
// RowRouter is the single reader of the source stream; the per-segment writers
// are independent (each is its own back-pressure-respecting pipe fed to a
// `COPY FROM STDIN` on that segment).
type RowRouter struct {
	split  *RowSplitter
	keyIdx []int               // distribution-key column positions, in distkey order
	keyOid []uint32            // the key columns' type OIDs (same order as keyIdx)
	segW   []*BinaryCopyStream // one per destination segment (content 0..D-1)
	numSeg int32

	rows   int64
	perSeg []int64 // rows routed to each segment
}

// NewRowRouter builds a router over src (a source binary-COPY stream). keyIdx
// are the distribution-key column positions (0-based, distkey order); keyOid
// the matching type OIDs; segWriters one io.Writer per destination segment
// (content 0..numSegments-1). It validates the key types up front so an
// unsupported distribution-key type fails fast (the caller degrades to the
// coordinator path) rather than mid-copy.
func NewRowRouter(src io.Reader, keyIdx []int, keyOid []uint32, numSegments int32, segWriters []io.Writer) (*RowRouter, error) {
	if numSegments < 1 {
		return nil, fmt.Errorf("rowrouter: numSegments %d < 1", numSegments)
	}
	if len(segWriters) != int(numSegments) {
		return nil, fmt.Errorf("rowrouter: %d segment writers, want %d", len(segWriters), numSegments)
	}
	if len(keyIdx) != len(keyOid) {
		return nil, fmt.Errorf("rowrouter: %d key idx, %d key oids", len(keyIdx), len(keyOid))
	}
	// Validate the key types now (fail fast on an unsupported dist key).
	for i, oid := range keyOid {
		if _, err := route.HashKeyDatum(oid, nil); err != nil && err != route.ErrShortKeyDatum {
			return nil, fmt.Errorf("rowrouter: key column %d: %w", keyIdx[i], err)
		}
	}
	segW := make([]*BinaryCopyStream, numSegments)
	for i, w := range segWriters {
		segW[i] = NewBinaryCopyStream(w)
	}
	return &RowRouter{
		split:  NewRowSplitter(src),
		keyIdx: keyIdx,
		keyOid: keyOid,
		segW:   segW,
		numSeg: numSegments,
		perSeg: make([]int64, numSegments),
	}, nil
}

// Rows returns the total rows routed.
func (rr *RowRouter) Rows() int64 { return rr.rows }

// PerSegment returns the per-segment row counts (content 0..D-1).
func (rr *RowRouter) PerSegment() []int64 { return rr.perSeg }

// hashSeg computes the destination segment for one row's key fields.
// data/isNull are per key (aligned with rr.keyIdx); each datum's wire bytes
// are converted to the kernel's hkey by type, then combined per cdbhash().
func (rr *RowRouter) hashSeg(data [][]byte, isNull []bool) (int32, error) {
	var h uint32
	for i, idx := range rr.keyIdx {
		if isNull[i] {
			// NULL attribute: rotl1 only, no XOR.
			h = route.Combine(h, 0, true)
			continue
		}
		hkey, err := route.HashKeyDatum(rr.keyOid[i], data[i])
		if err != nil {
			return 0, fmt.Errorf("rowrouter: key col %d: %w", idx, err)
		}
		h = route.Combine(h, hkey, false)
	}
	return route.JumpConsistentHash(uint64(h), rr.numSeg), nil
}

// Run drains the source stream, routing every row to its destination segment.
// It returns the total rows routed (== the source row count for a clean
// stream). All per-segment streams are End'ed (trailer written) before
// returning, so each destination `COPY FROM STDIN` sees a complete stream.
// A per-segment write error (a dead segment) is returned after the remaining
// segments are drained; the caller is responsible for closing the pipes.
func (rr *RowRouter) Run() (int64, error) {
	var firstErr error
	defer func() {
		// Always finish the per-segment streams so the destination COPYs see
		// a complete (header+rows+trailer) stream even on error.
		for _, w := range rr.segW {
			_ = w.End()
		}
	}()
	for {
		row, err := rr.split.NextRow()
		if err == io.EOF {
			return rr.rows, firstErr
		}
		if err != nil {
			return rr.rows, err
		}
		data, isNull, perr := SplitRowFields(row, rr.keyIdx)
		if perr != nil {
			return rr.rows, perr
		}
		seg, herr := rr.hashSeg(data, isNull)
		if herr != nil {
			return rr.rows, herr
		}
		if _, werr := rr.segW[seg].WriteRow(row); werr != nil && firstErr == nil {
			// A segment write failed (dead/aborted segment). Keep draining the
			// source (so it does not block forever on a full pipe) but record
			// the first error.
			firstErr = werr
			continue
		}
		rr.rows++
		rr.perSeg[seg]++
	}
}
