package segstream

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// RoutedSender (P9) is the source-side router of the routed segment-direct
// transport (S≠D). It runs inside gpstream-segreceiver on each primary source
// segment: the kernel hands it the segment's LOCAL slice of the table
// (a self-contained binary-COPY stream on stdin, read under the coordinator's
// snapshot), and it places every row on the destination cluster:
//
//  1. d = transfer.RouteKeySegment(...) — the destination kernel's placement
//     (cdbhash over the distribution key + jump-consistent-hash over D
//     segments). The row MUST land on destination segment d.
//  2. w = counter[d] % W — a per-pair round-robin: the row goes to exactly
//     one of the W sub-streams of the (s,d) pair.
//
// The (s,d) pair is fanned out over W sub-streams (W = the operator's
// --workers), so one segreceiver opens up to D×W connections to the
// destination agent (S×D×W across the cluster at parallelism W). Each
// sub-stream is an independent binary-COPY stream (its own header + EOF)
// over its own connection, compressed + framed like a P4 slice, tagged with
// ContentID = d (the destination segment it belongs to) and a sub-stream
// index carried in the frame Seq's high bits is NOT needed: the destination
// merges every sub-stream addressed to a segment with one SliceMux, so the
// sub-stream identity is invisible to the load. The split is a disjoint
// partition (each row is written to exactly one pipe), so the destination
// receives exactly the source's row multiset — no duplication, no loss — and
// row order is irrelevant (COPY FROM STDIN is append-only and the placement
// is already hashed).
//
// Back-pressure: each sub-stream is an unbuffered io.Pipe fed by its own
// sender goroutine; a stalled destination blocks that pipe, which blocks the
// router, which blocks the kernel's COPY on this segment (the kernel's own
// flow control — no unbounded buffering in the middle, FR-01).
type RoutedSender struct {
	ctx     context.Context
	in      io.Reader // the segment's binary-COPY slice (os.Stdin)
	keyIdx  []int
	keyOid  []uint32
	numDest int32 // D — destination primary segments
	workers int   // W — sub-streams per (s,d) pair
	jobID   uint32
	unitID  uint32
	content int16 // this source segment's content id
	mode    compress.Mode
	level   int
	// conns[d][w] is the connected sub-stream to destination segment d,
	// sub-stream w (already dialled + TLS'd by the caller).
	conns [][]transfer.Pipe

	rows   int64
	perSeg []int64
}

// NewRoutedSender builds a sender over the segment slice in. conns must have
// len(conns) == numDest and len(conns[d]) == workers.
func NewRoutedSender(ctx context.Context, in io.Reader, keyIdx []int, keyOid []uint32, numDest int32, workers int, jobID, unitID uint32, content int16, mode compress.Mode, level int, conns [][]transfer.Pipe) *RoutedSender {
	return &RoutedSender{
		ctx:     ctx,
		in:      in,
		keyIdx:  keyIdx,
		keyOid:  keyOid,
		numDest: numDest,
		workers: workers,
		jobID:   jobID,
		unitID:  unitID,
		content: content,
		mode:    mode,
		level:   level,
		conns:   conns,
		perSeg:  make([]int64, numDest),
	}
}

// Rows returns the total rows routed (a cross-check; the authoritative count
// is the destination agent's).
func (rs *RoutedSender) Rows() int64 { return rs.rows }

// PerSegment returns the per-destination-segment row distribution.
func (rs *RoutedSender) PerSegment() []int64 { return rs.perSeg }

// Run drains the slice, routing every row to its destination sub-stream, and
// waits for all senders to finish (every sub-stream closed + trailer sent).
// A send error (a dead destination) is returned after the remaining
// sub-streams are drained; the caller closes the connections.
func (rs *RoutedSender) Run() (int64, error) {
	split := transfer.NewRowSplitter(rs.in)
	pws := make([][]*io.PipeWriter, rs.numDest)
	readers := make([][]*io.PipeReader, rs.numDest)
	// Each sub-stream must be a self-contained binary-COPY stream (header +
	// its rows + EOF) so the destination's per-segment COPY FROM STDIN
	// accepts it: wrap each pipe with a BinaryCopyStream (the P5 RowRouter's
	// per-segment writer).
	streams := make([][]*transfer.BinaryCopyStream, rs.numDest)
	for d := 0; d < int(rs.numDest); d++ {
		pws[d] = make([]*io.PipeWriter, rs.workers)
		readers[d] = make([]*io.PipeReader, rs.workers)
		streams[d] = make([]*transfer.BinaryCopyStream, rs.workers)
		for w := 0; w < rs.workers; w++ {
			pr, pw := io.Pipe()
			pws[d][w] = pw
			readers[d][w] = pr
			streams[d][w] = transfer.NewBinaryCopyStream(pw)
		}
	}
	// One sender per sub-stream: reads its pipe (a binary-COPY stream written
	// by the router) and streams it compressed + content-tagged to the
	// destination. Closes the pipe reader on exit so a dead destination does
	// not wedge the router on that sub-stream's next write.
	var (
		sendWG   sync.WaitGroup
		sendErrs = make(chan error, int(rs.numDest)*rs.workers)
	)
	for d := 0; d < int(rs.numDest); d++ {
		for w := 0; w < rs.workers; w++ {
			sendWG.Add(1)
			go func(d, w int) {
				defer sendWG.Done()
				defer func() { _ = readers[d][w].Close() }()
				_, eerr := SendCompressed(rs.ctx, rs.conns[d][w], readers[d][w],
					rs.jobID, rs.unitID, int16(d), rs.mode, rs.level)
				if eerr != nil {
					sendErrs <- fmt.Errorf("dest segment %d sub-stream %d: %v", d, w, eerr)
				}
			}(d, w)
		}
	}

	// Route every row.
	var firstErr error
	wrr := make([]int, rs.numDest) // per-destination sub-stream round-robin cursors
	for {
		row, err := split.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			firstErr = err
			break
		}
		data, isNull, perr := transfer.SplitRowFields(row, rs.keyIdx)
		if perr != nil {
			firstErr = perr
			break
		}
		seg, herr := transfer.RouteKeySegment(rs.keyIdx, rs.keyOid, rs.numDest, data, isNull)
		if herr != nil {
			firstErr = herr
			break
		}
		w := wrr[seg] % rs.workers
		wrr[seg]++
		// NextRow returns a copy of the row, so it is safe to hand to the
		// pipe without re-copying.
		if _, werr := streams[seg][w].WriteRow(row); werr != nil && firstErr == nil {
			firstErr = werr
			// Keep draining (so the kernel COPY does not block forever); the
			// error is reported after the drain.
		}
		rs.rows++
		rs.perSeg[seg]++
	}
	// End every sub-stream (header + EOF) so each destination COPY sees a
	// complete stream, then close the pipes and wait for all senders.
	for d := range streams {
		for w := range streams[d] {
			_ = streams[d][w].End()
			_ = pws[d][w].Close()
		}
	}
	sendWG.Wait()
	close(sendErrs)
	if firstErr == nil {
		for e := range sendErrs {
			firstErr = e
			break
		}
	}
	return rs.rows, firstErr
}
