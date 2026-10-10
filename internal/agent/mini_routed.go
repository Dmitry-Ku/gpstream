package agent

// mini_routed.go — the P4 ROUTED (S≠D) mesh unit path of the MINI-AGENT.
//
// Where ServeMini/serveMiniUnit (mini.go) is the DIRECT_1TO1 shape — one
// source slice into one utility COPY — this file is the ROUTED shape: S
// source segreceivers each fan their rows to THIS destination segment over W
// sub-streams (the source's RoutedSender: d = the destination segment the row
// belongs to, w = per-pair round-robin), so this mini-agent's data listener
// accepts S×W compressed sub-stream connections. Each sub-stream is a
// self-contained binary-COPY stream (its own header + rows + EOF) content-
// tagged with THIS destination segment's id, so the mini-agent MERGES them —
// one SliceStripper per sub-stream (strips the sub-stream's own header/EOF),
// all writing the single utility-COPY pipe under one lock — into ONE
// well-formed binary-COPY stream (header once, every sub-stream's rows, EOF
// once) loaded by the single utility-mode COPY. This is the same merge
// ServeRouted performs on the coordinator for the P9 (non-mesh) routed path;
// here it runs on the segment host (design 04.10: the agent that opens the pg
// connection runs where the connection is made).
//
// Correctness invariants:
//   - The S×W sub-streams are a DISJOINT partition of the source's rows
//     (RoutedSender writes each row to exactly one (d,w) sub-stream), so the
//     merge is complete and duplicate-free.
//   - The per-row digest (FR-02) is a row-order-independent XOR accumulator,
//     so the merge order (and the sub-stream accept order) is immaterial:
//     srcContrib = XOR over the S×W sub-stream trailers; dstAcc = the
//     destination's accumulator over the merged stream (row-order-independent).
//     Both compose: the supervisor XORs the D mini-agents' values.
//   - The destination digest is teed on the MERGED stream (header + rows +
//     EOF) so it is a valid binary-COPY payload for the ContentDigestWriter.

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// miniTeeWriter writes p to two writers (the digest accumulator + the COPY pipe).
// The digest writer never blocks (O(1) memory); the COPY pipe's back-pressure
// is preserved (the second write blocks until the COPY reads).
type miniTeeWriter struct {
	a, b io.Writer
}

func (t *miniTeeWriter) Write(p []byte) (int, error) {
	if _, err := t.a.Write(p); err != nil {
		return 0, err
	}
	return t.b.Write(p)
}

// serveMiniUnitRouted serves ONE ROUTED (S≠D) unit on a mini-agent (P4). It
// accepts the S×W compressed sub-stream connections on its data listener,
// merges them into one binary-COPY stream (one SliceStripper per sub-stream,
// all writing the single utility-COPY pipe under one lock), tees the
// destination digest over the merged stream, takes srcContrib as the XOR over
// the S×W sub-stream trailers, and returns the report. It assumes the staging
// slice already exists (the supervisor created/truncated it on the
// coordinator); the mini-agent only performs the utility COPY.
func serveMiniUnitRouted(ctx context.Context, o MiniOptions, l net.Listener, req MiniUnitReq) MiniUnitResp {
	resp := MiniUnitResp{Op: "mini-unit-resp", ReqID: req.ReqID, UnitID: req.UnitID}
	fail := func(format string, a ...any) MiniUnitResp {
		resp.Ok = false
		resp.Error = fmt.Sprintf(format, a...)
		return resp
	}
	S := int(req.SourceSegments)
	W := req.Workers
	if S < 1 {
		S = 1
	}
	if W < 1 {
		W = 1
	}
	nTotal := S * W
	mode, _, perr := compress.Parse(req.Compress)
	if perr != nil {
		return fail("compress %q: %v", req.Compress, perr)
	}
	sto := o.StreamTimeout
	sctx, scancel := context.WithTimeout(ctx, sto)
	defer scancel()

	// 1. Single utility-COPY pipe + COPY goroutine (staging slice exists — the
	// supervisor created it on the coordinator; the mini only writes it). The
	// destination digest tees the MERGED stream (header + rows + EOF) into a
	// ContentDigestWriter, so the per-row XOR accumulator is computed over the
	// exact bytes the utility COPY receives (row-order-independent).
	pr, pw := io.Pipe()
	// The destination digest is ALWAYS computed (no verify gate): the
	// destination verifies on the data path without a re-read.
	var dsum = transfer.NewContentDigest()
	var mergedW io.Writer = pw
	if dsum != nil {
		mergedW = &miniTeeWriter{a: dsum, b: pw}
	}
	mw := &mutexWriter{mu: &sync.Mutex{}, w: mergedW}
	copyRes := make(chan segCopyRes, 1)
	go func() {
		defer pr.Close() // unblock the sub-stream drains on exit
		res := segCopyRes{seg: 0}
		func() {
			defer func() {
				if p := recover(); p != nil {
					res.err = fmt.Errorf("mini utility copy panic: %v", p)
				}
			}()
			c, cerr := pg.NewConn(sctx, o.PrimaryDSN, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
			if cerr != nil {
				res.err = fmt.Errorf("connect segment (utility): %w", cerr)
				return
			}
			defer c.Close()
			sql := fmt.Sprintf(`COPY %s.%s FROM STDIN (FORMAT binary)`,
				quoteIdentAgent(req.Schema), quoteIdentAgent(req.Staging))
			n, werr := c.WriteCopy(sctx, pr, sql)
			if werr != nil {
				res.err = fmt.Errorf("utility COPY %s.%s: %w", req.Schema, req.Staging, werr)
				return
			}
			res.rows = n
		}()
		copyRes <- res
	}()
	// Close the pipe on every exit path after the sub-streams are drained.
	closePipe := func(err error) {
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
	}

	// 2. The merged binary-COPY header (the COPY goroutine is already reading).
	if err := segstream.WriteBinaryHeader(mw); err != nil {
		closePipe(err)
		<-copyRes
		return fail("merged header: %v", err)
	}

	// 3. Accept the S×W sub-stream connections (bounded by the stream timeout).
	type subOut struct {
		res  *segstream.SliceResult
		wire uint64
		err  error
	}
	results := make(chan subOut, nTotal)
	acceptDone := make(chan error, 1)
	var logicalN atomic.Uint64
	// L2 accept-gap: each sub-stream connection must arrive within the
	// accept timeout (a segreceiver that never dials must not hold the unit
	// open for the whole 6-hour stream timeout). The budget is PER ACCEPT
	// (a slow start of N connections does not consume the later ones).
	acceptTO := o.AcceptTimeout
	go func() {
		for i := 0; i < nTotal; i++ {
			acceptCtx, acceptCancel := context.WithTimeout(ctx, acceptTO)
			acc := make(chan net.Conn, 1)
			accErr := make(chan error, 1)
			go func() {
				c, err := l.Accept()
				if err != nil {
					accErr <- err
					return
				}
				acc <- c
			}()
			var c net.Conn
			select {
			case c = <-acc:
				acceptCancel()
			case e := <-accErr:
				acceptCancel()
				acceptDone <- e
				return
			case <-acceptCtx.Done():
				acceptCancel()
				// Do not steal the connection: if the late accept lands
				// (a segreceiver dialing this timed-out unit), close it so
				// the sender's stream fails fast and the NEXT unit's Accept
				// is not consumed by this unit's orphaned accept goroutine.
				go func() {
					select {
					case c := <-acc:
						_ = c.Close()
					case <-ctx.Done():
					}
				}()
				acceptDone <- fmt.Errorf("accept sub-stream %d: no connection within %v (segreceiver did not dial): %w", i, acceptTO, acceptCtx.Err())
				return
			}
			cc := &countingConn{c: c}
			var slicePipe transfer.Pipe = cc
			if o.FrameAck {
				if rp, rerr := wrapSliceConn(o, l, c); rerr == nil {
					slicePipe = rp
				} else {
					acceptDone <- fmt.Errorf("frame-ack transport: %w", rerr)
					return
				}
			}
			if o.DataIdleTimeout > 0 {
				// L2 data-idle watchdog: per sub-stream (one dead source
				// segment's sub-stream must fail the unit even while the
				// others still flow). Frame-ack: ReliablePipe.Close (markDead:
				// blocked Read EOF, no re-accept); raw: close the connection.
				idleClose := func() error { return c.Close() }
				if rp, ok := slicePipe.(*transfer.ReliablePipe); ok {
					idleClose = rp.Close
				}
				slicePipe = newIdlePipe(slicePipe, idleClose, o.DataIdleTimeout)
			}
			res := &segstream.SliceResult{}
			go func(cc *countingConn, pipe transfer.Pipe, res *segstream.SliceResult) {
				defer cc.c.Close()
				// One SliceStripper per sub-stream: it strips the
				// sub-stream's own header/EOF and forwards complete rows to
				// the merged pipe (a single locked Write per row, so
				// concurrent sub-streams cannot interleave inside a row).
				var (
					stripOnce sync.Once
					strip     *segstream.SliceStripper
				)
				writer := func(contentID int16, p []byte) error {
					logicalN.Add(uint64(len(p)))
					stripOnce.Do(func() { strip = segstream.NewSliceStripper(mw) })
					return strip.Write(p)
				}
				derr := segstream.ReadStreamCompressed(sctx, pipe, res, mode, writer)
				results <- subOut{res: res, wire: cc.Bytes(), err: derr}
			}(cc, slicePipe, res)
		}
		close(acceptDone)
	}()

	var accErr error
	select {
	case accErr = <-acceptDone:
		if accErr != nil {
			closePipe(accErr)
			<-copyRes
			return fail("accept sub-stream: %v", accErr)
		}
	case <-sctx.Done():
		closePipe(sctx.Err())
		<-copyRes
		return fail("accept sub-streams: %v", sctx.Err())
	}

	// 4. Drain all S×W sub-streams, XORing their trailers' per-row
	// accumulators (the source's contribution for this destination segment =
	// XOR over the S×W sub-streams; XOR is commutative/associative).
	var (
		srcAcc   [32]byte
		trailerN int64
		wire     uint64
		firstErr error
	)
	for i := 0; i < nTotal; i++ {
		sub := <-results
		wire += sub.wire
		if sub.err != nil && firstErr == nil {
			firstErr = sub.err
		}
		if sub.res.Trailer != nil && sub.res.Err == nil {
			trailerN += sub.res.Rows
			for j := 0; j < 32; j++ {
				srcAcc[j] ^= sub.res.Digest[j]
			}
		}
	}
	if firstErr != nil {
		closePipe(firstErr)
		<-copyRes
		return fail("sub-stream: %v", firstErr)
	}

	// 5. End the merged stream (EOF marker), wait for the utility COPY.
	if err := segstream.WriteBinaryEOF(mw); err != nil {
		closePipe(err)
		<-copyRes
		return fail("merged EOF: %v", err)
	}
	closePipe(nil)
	cres := <-copyRes
	if cres.err != nil {
		return fail("utility copy: %v", cres.err)
	}
	if trailerN != cres.rows {
		return fail("trailer rows %d != dest rows %d", trailerN, cres.rows)
	}

	resp.Wire = wire
	resp.Logical = logicalN.Load()
	resp.TrailerRows = trailerN
	resp.Rows = cres.rows
	resp.SrcContrib = hexBytes(srcAcc)
	// dst_acc = the destination's per-row XOR accumulator over the merged
	// stream (row-order-independent: the merge order is immaterial). ALWAYS
	// computed (no verify gate).
	acc32, accRows, aerr := dsum.Accumulator()
	if aerr != nil {
		return fail("destination digest: %v", aerr)
	}
	if accRows != cres.rows {
		return fail("digest rows %d != dest rows %d", accRows, cres.rows)
	}
	resp.DstAcc = hexBytes(acc32)
	resp.Ok = true
	return resp
}
