package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// SegmentsOptions configures one destination-agent serve of an ON-SEGMENT unit
// (P4). The GPDB kernel launches gpstream-segreceiver on every primary source
// segment; each segreceiver streams its slice COMPRESSED to this destination
// agent over its own TCP connection. The agent accepts exactly o.Segments
// connections, deframes + decompresses each slice, MERGES them into one
// binary-COPY stream (SliceMux, moved from the single-controller manager into
// the dest agent by the ADR-002 plan) and loads it into the staging table.
//
// The unit shape (schema/staging/columns/distribution/partition + the
// compression mode) is passed by the controller at launch (the segreceivers
// are kernel-launched and cannot carry it), so the agent builds the staging
// table itself — exactly as the P3 coordinator-mode dest agent does.
type SegmentsOptions struct {
	// Listen is the address the source segments' segreceivers connect to.
	Listen string
	// DestConn is the destination postgres conn string.
	DestConn string
	// PG-plane TLS (the agent is a client of the destination DB).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Data-plane TLS for the segment<->agent sockets (the dest agent = server).
	DataTLSCert, DataTLSKey, DataTLSCA, DataTLSMinTLS string
	// Spec is the unit's destination shape (the controller builds it from the
	// plan: destination schema/table/staging, the source columns +
	// distribution + partition, and the FR-12 compress mode string).
	Spec UnitSpec
	// Segments is the number of source primary segments (one connection each).
	Segments int
	// Swap makes the agent perform the atomic swap after a successful load
	// (a controller decision; the runner passes it per unit).
	Swap bool
	// AcceptTimeout bounds the wait for the first segment connection;
	// StreamTimeout bounds the whole serve.
	AcceptTimeout, StreamTimeout time.Duration
	// SharedListen, when non-nil, is the agent long-lived data listener
	// (design 04.10: one data port per agent; units multiplex by unit_id).
	// The serve accepts its connections from it and does NOT close it on
	// exit (the long-lived server owns it).
	SharedListen net.Listener
	// FrameAck enables the frame-level ACK + RST-resume transport on the
	// segment<->agent data links (DESIGN §4.6, Inc3): each slice is its own
	// ReliablePipe, a broken slice's re-dial is routed to its own pipe by
	// the ResumeRouter (identity HELLO), and the destination's merged COPY
	// keeps reading the in-order stream across the break (no re-COPY).
	// Requires ReliablePipe for the transport parameters.
	FrameAck     bool
	ReliablePipe transfer.ReliablePipeConfig
}

// SegmentsResult is what the destination agent reports after an on_segment
// serve (FR-04 volume + the authoritative row count the destination wrote).
type SegmentsResult struct {
	Spec UnitSpec
	// Rows is the row count the destination wrote (COPY command tag).
	Rows int64
	// Wire is the number of RAW bytes read from the network (frames +
	// compressed payloads, all N segment connections) — the bytes the
	// segment<->agent links actually carried (ADR-002 acceptance #1).
	Wire uint64
	// Logical is the decompressed byte volume merged into the COPY.
	Logical uint64
	// TrailerRows is the sum of the per-slice trailer row counts (the
	// cross-check against Rows).
	TrailerRows int64
	// Swapped reports whether the atomic rename was performed.
	Swapped bool
}

// countingConn wraps a net.Conn and counts every byte read from the network
// (the on-wire volume, ADR-002 acceptance #1: gpstream_bytes_total must equal
// the bytes measured on the network). It satisfies transfer.Pipe.
type countingConn struct {
	c net.Conn
	n atomic.Uint64
}

func (cc *countingConn) Read(p []byte) (int, error) {
	n, err := cc.c.Read(p)
	if n > 0 {
		cc.n.Add(uint64(n))
	}
	return n, err
}
func (cc *countingConn) Write(p []byte) (int, error) { return cc.c.Write(p) }
func (cc *countingConn) Close() error                { return cc.c.Close() }
func (cc *countingConn) Bytes() uint64               { return cc.n.Load() }

// logicalTee writes to w and counts the bytes (the decompressed logical
// volume, FR-04).
type logicalTee struct {
	w io.Writer
	n *atomic.Uint64
}

func (lt *logicalTee) Write(p []byte) (int, error) {
	lt.n.Add(uint64(len(p)))
	return lt.w.Write(p)
}

// ServeSegments serves one on_segment unit: it builds the staging table,
// accepts the N segment slice streams (compressed), merges them (SliceMux)
// into one binary-COPY stream and loads it into the staging table. It returns
// the authoritative destination row count + the on-network wire volume + the
// logical (decompressed) volume.
func ServeSegments(ctx context.Context, o SegmentsOptions) (SegmentsResult, error) {
	var out SegmentsResult
	if o.AcceptTimeout <= 0 {
		o.AcceptTimeout = 5 * time.Minute
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 6 * time.Hour
	}
	if o.Segments < 1 {
		return out, fmt.Errorf("dest agent: segments %d (want >= 1)", o.Segments)
	}

	// 1. Destination pg connection.
	dq, err := pg.NewConn(ctx, o.DestConn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
	if err != nil {
		return out, fmt.Errorf("dest agent: connect dest: %w", err)
	}
	defer dq.Close()
	da := dest.New(dq)

	sctx, scancel := context.WithTimeout(ctx, o.StreamTimeout)
	defer scancel()

	// 2. Build the staging table (EnsureSchema + create; a surviving staging
	// from a crashed run is emptied first — a killed COPY FROM leaves partial
	// rows). Mirrors Serve (P3) and the single-controller manager.
	spec := o.Spec
	if err := da.EnsureSchema(sctx, spec.Schema); err != nil {
		return out, fmt.Errorf("dest agent: ensure schema %s: %w", spec.Schema, err)
	}
	if da.Exists(sctx, spec.Schema, spec.Staging) {
		if terr := da.Truncate(sctx, spec.Schema, spec.Staging); terr != nil {
			return out, fmt.Errorf("dest agent: truncate surviving staging %s.%s: %w", spec.Schema, spec.Staging, terr)
		}
	}
	if err := da.CreateStaging(sctx, spec.Schema, spec.Cols, spec.Staging, spec.Dist, spec.Part); err != nil {
		return out, fmt.Errorf("dest agent: create staging %s.%s: %w", spec.Schema, spec.Staging, err)
	}
	out.Spec = spec

	// 3. Compression mode (the controller told both sides; a mismatch on a
	// slice's trailer is an error, not a silent mis-decode).
	mode, _, perr := compress.Parse(spec.Compress)
	if perr != nil {
		return out, fmt.Errorf("dest agent: compress %q: %w", spec.Compress, perr)
	}

	// 4. Listen for the N segment connections (optionally a TLS server).
	var rawL net.Listener
	if o.SharedListen != nil {
		rawL = o.SharedListen // long-lived: the server closes it, not the serve
	} else {
		rawL, err = net.Listen("tcp", o.Listen)
		if err != nil {
			return out, fmt.Errorf("dest agent: listen %s: %w", o.Listen, err)
		}
		defer rawL.Close()
	}
	l := net.Listener(rawL)
	if transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
		scfg, serr := transfer.ServerTLSConfig(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
		if serr != nil {
			return out, fmt.Errorf("dest agent: tls config: %w", serr)
		}
		// A refused/noisy handshake must not abort the in-flight unit: a
		// stray connection is dropped and does not count toward the N.
		l = segstream.HandshakeFilter(tls.NewListener(rawL, scfg))
	}

	// 5. Merged stream: SliceMux (N slices -> one binary-COPY stream) feeds an
	// io.Pipe consumed by the COPY FROM. Back-pressure: a full pipe blocks the
	// slice drains, bounding memory (FR-01, no disk in the middle). The COPY
	// goroutine MUST start before mux.Begin(): the pipe is unbuffered, so
	// Begin's header write would block until the COPY is reading it.
	pr, pw := io.Pipe()
	// logicalN accumulates the decompressed (logical) byte volume as the mux
	// feeds the COPY (FR-04 in/out). It is written by the single mux writer
	// goroutine and read once after the join, so a local (not an embedded
	// field) keeps the result value copy-safe.
	var logicalN atomic.Uint64
	mux := segstream.NewSliceMux(&logicalTee{w: pw, n: &logicalN}, o.Segments)

	copyDone := make(chan struct{}, 1)
	var copyRows int64
	var copyErr error
	go func() {
		defer close(copyDone)
		rows, cerr := da.CopyIntoStream(sctx, spec.Schema, spec.Staging, pr)
		copyRows = rows
		copyErr = cerr
		if cerr != nil {
			// Unblock the slice drains (a full pipe would hang them, FR-06).
			_ = pw.CloseWithError(cerr)
		}
	}()
	if err := mux.Begin(); err != nil {
		_ = pw.CloseWithError(err)
		<-copyDone
		return out, fmt.Errorf("dest agent: mux begin: %w", err)
	}

	// 6. Accept the N segment connections and drain each (compressed) slice
	// into the mux. Each drain is bounded by sctx; a segment that never
	// connects is caught by the stream deadline (the controller kills the
	// agent).
	var trailerRows int64
	var firstErr error
	var wire uint64
	var accErr error
	var faWire uint64
	if o.FrameAck {
		// Inc3: each slice is its own ReliablePipe; the ResumeRouter routes
		// the initial + resume connections (identity HELLO) to their own
		// pipes, so a broken slice's re-dial reaches its own pipe and the
		// merged COPY keeps reading the in-order stream.
		results := make(chan frameAckSliceOut, o.Segments)
		accErr, faWire = serveSegmentsFrameAck(sctx, o, l, mux, mode, results)
		if accErr == nil {
			for i := 0; i < o.Segments; i++ {
				s := <-results
				if s.err != nil && firstErr == nil {
					firstErr = s.err
				}
				if s.res.Trailer != nil && s.res.Err == nil {
					trailerRows += s.res.Rows
				}
			}
			// The frame-ack path carries the bytes inside the ReliablePipe
			// transport: the router's raw-network counter is the on-wire
			// volume (FR-04 / ADR-002 acceptance #1).
			wire = faWire
		}
	} else {
		type sliceOut struct {
			res  *segstream.SliceResult
			wire uint64
			err  error
		}
		results := make(chan sliceOut, o.Segments)
		acceptDone := make(chan error, 1)
		go func() {
			for i := 0; i < o.Segments; i++ {
				c, aerr := l.Accept()
				if aerr != nil {
					acceptDone <- aerr
					return
				}
				cc := &countingConn{c: c}
				res := &segstream.SliceResult{}
				// Drain this slice in its own goroutine so all N flow in
				// parallel (the mux serialises writes; the io.Pipe serialises
				// the COPY).
				go func(cc *countingConn, res *segstream.SliceResult) {
					derr := segstream.ReadStreamCompressed(sctx, cc, res, mode, mux.Write)
					results <- sliceOut{res: res, wire: cc.Bytes(), err: derr}
				}(cc, res)
			}
			close(acceptDone)
		}()

		// Wait for either an accept failure or all N accepted.
		select {
		case accErr = <-acceptDone:
			if accErr != nil {
				// An accept error before all N were accepted: fail the unit.
				_ = pw.CloseWithError(fmt.Errorf("accept: %w", accErr))
				<-copyDone
				return out, fmt.Errorf("dest agent: accept segment connection: %w", accErr)
			}
			// All N accepted: fall through to collect the drains.
		case <-sctx.Done():
			_ = pw.CloseWithError(sctx.Err())
			<-copyDone
			return out, fmt.Errorf("dest agent: accept segments: %w", sctx.Err())
		}
		for i := 0; i < o.Segments; i++ {
			s := <-results
			wire += s.wire
			if s.err != nil && firstErr == nil {
				firstErr = s.err
			}
			if s.res.Trailer != nil && s.res.Err == nil {
				trailerRows += s.res.Rows
			}
		}
	}
	if accErr != nil {
		_ = pw.CloseWithError(fmt.Errorf("accept: %w", accErr))
		<-copyDone
		return out, fmt.Errorf("dest agent: accept segment connection: %w", accErr)
	}

	// 7. All slices done: close the merged stream (mux EOF) so the COPY FROM
	// sees end-of-stream, then wait for the COPY to finish.
	if err := mux.End(); err != nil && firstErr == nil {
		firstErr = err
	}
	_ = pw.Close() // signal end-of-stream to the COPY FROM (idempotent)
	<-copyDone

	out.Wire = wire
	out.Logical = logicalN.Load()
	out.TrailerRows = trailerRows
	if firstErr != nil {
		return out, fmt.Errorf("dest agent: segment slices: %w", firstErr)
	}
	if copyErr != nil {
		return out, fmt.Errorf("dest agent: copy into %s.%s: %w", spec.Schema, spec.Staging, copyErr)
	}
	if trailerRows != copyRows {
		return out, fmt.Errorf("dest agent: trailer rows %d != dest rows %d", trailerRows, copyRows)
	}
	out.Rows = copyRows

	// 8. The swap is a controller decision (the runner passes o.Swap per unit).
	if o.Swap {
		if werr := da.Swap(sctx, spec.Schema, spec.Table, spec.Staging, spec.Overwrite); werr != nil {
			return out, fmt.Errorf("dest agent: swap %s.%s: %w", spec.Schema, spec.Table, werr)
		}
		out.Swapped = true
	}
	return out, nil
}

// frameAckSliceOut is one slice's result on the frame-ack path (the router
// counts the raw network bytes, so wire is always 0 here).
type frameAckSliceOut struct {
	res  *segstream.SliceResult
	wire uint64
	err  error
}

// serveSegmentsFrameAck runs the Inc3 frame-ack data path for one on_segment
// unit: each of the S slices is its own ReliablePipe, and a ResumeRouter over
// the unit's data listener routes the initial + resume connections (identity
// HELLO) to their own pipes. The destination does NOT know the source segment
// content ids (the kernel substitutes <SEGID>), so it pre-creates S pool slots
// and the router binds a StreamID to a free slot on first sight of that
// StreamID's connection. A broken slice's re-dial reaches its own pipe (not a
// sibling's), and the merged COPY keeps reading the in-order stream (no
// re-COPY, no re-UnitSpec). It returns the raw network bytes the router read
// (FR-04 wire volume) and an error if a slice's pipe fails to establish or
// the stream deadline elapses.
func serveSegmentsFrameAck(sctx context.Context, o SegmentsOptions, l net.Listener, mux *segstream.SliceMux, mode compress.Mode, results chan frameAckSliceOut) (error, uint64) {
	router := transfer.NewResumeRouter(l)
	go router.Run()
	// The per-unit listener is closed by the router; the long-lived listener
	// (SharedListen) is owned by the dest server, not the serve.
	closer := router.Stop
	if o.SharedListen == nil {
		closer = router.Close
	}
	defer closer()

	acceptTO := o.AcceptTimeout
	if acceptTO <= 0 {
		acceptTO = 5 * time.Minute
	}
	finished := make(chan struct{}, o.Segments)
	for i := 0; i < o.Segments; i++ {
		slot := router.NewPoolSlot()
		go func(slot <-chan transfer.DupConn) {
			res := &segstream.SliceResult{}
			rp, err := transfer.NewReliablePipeReceiverOnSlot(o.ReliablePipe, slot, acceptTO)
			if err != nil {
				res.Err = err
				results <- frameAckSliceOut{res: res, wire: 0, err: err}
				finished <- struct{}{}
				return
			}
			derr := segstream.ReadStreamCompressed(sctx, rp, res, mode, mux.Write)
			results <- frameAckSliceOut{res: res, wire: 0, err: derr}
			finished <- struct{}{}
		}(slot)
	}
	for i := 0; i < o.Segments; i++ {
		select {
		case <-finished:
		case <-sctx.Done():
			return sctx.Err(), 0
		}
	}
	return nil, router.WireBytes()
}
