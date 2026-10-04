package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// RoutedLaunch is the P9 routed-mode launch payload the controller hands the
// destination agent (--routed-spec JSON). It extends DirectLaunch with the
// source segment count S and the per-pair sub-stream count W: the destination
// accepts S×D×W sub-stream connections (S source segments × D destination
// segments × W workers, each content-tagged with its DESTINATION segment id)
// and, per destination segment d, merges the S×W sub-streams addressed to it
// into one binary-COPY stream loaded by one utility-mode COPY FROM STDIN.
// Neither cluster coordinator is in the data path: the source coordinator
// only dispatched the ON SEGMENT read, the destination coordinator only built
// the staging DDL and (optionally) swaps it.
type RoutedLaunch struct {
	DirectLaunch
	// SourceSegments (S) — how many source segments stream.
	SourceSegments int32 `json:"source_segments"`
	// Workers (W) — sub-streams per (source segment, destination segment)
	// pair (the operator's --workers).
	Workers int `json:"workers"`
}

// RoutedOptions configures one destination-agent serve of a routed (S≠D)
// unit.
type RoutedOptions struct {
	// Listen is the address the source segreceivers dial (S×D×W times in
	// total).
	Listen string
	// DestConn is the destination COORDINATOR conn string (staging DDL +
	// swap). The per-segment data COPYs use Launch.DstDSNs.
	DestConn string
	// PG-plane TLS (the agent is a client of the destination DB).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Data-plane TLS for the segment<->agent sockets (the dest agent =
	// server).
	DataTLSCert, DataTLSKey, DataTLSCA, DataTLSMinTLS string
	// Launch is the routed-mode launch payload.
	Launch RoutedLaunch
	// Swap makes the agent perform the atomic swap after a successful load
	// (a controller decision; the runner passes it per unit).
	Swap bool
	// AcceptTimeout bounds the wait for the first sub-stream connection;
	// StreamTimeout bounds the whole serve.
	AcceptTimeout, StreamTimeout time.Duration
	// SharedListen, when non-nil, is the agent long-lived data listener
	// (design 04.10: one data port per agent; units multiplex by unit_id).
	// The serve accepts its connections from it and does NOT close it on
	// exit (the long-lived server owns it).
	SharedListen net.Listener
}

// RoutedResult is what the destination agent reports after a routed serve.
type RoutedResult struct {
	Launch RoutedLaunch
	// Rows is the row count the destination wrote (sum of the D per-segment
	// COPY command tags) — the authoritative count.
	Rows int64
	// Wire is the raw byte volume read from the S×D×W sub-stream connections
	// (frames + compressed payloads).
	Wire uint64
	// Logical is the decompressed byte volume served into the COPYs.
	Logical uint64
	// TrailerRows is the sum of the per-sub-stream trailer row counts (the
	// cross-check against Rows).
	TrailerRows int64
	// PerSeg is the per-destination-segment row count the destination wrote
	// (content 0..D-1) — the balance cross-check.
	PerSeg []int64
	// Swapped reports whether the atomic rename was performed.
	Swapped bool
}

// mutexWriter serialises writes from several goroutines to one underlying
// writer (an io.Pipe) so the per-segment merged stream stays well-formed:
// the 19-byte header, every sub-stream's rows, and the EOF marker are all
// single writes under the same lock, in a total order.
type mutexWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (m *mutexWriter) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.w.Write(p)
}

// ServeRouted serves one routed (S≠D) unit (P9): it builds the staging
// table, opens D utility-mode per-segment COPYs, accepts the S×D×W sub-stream
// connections (each content-tagged with its destination segment), and per
// destination segment merges the S×W sub-streams addressed to it (one
// SliceStripper per sub-stream, all writing the segment's pipe under one
// lock) into one binary-COPY stream. It returns the authoritative
// destination row count + the on-network wire volume + the per-segment
// distribution.
func ServeRouted(ctx context.Context, o RoutedOptions) (RoutedResult, error) {
	var out RoutedResult
	if o.AcceptTimeout <= 0 {
		o.AcceptTimeout = 5 * time.Minute
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 6 * time.Hour
	}
	S := int(o.Launch.SourceSegments)
	W := o.Launch.Workers
	D := int(o.Launch.D)
	if S < 1 || W < 1 || D < 1 {
		return out, fmt.Errorf("routed dest: S=%d W=%d D=%d (want >= 1)", S, W, D)
	}
	if len(o.Launch.DstDSNs) != D {
		return out, fmt.Errorf("routed dest: %d DstDSNs, want D=%d", len(o.Launch.DstDSNs), D)
	}
	nTotal := S * D * W
	mode, _, perr := compress.Parse(o.Launch.Compress)
	if perr != nil {
		return out, fmt.Errorf("routed dest: compress %q: %w", o.Launch.Compress, perr)
	}

	sctx, scancel := context.WithTimeout(ctx, o.StreamTimeout)
	defer scancel()

	// 1. Coordinator conn: build the staging table (DDL replicated to the
	// segments) + hold it for the swap. Mirrors ServeDirect.
	dq, err := pg.NewConn(ctx, o.DestConn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
	if err != nil {
		return out, fmt.Errorf("routed dest: connect coordinator: %w", err)
	}
	defer dq.Close()
	da := dest.New(dq)
	spec := o.Launch.DirectLaunch
	if err := da.EnsureSchema(sctx, spec.Schema); err != nil {
		return out, fmt.Errorf("routed dest: ensure schema %s: %w", spec.Schema, err)
	}
	if da.Exists(sctx, spec.Schema, spec.Staging) {
		if terr := da.Truncate(sctx, spec.Schema, spec.Staging); terr != nil {
			return out, fmt.Errorf("routed dest: truncate surviving staging %s.%s: %w", spec.Schema, spec.Staging, terr)
		}
	}
	if err := da.CreateStaging(sctx, spec.Schema, spec.Cols, spec.Staging, spec.Dist, spec.Part); err != nil {
		return out, fmt.Errorf("routed dest: create staging %s.%s: %w", spec.Schema, spec.Staging, err)
	}
	out.Launch = o.Launch

	// 2. Per-segment pipes + utility-mode COPY sessions. Each segment d
	// receives its merged binary-COPY stream (one header, all rows from the
	// S×W sub-streams addressed to it, one EOF) over its pipe; the utility
	// `COPY FROM STDIN` on that segment reads the pipe's read end. The COPY
	// goroutines start BEFORE any sub-stream is written (an unbuffered pipe
	// write would otherwise block until a COPY reads).
	type segPipe struct {
		pw *io.PipeWriter
		mw *mutexWriter
	}
	segs := make([]segPipe, D)
	copyRes := make(chan segCopyRes, D)
	pws := make([]*io.PipeWriter, D)
	for i := 0; i < D; i++ {
		pr, pw := io.Pipe()
		segs[i] = segPipe{pw: pw, mw: &mutexWriter{mu: &sync.Mutex{}, w: pw}}
		pws[i] = pw
		dsn := o.Launch.DstDSNs[i]
		go func(i int, dsn string, r io.ReadCloser) {
			defer r.Close() // unblock the sub-stream drains on exit
			res := segCopyRes{seg: i}
			func() {
				defer func() {
					if p := recover(); p != nil {
						res.err = fmt.Errorf("segment %d copy panic: %v", i, p)
					}
				}()
				c, cerr := pg.NewConn(sctx, dsn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
				if cerr != nil {
					res.err = fmt.Errorf("connect segment %d: %w", i, cerr)
					return
				}
				defer c.Close()
				sql := fmt.Sprintf(`COPY %s.%s FROM STDIN (FORMAT binary)`,
					quoteIdentAgent(spec.Schema), quoteIdentAgent(spec.Staging))
				n, werr := c.WriteCopy(sctx, r, sql)
				if werr != nil {
					res.err = fmt.Errorf("segment %d COPY: %w", i, werr)
					return
				}
				res.rows = n
			}()
			copyRes <- res
		}(i, dsn, pr)
	}

	// 3. Listen for the S×D×W sub-stream connections (optionally a TLS
	// server). Each source segreceiver dials this address D×W times.
	var rawL net.Listener
	var lerr error
	if o.SharedListen != nil {
		rawL = o.SharedListen // long-lived: the server closes it, not the serve
	} else {
		rawL, lerr = net.Listen("tcp", o.Listen)
		if lerr != nil {
			_ = closePipesErr(pws, fmt.Errorf("listen: %w", lerr))
			_, _, _ = collectDirectCopies(copyRes, D)
			return out, fmt.Errorf("routed dest: listen %s: %w", o.Listen, lerr)
		}
		defer rawL.Close()
	}
	l := net.Listener(rawL)
	if transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
		scfg, serr := transfer.ServerTLSConfig(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
		if serr != nil {
			_ = closePipesErr(pws, serr)
			_, _, _ = collectDirectCopies(copyRes, D)
			return out, fmt.Errorf("routed dest: tls config: %w", serr)
		}
		l = segstream.HandshakeFilter(tls.NewListener(rawL, scfg))
	}

	// 4. Write each segment's merged header (the COPY goroutine is already
	// reading, so the unbuffered pipe write completes).
	for d := 0; d < D; d++ {
		if err := segstream.WriteBinaryHeader(segs[d].mw); err != nil {
			_ = closePipesErr(pws, err)
			_, _, _ = collectDirectCopies(copyRes, D)
			return out, fmt.Errorf("routed dest: segment %d header: %w", d, err)
		}
	}

	// 5. Accept the S×D×W sub-stream connections and drain each (compressed)
	// sub-stream into its segment's merged stream, in parallel. One
	// SliceStripper per connection: it strips the sub-stream's own
	// header/EOF and forwards complete rows to the destination segment's
	// pipe (a single locked Write per row, so concurrent sub-streams cannot
	// interleave inside a row).
	type subOut struct {
		res  *segstream.SliceResult
		wire uint64
		err  error
	}
	var logicalN atomic.Uint64
	results := make(chan subOut, nTotal)
	acceptDone := make(chan error, 1)
	go func() {
		for i := 0; i < nTotal; i++ {
			c, aerr := l.Accept()
			if aerr != nil {
				acceptDone <- aerr
				return
			}
			cc := &countingConn{c: c}
			res := &segstream.SliceResult{}
			go func(cc *countingConn, res *segstream.SliceResult) {
				// The sub-stream's content id is its DESTINATION segment;
				// the (s,w) position is irrelevant to the load — every
				// sub-stream addressed to a segment feeds that segment's
				// merged stream. The stripper is assigned lazily on the
				// first data frame (a zero-row sub-stream sends only a
				// trailer and never allocates one).
				var (
					stripOnce sync.Once
					strip     *segstream.SliceStripper
				)
				writer := func(contentID int16, p []byte) error {
					d := int(contentID)
					if d < 0 || d >= D {
						return fmt.Errorf("routed dest: sub-stream content %d out of range (0..%d)", contentID, D-1)
					}
					stripOnce.Do(func() { strip = segstream.NewSliceStripper(segs[d].mw) })
					logicalN.Add(uint64(len(p)))
					return strip.Write(p)
				}
				derr := segstream.ReadStreamCompressed(sctx, cc, res, mode, writer)
				results <- subOut{res: res, wire: cc.Bytes(), err: derr}
			}(cc, res)
		}
		close(acceptDone)
	}()

	// Wait for either an accept failure or all S×D×W accepted.
	var accErr error
	select {
	case accErr = <-acceptDone:
		if accErr != nil {
			_ = closePipesErr(pws, fmt.Errorf("accept: %w", accErr))
			_, _, _ = collectDirectCopies(copyRes, D)
			return out, fmt.Errorf("routed dest: accept sub-stream: %w", accErr)
		}
	case <-sctx.Done():
		_ = closePipesErr(pws, sctx.Err())
		_, _, _ = collectDirectCopies(copyRes, D)
		return out, fmt.Errorf("routed dest: accept sub-streams: %w", sctx.Err())
	}

	// 6. Collect the S×D×W drains (exactly-once).
	var (
		trailerRows int64
		firstErr    error
		wire        uint64
	)
	for i := 0; i < nTotal; i++ {
		s := <-results
		wire += s.wire
		if s.err != nil && firstErr == nil {
			firstErr = s.err
		}
		if s.res.Trailer != nil && s.res.Err == nil {
			trailerRows += s.res.Rows
		}
	}

	// 7. Close each segment's merged stream (EOF marker), then the pipes,
	// and wait for the D COPYs to finish.
	for d := 0; d < D; d++ {
		if err := segstream.WriteBinaryEOF(segs[d].mw); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		_ = closePipesErr(pws, firstErr)
	} else {
		_ = closePipesW(pws)
	}
	n, perSeg, cErr := collectDirectCopies(copyRes, D)

	out.Wire = wire
	out.Logical = logicalN.Load()
	out.TrailerRows = trailerRows
	out.PerSeg = perSeg
	if firstErr != nil {
		return out, fmt.Errorf("routed dest: sub-stream: %w", firstErr)
	}
	if cErr != nil {
		return out, fmt.Errorf("routed dest: segment copy: %w", cErr)
	}
	if trailerRows != n {
		return out, fmt.Errorf("routed dest: trailer rows %d != dest rows %d", trailerRows, n)
	}
	out.Rows = n

	// 8. The swap is a controller decision (the runner passes o.Swap per
	// unit).
	if o.Swap {
		if werr := da.Swap(sctx, spec.Schema, spec.Table, spec.Staging, spec.Overwrite); werr != nil {
			return out, fmt.Errorf("routed dest: swap %s.%s: %w", spec.Schema, spec.Table, werr)
		}
		out.Swapped = true
	}
	return out, nil
}
