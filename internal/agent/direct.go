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

// DirectOptions configures one destination-agent serve of a DIRECT unit (P5,
// R5 through the agents). The source agent routes each row to one of D
// destination segments and streams D per-segment COMPRESSED slices (one
// content-tagged connection each). The destination agent builds the staging
// table (on the coordinator conn), opens D utility-mode per-segment COPYs, and
// accepts the D slice connections, routing each by its content-id to the
// matching segment's COPY. The destination coordinator is out of the write
// path (each row is written directly to its segment), so the copy is bounded by
// the segment hosts, not the coordinator — the R5 endgame.
type DirectOptions struct {
	// Listen is the address the source agent connects to (the source dials it
	// D times, once per destination segment).
	Listen string
	// DestConn is the destination COORDINATOR conn string, used for the staging
	// DDL (CREATE TABLE, replicated to the segments) and the swap. The per-
	// segment data COPYs use Launch.DstDSNs instead.
	DestConn string
	// PG-plane TLS (the agent is a client of the destination DB).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Data-plane TLS for the agent<->agent socket (the dest agent = server).
	DataTLSCert, DataTLSKey, DataTLSCA, DataTLSMinTLS string
	// Launch is the direct-mode launch payload (staging shape + DstDSNs + D).
	Launch DirectLaunch
	// Swap makes the agent perform the atomic swap after a successful load
	// (a controller decision; the runner passes it per unit).
	Swap bool
	// AcceptTimeout bounds the wait for the first slice connection;
	// StreamTimeout bounds the whole serve.
	AcceptTimeout, StreamTimeout time.Duration
	// SharedListen, when non-nil, is the agent long-lived data listener
	// (design 04.10: one data port per agent; units multiplex by unit_id).
	// The serve accepts its connections from it and does NOT close it on
	// exit (the long-lived server owns it).
	SharedListen net.Listener
}

// DirectResult is what the destination agent reports after a direct serve (the
// authoritative row count the destination wrote + the on-network wire volume +
// the decompressed logical volume + the per-segment distribution).
type DirectResult struct {
	Launch DirectLaunch
	// Rows is the row count the destination wrote (sum of the D per-segment
	// COPY command tags) — the authoritative count.
	Rows int64
	// Wire is the raw byte volume read from the D agent<->agent connections
	// (frames + compressed payloads) — the bytes the network carried
	// (ADR-002 acceptance #1).
	Wire uint64
	// Logical is the decompressed byte volume served into the COPYs.
	Logical uint64
	// TrailerRows is the sum of the per-slice trailer row counts (the
	// cross-check against Rows).
	TrailerRows int64
	// PerSeg is the per-segment row count the destination wrote (content
	// 0..D-1) — the F-15 balance cross-check.
	PerSeg []int64
	// Swapped reports whether the atomic rename was performed.
	Swapped bool
}

// directSliceWriter routes each decompressed chunk to the pipe of the segment
// its frame's content-id names, and counts the decompressed (logical) volume.
type directSliceWriter struct {
	pws     []*io.PipeWriter
	logical *atomic.Uint64
}

func (d *directSliceWriter) Write(contentID int16, p []byte) error {
	if int(contentID) < 0 || int(contentID) >= len(d.pws) {
		return fmt.Errorf("direct: slice content %d out of range (0..%d)", contentID, len(d.pws)-1)
	}
	d.logical.Add(uint64(len(p)))
	_, err := d.pws[contentID].Write(p)
	return err
}

// segCopyRes is the result of one per-segment utility-mode COPY.
type segCopyRes struct {
	seg  int
	rows int64
	err  error
}

// ServeDirect serves one direct unit: it builds the staging table, opens D
// utility-mode per-segment COPYs, accepts the D compressed slice connections
// (routing each by content-id to its segment), and loads them. It returns the
// authoritative destination row count + the on-network wire volume + the
// logical (decompressed) volume + the per-segment distribution.
func ServeDirect(ctx context.Context, o DirectOptions) (DirectResult, error) {
	var out DirectResult
	if o.AcceptTimeout <= 0 {
		o.AcceptTimeout = 5 * time.Minute
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 6 * time.Hour
	}
	D := int(o.Launch.D)
	if D < 1 {
		return out, fmt.Errorf("direct dest: D %d (want >= 1)", o.Launch.D)
	}
	if len(o.Launch.DstDSNs) != D {
		return out, fmt.Errorf("direct dest: %d DstDSNs, want D=%d", len(o.Launch.DstDSNs), D)
	}
	mode, _, perr := compress.Parse(o.Launch.Compress)
	if perr != nil {
		return out, fmt.Errorf("direct dest: compress %q: %w", o.Launch.Compress, perr)
	}
	sctx, scancel := context.WithTimeout(ctx, o.StreamTimeout)
	defer scancel()

	// 1. Coordinator conn: build the staging table (DDL replicated to the
	// segments) + hold it for the swap. Mirrors Serve/ServeSegments.
	dq, err := pg.NewConn(ctx, o.DestConn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
	if err != nil {
		return out, fmt.Errorf("direct dest: connect coordinator: %w", err)
	}
	defer dq.Close()
	da := dest.New(dq)
	spec := o.Launch
	if err := da.EnsureSchema(sctx, spec.Schema); err != nil {
		return out, fmt.Errorf("direct dest: ensure schema %s: %w", spec.Schema, err)
	}
	if da.Exists(sctx, spec.Schema, spec.Staging) {
		if terr := da.Truncate(sctx, spec.Schema, spec.Staging); terr != nil {
			return out, fmt.Errorf("direct dest: truncate surviving staging %s.%s: %w", spec.Schema, spec.Staging, terr)
		}
	}
	if err := da.CreateStaging(sctx, spec.Schema, spec.Cols, spec.Staging, spec.Dist, spec.Part); err != nil {
		return out, fmt.Errorf("direct dest: create staging %s.%s: %w", spec.Schema, spec.Staging, err)
	}
	out.Launch = spec

	// 2. Per-segment pipes + utility-mode COPY sessions. Each segment i
	// receives its well-formed binary-COPY stream (header + its rows + trailer)
	// over its pipe; the utility `COPY FROM STDIN` on that segment reads the
	// pipe's read end. The COPY goroutines start BEFORE any slice is written
	// (an unbuffered pipe write would otherwise block until a COPY reads).
	pws := make([]*io.PipeWriter, D)
	copyRes := make(chan segCopyRes, D)
	for i := 0; i < D; i++ {
		pr, pw := io.Pipe()
		pws[i] = pw
		dsn := o.Launch.DstDSNs[i]
		go func(i int, dsn string, r io.ReadCloser) {
			defer r.Close() // unblock the slice drain on exit
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

	// 3. Listen for the D slice connections (optionally a TLS server). The
	// source dials this address once per destination segment.
	var rawL net.Listener
	var lerr error
	if o.SharedListen != nil {
		rawL = o.SharedListen // long-lived: the server closes it, not the serve
	} else {
		rawL, lerr = net.Listen("tcp", o.Listen)
		if lerr != nil {
			_ = closePipesErr(pws, fmt.Errorf("listen: %w", lerr))
			_, _, _ = collectDirectCopies(copyRes, D)
			return out, fmt.Errorf("direct dest: listen %s: %w", o.Listen, lerr)
		}
		defer rawL.Close()
	}
	l := net.Listener(rawL)
	if transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
		scfg, serr := transfer.ServerTLSConfig(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
		if serr != nil {
			_ = closePipesErr(pws, serr)
			_, _, _ = collectDirectCopies(copyRes, D)
			return out, fmt.Errorf("direct dest: tls config: %w", serr)
		}
		l = tls.NewListener(rawL, scfg)
	}

	// 4. Accept the D slice connections and drain each (compressed) slice into
	// its segment's pipe (routed by content-id). Each drain is bounded by sctx.
	var logicalN atomic.Uint64
	writer := &directSliceWriter{pws: pws, logical: &logicalN}
	type sliceOut struct {
		res  *segstream.SliceResult
		wire uint64
		err  error
	}
	results := make(chan sliceOut, D)
	acceptDone := make(chan error, 1)
	go func() {
		for i := 0; i < D; i++ {
			c, aerr := l.Accept()
			if aerr != nil {
				acceptDone <- aerr
				return
			}
			cc := &countingConn{c: c}
			res := &segstream.SliceResult{}
			go func(cc *countingConn, res *segstream.SliceResult) {
				derr := segstream.ReadStreamCompressed(sctx, cc, res, mode, writer.Write)
				results <- sliceOut{res: res, wire: cc.Bytes(), err: derr}
			}(cc, res)
		}
		close(acceptDone)
	}()

	// Wait for either an accept failure or all D accepted.
	var accErr error
	select {
	case accErr = <-acceptDone:
		if accErr != nil {
			_ = closePipesErr(pws, fmt.Errorf("accept: %w", accErr))
			_, _, _ = collectDirectCopies(copyRes, D)
			return out, fmt.Errorf("direct dest: accept slice connection: %w", accErr)
		}
	case <-sctx.Done():
		_ = closePipesErr(pws, sctx.Err())
		_, _, _ = collectDirectCopies(copyRes, D)
		return out, fmt.Errorf("direct dest: accept slices: %w", sctx.Err())
	}

	// 5. Collect the D drains (exactly-once).
	var (
		trailerRows int64
		firstErr    error
		wire        uint64
	)
	for i := 0; i < D; i++ {
		s := <-results
		wire += s.wire
		if s.err != nil && firstErr == nil {
			firstErr = s.err
		}
		if s.res.Trailer != nil && s.res.Err == nil {
			trailerRows += s.res.Rows
		}
	}

	// 6. Close the per-segment pipes so each COPY sees end-of-stream (or an
	// error), then wait for the D COPYs to finish and sum their row counts.
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
		return out, fmt.Errorf("direct dest: slice: %w", firstErr)
	}
	if cErr != nil {
		return out, fmt.Errorf("direct dest: segment copy: %w", cErr)
	}
	if trailerRows != n {
		return out, fmt.Errorf("direct dest: trailer rows %d != dest rows %d", trailerRows, n)
	}
	out.Rows = n

	// 7. The swap is a controller decision (the runner passes o.Swap per unit).
	if o.Swap {
		if werr := da.Swap(sctx, spec.Schema, spec.Table, spec.Staging, spec.Overwrite); werr != nil {
			return out, fmt.Errorf("direct dest: swap %s.%s: %w", spec.Schema, spec.Table, werr)
		}
		out.Swapped = true
	}
	return out, nil
}

// quoteIdentAgent quotes a SQL identifier for the destination agent's COPY DDL.
func quoteIdentAgent(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '"')
	return string(out)
}

// closePipesW closes each per-segment pipe write-end cleanly (EOF for the COPY).
func closePipesW(pws []*io.PipeWriter) error {
	var first error
	for _, pw := range pws {
		if pw != nil {
			if err := pw.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

// closePipesErr closes each pipe write-end with err (unblocks a stuck COPY with
// an error rather than EOF).
func closePipesErr(pws []*io.PipeWriter, err error) error {
	var first error
	for _, pw := range pws {
		if pw != nil {
			if cerr := pw.CloseWithError(err); cerr != nil && first == nil {
				first = cerr
			}
		}
	}
	return first
}

// collectDirectCopies waits for the D per-segment COPYs to finish (exactly-once)
// and returns their summed row count, the per-segment distribution, and the
// first error (if any). The channel is buffered to D, so every goroutine can
// post its result without blocking even after a failure.
func collectDirectCopies(copyRes chan segCopyRes, D int) (int64, []int64, error) {
	perSeg := make([]int64, D)
	var (
		n     int64
		first error
	)
	for i := 0; i < D; i++ {
		r := <-copyRes
		if r.seg >= 0 && r.seg < D {
			perSeg[r.seg] = r.rows
		}
		n += r.rows
		if r.err != nil && first == nil {
			first = r.err
		}
	}
	return n, perSeg, first
}
