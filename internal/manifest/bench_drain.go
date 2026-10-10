package manifest

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// benchDrainAcceptTO bounds the wait for the source to dial the bench drain
// (the segreceivers dial right after the kernel launches them; a wedged
// dispatch must not hold the unit open).
const benchDrainAcceptTO = 60 * time.Second

// benchDrainAddr picks the drain listen + dial addresses. A LOCAL source agent
// (same host as the controller — the gp6lab / single-host layout) keeps the
// legacy loopback prebind. A REMOTE source agent (the multi-host GP6 layout)
// must dial an address routable from the source (segment) hosts: the
// controller's primary non-loopback IPv4 + a free port, bound on 0.0.0.0.
func (r *processTwoAgentRunner) benchDrainAddr() (listenAddr, dialAddr string, err error) {
	host := r.agentHost(r.cfg.SourceAgentSSH)
	if host == "" || agent.IsLocalHost(host) {
		addr, e := r.prebindLocal()
		return addr, addr, e
	}
	ip, e := config.PrimaryIPv4()
	if e != nil {
		return "", "", fmt.Errorf("bench: drain address: %w (set a routable controller NIC; the source segments must reach it)", e)
	}
	port, e := config.FreePortOnHost("0.0.0.0")
	if e != nil {
		return "", "", fmt.Errorf("bench: no free drain port: %w", e)
	}
	return net.JoinHostPort("0.0.0.0", fmt.Sprint(port)), net.JoinHostPort(ip, fmt.Sprint(port)), nil
}

// benchDrainListen binds the drain listener (0.0.0.0 or loopback per the
// layout) so the source agent / segreceivers can dial it.
func (r *processTwoAgentRunner) benchDrainListen(listenAddr string) (net.Listener, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("bench: bind drain %s: %w", listenAddr, err)
	}
	return l, nil
}

// benchDrainTLS upgrades an accepted drain connection to TLS when the
// operator configured the data-plane TLS (--seg-tls-*): the drain serves with
// the same keypair the agents use, so the benchmark measures the real
// (TLS-included) wire volume.
func benchDrainTLS(cfg *config.Config, conn net.Conn) (net.Conn, error) {
	if !transfer.TLSEnabled(cfg.SegTLSCert, cfg.SegTLSKey, cfg.SegTLSCA) {
		return conn, nil
	}
	scfg, err := transfer.ServerTLSConfig(cfg.SegTLSCert, cfg.SegTLSKey, cfg.SegTLSCA, "")
	if err != nil {
		return conn, err
	}
	tc := tls.Server(conn, scfg)
	if err := tc.Handshake(); err != nil {
		return conn, fmt.Errorf("bench: drain TLS handshake: %w", err)
	}
	return tc, nil
}

// benchDrainCountedConn counts every byte read from the drain connection —
// the raw on-network wire volume (framed payload + protocol overhead:
// frame-ack control, TLS records). It satisfies transfer.Pipe and the
// ReliablePipe dupConn contract, so the reliable pipe can ride on top of it
// (the raw bytes are still counted).
type benchDrainCountedConn struct {
	c net.Conn
	n atomic.Uint64
}

func (b *benchDrainCountedConn) Read(p []byte) (int, error) {
	n, err := b.c.Read(p)
	if n > 0 {
		b.n.Add(uint64(n))
	}
	return n, err
}
func (b *benchDrainCountedConn) Write(p []byte) (int, error) { return b.c.Write(p) }
func (b *benchDrainCountedConn) Close() error                { return b.c.Close() }
func (b *benchDrainCountedConn) Bytes() uint64               { return b.n.Load() }

// benchDrainAccept blocks for the next drain connection, bounded by ctx.
func benchDrainAccept(ctx context.Context, l net.Listener) (net.Conn, error) {
	type accOut struct {
		c   net.Conn
		err error
	}
	ch := make(chan accOut, 1)
	go func() {
		c, err := l.Accept()
		ch <- accOut{c, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case a := <-ch:
		if a.err != nil {
			return nil, a.err
		}
		return a.c, nil
	}
}

// drainCoordinatorSlice reads ONE coordinator-mode stream off the drain
// connection: the UnitSpec header, then the framed (optionally compressed)
// payload, discarded. With frame-ack on the framed stream rides a ReliablePipe
// receiver (the source agent's stream op wraps the socket the same way,
// dest.go pattern), so the drain must speak the frame-ack protocol or the
// source's HELLO would be misread as a frame and the unit would hang.
func drainCoordinatorSlice(ctx context.Context, cfg *config.Config, l net.Listener, conn net.Conn) error {
	defer conn.Close()
	spec, err := agent.ReadUnitSpec(conn)
	if err != nil {
		return fmt.Errorf("bench: drain UnitSpec: %w", err)
	}
	mode, _, perr := compress.Parse(spec.Compress)
	if perr != nil {
		return fmt.Errorf("bench: drain compress %q: %w", spec.Compress, perr)
	}
	var pipe transfer.Pipe = conn
	if cfg.FrameAckOn() {
		rp, rerr := transfer.NewReliablePipeReceiverOnConn(cfg.ReliablePipeConfig(), conn, l)
		if rerr != nil {
			return fmt.Errorf("bench: drain frame-ack: %w", rerr)
		}
		defer rp.Close()
		pipe = rp
	}
	fr := transfer.NewFramedReader(pipe, compress.NewDec(mode))
	if _, cerr := io.Copy(io.Discard, fr); cerr != nil {
		return fmt.Errorf("bench: drain stream: %w", cerr)
	}
	return nil
}

// drainMeshSlice reads ONE mesh slice off the drain connection: the
// segreceiver's framed (optionally compressed) stream, discarded. It returns
// the slice's trailer row count, the raw on-network wire bytes (including
// protocol overhead), and the decompressed logical bytes — the mesh metrics
// the source agent cannot report (it only sees the kernel's row count).
func drainMeshSlice(ctx context.Context, cfg *config.Config, l net.Listener, conn net.Conn, wantMode compress.Mode) (rows int64, wire, logical uint64, err error) {
	defer conn.Close()
	cc := &benchDrainCountedConn{c: conn}
	var pipe transfer.Pipe = cc
	if cfg.FrameAckOn() {
		rp, rerr := transfer.NewReliablePipeReceiverOnConn(cfg.ReliablePipeConfig(), cc, l)
		if rerr != nil {
			return 0, 0, 0, fmt.Errorf("bench: drain frame-ack: %w", rerr)
		}
		defer rp.Close()
		pipe = rp
	}
	var logicalN atomic.Uint64
	res := &segstream.SliceResult{}
	derr := segstream.ReadStreamCompressed(ctx, pipe, res, wantMode, func(contentID int16, p []byte) error {
		logicalN.Add(uint64(len(p)))
		return nil
	})
	wire = cc.Bytes()
	logical = logicalN.Load()
	if derr != nil {
		return 0, wire, logical, fmt.Errorf("bench: drain slice content=%d: %w", res.ContentID, derr)
	}
	if res.Trailer == nil {
		return 0, wire, logical, fmt.Errorf("bench: drain slice content=%d: truncated (no trailer)", res.ContentID)
	}
	return res.Rows, wire, logical, nil
}

// benchMeshDrainResult is the drain's per-unit report (rows from the slice
// trailers, the on-network wire + decompressed logical volumes).
type benchMeshDrainResult struct {
	Rows    int64
	Wire    uint64
	Logical uint64
	Err     error
}

// benchMeshDrain accepts S slices on l (one per source segreceiver), reads
// each to completion and aggregates. It returns when all S slices are done
// (or ctx fires / an accept fails — the first error is reported, the rest of
// the accepted slices are still drained so the segreceivers can finish).
func benchMeshDrain(ctx context.Context, cfg *config.Config, l net.Listener, S int, wantMode compress.Mode) benchMeshDrainResult {
	out := benchMeshDrainResult{}
	for s := 0; s < S; s++ {
		conn, aerr := benchDrainAccept(ctx, l)
		if aerr != nil {
			if out.Err == nil {
				out.Err = fmt.Errorf("bench: drain accept slice %d/%d: %w", s, S, aerr)
			}
			continue
		}
		tc, terr := benchDrainTLS(cfg, conn)
		if terr != nil {
			_ = conn.Close()
			if out.Err == nil {
				out.Err = fmt.Errorf("bench: drain TLS slice %d/%d: %w", s, S, terr)
			}
			continue
		}
		rows, wire, logical, rerr := drainMeshSlice(ctx, cfg, l, tc, wantMode)
		if rerr != nil && out.Err == nil {
			out.Err = rerr
		}
		out.Rows += rows
		out.Wire += wire
		out.Logical += logical
	}
	return out
}

// BenchMesh streams ONE table through the MESH on_segment source read (F-14,
// the mesh input-stream benchmark): the source agent runs the real
// `COPY ... TO PROGRAM (gpstream-segreceiver ...) ON SEGMENT` on its snapshot
// connection; the GP kernel launches the segreceiver on every primary source
// segment and each segreceiver streams its slice (frame v2 + compression,
// frame-ack + data TLS per the operator config) to the bench drain on the
// controller (the destination cluster is NOT involved — the payload is
// discarded). The metrics are the honest ones for this path: rows from the
// GP kernel's COPY row count (cross-checked against the per-slice trailer
// sum), wire/logical counted by the drain (the source agent cannot see them —
// the segreceivers stream independently per segment).
func (r *processTwoAgentRunner) BenchMesh(ctx context.Context, unitID, jobID uint32, schema, table, compressStr string) (int64, uint64, uint64, error) {
	if r.cfg.SegBinary == "" {
		return 0, 0, 0, fmt.Errorf("bench: mesh needs the segreceiver binary (resolve --seg-binary); got empty")
	}
	// S: the source primary segment count (the segreceiver count for the ON
	// SEGMENT dispatch) — read on the source agent (its local pool), like the
	// production run's srcPrimarySegmentCount.
	resp, err := r.Catalog(ctx, agent.CatalogReq{Action: "segment-count"})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("bench: mesh segment count: %w", err)
	}
	S := int(resp.SegmentCount)
	if S <= 0 {
		return 0, 0, 0, fmt.Errorf("bench: mesh needs a Greenplum source with up segments (segment-count=%d)", S)
	}

	listenAddr, dialAddr, err := r.benchDrainAddr()
	if err != nil {
		return 0, 0, 0, err
	}
	l, err := r.benchDrainListen(listenAddr)
	if err != nil {
		return 0, 0, 0, err
	}
	defer l.Close()

	wantMode, _, perr := compress.Parse(compressStr)
	if perr != nil {
		return 0, 0, 0, fmt.Errorf("bench: mesh compress %q: %w", compressStr, perr)
	}

	// The D-entry address book: S identical drain entries — segreceiver s
	// (source segment s) dials entry s (the bench drain, which accepts all S
	// slices on one listener).
	book := strings.TrimSuffix(strings.Repeat(dialAddr+",", S), ",")

	drainDone := make(chan benchMeshDrainResult, 1)
	go func() { drainDone <- benchMeshDrain(ctx, r.cfg, l, S, wantMode) }()

	req := agent.OnSegmentReq{
		Op:            "onsegment",
		Schema:        schema,
		Table:         table,
		JobID:         jobID,
		UnitID:        unitID,
		Mesh:          true,
		DestAddr:      book,
		RouteD:        int32(S),
		SegBinary:     r.cfg.SegBinary,
		Compress:      compressStr,
		FrameAck:      r.cfg.FrameAckArg(),
		AckTimeout:    r.cfg.AckTimeout,
		MaxReconnects: r.cfg.MaxReconnects,
		RetransmitMax: r.cfg.RetransmitBufferMax,
		DataTLSCert:   r.cfg.SegTLSCert,
		DataTLSKey:    r.cfg.SegTLSKey,
		DataTLSCA:     r.cfg.SegTLSCA,
	}
	// PeerAddr is empty: the bench has no destination agent, so the 1F
	// version exchange is skipped (handleOnSegment gates it on PeerAddr).
	oreq, oerr := r.ctrl.OnSegment(req)
	if oerr != nil {
		res := <-drainDone
		if res.Err != nil {
			return 0, 0, 0, fmt.Errorf("bench: mesh onsegment %s.%s: %w (drain: %v)", schema, table, oerr, res.Err)
		}
		return 0, 0, 0, fmt.Errorf("bench: mesh onsegment %s.%s: %w", schema, table, oerr)
	}
	if !oreq.Ok {
		res := <-drainDone
		if res.Err != nil {
			return 0, 0, 0, fmt.Errorf("bench: mesh onsegment %s.%s: %s (drain: %v)", schema, table, oreq.Error, res.Err)
		}
		return 0, 0, 0, fmt.Errorf("bench: mesh onsegment %s.%s: %s", schema, table, oreq.Error)
	}
	res := <-drainDone
	if res.Err != nil {
		return 0, 0, 0, fmt.Errorf("bench: mesh drain %s.%s: %w", schema, table, res.Err)
	}
	// The kernel's row count is authoritative; the per-slice trailer sum must
	// agree (a mismatch means a slice was dropped or a segreceiver miscounted
	// — fail loud, the measurement is void).
	if res.Rows != oreq.Rows {
		return 0, 0, 0, fmt.Errorf("bench: mesh %s.%s: kernel row count %d != slice trailer sum %d (slice lost or miscounted)", schema, table, oreq.Rows, res.Rows)
	}
	return oreq.Rows, res.Wire, res.Logical, nil
}

// InProcTwoAgentRunner has no mesh source read (the in-process test runner
// has no real Greenplum kernel to dispatch ON SEGMENT on).
func (r *InProcTwoAgentRunner) BenchMesh(ctx context.Context, unitID, jobID uint32, schema, table, compress string) (int64, uint64, uint64, error) {
	return 0, 0, 0, fmt.Errorf("bench: mesh needs the process runner (a real Greenplum source with the GP kernel's ON SEGMENT dispatch); the in-process runner cannot run it")
}
