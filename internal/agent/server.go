package agent

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// SourceServer is the long-lived source agent (ADR-002 remediation, P3). It
// owns the source pg connection pool AND the FR-02 snapshot: a REPEATABLE READ
// transaction lives on one server-side backend, so ONE process must run every
// unit's data COPY under that single snapshot to keep the whole run consistent
// (FR-02). The controller drives it over a control socket: it sends
// one StreamReq per unit (the source dials the destination agent and streams)
// and a final ReleaseReq (commit the snapshot). Catalog reads (columns /
// distribution / partition) run on the pool, NOT the pinned snapshot
// connection — a catalog SQL fault on the pinned conn aborts the whole snapshot
// transaction (the FR-02 snapshot pitfall); heal re-establishes it after a unit
// failure. This mirrors the single-controller manager's srcRead()/healSnapshot
// design exactly.
type SourceServer struct {
	opts     ServerOptions
	pool     *pg.Pool
	pinned   pg.StreamingQuerier // the pinned snapshot connection (data COPYs)
	pinRel   func()
	snapMode SnapshotMode
	snapTx   bool
	healMu   sync.Mutex
	ctrlAddr string
	stop     chan struct{}
	stopped  chan struct{}
}

// ServerOptions configures a long-lived source agent.
type ServerOptions struct {
	// SrcConn is the source postgres conn string.
	SrcConn string
	// PG-plane TLS (the agent is a client of the source DB).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Snapshot is the FR-02 scope held across the whole run.
	Snapshot SnapshotMode
	// Listen is the control socket the controller connects to (host:port).
	Listen string
	// Data-plane TLS for the agent<->agent socket (source = client).
	DataTLSCert, DataTLSKey, DataTLSCA string
	// ControlTLS for the control socket (this agent = server). CA present =
	// mTLS (require + verify the controller's client cert).
	ControlTLSCert, ControlTLSKey, ControlTLSCA string
	// ConnectTimeout bounds the per-unit dest dial; StreamTimeout the stream.
	ConnectTimeout, StreamTimeout time.Duration
	// FrameAck enables the frame-level ACK + RST-resume transport (DESIGN
	// §4.6) on the data socket: the framed stream rides a ReliablePipe that
	// re-dials the destination on a break and replays the un-ACKed frames.
	FrameAck bool
	// ReliablePipe carries the §4.6 transport parameters (ack timeout,
	// reconnect budget, retransmit window, resume timeout).
	ReliablePipe transfer.ReliablePipeConfig
}

// NewSourceServer builds a long-lived source agent with the given options.
func NewSourceServer(o ServerOptions) *SourceServer {
	return &SourceServer{opts: o}
}

// Start opens the source pool, establishes the FR-02 snapshot, and starts the
// control listener. It returns the control address the controller connects to.
func (s *SourceServer) Start(ctx context.Context) (string, error) {
	if s.opts.ConnectTimeout <= 0 {
		s.opts.ConnectTimeout = 10 * time.Second
	}
	if s.opts.StreamTimeout <= 0 {
		s.opts.StreamTimeout = 6 * time.Hour
	}
	s.stop = make(chan struct{})
	s.stopped = make(chan struct{})

	pool, err := pg.NewPool(ctx, s.opts.SrcConn, s.opts.SSLCert, s.opts.SSLKey, s.opts.SSLCA, s.opts.SSLMinTLS, 4)
	if err != nil {
		return "", fmt.Errorf("source server: connect source: %w", err)
	}
	s.pool = pool

	// FR-02 snapshot on a pinned connection (the manager's establishSnapshot).
	mode := s.opts.Snapshot
	if mode != SnapNone {
		q, release, perr := pool.Pin(ctx)
		if perr != nil {
			pool.Close()
			return "", fmt.Errorf("source server: pin snapshot conn: %w", perr)
		}
		if berr := beginSnapshot(ctx, q, mode); berr != nil {
			release()
			pool.Close()
			return "", berr
		}
		s.pinned = q
		s.pinRel = release
		s.snapMode = mode
		s.snapTx = true
	}
	return s.startControl()
}

// startControl begins the control listener and returns its address.
func (s *SourceServer) startControl() (string, error) {
	l, err := net.Listen("tcp", s.opts.Listen)
	if err != nil {
		if s.pool != nil {
			s.pool.Close()
		}
		return "", fmt.Errorf("source server: control listen %s: %w", s.opts.Listen, err)
	}
	if transfer.TLSEnabled(s.opts.ControlTLSCert, s.opts.ControlTLSKey, s.opts.ControlTLSCA) {
		cfg, terr := transfer.ServerTLSConfig(s.opts.ControlTLSCert, s.opts.ControlTLSKey, s.opts.ControlTLSCA, "")
		if terr != nil {
			_ = l.Close()
			if s.pool != nil {
				s.pool.Close()
			}
			return "", fmt.Errorf("source server: control tls config: %w", terr)
		}
		l = tls.NewListener(l, cfg)
	}
	s.ctrlAddr = l.Addr().String()
	go s.serveControl(l)
	return s.ctrlAddr, nil
}

// serveControl accepts the controller's control connection and serves
// StreamReq/ReleaseReq messages for the run's lifetime.
func (s *SourceServer) serveControl(l net.Listener) {
	defer close(s.stopped)
	defer l.Close()
	conn, err := l.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		line, rerr := r.ReadBytes('\n')
		if rerr != nil {
			return // controller closed the control conn (run end / stop)
		}
		var probe struct {
			Op string `json:"op"`
		}
		if uerr := json.Unmarshal(line, &probe); uerr != nil {
			continue
		}
		// A unit-level panic (e.g. a pg conn torn down mid-stream during a
		// crash) reports THAT unit as failed instead of killing the agent:
		// the run's resilience contract is per-unit, not per-process.
		func() {
			defer func() {
				if p := recover(); p != nil {
					fmt.Fprintf(os.Stderr, "gpstream-source-agent: unit panic: %v\n", p)
					_ = SendLine(conn, StreamResp{Ok: false, Error: fmt.Sprintf("unit panic: %v", p)})
				}
			}()
			switch probe.Op {
			case "stream":
				var req StreamReq
				if uerr := json.Unmarshal(line, &req); uerr != nil {
					_ = SendLine(conn, StreamResp{Ok: false, Error: uerr.Error()})
					return
				}
				_ = SendLine(conn, s.handleStream(req))
			case "onsegment":
				var req OnSegmentReq
				if uerr := json.Unmarshal(line, &req); uerr != nil {
					_ = SendLine(conn, OnSegmentResp{Ok: false, Error: uerr.Error()})
					return
				}
				_ = SendLine(conn, s.handleOnSegment(req))
			case "checksum":
				var req ChecksumReq
				if uerr := json.Unmarshal(line, &req); uerr != nil {
					_ = SendLine(conn, ChecksumResp{Ok: false, Error: uerr.Error()})
					return
				}
				_ = SendLine(conn, s.handleChecksum(req))
			case "release":
				_ = SendLine(conn, s.handleRelease())
			default:
				_ = SendLine(conn, StreamResp{Ok: false, Error: fmt.Sprintf("unknown op %q", probe.Op)})
			}
		}()
		if probe.Op == "release" {
			return
		}
	}
}

// handleStream streams one unit to the destination agent and returns the report.
func (s *SourceServer) handleStream(req StreamReq) StreamResp {
	ctx := context.Background()
	// 1. Capture the source table's shape on the POOL (not the pinned snapshot
	// conn — a catalog fault must not abort the snapshot; the manager's
	// srcRead() convention).
	cq := s.catalogQuerier()
	cols, err := source.New(cq).Columns(ctx, req.Schema, req.Table)
	if err != nil {
		return StreamResp{Ok: false, Error: fmt.Sprintf("read columns %s.%s: %v", req.Schema, req.Table, err)}
	}
	dist := source.New(cq).Distribution(ctx, req.Schema, req.Table)
	part, perr := pg.CapturePartitionSpec(cq, ctx, req.Schema, req.Table)
	if perr != nil {
		part = pg.PartitionSpec{} // non-fatal (plain PG / transient), like the planner
	}
	spec := UnitSpec{
		JobID: req.JobID, UnitID: req.UnitID,
		Schema: req.DstSchema, Table: req.DstTable, Staging: req.Staging, Overwrite: req.Overwrite,
		Compress:  req.Compress,
		SrcSchema: req.Schema, SrcTable: req.Table,
		Cols: cols, Dist: dist, Part: part,
	}

	// 2. Stream the unit (data COPY on the pinned snapshot conn) to the dest.
	res, serr := s.streamUnit(ctx, spec, req.DestAddr)
	if serr != nil {
		s.heal(ctx) // a failed unit may have aborted the snapshot tx (F-06)
		return StreamResp{Ok: false, Error: serr.Error()}
	}
	return StreamResp{Ok: true, Rows: res.Rows, Wire: res.Wire, Logical: res.Logical}
}

// handleOnSegment runs the P4 on_segment data path on the source agent's FR-02
// snapshot connection: it builds the COPY ... TO PROGRAM ... ON SEGMENT BINARY
// statement (the segreceiver program string points at the destination agent,
// with the per-slice compression mode + the data-plane TLS paths) and executes
// it. The GPDB kernel launches the segreceiver on every primary source segment
// (dispatched with DF_WITH_SNAPSHOT, so every slice reads the coordinator's
// pinned snapshot) and waits for all of them to exit before the COPY returns
// (DF_NEED_TWO_PHASE) — so when this returns, every slice has been streamed to
// the destination agent. It returns the kernel's row count (a cross-check; the
// authoritative count is the destination agent's).
func (s *SourceServer) handleOnSegment(req OnSegmentReq) OnSegmentResp {
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.StreamTimeout)
	defer cancel()
	var prog string
	var err error
	if req.Routed {
		prog, err = segstream.ProgramCommandRouted(req.SegBinary, req.DestAddr, req.JobID, req.UnitID,
			req.DataTLSCert, req.DataTLSKey, req.DataTLSCA, req.Compress, req.RouteKey, int(req.RouteD), req.RouteWorkers)
	} else {
		prog, err = segstream.ProgramCommandFull(req.SegBinary, req.DestAddr, req.JobID, req.UnitID,
			req.DataTLSCert, req.DataTLSKey, req.DataTLSCA, req.Compress)
	}
	if err != nil {
		return OnSegmentResp{Ok: false, Error: fmt.Sprintf("build program: %v", err)}
	}
	sql := segstream.CopyOnSegmentSQL(req.Schema, req.Table, prog)
	// The ON SEGMENT dispatch must run on the unit's FR-02 snapshot
	// connection (the kernel dispatches with DF_WITH_SNAPSHOT); the pinned
	// conn is that connection.
	rows, err := s.copyQuerier().Exec(ctx, sql)
	if err != nil {
		s.heal(ctx) // a failed dispatch may have aborted the snapshot tx (F-06)
		return OnSegmentResp{Ok: false, Error: fmt.Sprintf("COPY ON SEGMENT: %v", err)}
	}
	return OnSegmentResp{Ok: true, Rows: rows}
}

// countingWriteConn wraps a net.Conn and counts every byte written to it (the
// on-wire volume the source agent reports). Embedding net.Conn delegates all
// other methods, so it satisfies the interface segstream.SendCompressed takes.
type countingWriteConn struct {
	net.Conn
	n atomic.Uint64
}

func (w *countingWriteConn) Write(p []byte) (int, error) {
	n, err := w.Conn.Write(p)
	if n > 0 {
		w.n.Add(uint64(n))
	}
	return n, err
}

func (w *countingWriteConn) Bytes() uint64 { return w.n.Load() }

// dialDestTLS dials the destination agent's data socket (retrying while it is
// still binding) and applies the data-plane TLS config when enabled (the
// source agent is the client). Shared by streamUnit-style dials and the P5
// direct path (which dials once per destination segment).
// dialDestOnce performs a SINGLE dial + (optional) TLS handshake to the
// destination agent. The initial dial retries this (dialDestRetry) while the
// destination binds its socket; the frame-ack re-dial on a break bounds the
// retries itself (a dead destination must fail the unit, not hang).
func (s *SourceServer) dialDestOnce(ctx context.Context, addr string) (net.Conn, error) {
	raw, err := net.DialTimeout("tcp", addr, s.opts.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	if !transfer.TLSEnabled(s.opts.DataTLSCert, s.opts.DataTLSKey, s.opts.DataTLSCA) {
		return raw, nil
	}
	host, herr := hostOf(addr)
	if herr != nil {
		raw.Close()
		return nil, fmt.Errorf("bad dest %q: %w", addr, herr)
	}
	ccfg, cerr := transfer.ClientTLSConfig(host, s.opts.DataTLSCert, s.opts.DataTLSKey, s.opts.DataTLSCA, "")
	if cerr != nil {
		raw.Close()
		return nil, fmt.Errorf("tls config: %w", cerr)
	}
	tc := tls.Client(raw, ccfg)
	if hsErr := tc.HandshakeContext(ctx); hsErr != nil {
		raw.Close()
		return nil, fmt.Errorf("tls handshake with dest: %w", hsErr)
	}
	return tc, nil
}

// dialDestTLS dials + TLS-handshakes to the destination, retrying while the
// destination binds its socket (the controller launches it just before the
// source is told to dial). A refused connection is NOT the dest's "one
// accept" (it never reached the accept), so retrying is safe.
func (s *SourceServer) dialDestTLS(ctx context.Context, addr string) (net.Conn, error) {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		conn, err := s.dialDestOnce(ctx, addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("dial %s (retries exhausted): %w", addr, lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// handleChecksum re-derives the content checksum of a table on the pinned
// snapshot connection (the same FR-02 snapshot the data COPY used) — the
// controller's --verify path compares it against the destination.
func (s *SourceServer) handleChecksum(req ChecksumReq) ChecksumResp {
	ctx := context.Background()
	sum, err := source.New(s.copyQuerier()).Checksum(ctx, req.Schema, req.Table)
	if err != nil {
		return ChecksumResp{Ok: false, Error: err.Error()}
	}
	return ChecksumResp{Ok: true, Checksum: sum}
}

// Checksum re-derives a table's content checksum on the run's FR-02 snapshot
// (the same connection every data COPY used). It is the direct API for an
// in-process controller (the integration-test runner); the control-socket
// path (handleChecksum) wraps it for the binary agent.
func (s *SourceServer) Checksum(ctx context.Context, schema, table string) (string, error) {
	return source.New(s.copyQuerier()).Checksum(ctx, schema, table)
}

// catalogQuerier returns the querier for catalog reads: the pool (separate
// connections) when a snapshot is pinned, so a catalog fault cannot abort the
// snapshot transaction.
func (s *SourceServer) catalogQuerier() pg.StreamingQuerier {
	if s.snapTx && s.pool != nil {
		return s.pool
	}
	return s.copyQuerier()
}

// copyQuerier returns the querier the data COPY runs on: the pinned snapshot
// connection (so every unit's COPY shares the one FR-02 snapshot), or the pool
// when no snapshot is held.
func (s *SourceServer) copyQuerier() pg.StreamingQuerier {
	if s.pinned != nil {
		return s.pinned
	}
	return s.pool
}

// streamUnit dials the destination agent and streams the unit (the data COPY)
// from the pinned snapshot connection, framing + compressing on the wire.
func (s *SourceServer) streamUnit(ctx context.Context, spec UnitSpec, destAddr string) (SourceResult, error) {
	var out SourceResult
	dctx, dcancel := context.WithTimeout(ctx, s.opts.StreamTimeout)
	defer dcancel()
	// The destination agent may still be binding its listen socket (the
	// controller launches it right before instructing the source to dial), so
	// retry the dial briefly. A refused connection is NOT the dest agent's
	// "one accept" — it never reached the accept — so retrying is safe.
	conn, err := s.dialDestTLS(dctx, destAddr)
	if err != nil {
		return out, fmt.Errorf("dial dest %s: %w", destAddr, err)
	}

	// UnitSpec header FIRST, on the INITIAL connection: the destination reads
	// it to build the staging table before it reads the framed data stream.
	// (It is NOT re-sent on a resume: the re-dial replays un-ACKed FRAMES only.)
	if werr := WriteUnitSpec(conn, spec); werr != nil {
		conn.Close()
		return out, werr
	}

	mode, level, perr := compress.Parse(spec.Compress)
	if perr != nil {
		conn.Close()
		return out, fmt.Errorf("compress %q: %w", spec.Compress, perr)
	}

	// Data channel: a plain conn, or (frame-ack on, DESIGN §4.6) a
	// ReliablePipe that re-dials the destination on a break and replays the
	// un-ACKed frames. The destination's receiver dedups by seq, so its open
	// COPY FROM keeps reading the in-order stream across the break (no
	// re-COPY, no re-UnitSpec).
	frameConn := transfer.Pipe(conn)
	var rp *transfer.ReliablePipe
	if s.opts.FrameAck {
		var rerr error
		rp, rerr = transfer.NewReliablePipeSenderOnConn(s.opts.ReliablePipe, conn, func() (transfer.DupConn, error) {
			// Re-dial on a break: reproduce the full connection setup
			// (dial + TLS). A single attempt — the destination's resumer
			// is already blocked in its accept (ResumeTimeout window), so a
			// live destination accepts; a dead one refuses, which must fail
			// the unit fast (no retry storm).
			return s.dialDestOnce(dctx, destAddr)
		})
		if rerr != nil {
			conn.Close()
			return out, fmt.Errorf("frame-ack transport: %w", rerr)
		}
		frameConn = rp
	}
	fw := transfer.NewFramedWriter(frameConn, spec.JobID, spec.UnitID, compress.New(mode, level))
	copyConn := s.copyQuerier()
	counted := &logicalCounter{}
	tee := &teeWriter{logical: counted, out: fw}
	rows, cerr := source.New(copyConn).CopyToStream(dctx, spec.SrcSchema, spec.SrcTable, tee)
	if rp != nil {
		// Flush the framed tail, then signal clean end-of-stream (FIN) over
		// the pipe — not by closing the conn (the pipe owns the conn).
		if flerr := fw.Flush(); cerr == nil {
			cerr = flerr
		}
		_ = rp.Close()
	} else {
		if flerr := fw.CloseFlush(); cerr == nil {
			cerr = flerr
		}
	}
	conn.Close()
	out.Rows = rows
	out.Wire = fw.Wire()
	out.Logical = uint64(counted.n)
	if cerr != nil {
		return out, cerr
	}
	return out, nil
}

// handleRelease ends the run: commits the snapshot and signals stop.
func (s *SourceServer) handleRelease() ReleaseResp {
	ctx := context.Background()
	s.healMu.Lock()
	if s.snapTx && s.pinned != nil {
		_, _ = s.pinned.Exec(ctx, "COMMIT")
		s.snapTx = false
	}
	if s.pinRel != nil {
		s.pinRel()
		s.pinRel = nil
		s.pinned = nil
	}
	s.healMu.Unlock()
	close(s.stop)
	return ReleaseResp{Ok: true}
}

// heal re-establishes the FR-02 snapshot after a unit failure (the manager's
// healSnapshot): probe the pinned conn; if the transaction is aborted, ROLLBACK
// and begin a FRESH REPEATABLE READ on the same connection.
func (s *SourceServer) heal(ctx context.Context) {
	// Every pinned-conn access (heal / Release / Close) is serialized on
	// healMu: Close/Release may release the pinned conn concurrently with a
	// failed unit's heal, and a released pgxpool.Conn nil-derefs in Conn()
	// (observed as a live cross-VM crash).
	s.healMu.Lock()
	defer s.healMu.Unlock()
	if !s.snapTx || s.pinned == nil {
		return // already released (Close/Release won the race)
	}
	// Probe the pinned connection.
	if _, err := s.pinned.Query(ctx, "SELECT 1"); err == nil {
		return // transaction intact
	}
	// Aborted: roll back and begin a fresh snapshot on the same connection.
	_, _ = s.pinned.Exec(ctx, "ROLLBACK")
	_ = beginSnapshot(ctx, s.pinned, s.snapMode)
}

// Close stops the source server (rolling back any open snapshot).
func (s *SourceServer) Close() {
	s.healMu.Lock()
	if s.snapTx && s.pinned != nil {
		_, _ = s.pinned.Exec(context.Background(), "ROLLBACK")
		s.snapTx = false
	}
	if s.pinRel != nil {
		s.pinRel()
		s.pinRel = nil
		s.pinned = nil
	}
	s.healMu.Unlock()
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	if s.pool != nil {
		s.pool.Close()
	}
}

// Done is closed when the control server stops (release or controller close).
func (s *SourceServer) Done() <-chan struct{} { return s.stopped }
