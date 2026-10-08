package agent

// supervisor.go — the P3 SUPERVISOR (the dest-agent on the destination
// coordinator) for the direct_1to1 mesh.
//
// The supervisor is the coordinator-side counterpart of the P2 mini-agent:
// the mini-agents run on the destination SEGMENT hosts (P2, ServeMini) and
// each dials the supervisor's REPORT listener once for the whole run (a
// persistent connection, orphan-exit on drop). The supervisor:
//
//   - accepts D mini-agent report connections and reads each one's hello
//     {seg, dataPort, ready}, building a MiniMap (seg ordinal -> this
//     segment's data-listener address) that the controller folds into the
//     segreceivers' ProgramCommand (so the source streams each segment's
//     slice to the right mini-agent);
//   - per unit, sends a mini-unit request to EVERY mini-agent (on its
//     persistent report connection) and collects the D mini-unit-resp
//     reports;
//   - COMBINES the D per-segment contributions into the unit's digests:
//     srcDigest = XOR_D(srcContrib) and dstDigest = XOR_D(dstAcc) over the
//     summed row count (the per-row XOR accumulator is commutative/
//     associative, so the order of the segments does not matter — this is
//     what makes the direct_1to1 mesh verifiable without re-reading any
//     table, FR-02);
//   - HEALTH: a mini-agent whose report connection drops (the process died)
//     is an ORPHAN — its unit report is a failure, so the unit fails and the
//     run resumes (the staging slice on that segment is truncated on
//     re-copy, logic that already exists);
//   - on STOP, sends a mini-done to each mini-agent (clean exit) and closes
//     the report listener (best-effort; the mini-agent's orphan-exit is the
//     guarantee if the supervisor itself dies first).
//
// Launching the D mini-agents is OUT-OF-BAND relative to this type: in the
// production layout the CONTROLLER (the run process,
// processTwoAgentRunner.launchMinis) launches them — one
// `gpstream-dest-agent --mini` per destination segment host, local
// subprocess or SSH (the same launch machinery as the source/dest agents),
// reusing internal/launch, no sudo; in the test they are spawned in-process
// (ServeMini goroutines). Either way, the mini-agents dial the supervisor's
// report listener, so this type's contract is: accept the D report
// connections, collect their reports, combine them.
//
// Design 04.10: the supervisor holds NO data connection — it only speaks the
// report protocol to the mini-agents. The data (compressed framed slices)
// flows source segreceiver -> mini-agent data listener directly.
//
// PG 9.4 (GP6): nothing here touches pg directly (the mini-agents do the
// utility COPYs), so there is no PG-version-specific SQL in this file.

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gpstream/gpstream/internal/portalloc"
	"github.com/gpstream/gpstream/internal/transfer"
)

// MiniMap maps a destination-segment ordinal (0..D-1) to that segment's
// mini-agent DATA-listener address (host:port). The controller folds this
// into the segreceivers' ProgramCommand so each source segment streams its
// slice to the matching mini-agent.
type MiniMap map[int]string

// UnitReportReq is the supervisor's per-unit instruction, broadcast to every
// mini-agent (each one serves its own segment's slice of this unit).
type UnitReportReq struct {
	// ReqID is the run's correlation id (echoed by every mini's report).
	ReqID uint64
	// UnitID / JobID identify the unit (the mini's report echoes them).
	UnitID, JobID uint32
	// Schema / Staging is the destination staging slice the mini loads (the
	// supervisor created/truncated it on the coordinator; it is replicated to
	// the segment).
	Schema, Staging string
	// Compress is the FR-12 mode the source segreceivers used (the mini
	// decompresses).
	Compress string
	// VerifyDigest asks each mini to tee the data-path digest (the supervisor
	// combines the per-segment accumulators).
	VerifyDigest bool
	// SourceSegments (S) — how many source segreceivers stream sub-streams to
	// each mini-agent (0 or 1 = direct: one slice; >1 = routed mesh, P4).
	SourceSegments int32 `json:"source_segments,omitempty"`
	// Workers (W) — sub-streams per source segment (each source segment fans
	// its rows to a destination segment over W sub-streams).
	Workers int `json:"workers,omitempty"`
	// Final is the FINAL destination table name (the staging is renamed to it
	// by the destination agent's swap/commit decision, runUnit step 9).
	// Overwrite is the conflict policy (drop the existing final table first).
	Final     string
	Overwrite bool
}

// CombinedUnitResult is the supervisor's per-unit report after combining the
// D mini-agent reports (what the controller reads as the unit's destination
// result, the mesh analogue of DestUnitDone).
type CombinedUnitResult struct {
	// Rows is the summed authoritative row count (sum of the D utility COPYs).
	Rows int64
	// SrcDigest / DstDigest are the combined "<rows>:<hex>" content digests
	// (XOR_D of the per-segment accumulators over the summed row count),
	// composable and row-order-independent (FR-02). Empty when verify is off.
	SrcDigest, DstDigest string
	// Wire / Logical are the summed on-network / decompressed byte volumes.
	Wire, Logical uint64
	// PerSeg is the per-segment row count (content 0..D-1) — the F-15 balance
	// cross-check.
	PerSeg []int64
}

// Supervisor is the coordinator-side controller of the D destination-segment
// mini-agents for one run.
type Supervisor struct {
	reportL net.Listener
	tlsCfg  *tls.Config
	// minis maps seg ordinal -> the persistent report connection + data addr.
	mu    sync.Mutex
	minis map[int]*miniConn
	// stopped is closed by Stop (a clean shutdown signal to the unit loop).
	stopped chan struct{}
	// stopOnce makes Stop idempotent (the DestServer's release op and its
	// Close both call it; a second call must not re-close stopped).
	stopOnce sync.Once
}

// miniConn is one mini-agent's persistent report connection.
type miniConn struct {
	seg  int
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
	data string // this segment's data-listener address (from the hello)
}

// NewSupervisor binds the report listener (the mini-agents dial it once for
// the run). listen is "host:port" (port 0 = pick a free; Addr reports it).
// portAlloc, when enabled, restricts the auto-picked port to the range.
func NewSupervisor(listen string, portAlloc *portalloc.Allocator, dataTLSCert, dataTLSKey, dataTLSCA, dataTLSMinTLS string) (*Supervisor, error) {
	var l net.Listener
	var err error
	if portAlloc != nil && portAlloc.Enabled() {
		host := "0.0.0.0"
		if h, _, serr := net.SplitHostPort(listen); serr == nil && h != "" {
			host = h
		}
		l, err = portAlloc.Listen(host)
	} else {
		l, err = net.Listen("tcp", listen)
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: listen %s: %w", listen, err)
	}
	s := &Supervisor{
		reportL: l,
		minis:   make(map[int]*miniConn),
		stopped: make(chan struct{}),
	}
	if transfer.TLSEnabled(dataTLSCert, dataTLSKey, dataTLSCA) {
		scfg, terr := transfer.ServerTLSConfig(dataTLSCert, dataTLSKey, dataTLSCA, dataTLSMinTLS)
		if terr != nil {
			_ = l.Close()
			return nil, fmt.Errorf("supervisor: tls config: %w", terr)
		}
		s.tlsCfg = scfg
	}
	return s, nil
}

// Addr is the report listener's bound address (the mini-agents dial it).
func (s *Supervisor) Addr() string { return s.reportL.Addr().String() }

// AcceptMinis accepts D mini-agent report connections and reads each one's
// hello, returning the MiniMap (seg -> data addr). It blocks until D hellos
// are received or ctx ends. The report connections are kept open (persistent
// for the run).
func (s *Supervisor) AcceptMinis(ctx context.Context, D int) (MiniMap, error) {
	if D < 1 {
		return nil, fmt.Errorf("supervisor: D %d (want >= 1)", D)
	}
	type helloOut struct {
		seg  int
		conn net.Conn
		data string
		err  error
	}
	hellos := make(chan helloOut, D)
	for i := 0; i < D; i++ {
		go func() {
			c, aerr := s.reportL.Accept()
			if aerr != nil {
				hellos <- helloOut{err: aerr}
				return
			}
			if s.tlsCfg != nil {
				tc := tls.Server(c, s.tlsCfg)
				hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
				defer hcancel()
				if herr := tc.HandshakeContext(hctx); herr != nil {
					_ = c.Close()
					hellos <- helloOut{err: fmt.Errorf("report tls: %w", herr)}
					return
				}
				c = tc
			}
			// Bound the hello read so a stuck mini does not wedge the accept.
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
			var h MiniHello
			if rerr := recvLine(bufio.NewReader(c), &h); rerr != nil {
				_ = c.Close()
				hellos <- helloOut{err: fmt.Errorf("read mini hello: %w", rerr)}
				return
			}
			hellos <- helloOut{seg: h.Seg, conn: c, data: net.JoinHostPort("", fmt.Sprint(h.DataPort))}
		}()
	}
	mm := make(MiniMap, D)
	for i := 0; i < D; i++ {
		h := <-hellos
		if h.err != nil {
			s.Close()
			return nil, fmt.Errorf("supervisor: accept mini: %w", h.err)
		}
		if _, dup := s.minis[h.seg]; dup {
			s.Close()
			return nil, fmt.Errorf("supervisor: duplicate mini for seg %d", h.seg)
		}
		if h.seg < 0 || h.seg >= D {
			s.Close()
			return nil, fmt.Errorf("supervisor: mini seg %d out of range (0..%d)", h.seg, D-1)
		}
		// The hello's DataPort is the port only (the mini bound it on its own
		// host); the mini's host is the segment host. In the single-node test
		// and for same-host minis the host is 127.0.0.0/localhost; in the
		// production layout the supervisor learns each segment host from the
		// catalog (the DstDSNs). We carry the host from the DstDSN at launch;
		// for now the MiniMap value is host:port with the host filled by the
		// caller via SetMiniHost (see below) — here we store the port and let
		// the data addr be resolved against the segment host.
		s.minis[h.seg] = &miniConn{
			seg:  h.seg,
			conn: h.conn,
			w:    bufio.NewWriter(h.conn),
			r:    bufio.NewReader(h.conn),
			data: h.data,
		}
		mm[h.seg] = h.data
	}
	return mm, nil
}

// SetMiniHost sets the host part of a segment's data address (the segment
// host from the catalog's DstDSN). The hello carries only the port; the
// supervisor combines it with the known segment host to form the full
// data-listener address the segreceivers dial.
func (s *Supervisor) SetMiniHost(seg int, host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mc, ok := s.minis[seg]; ok {
		if _, port, err := net.SplitHostPort(mc.data); err == nil {
			mc.data = net.JoinHostPort(host, port)
		}
	}
}

// MiniDataAddr returns a segment's data-listener address (host:port).
func (s *Supervisor) MiniDataAddr(seg int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mc, ok := s.minis[seg]; ok {
		return mc.data
	}
	return ""
}

// ReportConn returns a segment's persistent report connection (the conn the
// supervisor holds for that mini-agent). It exists so a crash-matrix test can
// sever a mini's report channel to model the mini-agent process dying mid-unit
// (the supervisor's next report read on the dropped channel surfaces the
// orphan). Not used in production.
func (s *Supervisor) ReportConn(seg int) (net.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mc, ok := s.minis[seg]
	if !ok {
		return nil, false
	}
	return mc.conn, true
}

// ServeUnit drives one unit across the D mini-agents: it sends a mini-unit
// request to every mini-agent (concurrently, on each one's persistent report
// connection), collects the D mini-unit-resp reports, and combines them into
// the unit's digests (XOR_D of the per-segment accumulators over the summed
// row count). It returns an error (unit failure) if any mini-agent reports a
// failure OR a report connection drops (orphan: the mini-agent died).
func (s *Supervisor) ServeUnit(ctx context.Context, req UnitReportReq) (CombinedUnitResult, error) {
	var out CombinedUnitResult
	type segResult struct {
		seg  int
		resp MiniUnitResp
		err  error // transport error (orphan: the report connection dropped)
	}
	// D is the number of registered mini-agents.
	var segs []int
	s.mu.Lock()
	for seg := range s.minis {
		segs = append(segs, seg)
	}
	s.mu.Unlock()
	D := len(segs)
	if D == 0 {
		return out, fmt.Errorf("supervisor: no mini-agents registered (AcceptMinis first)")
	}
	out.PerSeg = make([]int64, D)

	results := make(chan segResult, D)
	for _, seg := range segs {
		go func(seg int) {
			s.mu.Lock()
			mc := s.minis[seg]
			s.mu.Unlock()
			mreq := MiniUnitReq{
				Op:             "mini-unit",
				ReqID:          req.ReqID,
				UnitID:         req.UnitID,
				JobID:          req.JobID,
				Schema:         req.Schema,
				Staging:        req.Staging,
				Compress:       req.Compress,
				VerifyDigest:   req.VerifyDigest,
				SourceSegments: req.SourceSegments,
				Workers:        req.Workers,
			}
			if werr := SendLine(mc.w, mreq); werr != nil {
				results <- segResult{seg: seg, err: fmt.Errorf("send mini-unit: %w", werr)}
				return
			}
			if ferr := mc.w.Flush(); ferr != nil {
				results <- segResult{seg: seg, err: fmt.Errorf("flush mini-unit: %w", ferr)}
				return
			}
			var resp MiniUnitResp
			if rerr := recvLine(mc.r, &resp); rerr != nil {
				// Orphan: the mini-agent's report connection dropped (it died).
				results <- segResult{seg: seg, err: fmt.Errorf("mini seg %d report channel dropped (orphan): %w", seg, rerr)}
				return
			}
			results <- segResult{seg: seg, resp: resp}
		}(seg)
	}

	var (
		srcAcc [32]byte
		dstAcc [32]byte
		rows   int64
		wire   uint64
		logic  uint64
		first  error
	)
	for i := 0; i < D; i++ {
		r := <-results
		if r.err != nil {
			if first == nil {
				first = r.err
			}
			continue
		}
		if !r.resp.Ok {
			if first == nil {
				first = fmt.Errorf("mini seg %d failed: %s", r.seg, r.resp.Error)
			}
			continue
		}
		rows += r.resp.Rows
		wire += r.resp.Wire
		logic += r.resp.Logical
		if r.seg >= 0 && r.seg < D {
			out.PerSeg[r.seg] = r.resp.Rows
		}
		// XOR the per-segment accumulators (order-independent: XOR is
		// commutative/associative).
		if r.resp.SrcContrib != "" {
			if a, err := hex.DecodeString(r.resp.SrcContrib); err == nil && len(a) == 32 {
				for j := 0; j < 32; j++ {
					srcAcc[j] ^= a[j]
				}
			}
		}
		if r.resp.DstAcc != "" {
			if a, err := hex.DecodeString(r.resp.DstAcc); err == nil && len(a) == 32 {
				for j := 0; j < 32; j++ {
					dstAcc[j] ^= a[j]
				}
			}
		}
	}
	out.Rows = rows
	out.Wire = wire
	out.Logical = logic
	if first != nil {
		return out, fmt.Errorf("supervisor: unit %d: %w", req.UnitID, first)
	}
	// Combine the XOR'd accumulators into the unit's "<rows>:<hex>" digests.
	// ALWAYS computed (no verify gate): the destination verifies on the data
	// path without a re-read.
	out.SrcDigest = transfer.CombinedDigestString(srcAcc, rows)
	out.DstDigest = transfer.CombinedDigestString(dstAcc, rows)
	return out, nil
}

// Stop signals a clean run end: it sends a mini-done to each mini-agent (its
// clean-exit path) and closes the report listener. Best-effort — a mini-agent
// whose report connection is already gone has already orphan-exited.
// Stop sends a mini-done to each mini-agent (clean exit) and closes the
// report listener's connections. It is idempotent (the DestServer's release
// op and its Close both call it).
func (s *Supervisor) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		close(s.stopped)
		for _, mc := range s.minis {
			_ = SendLine(mc.w, MiniDoneReq{Op: "mini-done"})
			_ = mc.w.Flush()
			_ = mc.conn.Close()
		}
		s.minis = make(map[int]*miniConn)
	})
}

// Stopped is closed when Stop is called (the run ended).
func (s *Supervisor) Stopped() <-chan struct{} { return s.stopped }

// Close closes the report listener and any open mini-agent report
// connections (a hard unwind; the mini-agents orphan-exit on the drop).
func (s *Supervisor) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reportL != nil {
		_ = s.reportL.Close()
		s.reportL = nil
	}
	for _, mc := range s.minis {
		_ = mc.conn.Close()
	}
	s.minis = make(map[int]*miniConn)
}
