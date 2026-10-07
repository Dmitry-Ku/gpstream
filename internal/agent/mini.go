package agent

// mini.go — the P2 MINI-AGENT (direct_1to1 mesh).
//
// A mini-agent runs ON ONE destination primary-segment host (NOT the
// coordinator). It is the D=1 generalisation of ServeDirect (direct.go): where
// ServeDirect sits on the coordinator and serves D per-segment slices into D
// utility COPYs, a mini-agent sits on a single segment and serves exactly ONE
// slice (this segment's slice of a unit) into ONE utility-mode COPY into its
// local staging slice. ~70-80% of the data path is shared with direct.go.
//
// Why a per-segment agent (design 04.10): the agent that opens a pg connection
// must run where that connection is made. A segment-direct write must be a
// utility-mode COPY to a LOCAL primary segment, so the agent that performs it
// must live on that segment host. The mini-agent connects to its own segment
// LOCALLY (PrimaryDSN, gp_session_role=utility) and never touches the
// coordinator for data. The coordinator (the dest-agent / P3 supervisor)
// creates the distributed staging table and dispatches units; the WAN leg
// carries only the compressed framed slice (source segreceiver -> mini-agent
// data listener).
//
// Control plane: ONE persistent dial to the supervisor's (dest-agent) report
// listener. The mini-agent speaks a newline-JSON protocol over it:
//   - at start it sends a hello {seg, dataPort, ready} so the supervisor learns
//     this segment's data endpoint (folds it into the segreceivers' ProgramCommand);
//   - per unit it receives a mini-unit request, accepts ONE compressed slice on
//     its data listener, loads it, and reports {unit, srcContrib, dstAcc, rows, ok};
//   - ORPHAN-EXIT: a report-channel drop (= the supervisor died) ends the
//     mini-agent IMMEDIATELY (not by timeout) — a dead supervisor means the run
//     is over, so the mini-agent must not linger.
//
// Staging: the mini-agent does NOT create or truncate the staging table. The
// supervisor (coordinator DSN) does the distributed CREATE TABLE / TRUNCATE
// (the logic already exists for resume re-copy); the mini-agent only performs
// the utility COPY into the existing local slice.
//
// Digests (composable across segments): the report carries the RAW per-row XOR
// accumulators (src_contrib from the slice trailer, dst_acc from the
// destination's own data-path digest), not a final "<rows>:<hex>" string. XOR
// is commutative/associative, so the supervisor (P3) XORs the D mini-agents'
// accumulators and sums their row counts to get the unit's combined source and
// destination digests, then compares them (FR-02).
//
// PG 9.4 (GP6): the utility COPY is `COPY ... FROM STDIN (FORMAT binary)` — no
// ON CONFLICT, no PG 9.5+ syntax. Safe on GP6 (PG 9.4.26).

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/portalloc"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// MiniOptions configures one long-lived mini-agent on a destination segment.
type MiniOptions struct {
	// Seg is this segment's ordinal (0..D-1) — reported in the hello and the
	// per-unit responses (the supervisor correlates reports by segment).
	Seg int
	// DataListen is the address the source segreceivers dial for THIS segment's
	// slice (host:port; port 0 = pick a free port, the real one is reported in
	// the hello). When PortAlloc is enabled the port is restricted to the
	// in-range allocation instead (a strict-firewall run).
	DataListen string
	// PortAlloc restricts the auto-picked (port 0) data listener to an allowed
	// range (nil or disabled = fully automatic). The data port is one of the
	// run's in-range ports (the P1 item 4 "4+D" formula: 3 base ports + D
	// mini-agent data ports, one per destination segment).
	PortAlloc *portalloc.Allocator
	// PrimaryDSN is the utility-mode connection string to THIS segment
	// (gp_session_role=utility). The mini-agent connects to it LOCALLY — it
	// never dials the coordinator for data (design 04.10).
	PrimaryDSN string
	// PG-plane TLS (the mini-agent is a client of its own segment).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Data-plane TLS for the segreceiver<->mini-agent socket (mini-agent = server).
	DataTLSCert, DataTLSKey, DataTLSCA, DataTLSMinTLS string
	// ReportAddr is the supervisor's (dest-agent on the coordinator) report
	// listener address. The mini-agent dials it ONCE for the whole run
	// (persistent) and sends/receives control lines over it.
	ReportAddr string
	// AcceptTimeout bounds the wait for a unit's slice connection;
	// StreamTimeout bounds one unit's whole serve.
	AcceptTimeout, StreamTimeout time.Duration
	// SharedListen, when non-nil, is a pre-bound data listener the mini-agent
	// accepts from (the runner owns and closes it). When nil the mini-agent
	// binds DataListen / PortAlloc itself.
	SharedListen net.Listener
}

// MiniResult is what a mini-agent reports after it exits (clean "mini-done" or
// orphan-exit on a report-channel drop).
type MiniResult struct {
	Seg int
	// UnitsServed is the number of units the mini-agent loaded before exit.
	UnitsServed int
	// Rows is the summed authoritative row count (the utility COPY command tags).
	Rows int64
	// Wire is the summed on-network byte volume read from the slice connections.
	Wire uint64
	// Logical is the summed decompressed byte volume served into the COPYs.
	Logical uint64
	// DataAddr is the real bound data address (auto :0 mode: the supervisor
	// learns this segment's endpoint from the hello / this).
	DataAddr string
}

// --- Report-protocol messages (newline-JSON over the persistent channel) ---

// MiniHello is the mini-agent's first message on the report channel (the
// supervisor learns this segment's data endpoint from it).
type MiniHello struct {
	Op       string `json:"op"` // "mini-hello"
	Seg      int    `json:"seg"`
	DataPort int    `json:"data_port"`
	Ready    bool   `json:"ready"`
}

// MiniUnitReq is the supervisor's per-unit instruction to a mini-agent: accept
// one compressed slice on the data listener and load it into the named staging
// slice (which the supervisor has already created/truncated on the coordinator).
type MiniUnitReq struct {
	Op           string `json:"op"` // "mini-unit"
	ReqID        uint64 `json:"req_id,omitempty"`
	UnitID       uint32 `json:"unit_id,omitempty"`
	JobID        uint32 `json:"job_id,omitempty"`
	Schema       string `json:"schema"`
	Staging      string `json:"staging"`
	Compress     string `json:"compress"`
	VerifyDigest bool   `json:"verify_digest"`
	// SourceSegments (S) — how many source segreceivers stream sub-streams to
	// this mini-agent for this unit (0 or 1 = direct: exactly one slice).
	SourceSegments int32 `json:"source_segments,omitempty"`
	// Workers (W) — sub-streams per source segment (each source segment fans
	// its rows to this segment over W sub-streams).
	Workers int `json:"workers,omitempty"`
}

// MiniUnitResp is the mini-agent's per-unit report over the report channel.
// SrcContrib and DstAcc are RAW per-row XOR accumulators (hex) — composable:
// the supervisor XORs the D mini-agents' values and sums their Rows to get the
// unit's combined source / destination digest (FR-02), then compares them.
type MiniUnitResp struct {
	Op          string `json:"op"` // "mini-unit-resp"
	ReqID       uint64 `json:"req_id,omitempty"`
	Ok          bool   `json:"ok"`
	UnitID      uint32 `json:"unit_id,omitempty"`
	SrcContrib  string `json:"src_contrib,omitempty"` // hex: the slice trailer's per-row XOR accumulator (source's contribution for this segment's slice)
	DstAcc      string `json:"dst_acc,omitempty"`     // hex: the destination's per-row XOR accumulator over the loaded bytes (empty when verify is off)
	Rows        int64  `json:"rows,omitempty"`        // the utility COPY command tag (authoritative)
	TrailerRows int64  `json:"trailer_rows,omitempty"`
	Wire        uint64 `json:"wire,omitempty"`
	Logical     uint64 `json:"logical,omitempty"`
	Error       string `json:"error,omitempty"`
}

// MiniDoneReq tells the mini-agent to end the run (a clean exit).
type MiniDoneReq struct {
	Op string `json:"op"` // "mini-done"
}

// miniSliceWriter routes each decompressed chunk to the single utility-COPY
// pipe and, when verify is on, tees it into a ContentDigestWriter (the
// destination's data-path digest, O(1) memory). It is the D=1 analogue of
// directSliceWriter (which routes by content-id to D pipes) + digestReader.
type miniSliceWriter struct {
	pw    *io.PipeWriter
	dsum  *transfer.ContentDigestWriter
	logic *atomic.Uint64
}

func (w *miniSliceWriter) Write(contentID int16, p []byte) error {
	if w.dsum != nil {
		if _, err := w.dsum.Write(p); err != nil {
			return err
		}
	}
	if w.logic != nil {
		w.logic.Add(uint64(len(p)))
	}
	_, err := w.pw.Write(p)
	return err
}

// ServeMini runs one long-lived mini-agent: it binds its data listener, dials
// the supervisor's report listener, sends the hello, then serves units until it
// receives "mini-done" (clean exit) or the report channel drops (orphan-exit:
// the supervisor died, so the mini-agent exits immediately). It returns the
// accumulated result and nil on a clean/orphan exit, or an error on a startup
// failure (bind / report dial / hello).
func ServeMini(ctx context.Context, o MiniOptions) (MiniResult, error) {
	var out MiniResult
	out.Seg = o.Seg
	if o.AcceptTimeout <= 0 {
		o.AcceptTimeout = 5 * time.Minute
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 6 * time.Hour
	}

	// 1. Bind the data listener (in-range when PortAlloc is enabled). The source
	// segreceivers dial this address for this segment's slices.
	var rawL net.Listener
	var lerr error
	if o.SharedListen != nil {
		rawL = o.SharedListen // long-lived: the runner owns it, not the serve
	} else if o.PortAlloc != nil && o.PortAlloc.Enabled() {
		host := "0.0.0.0"
		if h, _, serr := net.SplitHostPort(o.DataListen); serr == nil && h != "" {
			host = h
		}
		rawL, lerr = o.PortAlloc.Listen(host)
	} else {
		rawL, lerr = net.Listen("tcp", o.DataListen)
	}
	if lerr != nil {
		return out, fmt.Errorf("mini: listen %s: %w", o.DataListen, lerr)
	}
	if o.SharedListen == nil {
		defer rawL.Close()
	}
	out.DataAddr = rawL.Addr().String()

	// Data-plane TLS: wrap the data listener ONCE (every slice connection
	// handshakes without the per-unit serve wrapping it again).
	l := net.Listener(rawL)
	if transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
		scfg, serr := transfer.ServerTLSConfig(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
		if serr != nil {
			return out, fmt.Errorf("mini: data tls config: %w", serr)
		}
		l = tls.NewListener(rawL, scfg)
	}

	// 2. Dial the supervisor's report listener (persistent, one for the run) +
	// send the hello. A failure here is fatal: the mini-agent cannot report, so
	// it cannot function (the supervisor sees the process exit).
	dataPort := portFromAddr(rawL.Addr().String())
	report, rerr := net.DialTimeout("tcp", o.ReportAddr, 15*time.Second)
	if rerr != nil {
		return out, fmt.Errorf("mini: dial report %s: %w", o.ReportAddr, rerr)
	}
	defer report.Close()
	rw := bufio.NewWriter(report)
	rr := bufio.NewReader(report)
	hello := MiniHello{Op: "mini-hello", Seg: o.Seg, DataPort: dataPort, Ready: true}
	if err := SendLine(rw, hello); err != nil {
		return out, fmt.Errorf("mini: send hello: %w", err)
	}
	if err := rw.Flush(); err != nil {
		return out, fmt.Errorf("mini: flush hello: %w", err)
	}

	// 3. Serve units until "mini-done" (clean) or the report channel drops
	// (orphan-exit: supervisor death -> exit immediately, not by timeout).
	for {
		var req MiniUnitReq
		if err := recvLine(rr, &req); err != nil {
			// io.EOF (the supervisor closed the channel) or a transport error
			// (the supervisor process died): orphan-exit. A clean exit — the
			// run is over, so the mini-agent unwinds (defer report.Close()).
			return out, nil
		}
		switch req.Op {
		case "mini-unit":
			// P4: routed (S>1) units take the S×W merge path; direct units
			// take the P2 single-slice path.
			var resp MiniUnitResp
			if req.SourceSegments > 1 {
				resp = serveMiniUnitRouted(ctx, o, l, req)
			} else {
				resp = serveMiniUnit(ctx, o, l, req)
			}
			out.UnitsServed++
			out.Rows += resp.Rows
			out.Wire += resp.Wire
			out.Logical += resp.Logical
			if err := SendLine(rw, resp); err != nil {
				return out, fmt.Errorf("mini: report unit %d: %w", req.UnitID, err)
			}
			if err := rw.Flush(); err != nil {
				return out, fmt.Errorf("mini: flush report: %w", err)
			}
		case "mini-done":
			return out, nil
		default:
			// An unknown op: report an error for this line but keep serving
			// (a stray/corrupt line should not orphan a healthy mini-agent).
			_ = SendLine(rw, MiniUnitResp{Op: "mini-unit-resp", ReqID: req.ReqID, Ok: false, Error: fmt.Sprintf("unknown op %q", req.Op)})
			_ = rw.Flush()
		}
	}
}

// serveMiniUnit serves ONE unit on a mini-agent: it accepts ONE compressed
// slice on the data listener, decompresses it (teeing the destination digest
// when verify is on) into a single utility-mode COPY into the local staging
// slice, takes srcContrib from the slice trailer, and returns the report. It
// assumes the staging slice already exists (the supervisor created/truncated it
// on the coordinator); the mini-agent only performs the utility COPY.
func serveMiniUnit(ctx context.Context, o MiniOptions, l net.Listener, req MiniUnitReq) MiniUnitResp {
	resp := MiniUnitResp{Op: "mini-unit-resp", ReqID: req.ReqID, UnitID: req.UnitID}
	fail := func(format string, a ...any) MiniUnitResp {
		resp.Ok = false
		resp.Error = fmt.Sprintf(format, a...)
		return resp
	}
	mode, _, perr := compress.Parse(req.Compress)
	if perr != nil {
		return fail("compress %q: %v", req.Compress, perr)
	}
	sto := o.StreamTimeout
	sctx, scancel := context.WithTimeout(ctx, sto)
	defer scancel()

	// 1. Accept the ONE slice connection (bounded by the stream timeout).
	type acceptOut struct {
		c   net.Conn
		err error
	}
	acc := make(chan acceptOut, 1)
	go func() {
		c, err := l.Accept()
		acc <- acceptOut{c: c, err: err}
	}()
	var conn net.Conn
	select {
	case a := <-acc:
		if a.err != nil {
			return fail("accept slice: %v", a.err)
		}
		conn = a.c
	case <-sctx.Done():
		return fail("accept slice: %v", sctx.Err())
	}
	defer conn.Close()
	cc := &countingConn{c: conn}

	// 2. Single utility-COPY pipe + COPY goroutine. The staging slice exists
	// (the supervisor created it on the coordinator; it is replicated to this
	// segment). The COPY goroutine starts BEFORE any slice is written (an
	// unbuffered pipe write would otherwise block until the COPY reads).
	pr, pw := io.Pipe()
	copyRes := make(chan segCopyRes, 1)
	go func() {
		defer pr.Close() // unblock the slice drain on exit
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

	// 3. Decompress the slice into the pipe, teeing the destination digest when
	// verify is on (O(1) memory, on the data path — the P9-E pattern).
	var dsum *transfer.ContentDigestWriter
	var logicalN atomic.Uint64
	w := &miniSliceWriter{pw: pw, dsum: dsum, logic: &logicalN}
	if req.VerifyDigest {
		dsum = transfer.NewContentDigest()
		w.dsum = dsum
	}
	res := &segstream.SliceResult{}
	derr := segstream.ReadStreamCompressed(sctx, cc, res, mode, w.Write)
	wire := cc.Bytes()

	// 4. Close the pipe (EOF or error) so the COPY sees end-of-stream, then wait
	// for the utility COPY and take its authoritative row count.
	if derr != nil {
		_ = pw.CloseWithError(derr)
	} else {
		_ = pw.Close()
	}
	cres := <-copyRes

	resp.Wire = wire
	resp.Logical = logicalN.Load()
	resp.TrailerRows = res.Rows
	if derr != nil {
		return fail("slice: %v", derr)
	}
	if cres.err != nil {
		return fail("utility copy: %v", cres.err)
	}
	// srcContrib = the slice trailer's raw per-row XOR accumulator (the source's
	// contribution for this segment's slice); the supervisor XORs the D values.
	if res.Trailer != nil && res.Err == nil {
		resp.SrcContrib = hexBytes(res.Digest)
	}
	if res.Rows != cres.rows {
		return fail("trailer rows %d != dest rows %d", res.Rows, cres.rows)
	}
	resp.Rows = cres.rows
	// dst_acc = the destination's own per-row XOR accumulator over the loaded
	// bytes (composable; empty when verify is off).
	if req.VerifyDigest && dsum != nil {
		acc32, accRows, aerr := dsum.Accumulator()
		if aerr != nil {
			return fail("destination digest: %v", aerr)
		}
		if accRows != cres.rows {
			return fail("digest rows %d != dest rows %d", accRows, cres.rows)
		}
		resp.DstAcc = hexBytes(acc32)
	}
	resp.Ok = true
	return resp
}

// portFromAddr extracts the port from a "host:port" address (0 on parse error).
func portFromAddr(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(p)
	return n
}

// hexBytes renders a raw byte array as a lower-case hex string (the composable
// digest form the supervisor XORs across segments).
func hexBytes(b [32]byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hexd[c>>4], hexd[c&0x0f])
	}
	return string(out)
}
