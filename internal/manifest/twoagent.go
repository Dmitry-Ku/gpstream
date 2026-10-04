package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// twoAgentRunner is the control-plane interface the Manager uses to drive the
// two-agent data path (ADR-002 remediation, P3). It abstracts the two agent
// processes so the same orchestration runs (a) against the real agent binaries
// (production / live e2e) and (b) in-process (integration test). The data COPY
// happens entirely inside the agents (source agent -> destination agent over a
// real, compressed socket); the runner only exchanges control messages and the
// destination counts. The controller holds NO data connection: it never runs a
// COPY — the source agent does `COPY ... TO STDOUT` (source DB) and the
// destination agent does `COPY ... FROM STDIN` (destination DB).
type TwoAgentRunner interface {
	// Start launches the (long-lived) source agent and connects the control
	// client. The source agent owns the source pg pool + the run-wide FR-02
	// snapshot (a REPEATABLE READ tx lives on one server-side backend, so one
	// process must run every unit's data COPY under it).
	Start(ctx context.Context) error
	// Stream launches a destination agent for the unit, instructs the source
	// agent to stream the unit to it, and waits for the destination to finish
	// the COPY. It returns the authoritative row count + the wire/logical
	// volume. The destination staging table is created + loaded by the dest
	// agent; the swap is NOT performed (the Manager decides it).
	Stream(ctx context.Context, req unitStreamRequest) (rows int64, wire, logical uint64, err error)
	// StreamSegment launches a destination agent in SEGMENT mode (P4,
	// on_segment) and instructs the source agent to trigger the ON SEGMENT
	// dispatch: the kernel launches the segreceiver on every primary source
	// segment, and each streams its slice compressed to the destination
	// agent, which merges them (SliceMux) and loads the staging table. It
	// returns the authoritative (destination) row count + the on-network
	// wire volume + the logical (decompressed) volume. The swap is NOT
	// performed (the Manager decides it).
	StreamSegment(ctx context.Context, req unitSegmentRequest) (rows int64, wire, logical uint64, err error)
	// StreamRouted launches a destination agent in ROUTED mode (P9, S≠D)
	// and instructs the source agent to trigger the ON SEGMENT dispatch with
	// routing enabled: each source segreceiver routes its slice to its
	// destination segment (cdbhash + JCH over the distribution key) and fans
	// each (source-seg,dest-seg) pair over W sub-streams (S×D×W segment-to-
	// segment connections). NEITHER cluster coordinator is in the data path.
	// The destination agent merges the S×W sub-streams addressed to each
	// segment and performs D utility-mode per-segment COPYs. It returns the
	// authoritative (destination) row count + the wire + logical volume.
	StreamRouted(ctx context.Context, req unitRoutedRequest) (rows int64, wire, logical uint64, err error)
	// Checksum asks the source agent (which holds the FR-02 snapshot) for the
	// table's content checksum (the --verify path).
	Checksum(ctx context.Context, schema, table string) (string, error)
	// Release ends the run (commits the source snapshot) and stops the agents.
	Release(ctx context.Context) error
	// Bench streams ONE table through the two-agent source read (the F-14
	// input-stream benchmark, P7): the source agent runs the real
	// COPY ... TO STDOUT -> frame v2 -> compression -> socket pipeline and
	// reports the row count + on-wire + logical volume. The runner receives
	// the stream on a drain sink (it reads the UnitSpec header, decompresses
	// and discards the frames) — no destination database is involved. The
	// source agent's report is authoritative.
	Bench(ctx context.Context, unitID, jobID uint32, schema, table, compress string) (rows int64, wire, logical uint64, err error)
}

// benchViaCtrl is the shared Bench implementation for both runners: the
// runner's control client is already connected (Start), so it pre-binds a
// loopback drain sink, instructs the long-lived source agent to stream the
// unit to it, and returns the source agent's own report (rows + on-wire +
// logical volume). The sink reads the UnitSpec header, decompresses the
// framed stream and discards it; a source that dies mid-stream surfaces as a
// sink error (the io.Copy hits EOF only on a clean stream end).
func benchViaCtrl(ctrl *agent.ControlClient, ctx context.Context, unitID, jobID uint32, schema, table, compressStr string) (int64, uint64, uint64, error) {
	listen, err := prebindLoopback()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("bench: pre-bind drain sink: %w", err)
	}
	sinkDone := make(chan error, 1)
	go func() {
		l, lerr := net.Listen("tcp", listen)
		if lerr != nil {
			sinkDone <- lerr
			return
		}
		defer l.Close()
		conn, aerr := l.Accept()
		if aerr != nil {
			sinkDone <- aerr
			return
		}
		defer conn.Close()
		spec, serr := agent.ReadUnitSpec(conn)
		if serr != nil {
			sinkDone <- serr
			return
		}
		mode, _, perr := compress.Parse(spec.Compress)
		if perr != nil {
			sinkDone <- perr
			return
		}
		fr := transfer.NewFramedReader(conn, compress.NewDec(mode))
		if _, cerr := io.Copy(io.Discard, fr); cerr != nil {
			sinkDone <- cerr
			return
		}
		sinkDone <- nil
	}()
	sreq := agent.StreamReq{
		Op:       "stream",
		Schema:   schema,
		Table:    table,
		JobID:    jobID,
		UnitID:   unitID,
		Compress: compressStr,
		DestAddr: listen,
	}
	resp, serr := ctrl.Stream(sreq)
	if serr != nil {
		return 0, 0, 0, fmt.Errorf("bench: source agent stream %s.%s: %w", schema, table, serr)
	}
	if !resp.Ok {
		// The source finished (or failed) its stream; let the sink goroutine
		// drain its error/EOF before reporting.
		<-sinkDone
		return 0, 0, 0, fmt.Errorf("bench: source agent stream %s.%s: %s", schema, table, resp.Error)
	}
	if sinkErr := <-sinkDone; sinkErr != nil {
		return 0, 0, 0, fmt.Errorf("bench: drain sink %s.%s: %w", schema, table, sinkErr)
	}
	return resp.Rows, resp.Wire, resp.Logical, nil
}

func (r *processTwoAgentRunner) Bench(ctx context.Context, unitID, jobID uint32, schema, table, compress string) (int64, uint64, uint64, error) {
	return benchViaCtrl(r.ctrl, ctx, unitID, jobID, schema, table, compress)
}

func (r *InProcTwoAgentRunner) Bench(ctx context.Context, unitID, jobID uint32, schema, table, compress string) (int64, uint64, uint64, error) {
	return benchViaCtrl(r.ctrl, ctx, unitID, jobID, schema, table, compress)
}

// unitStreamRequest is the Manager's per-unit instruction to the runner.
type unitStreamRequest struct {
	agent.StreamReq
	// DstConn is the destination postgres conn string the dest agent uses.
	DstConn string
	// DstTLS are the dest agent's pg-plane TLS settings.
	DstCert, DstKey, DstCA string
}

// unitSegmentRequest is the Manager's per-unit instruction for the P4
// on_segment data path: the source agent triggers the ON SEGMENT dispatch and
// the destination agent (segment mode) merges the N compressed slices.
type unitSegmentRequest struct {
	agent.OnSegmentReq
	// Spec is the unit's destination shape the dest agent builds the staging
	// table from (the kernel-launched segreceivers cannot carry it).
	Spec agent.UnitSpec
	// Segments is the number of source primary segments.
	Segments int
	// SegListen is the routable base listen address the segreceivers dial;
	// the destination agent binds SegListen's host with port base+unitID.
	// (The segreceivers run on the SOURCE segment hosts — a different machine
	// than the destination agent — so the port must be reachable over the
	// network, unlike the P3 coordinator path which is loopback.)
	SegListen string
	// DstConn is the destination postgres conn string the dest agent uses.
	DstConn string
	// DstTLS are the dest agent's pg-plane TLS settings.
	DstCert, DstKey, DstCA string
	// DataTLS is the agent<->agent data-plane TLS (source segreceivers =
	// clients, dest agent = server). Empty = plain TCP.
	DataTLSCert, DataTLSKey, DataTLSCA string
	// DirectWrite (P8, direct_1to1 fast-path) makes the destination agent
	// load the N content-tagged slices with N per-segment utility-mode COPYs
	// (ServeDirect) instead of merging them (SliceMux) into one coordinator
	// COPY (ServeSegments). It is only set when the plan proves the route is
	// direct_1to1 (S==D, equal hash keys, same major): then source segment
	// s's slice == destination segment s's slice, so no merge and no
	// destination re-distribution are needed. The SOURCE side is unchanged
	// (the kernel's ON SEGMENT dispatch streams the same content-tagged
	// slices either way); only the destination's load method changes.
	// DirectLaunch carries the per-segment destination DSNs + D for ServeDirect.
	DirectWrite  bool
	DirectLaunch agent.DirectLaunch
}

// unitRoutedRequest is the Manager's per-unit instruction for the P9 routed
// (S≠D) data path: the source agent triggers the ON SEGMENT dispatch with
// routing (the segreceivers route each row to its destination segment and
// fan each pair over W sub-streams); the destination agent (routed mode)
// merges the per-segment sub-streams and performs D utility-mode COPYs.
type unitRoutedRequest struct {
	agent.OnSegmentReq // Routed=true, RouteKey/RouteD/RouteWorkers set
	// Launch is the destination agent's routed launch payload (staging
	// shape + S + D + W + per-segment DSNs).
	Launch agent.RoutedLaunch
	// Segments is the number of source primary segments (S).
	Segments int
	// SegListen is the routable base listen address the segreceivers dial;
	// the destination agent binds SegListen's host with port base+unitID.
	SegListen string
	// DstConn is the destination COORDINATOR conn (staging DDL + swap); the
	// per-segment data COPYs use Launch.DstDSNs.
	DstConn                string
	DstCert, DstKey, DstCA string
	// DataTLS is the agent<->agent data-plane TLS (segreceivers = clients).
	DataTLSCert, DataTLSKey, DataTLSCA string
}

// processTwoAgentRunner drives the real gpstream-source-agent /
// gpstream-dest-agent binaries as subprocesses (the production / live-e2e
// path). The source agent is long-lived (one process, the run-wide FR-02
// snapshot); the destination agent is per-unit. The controller pre-binds a
// loopback port for each socket and hands it to the agent, so it never has to
// parse the agent's output to learn an address.
type processTwoAgentRunner struct {
	cfg       *config.Config
	ctrl      *agent.ControlClient
	srcCmd    *exec.Cmd
	srcCancel context.CancelFunc
}

func newProcessTwoAgentRunner(cfg *config.Config) *processTwoAgentRunner {
	return &processTwoAgentRunner{cfg: cfg}
}

// Start launches the long-lived source agent (--serve) and connects the control
// client to its control socket (a pre-bound loopback port).
func (r *processTwoAgentRunner) Start(ctx context.Context) error {
	ctrlAddr, err := prebindLoopback()
	if err != nil {
		return fmt.Errorf("two-agent: pre-bind control port: %w", err)
	}
	sctx, cancel := context.WithCancel(ctx)
	r.srcCancel = cancel
	args := []string{
		"--src", r.cfg.Source,
		"--snapshot", r.snapshotMode(),
		"--serve", ctrlAddr,
		"--connect-timeout", "10s",
	}
	if r.cfg.SSLCert != "" {
		args = append(args, "--ssl-cert", r.cfg.SSLCert, "--ssl-key", r.cfg.SSLKey)
	}
	if r.cfg.SSLCA != "" {
		args = append(args, "--ssl-ca", r.cfg.SSLCA)
	}
	cmd := exec.CommandContext(sctx, r.cfg.SourceAgentBinary, args...)
	// The agent's report lines are not needed on the control plane (they carry
	// the same data), but discarding stderr entirely hides a failed agent from
	// the operator (a crash, a panic, or a SIGQUIT goroutine dump on a hang).
	// When GPSTREAM_AGENT_STDERR is set to a path, the source agent's stderr is
	// appended there for diagnosis; otherwise it is discarded as before.
	if p := os.Getenv("GPSTREAM_AGENT_STDERR"); p != "" {
		if f, ferr := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); ferr == nil {
			cmd.Stderr = f
		} else {
			cmd.Stderr = io.Discard
		}
	} else {
		cmd.Stderr = io.Discard
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("two-agent: start source agent: %w", err)
	}
	r.srcCmd = cmd

	// The source agent binds ctrlAddr and starts accepting; dial it. The bind
	// is fast, but dial with a short retry to tolerate the agent's startup.
	client, err := dialControlRetry(ctx, ctrlAddr, 15*time.Second)
	if err != nil {
		_ = cmd.Process.Kill()
		cancel()
		return fmt.Errorf("two-agent: control dial %s: %w", ctrlAddr, err)
	}
	r.ctrl = client
	return nil
}

// Stream launches a per-unit destination agent on a pre-bound port, waits for
// it to actually be listening, instructs the source agent to stream the unit to
// it, and waits for the destination to finish the COPY. The destination agent
// is the authoritative row-count source (the single-controller path uses the
// destination's count too); the wire volume comes from the source agent (the
// bytes it actually wrote to the socket). The swap is NOT performed here — the
// Manager decides it.
func (r *processTwoAgentRunner) Stream(ctx context.Context, req unitStreamRequest) (int64, uint64, uint64, error) {
	// 1. Pre-bind a loopback port and launch the per-unit destination agent.
	listen, err := prebindLoopback()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: pre-bind data port: %w", err)
	}
	var dstderr bytes.Buffer
	dargs := []string{
		"--dest", req.DstConn,
		"--listen", listen,
		"--accept-timeout", "5m",
	}
	if req.DstCert != "" {
		dargs = append(dargs, "--ssl-cert", req.DstCert, "--ssl-key", req.DstKey)
	}
	if req.DstCA != "" {
		dargs = append(dargs, "--ssl-ca", req.DstCA)
	}
	dcmd := exec.CommandContext(ctx, r.cfg.DestAgentBinary, dargs...)
	dcmd.Stderr = &dstderr
	dcmd.Stdout = io.Discard
	if err := dcmd.Start(); err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: start dest agent: %w", err)
	}
	// Reap the dest agent once it exits (it exits after serving one unit).
	done := make(chan error, 1)
	go func() { done <- dcmd.Wait() }()
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the source agent to stream this unit to the destination agent.
	//    The source agent retries its dial while the destination is still
	//    binding its listen socket (dialDestRetry), so there is no launch/dial
	//    race to wait out here.
	sreq := req.StreamReq
	sreq.DestAddr = listen
	resp, err := r.ctrl.Stream(sreq)
	if err != nil {
		return fail("two-agent: stream unit %d: %w", sreq.UnitID, err)
	}
	if !resp.Ok {
		return fail("two-agent: source agent stream unit %d: %s", sreq.UnitID, resp.Error)
	}

	// 4. Wait for the destination to finish the COPY (it exits after serving).
	var werr error
	select {
	case werr = <-done:
	case <-time.After(r.streamDeadline()):
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d: timed out", sreq.UnitID)
	}
	if werr != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d: %v (stderr: %s)", sreq.UnitID, werr, dstderr.String())
	}

	// 5. The destination is the authoritative row-count source; the wire volume
	// comes from the source agent (the bytes it wrote to the socket).
	drows, dlogical, perr := parseDestReport(dstderr.String())
	if perr != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: parse dest report unit %d: %v (stderr: %s)", sreq.UnitID, perr, dstderr.String())
	}
	return drows, resp.Wire, dlogical, nil
}

// StreamSegment (P4) launches a destination agent in SEGMENT mode, instructs
// the source agent to trigger the ON SEGMENT dispatch (the kernel launches the
// segreceiver on every primary source segment; each streams its slice
// compressed to the destination agent), and waits for the destination to
// finish merging + loading. The destination is the authoritative row-count
// source; the wire volume is the raw on-network byte volume it read from the
// N segment connections. The swap is NOT performed — the Manager decides it.
func (r *processTwoAgentRunner) StreamSegment(ctx context.Context, req unitSegmentRequest) (int64, uint64, uint64, error) {
	// 1. Bind the segment data port. The segreceivers run on the SOURCE
	//    segment hosts (a different machine than this destination agent), so
	//    the port must be reachable over the network: bind the --seg-listen
	//    host with port base+unitID (the same address the kernel's COPY ON
	//    SEGMENT embeds in the segreceiver --listen flag).
	listen, err := segstream.UnitListen(req.SegListen, uint32(req.OnSegmentReq.UnitID))
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: segment listen %q: %w", req.SegListen, err)
	}
	var dstderr bytes.Buffer
	var dargs []string
	if req.DirectWrite {
		// P8 direct_1to1 fast-path: the destination loads the N content-tagged
		// slices with N per-segment utility-mode COPYs (ServeDirect) — no
		// merge, no re-distribution. The source side is the same ON SEGMENT
		// dispatch (the kernel streams the same content-tagged slices).
		launchJSON, lerr := json.Marshal(req.DirectLaunch)
		if lerr != nil {
			return 0, 0, 0, fmt.Errorf("two-agent: marshal direct launch (1to1): %w", lerr)
		}
		dargs = []string{
			"--dest", req.DstConn,
			"--listen", listen,
			"--direct-spec", string(launchJSON),
			"--accept-timeout", "5m",
		}
	} else {
		specJSON, serr := json.Marshal(req.Spec)
		if serr != nil {
			return 0, 0, 0, fmt.Errorf("two-agent: marshal segment spec: %w", serr)
		}
		dargs = []string{
			"--dest", req.DstConn,
			"--listen", listen,
			"--segments", strconv.Itoa(req.Segments),
			"--spec", string(specJSON),
			"--accept-timeout", "5m",
		}
	}
	if req.DstCert != "" {
		dargs = append(dargs, "--ssl-cert", req.DstCert, "--ssl-key", req.DstKey)
	}
	if req.DstCA != "" {
		dargs = append(dargs, "--ssl-ca", req.DstCA)
	}
	if req.DataTLSCert != "" {
		dargs = append(dargs, "--data-tls-cert", req.DataTLSCert, "--data-tls-key", req.DataTLSKey)
	}
	if req.DataTLSCA != "" {
		dargs = append(dargs, "--data-tls-ca", req.DataTLSCA)
	}
	dcmd := exec.CommandContext(ctx, r.cfg.DestAgentBinary, dargs...)
	dcmd.Stderr = &dstderr
	dcmd.Stdout = io.Discard
	if err := dcmd.Start(); err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: start dest agent (segment): %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- dcmd.Wait() }()
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the source agent to run the ON SEGMENT dispatch. The
	//    segreceivers retry their dial while the destination is still binding
	//    (dialDestRetry in the segreceiver), so there is no launch/dial race.
	o := req.OnSegmentReq
	o.DestAddr = listen
	oresp, err := r.ctrl.OnSegment(o)
	if err != nil {
		return fail("two-agent: onsegment unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		return fail("two-agent: source agent onsegment unit %d: %s", o.UnitID, oresp.Error)
	}

	// 3. Wait for the destination to finish the COPY (it exits after serving).
	var werr error
	select {
	case werr = <-done:
	case <-time.After(r.streamDeadline()):
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (segment): timed out", o.UnitID)
	}
	if werr != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (segment): %v (stderr: %s)", o.UnitID, werr, dstderr.String())
	}

	// 4. Parse the destination report: "gpstream-dest-agent: unit=N rows=R
	// logical=L wire=W <verdict> ok".
	for _, line := range strings.Split(dstderr.String(), "\n") {
		var unit, rows, logical, wire int64
		var verdict string
		if n, perr := fmt.Sscanf(line, "gpstream-dest-agent: unit=%d rows=%d logical=%d wire=%d %s ok",
			&unit, &rows, &logical, &wire, &verdict); perr == nil && n == 5 {
			return rows, uint64(wire), uint64(logical), nil
		}
	}
	return 0, 0, 0, fmt.Errorf("two-agent: parse dest (segment) report unit %d: %s", o.UnitID, dstderr.String())
}

// StreamRouted (P9) launches a destination agent in ROUTED mode (S≠D),
// instructs the source agent to trigger the ON SEGMENT dispatch with routing
// (each segreceiver routes its slice to its destination segment and fans each
// (s,d) pair over W sub-streams), and waits for the destination to finish its
// D utility-mode per-segment COPYs. Neither cluster coordinator is in the
// data path. The destination is the authoritative row-count source.
func (r *processTwoAgentRunner) StreamRouted(ctx context.Context, req unitRoutedRequest) (int64, uint64, uint64, error) {
	// 1. Bind the segment data port (routable: the segreceivers run on the
	// source segment hosts).
	listen, err := segstream.UnitListen(req.SegListen, uint32(req.OnSegmentReq.UnitID))
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: routed listen %q: %w", req.SegListen, err)
	}
	var dstderr bytes.Buffer
	launchJSON, lerr := json.Marshal(req.Launch)
	if lerr != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: marshal routed launch: %w", lerr)
	}
	dargs := []string{
		"--dest", req.DstConn,
		"--listen", listen,
		"--routed-spec", string(launchJSON),
		"--accept-timeout", "5m",
	}
	if req.DstCert != "" {
		dargs = append(dargs, "--ssl-cert", req.DstCert, "--ssl-key", req.DstKey)
	}
	if req.DstCA != "" {
		dargs = append(dargs, "--ssl-ca", req.DstCA)
	}
	if req.DataTLSCert != "" {
		dargs = append(dargs, "--data-tls-cert", req.DataTLSCert, "--data-tls-key", req.DataTLSKey)
	}
	if req.DataTLSCA != "" {
		dargs = append(dargs, "--data-tls-ca", req.DataTLSCA)
	}
	dcmd := exec.CommandContext(ctx, r.cfg.DestAgentBinary, dargs...)
	dcmd.Stderr = &dstderr
	dcmd.Stdout = io.Discard
	if err := dcmd.Start(); err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: start dest agent (routed): %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- dcmd.Wait() }()
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the source agent to run the ON SEGMENT dispatch with
	// routing. The segreceivers retry their dials while the destination is
	// still binding (the dial-retry loop in runRouted).
	o := req.OnSegmentReq
	o.DestAddr = listen
	oresp, err := r.ctrl.OnSegment(o)
	if err != nil {
		return fail("two-agent: onsegment (routed) unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		return fail("two-agent: source agent onsegment (routed) unit %d: %s", o.UnitID, oresp.Error)
	}

	// 3. Wait for the destination to finish the COPY.
	var werr error
	select {
	case werr = <-done:
	case <-time.After(r.streamDeadline()):
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (routed): timed out", o.UnitID)
	}
	if werr != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (routed): %v (stderr: %s)", o.UnitID, werr, dstderr.String())
	}

	// 4. Parse the destination report: "gpstream-dest-agent: unit=N rows=R
	// logical=L wire=W <verdict> ok".
	for _, line := range strings.Split(dstderr.String(), "\n") {
		var unit, rows, logical, wire int64
		var verdict string
		if n, perr := fmt.Sscanf(line, "gpstream-dest-agent: unit=%d rows=%d logical=%d wire=%d %s ok",
			&unit, &rows, &logical, &wire, &verdict); perr == nil && n == 5 {
			return rows, uint64(wire), uint64(logical), nil
		}
	}
	return 0, 0, 0, fmt.Errorf("two-agent: parse dest (routed) report unit %d: %s", o.UnitID, dstderr.String())
}

// Checksum asks the source agent (FR-02 snapshot) for the table's checksum.
func (r *processTwoAgentRunner) Checksum(ctx context.Context, schema, table string) (string, error) {
	resp, err := r.ctrl.Checksum(schema, table)
	if err != nil {
		return "", fmt.Errorf("two-agent: checksum %s.%s: %w", schema, table, err)
	}
	if !resp.Ok {
		return "", fmt.Errorf("two-agent: source agent checksum %s.%s: %s", schema, table, resp.Error)
	}
	return resp.Checksum, nil
}

// Release ends the run (commits the source snapshot) and stops the agents.
func (r *processTwoAgentRunner) Release(ctx context.Context) error {
	var cerr error
	if r.ctrl != nil {
		if _, err := r.ctrl.Release(); err != nil {
			cerr = err
		}
		_ = r.ctrl.Close()
		r.ctrl = nil
	}
	if r.srcCmd != nil {
		done := make(chan struct{})
		go func() { _ = r.srcCmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = r.srcCmd.Process.Kill()
		}
		r.srcCmd = nil
	}
	if r.srcCancel != nil {
		r.srcCancel()
		r.srcCancel = nil
	}
	return cerr
}

// streamDeadline bounds the per-unit destination serve (the agents default to a
// 6-hour stream timeout; the controller's own unit deadline dominates in
// practice).
func (r *processTwoAgentRunner) streamDeadline() time.Duration {
	return 6*time.Hour + time.Minute
}

// snapshotMode maps the config's FR-02 scope to the agent's snapshot string.
func (r *processTwoAgentRunner) snapshotMode() string {
	// FR-02: the source agent always reads under the DB-scoped REPEATABLE
	// READ snapshot (P7 removed the cluster/per-table scopes).
	return "db"
}

// prebindLoopback binds a throwaway loopback listener to learn a free port,
// releases it, and returns the address. The agent re-binds it; the tiny race
// window (another process grabbing the port) is acceptable on loopback.
func prebindLoopback() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	l.Close()
	return addr, nil
}

// dialControlRetry dials addr until it succeeds or the deadline passes (the
// source agent binds the control socket shortly after it starts).
func dialControlRetry(ctx context.Context, addr string, deadline time.Duration) (*agent.ControlClient, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	const backoff = 50 * time.Millisecond
	var lastErr error
	for {
		client, err := agent.DialControl(deadlineCtx, addr)
		if err == nil {
			return client, nil
		}
		lastErr = err
		select {
		case <-deadlineCtx.Done():
			return nil, fmt.Errorf("%w (last: %v)", deadlineCtx.Err(), lastErr)
		case <-time.After(backoff):
		}
	}
}

// parseDestReport extracts (rows, logical) from the destination agent's stderr
// report. The COORDINATOR two-agent report is four fields —
// "gpstream-dest-agent: unit=N rows=R logical=L <verdict> ok" (no wire=; the
// on-wire volume is the SOURCE agent's, reported separately). The segment
// (P4) and direct (P5) reports carry a fifth wire= field and are parsed by
// their own inline Sscanf in StreamSegment/StreamDirect, not here.
func parseDestReport(stderr string) (rows int64, logical uint64, err error) {
	for _, line := range strings.Split(stderr, "\n") {
		var unit, r, l int64
		var verdict string
		if n, perr := fmt.Sscanf(line, "gpstream-dest-agent: unit=%d rows=%d logical=%d %s ok",
			&unit, &r, &l, &verdict); perr == nil && n == 4 {
			return r, uint64(l), nil
		}
	}
	return 0, 0, fmt.Errorf("no dest report line found")
}

// inProcTwoAgentRunner drives the agent package in-process (the integration
// test path): a long-lived agent.SourceServer (owns the source pool + FR-02
// snapshot) + a per-unit agent.Serve for the destination. The data still flows
// over a REAL TCP socket (source agent -> destination agent), exactly as in the
// binary path — only the process boundary is removed, so the Manager's
// two-agent orchestration (staging name contract, dest-authoritative count,
// wire volume, agent-side checksum) is exercised end to end.
type InProcTwoAgentRunner struct {
	cfg       *config.Config
	srv       *agent.SourceServer
	ctrl      *agent.ControlClient
	srcCancel context.CancelFunc
}

// NewInProcTwoAgentRunner builds an in-process two-agent runner for the
// given config (it reads cfg.Source for the source conn + FR-02 scope).
func NewInProcTwoAgentRunner(cfg *config.Config) *InProcTwoAgentRunner {
	return &InProcTwoAgentRunner{cfg: cfg}
}

// Start launches the in-process source agent (SourceServer) on a pre-bound
// loopback port and connects the control client to it.
func (r *InProcTwoAgentRunner) Start(ctx context.Context) error {
	ctrlAddr, err := prebindLoopback()
	if err != nil {
		return fmt.Errorf("two-agent: pre-bind control port: %w", err)
	}
	srv := agent.NewSourceServer(agent.ServerOptions{
		SrcConn:        r.cfg.Source,
		Snapshot:       r.snapshotMode(),
		Listen:         ctrlAddr,
		ConnectTimeout: 10 * time.Second,
		StreamTimeout:  6 * time.Hour,
	})
	sctx, cancel := context.WithCancel(ctx)
	r.srcCancel = cancel
	if _, err := srv.Start(sctx); err != nil {
		cancel()
		return fmt.Errorf("two-agent: start in-proc source agent: %w", err)
	}
	r.srv = srv
	client, err := agent.DialControl(ctx, ctrlAddr)
	if err != nil {
		srv.Close()
		cancel()
		return fmt.Errorf("two-agent: dial in-proc control: %w", err)
	}
	r.ctrl = client
	return nil
}

// Stream launches a per-unit in-process destination agent (agent.Serve) on a
// pre-bound port, instructs the source agent to stream the unit to it, and
// waits for the destination to finish. The destination is the authoritative
// row-count source; the wire volume comes from the source agent.
func (r *InProcTwoAgentRunner) Stream(ctx context.Context, req unitStreamRequest) (int64, uint64, uint64, error) {
	listen, err := prebindLoopback()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: pre-bind data port: %w", err)
	}
	// 1. Launch the per-unit destination agent (agent.Serve, no swap).
	dresCh := make(chan agent.DestResult, 1)
	derrCh := make(chan error, 1)
	go func() {
		res, err := agent.Serve(ctx, agent.DestOptions{
			Listen:        listen,
			DestConn:      req.DstConn,
			SSLCert:       req.DstCert,
			SSLKey:        req.DstKey,
			SSLCA:         req.DstCA,
			Swap:          false,
			AcceptTimeout: 5 * time.Minute,
			StreamTimeout: 6 * time.Hour,
		})
		if err != nil {
			derrCh <- err
			return
		}
		dresCh <- res
	}()
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		// Unblock the Serve accept (ctx cancel does not close the listener).
		if c, lerr := net.Dial("tcp", listen); lerr == nil {
			_ = c.Close()
		}
		select {
		case <-derrCh:
		case <-dresCh:
		case <-time.After(2 * time.Second):
		}
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the source agent to stream this unit to the destination agent.
	// The source agent retries its dial while the destination is still binding
	// its listen socket (dialDestRetry), so there is no launch/dial race to
	// wait out here.
	sreq := req.StreamReq
	sreq.DestAddr = listen
	resp, err := r.ctrl.Stream(sreq)
	if err != nil {
		return fail("two-agent: stream unit %d: %w", sreq.UnitID, err)
	}
	if !resp.Ok {
		return fail("two-agent: source agent stream unit %d: %s", sreq.UnitID, resp.Error)
	}

	// 3. Wait for the destination to finish the COPY.
	var dres agent.DestResult
	select {
	case derr := <-derrCh:
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d: %w", sreq.UnitID, derr)
	case dres = <-dresCh:
	case <-time.After(6*time.Hour + time.Minute):
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d: timed out", sreq.UnitID)
	}

	// 5. Destination row count is authoritative; wire volume from the source.
	return dres.Rows, resp.Wire, dres.Logical, nil
}

// StreamSegment (P4, in-proc) launches an in-process destination agent
// (agent.ServeSegments) on a pre-bound port, instructs the source agent to run
// the ON SEGMENT dispatch, and waits for the destination to finish. The
// destination is the authoritative row-count source; the wire volume is the
// raw on-network byte volume it read from the N segment connections.
func (r *InProcTwoAgentRunner) StreamSegment(ctx context.Context, req unitSegmentRequest) (int64, uint64, uint64, error) {
	// Bind the segment data port: the --seg-listen base when set (routable,
	// matching the segreceiver --listen flag), else a free loopback port (the
	// in-proc test's fake segreceivers are local).
	var listen string
	var err error
	if req.SegListen != "" {
		listen, err = segstream.UnitListen(req.SegListen, uint32(req.OnSegmentReq.UnitID))
	} else {
		listen, err = prebindLoopback()
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: segment data port: %w", err)
	}
	// 1. Launch the per-unit in-process destination agent: segment mode
	// (ServeSegments, merge) by default, or the P8 direct_1to1 fast-path
	// (ServeDirect, per-segment utility COPYs) when req.DirectWrite.
	sresCh := make(chan agent.SegmentsResult, 1)
	dresCh := make(chan agent.DirectResult, 1)
	serrCh := make(chan error, 1)
	if req.DirectWrite {
		go func() {
			res, err := agent.ServeDirect(ctx, agent.DirectOptions{
				Listen:        listen,
				DestConn:      req.DstConn,
				SSLCert:       req.DstCert,
				SSLKey:        req.DstKey,
				SSLCA:         req.DstCA,
				DataTLSCert:   req.DataTLSCert,
				DataTLSKey:    req.DataTLSKey,
				DataTLSCA:     req.DataTLSCA,
				Launch:        req.DirectLaunch,
				Swap:          false,
				AcceptTimeout: 5 * time.Minute,
				StreamTimeout: 6 * time.Hour,
			})
			if err != nil {
				serrCh <- err
				return
			}
			dresCh <- res
		}()
	} else {
		go func() {
			res, err := agent.ServeSegments(ctx, agent.SegmentsOptions{
				Listen:        listen,
				DestConn:      req.DstConn,
				SSLCert:       req.DstCert,
				SSLKey:        req.DstKey,
				SSLCA:         req.DstCA,
				Spec:          req.Spec,
				Segments:      req.Segments,
				Swap:          false,
				AcceptTimeout: 5 * time.Minute,
				StreamTimeout: 6 * time.Hour,
			})
			if err != nil {
				serrCh <- err
				return
			}
			sresCh <- res
		}()
	}
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		// Unblock the dest agent's accept (ctx cancel does not close the
		// listener): dial + close a probe connection (D times in direct mode).
		probes := 1
		if req.DirectWrite {
			probes = int(req.DirectLaunch.D)
		}
		for i := 0; i < probes; i++ {
			if c, lerr := net.Dial("tcp", listen); lerr == nil {
				_ = c.Close()
			}
		}
		select {
		case <-serrCh:
		case <-sresCh:
		case <-dresCh:
		case <-time.After(2 * time.Second):
		}
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the source agent to run the ON SEGMENT dispatch (unchanged
	//    for the fast-path: the kernel streams the same content-tagged slices).
	//    The segreceivers retry their dial while the destination is binding.
	o := req.OnSegmentReq
	o.DestAddr = listen
	oresp, err := r.ctrl.OnSegment(o)
	if err != nil {
		return fail("two-agent: onsegment unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		return fail("two-agent: source agent onsegment unit %d: %s", o.UnitID, oresp.Error)
	}

	// 3. Wait for the destination to finish the COPY.
	select {
	case serr := <-serrCh:
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (segment): %w", o.UnitID, serr)
	case dres := <-dresCh:
		return dres.Rows, dres.Wire, dres.Logical, nil
	case sres := <-sresCh:
		return sres.Rows, sres.Wire, sres.Logical, nil
	case <-time.After(6*time.Hour + time.Minute):
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (segment): timed out", o.UnitID)
	}
}

// Checksum asks the in-process source agent (FR-02 snapshot) for the checksum.
// StreamRouted (P9, in-proc) launches an in-process destination agent
// (agent.ServeRouted) on a pre-bound listen address, instructs the in-process
// source agent to trigger the ON SEGMENT dispatch with routing (a no-op on
// plain PG — the test drives fake routed segreceivers), and waits for the
// destination to finish its D utility-mode per-segment COPYs.
func (r *InProcTwoAgentRunner) StreamRouted(ctx context.Context, req unitRoutedRequest) (int64, uint64, uint64, error) {
	var listen string
	var err error
	if req.SegListen != "" {
		listen, err = segstream.UnitListen(req.SegListen, uint32(req.OnSegmentReq.UnitID))
	} else {
		listen, err = prebindLoopback()
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: routed data port: %w", err)
	}
	dresCh := make(chan agent.RoutedResult, 1)
	serrCh := make(chan error, 1)
	go func() {
		res, err := agent.ServeRouted(ctx, agent.RoutedOptions{
			Listen:        listen,
			DestConn:      req.DstConn,
			SSLCert:       req.DstCert,
			SSLKey:        req.DstKey,
			SSLCA:         req.DstCA,
			DataTLSCert:   req.DataTLSCert,
			DataTLSKey:    req.DataTLSKey,
			DataTLSCA:     req.DataTLSCA,
			Launch:        req.Launch,
			Swap:          false,
			AcceptTimeout: 5 * time.Minute,
			StreamTimeout: 6 * time.Hour,
		})
		if err != nil {
			serrCh <- err
			return
		}
		dresCh <- res
	}()
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		// Unblock the dest agent's accept: dial S×D×W probe connections.
		probes := int(req.Launch.SourceSegments) * int(req.Launch.D) * req.Launch.Workers
		for i := 0; i < probes; i++ {
			if c, lerr := net.Dial("tcp", listen); lerr == nil {
				_ = c.Close()
			}
		}
		select {
		case <-serrCh:
		case <-dresCh:
		case <-time.After(2 * time.Second):
		}
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// Instruct the source agent to run the ON SEGMENT dispatch with routing.
	o := req.OnSegmentReq
	o.DestAddr = listen
	oresp, err := r.ctrl.OnSegment(o)
	if err != nil {
		return fail("two-agent: onsegment (routed) unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		return fail("two-agent: source agent onsegment (routed) unit %d: %s", o.UnitID, oresp.Error)
	}

	select {
	case serr := <-serrCh:
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (routed): %w", o.UnitID, serr)
	case dres := <-dresCh:
		return dres.Rows, dres.Wire, dres.Logical, nil
	case <-time.After(6*time.Hour + time.Minute):
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (routed): timed out", o.UnitID)
	}
}

func (r *InProcTwoAgentRunner) Checksum(ctx context.Context, schema, table string) (string, error) {
	if r.srv == nil {
		return "", fmt.Errorf("two-agent: source agent not started")
	}
	return r.srv.Checksum(ctx, schema, table)
}

// Release ends the run (commits the source snapshot) and stops the source agent.
func (r *InProcTwoAgentRunner) Release(ctx context.Context) error {
	if r.ctrl != nil {
		if _, err := r.ctrl.Release(); err != nil {
			_ = r.ctrl.Close()
			r.ctrl = nil
			return fmt.Errorf("two-agent: release in-proc source agent: %w", err)
		}
		_ = r.ctrl.Close()
		r.ctrl = nil
	}
	if r.srv != nil {
		r.srv.Close()
		r.srv = nil
	}
	if r.srcCancel != nil {
		r.srcCancel()
		r.srcCancel = nil
	}
	return nil
}

// snapshotMode maps the config's FR-02 scope to the agent's SnapshotMode.
func (r *InProcTwoAgentRunner) snapshotMode() agent.SnapshotMode {
	// FR-02: the source agent always reads under the DB-scoped REPEATABLE
	// READ snapshot (P7 removed the cluster/per-table scopes).
	return agent.SnapDB
}
