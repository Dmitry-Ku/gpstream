package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/config"
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
	// StreamDirect launches a destination agent in DIRECT mode (P5, R5) and
	// instructs the source agent to run the direct data path: the source
	// agent routes each row to one of D destination segments and streams D
	// per-segment compressed slices; the destination agent performs D
	// utility-mode per-segment COPYs into the staging table. It returns the
	// authoritative (destination) row count + the on-network wire volume +
	// the logical volume. The swap is NOT performed (the Manager decides it).
	StreamDirect(ctx context.Context, req unitDirectRequest) (rows int64, wire, logical uint64, err error)
	// Checksum asks the source agent (which holds the FR-02 snapshot) for the
	// table's content checksum (the --verify path).
	Checksum(ctx context.Context, schema, table string) (string, error)
	// Release ends the run (commits the source snapshot) and stops the agents.
	Release(ctx context.Context) error
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
}

// unitDirectRequest is the Manager's per-unit instruction for the P5 direct
// data path (R5 through the agents). The source agent routes each row to one
// of D destination segments and streams D per-segment compressed slices; the
// destination agent performs D utility-mode per-segment COPYs. The controller
// (control plane) computes the routing key (KeyIdx/KeyOid/D) and the per-
// segment destination DSNs and hands both agents their halves.
type unitDirectRequest struct {
	agent.DirectReq
	// Launch is the destination agent's launch payload (staging shape +
	// per-segment DSNs + D). The controller builds it from the plan.
	Launch agent.DirectLaunch
	// DstConn is the destination COORDINATOR conn (staging DDL + swap); the
	// per-segment data COPYs use Launch.DstDSNs.
	DstConn string
	// DstTLS are the dest agent's pg-plane TLS settings.
	DstCert, DstKey, DstCA string
	// DataTLS is the agent<->agent data-plane TLS (source = client, dest =
	// server). Reuses the data TLS config (empty = plain TCP).
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
	cmd.Stderr = io.Discard // the agent's report lines are not needed (control carries them)
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
	specJSON, err := json.Marshal(req.Spec)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: marshal segment spec: %w", err)
	}
	var dstderr bytes.Buffer
	dargs := []string{
		"--dest", req.DstConn,
		"--listen", listen,
		"--segments", strconv.Itoa(req.Segments),
		"--spec", string(specJSON),
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

// StreamDirect (P5) launches a destination agent in DIRECT mode, instructs the
// source agent to run the direct data path (route each row to a destination
// segment + stream D per-segment compressed slices), and waits for the
// destination to finish its D utility-mode per-segment COPYs. Both agents run
// on the controller host, so the agent<->agent data socket is loopback; the
// destination agent's per-segment COPYs reach the destination segments
// (cross-host utility). The destination is the authoritative row-count source;
// the wire volume is the on-network byte volume it read from the D slice
// connections. The swap is NOT performed — the Manager decides it.
func (r *processTwoAgentRunner) StreamDirect(ctx context.Context, req unitDirectRequest) (int64, uint64, uint64, error) {
	// 1. Pre-bind a loopback data port (the agents are co-located on the
	// controller host, so loopback is reachable). The source agent dials it D
	// times (once per destination segment); the dest agent accepts D.
	listen, err := prebindLoopback()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: direct data port: %w", err)
	}
	launchJSON, err := json.Marshal(req.Launch)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: marshal direct launch: %w", err)
	}
	var dstderr bytes.Buffer
	dargs := []string{
		"--dest", req.DstConn,
		"--listen", listen,
		"--direct-spec", string(launchJSON),
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
		return 0, 0, 0, fmt.Errorf("two-agent: start dest agent (direct): %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- dcmd.Wait() }()
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the source agent to run the direct data path. The source
	// retries its dial while the destination is still binding (dialDestRetry).
	d := req.DirectReq
	d.DestAddr = listen
	d.DataTLSCert = req.DataTLSCert
	d.DataTLSKey = req.DataTLSKey
	d.DataTLSCA = req.DataTLSCA
	dresp, err := r.ctrl.Direct(d)
	if err != nil {
		return fail("two-agent: direct unit %d: %w", d.UnitID, err)
	}
	if !dresp.Ok {
		return fail("two-agent: source agent direct unit %d: %s", d.UnitID, dresp.Error)
	}

	// 3. Wait for the destination to finish the D per-segment COPYs.
	var werr error
	select {
	case werr = <-done:
	case <-time.After(r.streamDeadline()):
		_ = dcmd.Process.Kill()
		<-done
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (direct): timed out", d.UnitID)
	}
	if werr != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (direct): %v (stderr: %s)", d.UnitID, werr, dstderr.String())
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
	return 0, 0, 0, fmt.Errorf("two-agent: parse dest (direct) report unit %d: %s", d.UnitID, dstderr.String())
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
	if r.cfg.ConsistentCluster {
		return "cluster"
	}
	// FR-02 default is the DB-scoped REPEATABLE READ snapshot.
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
	// 1. Launch the per-unit in-process destination agent (segment mode).
	sresCh := make(chan agent.SegmentsResult, 1)
	serrCh := make(chan error, 1)
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
	fail := func(format string, a ...any) (int64, uint64, uint64, error) {
		// Unblock the ServeSegments accept (ctx cancel does not close the
		// listener): dial + close a probe connection.
		if c, lerr := net.Dial("tcp", listen); lerr == nil {
			_ = c.Close()
		}
		select {
		case <-serrCh:
		case <-sresCh:
		case <-time.After(2 * time.Second):
		}
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the source agent to run the ON SEGMENT dispatch. The
	//    segreceivers retry their dial while the destination is still binding.
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
	case sres := <-sresCh:
		return sres.Rows, sres.Wire, sres.Logical, nil
	case <-time.After(6*time.Hour + time.Minute):
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (segment): timed out", o.UnitID)
	}
}

// StreamDirect (P5, in-proc) launches an in-process destination agent
// (agent.ServeDirect) on a pre-bound loopback port, instructs the in-process
// source agent (SourceServer) to run the direct data path, and waits for the
// destination to finish its D utility-mode per-segment COPYs. The data flows
// over a real loopback TCP socket (source agent -> destination agent), exactly
// as in the binary path. The destination is the authoritative row-count source.
func (r *InProcTwoAgentRunner) StreamDirect(ctx context.Context, req unitDirectRequest) (int64, uint64, uint64, error) {
	listen, err := prebindLoopback()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: direct data port: %w", err)
	}
	// 1. Launch the per-unit in-process destination agent (direct mode).
	dresCh := make(chan agent.DirectResult, 1)
	derrCh := make(chan error, 1)
	go func() {
		res, err := agent.ServeDirect(ctx, agent.DirectOptions{
			Listen:        listen,
			DestConn:      req.DstConn,
			SSLCert:       req.DstCert,
			SSLKey:        req.DstKey,
			SSLCA:         req.DstCA,
			Launch:        req.Launch,
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
		// Unblock the ServeDirect accept (dial + close a probe connection).
		for i := 0; i < int(req.Launch.D); i++ {
			if c, lerr := net.Dial("tcp", listen); lerr == nil {
				_ = c.Close()
			}
		}
		select {
		case <-derrCh:
		case <-dresCh:
		case <-time.After(2 * time.Second):
		}
		return 0, 0, 0, fmt.Errorf(format, a...)
	}

	// 2. Instruct the in-process source agent to run the direct data path. The
	// source retries its dial while the destination is still binding.
	d := req.DirectReq
	d.DestAddr = listen
	dresp, err := r.ctrl.Direct(d)
	if err != nil {
		return fail("two-agent: direct unit %d: %w", d.UnitID, err)
	}
	if !dresp.Ok {
		return fail("two-agent: source agent direct unit %d: %s", d.UnitID, dresp.Error)
	}

	// 3. Wait for the destination to finish the D per-segment COPYs.
	select {
	case derr := <-derrCh:
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (direct): %w", d.UnitID, derr)
	case dres := <-dresCh:
		return dres.Rows, dres.Wire, dres.Logical, nil
	case <-time.After(6*time.Hour + time.Minute):
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (direct): timed out", d.UnitID)
	}
}

// Checksum asks the in-process source agent (FR-02 snapshot) for the checksum.
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
	if r.cfg.ConsistentCluster {
		return agent.SnapCluster
	}
	// FR-02 default is the DB-scoped REPEATABLE READ snapshot.
	return agent.SnapDB
}
