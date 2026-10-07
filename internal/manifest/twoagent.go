package manifest

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/launch"
	"github.com/gpstream/gpstream/internal/portalloc"
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
	// volume + (FR-02, P9-E) the inline content digests computed on the data
	// path when VerifyDigest is requested — srcDigest from the source agent,
	// dstDigest from the destination agent (both empty when not requested).
	// The destination staging table is created + loaded by the dest agent;
	// the swap is NOT performed (the Manager decides it).
	Stream(ctx context.Context, req unitStreamRequest) (rows int64, wire, logical uint64, srcDigest, dstDigest string, err error)
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
	cfg *config.Config
	// portAlloc restricts the controller's own pre-bound loopback ports to
	// --data-port-range (local / single-host layout); nil = auto. The remote
	// (SSH) layout passes the range to the agents instead (--port-range).
	portAlloc *portalloc.Allocator
	// Source agent (long-lived; owns the FR-02 snapshot).
	ctrl      *agent.ControlClient
	srcHandle *launch.Handle
	// Destination agent (long-lived; design 04.10 port economy — one data +
	// one control listener for the whole run, units multiplex by unit_id).
	destCtrl     *agent.DestControlClient
	destHandle   *launch.Handle
	destDataAddr string // the dest agent's real data address (from its hello)
	// destPersistAddr (P1c): the dest agent's dedicated persistent-
	// coordinator listener address (from its hello). Empty when the agent
	// has no persist listener (legacy build / off) -> coordinator units
	// use the legacy per-unit connection.
	destPersistAddr string
	// streamMu serializes the WHOLE per-unit dest exchange
	// (BeginServe -> source stream -> WaitUnitDone). The long-lived dest
	// agent has ONE control socket and a SEQUENTIAL control loop (one
	// serve-unit -> ack -> unit-done in flight at a time), so concurrent
	// manager workers (FR-03, WorkersPerPair > 1) must not interleave their
	// exchanges: BeginServe and WaitUnitDone lock the client mutex
	// separately, so without this a second worker's serve-unit would land on
	// the socket before the first's unit-done was read, desyncing the
	// request/response stream (a foreign unit-done read as the ack, a
	// foreign ack read as the unit-done with rows=0 — the unit's rows are
	// copied but its count is lost, corrupting manager.Rows / verify). The
	// dest agent serves one unit at a time anyway, so no parallelism is
	// given up.
	streamMu sync.Mutex
}

func newProcessTwoAgentRunner(cfg *config.Config) *processTwoAgentRunner {
	alloc, _ := portalloc.ParseRange(cfg.DataPortRange) // Validate already checked it
	return &processTwoAgentRunner{cfg: cfg, portAlloc: alloc}
}

// NewProcessTwoAgentRunner builds the process two-agent runner (the production
// path: it launches the real gpstream-source-agent / gpstream-dest-agent
// binaries as subprocesses, locally or over SSH per the config's
// SourceAgentSSH/DestAgentSSH). The integration harness injects this in live
// mode so the data path runs through the DEPLOYED package on the cluster
// hosts, not in-process agent code on the controller.
func NewProcessTwoAgentRunner(cfg *config.Config) *processTwoAgentRunner {
	return newProcessTwoAgentRunner(cfg)
}

// Start brings up both long-lived agents and connects the control clients.
// Each agent is launched per the operator's config (Inc2d): a LOCAL
// subprocess on the controller's host (the gp6lab / single-host layout), an
// SSH remote launch on the agent's own host (the multi-host GP6 layout — the
// agent runs where it opens its pg connection), or an adopted daemon
// (dial-first: the operator pre-ran it; the controller just dials). The
// destination agent binds ONE data + ONE control listener for the whole run;
// its hello reports the real data address the source agent / segreceivers
// dial.
func (r *processTwoAgentRunner) Start(ctx context.Context) error {
	if err := r.startSource(ctx); err != nil {
		return err
	}
	if err := r.startDest(ctx); err != nil {
		// Surface the real startDest error (dial failure, launch failure) —
		// stopSource is cleanup, not the cause. Swallowing it (the old
		// `return r.stopSource()`) turned every dest-start failure into the
		// misleading "destination agent not started" at stream time.
		stopErr := r.stopSource()
		if stopErr != nil {
			return fmt.Errorf("start dest: %w (source stop: %v)", err, stopErr)
		}
		return err
	}
	return nil
}

// controlDialDeadline bounds the bounded-retry dial after an agent launch.
// A remote (SSH-launched) agent pays for the SSH round-trip PLUS its own PG
// connect to the Greenplum coordinator before it binds the control listener —
// on the lab stand that is ~10-20s, so the local 15s margin is too tight
// (the agent becomes ready a few seconds AFTER the dial gives up, then exits
// on its one-shot control timeout). 90s is generous without hiding a real
// hang (the launch itself is separately bounded by launch.Ensure's WaitTimeout).
func controlDialDeadline(sshHost string) time.Duration {
	if sshHost != "" {
		return 90 * time.Second
	}
	return 15 * time.Second
}

// startSource brings up the long-lived source agent (owns the FR-02 snapshot)
// and dials its control socket.
func (r *processTwoAgentRunner) startSource(ctx context.Context) error {
	srcCtrlAddr, err := r.controlListen(r.cfg.SourceAgentSSH, r.cfg.SourceDialFirst, r.cfg.SourceControlAddr)
	if err != nil {
		return err
	}
	srcArgs := []string{
		"--src", r.cfg.Source,
		"--snapshot", r.snapshotMode(),
		"--serve", srcCtrlAddr,
		"--connect-timeout", "10s",
		"--frame-ack", r.cfg.FrameAckArg(),
		"--ack-timeout", r.cfg.AckTimeout,
		"--max-reconnects", strconv.Itoa(r.cfg.MaxReconnects),
		"--retransmit-buffer-max", strconv.FormatInt(r.cfg.RetransmitBufferMax, 10),
		// P1d: --workers > 1 = snapshot-epoch (N pinned conns); the binary
		// sizes its source pool accordingly and validates the pin spread.
		"--workers", strconv.Itoa(r.cfg.WorkersPerPair),
		"--max-snapshot-delta", strconv.FormatInt(int64(r.cfg.MaxSnapshotDelta.Seconds()), 10),
	}
	if r.cfg.SSLCert != "" {
		srcArgs = append(srcArgs, "--ssl-cert", r.cfg.SSLCert, "--ssl-key", r.cfg.SSLKey)
	}
	if r.cfg.SSLCA != "" {
		srcArgs = append(srcArgs, "--ssl-ca", r.cfg.SSLCA)
	}
	// Data-plane TLS for the source->dest data socket (source = client).
	if r.cfg.SegTLSCert != "" {
		srcArgs = append(srcArgs, "--data-tls-cert", r.cfg.SegTLSCert, "--data-tls-key", r.cfg.SegTLSKey)
	}
	if r.cfg.SegTLSCA != "" {
		srcArgs = append(srcArgs, "--data-tls-ca", r.cfg.SegTLSCA)
	}
	h, err := launch.Ensure(ctx, r.target("gpstream-source-agent", srcArgs, srcCtrlAddr, r.cfg.SourceAgentSSH, r.cfg.SourceDialFirst))
	if err != nil {
		return fmt.Errorf("two-agent: launch source agent: %w", err)
	}
	r.srcHandle = h
	client, err := dialControlRetry(ctx, srcCtrlAddr, controlDialDeadline(r.cfg.SourceAgentSSH), r.controlTLS())
	if err != nil {
		h.Stop()
		r.srcHandle = nil
		return fmt.Errorf("two-agent: source control dial %s: %w", srcCtrlAddr, err)
	}
	r.ctrl = client
	return nil
}

// startDest brings up the long-lived dest agent (one data + one control
// listener) and dials its control socket (reading the hello for the data
// address).
func (r *processTwoAgentRunner) startDest(ctx context.Context) error {
	destCtrlAddr, err := r.controlListen(r.cfg.DestAgentSSH, r.cfg.DestDialFirst, r.cfg.DestControlAddr)
	if err != nil {
		return err
	}
	dataAddr, err := r.dataListen(r.cfg.DestAgentSSH, r.cfg.DestDialFirst, r.cfg.DestDataAddr)
	if err != nil {
		return err
	}
	// P1c: the dedicated persistent-coordinator listener (a third listener;
	// the source agent dials it ONCE per run and every coordinator unit
	// rides the same connection). Local agents get a pre-bound loopback
	// port; remote/dial-first agents use auto :0 (the agent restricts it to
	// --port-range itself) — the hello reports the real address.
	persistListen, perr := r.dataListen(r.cfg.DestAgentSSH, r.cfg.DestDialFirst, "")
	if perr != nil {
		return fmt.Errorf("two-agent: persist listen: %w", perr)
	}
	// --dest comes FIRST so the agent stays identifiable by its conn string
	// (the crash-recovery test finds it via pgrep "<bin> --dest ").
	dargs := []string{
		"--dest", r.cfg.Dest,
		"--serve",
		"--data-listen", dataAddr,
		"--control-listen", destCtrlAddr,
		"--persist-listen", persistListen,
		"--accept-timeout", "5m",
		"--stream-timeout", "6h",
		"--frame-ack", r.cfg.FrameAckArg(),
		"--ack-timeout", r.cfg.AckTimeout,
		"--max-reconnects", strconv.Itoa(r.cfg.MaxReconnects),
		"--retransmit-buffer-max", strconv.FormatInt(r.cfg.RetransmitBufferMax, 10),
	}
	// --port-range matters only when a listener address is auto (":0"): the
	// remote/dial-first layout, where the agent binds its own data port and
	// the controller cannot pre-bind it. Local (same-host) agents get
	// pre-bound fixed ports from the controller's own allocator, so the range
	// is already applied there — passing it is a harmless no-op.
	if r.cfg.DataPortRange != "" {
		dargs = append(dargs, "--port-range", r.cfg.DataPortRange)
	}
	if r.cfg.SSLCert != "" {
		dargs = append(dargs, "--ssl-cert", r.cfg.SSLCert, "--ssl-key", r.cfg.SSLKey)
	}
	if r.cfg.SSLCA != "" {
		dargs = append(dargs, "--ssl-ca", r.cfg.SSLCA)
	}
	if r.cfg.SegTLSCert != "" {
		dargs = append(dargs, "--data-tls-cert", r.cfg.SegTLSCert, "--data-tls-key", r.cfg.SegTLSKey)
	}
	if r.cfg.SegTLSCA != "" {
		dargs = append(dargs, "--data-tls-ca", r.cfg.SegTLSCA)
	}
	h, err := launch.Ensure(ctx, r.target("gpstream-dest-agent", dargs, destCtrlAddr, r.cfg.DestAgentSSH, r.cfg.DestDialFirst))
	if err != nil {
		return fmt.Errorf("two-agent: launch dest agent: %w", err)
	}
	r.destHandle = h
	client, err := dialDestControlRetry(ctx, destCtrlAddr, controlDialDeadline(r.cfg.DestAgentSSH), r.controlTLS())
	if err != nil {
		h.Stop()
		r.destHandle = nil
		return fmt.Errorf("two-agent: dest control dial %s: %w", destCtrlAddr, err)
	}
	r.destCtrl = client
	// The hello reports the real bound data address (auto :0 mode). It is the
	// authoritative endpoint the source / segreceivers dial.
	r.destDataAddr = client.Hello.Data
	if r.destDataAddr == "" {
		r.destDataAddr = dataAddr
	}
	// P1c: the persistent-coordinator address (empty = the agent has no
	// persist listener: coordinator units fall back to the per-unit conn).
	r.destPersistAddr = client.Hello.Persist
	return nil
}

// target builds a launch.Target for one agent. For a remote (SSH) launch the
// binary path is the operator-configured install dir on the AGENT host; for a
// local launch it is the convention-resolved binary on the controller's host.
// dial-first agents are not launched (the operator pre-ran them), so the
// binary is irrelevant (adopted before use).
func (r *processTwoAgentRunner) target(base string, args []string, controlAddr, sshHost string, dialFirst bool) launch.Target {
	bin := r.cfg.SourceAgentBinary
	if base == "gpstream-dest-agent" {
		bin = r.cfg.DestAgentBinary
	}
	if sshHost != "" && !dialFirst {
		bin = filepath.Join(r.cfg.AgentBinDir, base)
	}
	return launch.Target{
		Binary:      bin,
		Args:        args,
		ControlAddr: controlAddr,
		SSHHost:     sshHost,
		SSHKey:      r.cfg.AgentSSHKey,
		RunAs:       r.cfg.AgentSSHRunAs,
		DialFirst:   dialFirst,
		LogPath:     agentLogPath(base),
		WaitTimeout: 30 * time.Second,
	}
}

// controlListen returns the control address to use. A remote or dial-first
// agent needs a FIXED routable address (the controller dials it); a local
// agent gets a pre-bound loopback port.
func (r *processTwoAgentRunner) controlListen(sshHost string, dialFirst bool, fixed string) (string, error) {
	if sshHost != "" || dialFirst {
		if fixed == "" {
			return "", fmt.Errorf("two-agent: a remote/dial-first agent needs a fixed control address (source_control_addr / dest_control_addr)")
		}
		return fixed, nil
	}
	return r.prebindLocal()
}

// dataListen returns the data address for the dest agent. A local agent gets a
// pre-bound loopback port (auto :0, reported in the hello); a remote/dial-first
// agent uses its fixed address, or "0.0.0.0:0" (auto :0, the hello carries the
// real port — the agent restricts it to --port-range itself).
func (r *processTwoAgentRunner) dataListen(sshHost string, dialFirst bool, fixed string) (string, error) {
	if sshHost != "" || dialFirst {
		if fixed == "" {
			return "0.0.0.0:0", nil
		}
		return fixed, nil
	}
	return r.prebindLocal()
}

// prebindLocal learns a free loopback port for a local (same-host) agent and
// returns it as "127.0.0.1:port", honoring --data-port-range when set (the
// allocator reserves it so the three prebinds — source control, dest control,
// dest data — never collide). With no range it is the legacy kernel-pick.
func (r *processTwoAgentRunner) prebindLocal() (string, error) {
	if r.portAlloc == nil {
		return prebindLoopback()
	}
	port, err := r.portAlloc.Next("127.0.0.1")
	if err != nil {
		return "", err
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil
}

// agentLogPath is the agent's stderr sink path (GPSTREAM_AGENT_STDERR when set
// for diagnosis, else a per-agent default under /tmp). Remote agents append
// here; local agents use the same path for parity.
func agentLogPath(base string) string {
	if p := os.Getenv("GPSTREAM_AGENT_STDERR"); p != "" {
		return p
	}
	return filepath.Join(os.TempDir(), base+".log")
}

// stopSource tears down the source agent (used when the dest launch fails).
func (r *processTwoAgentRunner) stopSource() error {
	var cerr error
	if r.ctrl != nil {
		_, _ = r.ctrl.Release()
		_ = r.ctrl.Close()
		r.ctrl = nil
	}
	if r.srcHandle != nil {
		r.srcHandle.Stop()
		r.srcHandle = nil
	}
	return cerr
}

// controlTLS is the controller's control-channel TLS client settings (both
// agents verify against the same CA; empty = plain TCP, the lab layout).
func (r *processTwoAgentRunner) controlTLS() agent.ControlTLS {
	return agent.ControlTLS{
		Cert: r.cfg.ControlTLSCert,
		Key:  r.cfg.ControlTLSKey,
		CA:   r.cfg.ControlTLSCA,
	}
}

// stopSource tears down the source agent (used when the dest launch fails).
// agentStderr returns the agent stderr sink: GPSTREAM_AGENT_STDERR (a path,
// appended) when set for diagnosis, else io.Discard.
func agentStderr() io.Writer {
	if p := os.Getenv("GPSTREAM_AGENT_STDERR"); p != "" {
		if f, ferr := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); ferr == nil {
			return f
		}
	}
	return io.Discard
}

// Stream (coordinator) drives one unit through the LONG-LIVED destination
// agent (design 04.10 port economy): the agent is already up (Start bound
// its one data + one control listener). The controller tells it to serve the
// unit on the shared data listener (BeginServe, acked immediately), then
// instructs the source agent to stream the unit to the agent's data address
// (from its hello), then waits for the unit-done report. The destination is
// the authoritative row-count source; the wire volume comes from the source
// agent (the bytes it wrote to the socket). The swap is NOT performed here —
// the Manager decides it.
func (r *processTwoAgentRunner) Stream(ctx context.Context, req unitStreamRequest) (int64, uint64, uint64, string, string, error) {
	if r.destCtrl == nil {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: destination agent not started")
	}
	// Serialize the whole exchange (see streamMu): the dest agent's control
	// loop is sequential, so BeginServe/WaitUnitDone must not interleave
	// across concurrent workers.
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	// 1. Tell the long-lived dest agent to serve this unit on the shared data
	//    listener. The ack returns immediately (the serve is running,
	//    accepting the source agent's connection).
	ack, err := r.destCtrl.BeginServe(agent.DestServeReq{Kind: "coordinator"})
	if err != nil {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %w", req.UnitID, err)
	}
	if !ack.Ok {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %s", req.UnitID, ack.Error)
	}
	// 2. Instruct the source agent to stream the unit. P1c: when the dest
	//    agent exposes a persistent-coordinator listener, the unit rides the
	//    run's ONE persistent channel (UnitBegin + frames + Trailer, the
	//    connection stays open); otherwise the legacy per-unit connection.
	sreq := req.StreamReq
	if r.destPersistAddr != "" {
		sreq.DestAddr = r.destPersistAddr
		sreq.Persistent = true
	} else {
		sreq.DestAddr = r.destDataAddr
	}
	resp, err := r.ctrl.Stream(sreq)
	if err != nil {
		// Drain the in-flight serve so the next unit's BeginServe is clean.
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: stream unit %d: %w", req.UnitID, err)
	}
	if !resp.Ok {
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: source agent stream unit %d: %s", req.UnitID, resp.Error)
	}
	// 3. Wait for the destination to finish the COPY (the unit-done report).
	done, err := r.destCtrl.WaitUnitDone()
	if err != nil {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: dest unit-done unit %d: %w", req.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: dest agent unit %d: %s", req.UnitID, done.Error)
	}
	// P9-E: return the inline content digests (both empty when the controller
	// did not request verify): the source's (from the stream reply) and the
	// destination's (from the unit-done report), for the manager to compare.
	return done.Rows, resp.Wire, done.Logical, resp.Digest, done.Digest, nil
}

// StreamSegment (P4 on_segment) drives one unit through the LONG-LIVED
// destination agent: the kernel launches the segreceiver on every primary
// source segment; each streams its compressed slice to the dest agent's
// shared data address (from its hello — no per-unit port). The dest agent
// either merges the N slices (ServeSegments, sub-path (c)) or loads them
// with per-segment utility COPYs (ServeDirect, the direct_1to1 fast-path
// (a)). The destination is the authoritative row-count source; the wire
// volume is the raw on-network byte volume it read. The swap is NOT
// performed — the Manager decides it.
func (r *processTwoAgentRunner) StreamSegment(ctx context.Context, req unitSegmentRequest) (int64, uint64, uint64, error) {
	if r.destCtrl == nil {
		return 0, 0, 0, fmt.Errorf("two-agent: destination agent not started")
	}
	r.streamMu.Lock() // serialize the whole exchange (see streamMu)
	defer r.streamMu.Unlock()
	// 1. Tell the long-lived dest agent to serve this unit. Kind selects the
	//    load method: direct_1to1 (a) = per-segment utility COPYs; else
	//    segments (c) = SliceMux merge -> one coordinator COPY.
	kind := "segments"
	dreq := agent.DestServeReq{
		Kind:     kind,
		Spec:     &req.Spec,
		Segments: req.Segments,
	}
	if req.DirectWrite {
		kind = "direct"
		dreq = agent.DestServeReq{Kind: "direct", Direct: &req.DirectLaunch}
	}
	ack, err := r.destCtrl.BeginServe(dreq)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: begin serve (%s) unit %d: %w", kind, req.UnitID, err)
	}
	if !ack.Ok {
		return 0, 0, 0, fmt.Errorf("two-agent: begin serve (%s) unit %d: %s", kind, req.UnitID, ack.Error)
	}
	// 2. Instruct the source agent to run the ON SEGMENT dispatch; the
	//    segreceivers dial the dest agent's shared data address.
	o := req.OnSegmentReq
	o.DestAddr = r.destDataAddr
	oresp, err := r.ctrl.OnSegment(o)
	if err != nil {
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, fmt.Errorf("two-agent: onsegment unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, fmt.Errorf("two-agent: source agent onsegment unit %d: %s", o.UnitID, oresp.Error)
	}
	// 3. Wait for the destination to finish (the unit-done report).
	done, err := r.destCtrl.WaitUnitDone()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: dest unit-done (%s) unit %d: %w", kind, o.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (%s): %s", o.UnitID, kind, done.Error)
	}
	return done.Rows, done.Wire, done.Logical, nil
}

// StreamRouted (P9, S≠D) drives one unit through the LONG-LIVED destination
// agent in ROUTED mode: each source segreceiver routes its slice to its
// destination segment and fans each (s,d) pair over W sub-streams to the
// dest agent's shared data address (from its hello). The dest agent merges
// the per-segment sub-streams and performs D utility-mode per-segment COPYs.
// Neither cluster coordinator is in the data path. The destination is the
// authoritative row-count source.
func (r *processTwoAgentRunner) StreamRouted(ctx context.Context, req unitRoutedRequest) (int64, uint64, uint64, error) {
	if r.destCtrl == nil {
		return 0, 0, 0, fmt.Errorf("two-agent: destination agent not started")
	}
	r.streamMu.Lock() // serialize the whole exchange (see streamMu)
	defer r.streamMu.Unlock()
	// 1. Tell the long-lived dest agent to serve this unit in routed mode.
	ack, err := r.destCtrl.BeginServe(agent.DestServeReq{Kind: "routed", Routed: &req.Launch})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: begin serve (routed) unit %d: %w", req.UnitID, err)
	}
	if !ack.Ok {
		return 0, 0, 0, fmt.Errorf("two-agent: begin serve (routed) unit %d: %s", req.UnitID, ack.Error)
	}
	// 2. Instruct the source agent to run the ON SEGMENT dispatch with
	//    routing; the segreceivers dial the dest agent's shared data address.
	o := req.OnSegmentReq
	o.DestAddr = r.destDataAddr
	oresp, err := r.ctrl.OnSegment(o)
	if err != nil {
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, fmt.Errorf("two-agent: onsegment (routed) unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, fmt.Errorf("two-agent: source agent onsegment (routed) unit %d: %s", o.UnitID, oresp.Error)
	}
	// 3. Wait for the destination to finish (the unit-done report).
	done, err := r.destCtrl.WaitUnitDone()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: dest unit-done (routed) unit %d: %w", o.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (routed): %s", o.UnitID, done.Error)
	}
	return done.Rows, done.Wire, done.Logical, nil
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
	// Destination agent (long-lived): release over its control socket (it
	// closes both listeners and exits); the launch handle's Stop is the
	// last-resort kill (local) / remote pkill (SSH).
	if r.destCtrl != nil {
		if _, err := r.destCtrl.Release(); err != nil {
			if cerr == nil {
				cerr = err
			}
		}
		_ = r.destCtrl.Close()
		r.destCtrl = nil
	}
	if r.destHandle != nil {
		r.destHandle.Stop()
		r.destHandle = nil
	}
	if r.ctrl != nil {
		if _, err := r.ctrl.Release(); err != nil {
			if cerr == nil {
				cerr = err
			}
		}
		_ = r.ctrl.Close()
		r.ctrl = nil
	}
	if r.srcHandle != nil {
		r.srcHandle.Stop()
		r.srcHandle = nil
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

// prebindData learns a free loopback port for an in-proc listener, honoring
// --data-port-range (r.portAlloc) when set. It returns the address plus a
// release func: the per-unit data ports are bound serially (one per unit), so
// the caller releases the port once the unit's listener is closed — otherwise
// the allocator's reserved set grows one port per unit and a small range runs
// out. With no range the release is a no-op and this is the legacy
// prebindLoopback.
func (r *InProcTwoAgentRunner) prebindData() (string, func(), error) {
	if r.portAlloc == nil {
		addr, err := prebindLoopback()
		return addr, func() {}, err
	}
	port, err := r.portAlloc.Next("127.0.0.1")
	if err != nil {
		return "", nil, err
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), func() { r.portAlloc.Release(port) }, nil
}

// dialControlRetry dials addr until it succeeds or the deadline passes (the
// source agent binds the control socket shortly after it starts). tls is the
// control-channel TLS client settings (design 04.10; empty = plain TCP, the
// pre-redesign local layout).
func dialControlRetry(ctx context.Context, addr string, deadline time.Duration, tls agent.ControlTLS) (*agent.ControlClient, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	const backoff = 50 * time.Millisecond
	var lastErr error
	for {
		client, err := agent.DialControlTLS(deadlineCtx, addr, tls)
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

// dialDestControlRetry dials a long-lived dest agent's control socket until
// it succeeds or the deadline passes (the agent binds its control listener
// shortly after it starts), reading the hello.
func dialDestControlRetry(ctx context.Context, addr string, deadline time.Duration, tls agent.ControlTLS) (*agent.DestControlClient, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	const backoff = 50 * time.Millisecond
	var lastErr error
	for {
		client, err := agent.DialDestControlTLS(deadlineCtx, addr, tls)
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
	portAlloc *portalloc.Allocator // --data-port-range for the pre-bound loopback ports (nil = auto)
	srv       *agent.SourceServer
	ctrl      *agent.ControlClient
	srcCancel context.CancelFunc
	// P1c: the LONG-LIVED in-proc destination agent (one data + one control
	// + one persistent-coordinator listener for the whole run). Coordinator
	// units ride its persistent channel; mesh units (StreamSegment /
	// StreamRouted) still use their per-unit serves.
	dest       *agent.DestServer
	destCtrl   *agent.DestControlClient
	destCancel context.CancelFunc
	// finalPersAccepts (P1c): the persistent-connection count captured when
	// the dest agent is torn down (Release / KillDestAgent) — PersistAccepts
	// falls back to it, so the count survives the runner's cleanup. Atomic:
	// KillDestAgent runs in a foreign goroutine (the test's kill).
	finalPersAccepts atomic.Int64
	// streamMu serializes the whole per-unit dest exchange (BeginServe ->
	// stream -> WaitUnitDone): the dest agent's control loop is sequential,
	// so concurrent manager workers must not interleave (mirrors the
	// process runner).
	streamMu sync.Mutex
}

// KillDestAgent (P1c test hook) kills the in-proc destination agent the way
// a SIGKILL kills the real one: its listeners (control + data + persistent)
// and its pg connection are closed WITHOUT a graceful release, so an
// in-flight unit's data COPY and control exchange break hard. The source
// agent survives (a dest-process crash does not take the source down); the
// next unit's Stream finds the dest gone and fails fast (the run continues,
// the manager records the failures, and a resume with a fresh runner
// completes them — the FR-06/FR-07 contract).
func (r *InProcTwoAgentRunner) KillDestAgent() {
	// No streamMu and no field writes: this simulates an EXTERNAL process
	// death (no runner coordination) — a concurrent in-flight exchange may
	// be using the client/server pointers, so only the objects are closed
	// (Close is safe under concurrent use): the in-flight unit's
	// WaitUnitDone/BeginServe unblock on the closed conns and fail the unit.
	// The fields are niled by the runner's own Release (run end, no
	// concurrency).
	if c := r.destCtrl; c != nil {
		_ = c.Close()
	}
	if d := r.dest; d != nil {
		r.finalPersAccepts.Store(d.PersistAccepts())
		d.Kill() // hard network-plane death (no pg-conn close: no race)
	}
	if c := r.destCancel; c != nil {
		c()
	}
}

// PersistAccepts (P1c) is the number of persistent coordinator connections
// the in-proc dest agent has accepted (the first unit's dial + one per
// re-dial after a break). A test signal that the persistent channel is in
// use; 0 when the dest server is not started.
func (r *InProcTwoAgentRunner) PersistAccepts() int64 {
	if r.dest != nil {
		return r.dest.PersistAccepts()
	}
	return r.finalPersAccepts.Load()
}

// NewInProcTwoAgentRunner builds an in-process two-agent runner for the
// given config (it reads cfg.Source for the source conn + FR-02 scope).
func NewInProcTwoAgentRunner(cfg *config.Config) *InProcTwoAgentRunner {
	alloc, _ := portalloc.ParseRange(cfg.DataPortRange) // Validate already checked it
	return &InProcTwoAgentRunner{cfg: cfg, portAlloc: alloc}
}

// Start launches the in-process source agent (SourceServer) on a pre-bound
// loopback port and connects the control client to it.
func (r *InProcTwoAgentRunner) Start(ctx context.Context) error {
	ctrlAddr, releaseCtrl, err := r.prebindData()
	if err != nil {
		return fmt.Errorf("two-agent: pre-bind control port: %w", err)
	}
	_ = releaseCtrl // long-lived: the port stays reserved for the whole run
	srv := agent.NewSourceServer(agent.ServerOptions{
		SrcConn:        r.cfg.Source,
		Snapshot:       r.snapshotMode(),
		Listen:         ctrlAddr,
		ConnectTimeout: 10 * time.Second,
		StreamTimeout:  6 * time.Hour,
		FrameAck:       r.cfg.FrameAckOn(),
		ReliablePipe:   r.cfg.ReliablePipeConfig(),
		// P1d: --workers > 1 switches the source agent to the snapshot-epoch
		// (N pinned conns); --max-snapshot-delta bounds the pin spread.
		Workers:          r.cfg.WorkersPerPair,
		MaxSnapshotDelta: r.cfg.MaxSnapshotDelta,
	})
	sctx, cancel := context.WithCancel(ctx)
	r.srcCancel = cancel
	if _, err := srv.Start(sctx); err != nil {
		cancel()
		return fmt.Errorf("two-agent: start in-proc source agent: %w", err)
	}
	r.srv = srv
	client, err := agent.DialControl(ctx, ctrlAddr) // in-proc test: plain control
	if err != nil {
		srv.Close()
		cancel()
		return fmt.Errorf("two-agent: dial in-proc control: %w", err)
	}
	r.ctrl = client

	// P1c: the long-lived in-proc destination agent. Its dedicated
	// persistent-coordinator listener is what the in-proc source agent
	// dials for coordinator units (the mesh path keeps its per-unit
	// serves). DstConn is read from the first unit's request at Stream
	// time — but the agent needs a dest conn at start, so use cfg.Dest
	// (the manager's destination; the per-unit DstConn is the same value).
	dataAddr, releaseData, err := r.prebindData()
	if err != nil {
		return fmt.Errorf("two-agent: pre-bind in-proc dest data port: %w", err)
	}
	_ = releaseData // long-lived: the port stays bound for the run
	persistAddr, releasePersist, err := r.prebindData()
	if err != nil {
		return fmt.Errorf("two-agent: pre-bind in-proc dest persist port: %w", err)
	}
	_ = releasePersist
	ctlAddr, releaseCtl, err := r.prebindData()
	if err != nil {
		return fmt.Errorf("two-agent: pre-bind in-proc dest control port: %w", err)
	}
	_ = releaseCtl
	dctx, dcancel := context.WithCancel(ctx)
	r.destCancel = dcancel
	dsrv, derr := agent.NewDestServer(agent.DestServerOptions{
		DataListen:    dataAddr,
		ControlListen: ctlAddr,
		PersistListen: persistAddr,
		PortAlloc:     r.portAlloc,
		DestConn:      r.cfg.Dest,
		AcceptTimeout: 5 * time.Minute,
		StreamTimeout: 6 * time.Hour,
		FrameAck:      r.cfg.FrameAckOn(),
		ReliablePipe:  r.cfg.ReliablePipeConfig(),
		// Bound the serves by the run context: a run abort / crash-simulation
		// (a soft ctx cancel in local mode) interrupts an in-flight unit's
		// COPY exactly as the legacy per-unit agent.Serve(ctx) did.
		RunCtx: dctx,
	})
	if derr != nil {
		dcancel()
		return fmt.Errorf("two-agent: start in-proc dest server: %w", derr)
	}
	r.dest = dsrv
	dclient, derr := agent.DialDestControl(dctx, dsrv.ControlAddr())
	if derr != nil {
		dcancel()
		dsrv.Close()
		return fmt.Errorf("two-agent: dial in-proc dest control: %w", derr)
	}
	r.destCtrl = dclient
	return nil
}

// Stream drives one coordinator unit through the LONG-LIVED in-proc
// destination agent (P1c): BeginServe (the agent acks immediately and blocks
// on the persistent channel's next UnitBegin) -> the source agent streams the
// unit over the run's ONE persistent coordinator channel -> WaitUnitDone.
// The destination is the authoritative row-count source; the wire volume
// comes from the source agent. The swap is NOT performed (the Manager does it).
func (r *InProcTwoAgentRunner) Stream(ctx context.Context, req unitStreamRequest) (int64, uint64, uint64, string, string, error) {
	if r.dest == nil || r.destCtrl == nil {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: in-proc dest server not started")
	}
	// Serialize the whole exchange (see streamMu): the dest agent's control
	// loop is sequential, so BeginServe/WaitUnitDone must not interleave
	// across concurrent workers.
	r.streamMu.Lock()
	defer r.streamMu.Unlock()

	// 1. Tell the long-lived dest agent to serve this unit on the persistent
	//    channel. The ack returns immediately (the serve is running, blocked
	//    on the next UnitBegin).
	ack, err := r.destCtrl.BeginServe(agent.DestServeReq{Kind: "coordinator"})
	if err != nil {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %w", req.UnitID, err)
	}
	if !ack.Ok {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %s", req.UnitID, ack.Error)
	}

	// 2. Instruct the source agent to stream the unit over the persistent
	//    channel (the dest's persist address, from its hello). The source
	//    dials it on the first unit and keeps the connection for the run.
	sreq := req.StreamReq
	sreq.DestAddr = r.dest.PersistAddr()
	sreq.Persistent = true
	resp, err := r.ctrl.Stream(sreq)
	if err != nil {
		// Drain the in-flight serve so the next unit's BeginServe is clean.
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: stream unit %d: %w", req.UnitID, err)
	}
	if !resp.Ok {
		_, _ = r.destCtrl.WaitUnitDone()
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: source agent stream unit %d: %s", req.UnitID, resp.Error)
	}

	// 3. Wait for the destination to finish the COPY (the unit-done report).
	done, err := r.destCtrl.WaitUnitDone()
	if err != nil {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: dest unit-done unit %d: %w", req.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, "", "", fmt.Errorf("two-agent: dest agent unit %d: %s", req.UnitID, done.Error)
	}

	// Destination row count is authoritative; wire volume from the source.
	// P9-E: return the inline content digests (both empty when the controller
	// did not request verify): the source's (from the stream reply) and the
	// destination's (from the unit-done report), for the manager to compare.
	return done.Rows, resp.Wire, done.Logical, resp.Digest, done.Digest, nil
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
	var release = func() {}
	var err error
	if req.SegListen != "" {
		listen, err = segstream.UnitListen(req.SegListen, uint32(req.OnSegmentReq.UnitID))
	} else {
		listen, release, err = r.prebindData()
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: segment data port: %w", err)
	}
	defer release()
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
	var release = func() {}
	var err error
	if req.SegListen != "" {
		listen, err = segstream.UnitListen(req.SegListen, uint32(req.OnSegmentReq.UnitID))
	} else {
		listen, release, err = r.prebindData()
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: routed data port: %w", err)
	}
	defer release()
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

// Release ends the run (commits the source snapshot) and stops the agents.
func (r *InProcTwoAgentRunner) Release(ctx context.Context) error {
	if r.destCtrl != nil {
		_, _ = r.destCtrl.Release() // end the run on the dest agent (best effort)
		_ = r.destCtrl.Close()
		r.destCtrl = nil
	}
	if r.dest != nil {
		r.finalPersAccepts.Store(r.dest.PersistAccepts())
		r.dest.Close()
		r.dest = nil
	}
	if r.destCancel != nil {
		r.destCancel()
		r.destCancel = nil
	}
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
