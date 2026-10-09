package manifest

import (
	"context"
	"encoding/json"
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
	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/portalloc"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
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
	// Plan (Phase 2, 2A) asks the SOURCE agent to build the copy plan: it
	// runs plan.Build with its LOCAL source pool + a destination querier
	// (production: a remoteQuerier that proxies read-only destination-catalog
	// queries to the destination agent over the persistent source->dest peer
	// channel; in-process test runner: the local destination pool). The
	// controller therefore opens NO direct destination connection for
	// planning (architecture 08.10). The process runner issues the
	// "build-plan" control op; the InProc runner (test double) builds the
	// plan in-process.
	Plan(ctx context.Context) (*plan.Plan, error)
	// Stream launches a destination agent for the unit, instructing the source
	// agent to stream the unit to it, and waits for the destination to finish
	// the COPY. It returns the authoritative row count + the wire/logical
	// volume + (FR-02, P9-E) the inline content digests computed on the data
	// path when VerifyDigest is requested — srcDigest from the source agent,
	// dstDigest from the destination agent (both empty when not requested) +
	// (2B-1) the destination's verdict: swapped (the destination performed the
	// atomic rename itself) and verified (the destination compared the SOURCE
	// vs DEST digests and they matched). The destination staging table is
	// created + loaded by the dest agent; the verify + swap decision is the
	// DESTINATION's (2B-1, architecture 08.10): the source digest rides the
	// unit's Trailer, so the controller no longer re-reads the tables and no
	// longer swaps the coordinator units.
	Stream(ctx context.Context, req unitStreamRequest) (rows int64, wire, logical uint64, srcDigest, dstDigest string, swapped, verified bool, err error)
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
	// StreamMini (P3 production-dispatch, mesh) drives one unit through the
	// MESH mini path: the source agent triggers the ON SEGMENT dispatch with
	// the D-entry address book (the destination-segment mini-agent data
	// endpoints, seg order); each source segreceiver dials its destination
	// segment's mini-agent (direct_1to1: entry s; routed S!=D: it routes by
	// cdbhash/JCH and fans (s,d) over W sub-streams to entry d). The D
	// mini-agents (launched at Start) each perform a local utility COPY on
	// their segment; the supervisor (the dest-agent in supervisor mode)
	// broadcasts the unit, collects the D reports, and combines them (XOR_D).
	// NEITHER cluster coordinator is in the data path (design 04.10). It
	// returns the authoritative (combined) row count + the on-network wire
	// volume + the logical volume + (FR-02, P9-E) the inline content digests
	// when verify is on (srcDigest from the segreceivers' per-slice trailers
	// combined by the supervisor, dstDigest from the minis' data-path
	// digests). The swap is NOT performed (the Manager decides it).
	// StreamMini returns (in addition to the digests) whether the destination
	// agent performed the swap/commit itself (swapped) and whether the SOURCE
	// vs DEST digests matched (verified). The destination owns the swap
	// decision (architecture 08.10); the controller reads the verdict and no
	// longer verifies or swaps the mesh unit itself.
	// events carries the destination agent's diagnostic lines for the unit
	// (1E: swap_done / verify_mismatch / mini errors / journal errors) — the
	// manager forwards them to its own events.jsonl (source=dest-agent).
	StreamMini(ctx context.Context, req unitMiniRequest) (rows int64, wire, logical uint64, srcDigest, dstDigest string, swapped, verified bool, events []string, err error)
	// Checksum asks the source agent (which holds the FR-02 snapshot) for the
	// table's content checksum (the --verify path).
	Checksum(ctx context.Context, schema, table string) (string, error)
	// DDL (2B-2, architecture 08.10) delegates ONE destination-DDL operation
	// to the destination agent (the controller holds no direct destination
	// connection). The runner issues the "ddl" control op to the long-lived
	// dest agent, which runs it on a fresh LOCAL coordinator connection and
	// reports back. req carries the action + the object layout; the response
	// carries the exists-answer (exists action) or an error.
	DDL(ctx context.Context, req agent.DDReq) (agent.DDResp, error)
	// Catalog (2B-3, architecture 08.10) reads one source-catalog object on
	// the SOURCE agent (sequence discovery, sequence last_value, constraint
	// definitions, the FR-17 index definitions, the per-unit table layout,
	// the zero-columns probe, the source segment count) — the controller
	// holds no direct source connection for the per-unit catalog ops. The
	// runner issues the "catalog" control op to the long-lived source
	// agent, which reads it on its LOCAL source pool (the pool, not the
	// pinned snapshot) and reports back.
	Catalog(ctx context.Context, req agent.CatalogReq) (agent.CatalogResp, error)
	// DryRunChecks (2C-3, architecture 08.10) reads ONE side's dry-run
	// values on that side's agent (its LOCAL database connection) and
	// reports them back: side "source" -> the source agent, side "dest" ->
	// the destination agent. The controller holds no direct database
	// connection; it collects both sides and diffs them in memory
	// (dryrun.FromSides).
	DryRunChecks(ctx context.Context, req agent.DryRunChecksReq) (agent.DryRunChecksResp, error)
	// TableExists (2B-5, architecture 08.10) reports whether schema.table
	// exists on the source — the per-unit zero-columns guard's probe (a
	// vanished source table must fail the unit early, before any DDL or
	// data, on a POOL connection so the fault cannot abort the FR-02
	// snapshot). The runner issues it on the source agent (its local pool);
	// the controller holds no direct source connection.
	TableExists(ctx context.Context, schema, table string) bool
	// PeerInfo (1F) is the mesh version-exchange wiring: the destination
	// agent's peer listener address (the source agent dials it once before
	// the first mesh unit) + the "user@" prefix the source SSHes to its
	// source segments to gather "segriver --version". Empty peer = no
	// exchange (the in-process runner / a dest without a peer listener).
	PeerInfo() (peerAddr string, segSSH string)
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
	// Journal (2B-6, architecture 08.10) is the unit's gpstream_units
	// reference the destination agent writes for the COORDINATOR unit it
	// copies, verifies and swaps (the control-plane analogue of the mesh
	// MiniLaunch's journal fields). The runner passes it on the serve-unit
	// control op so the dest agent journals the row itself; the controller
	// records only the file mirror.
	Journal agent.JournalRef
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

// unitMiniRequest is the Manager's per-unit instruction for the P3 mesh mini
// data path (direct_1to1 or routed S!=D): the source agent triggers the ON
// SEGMENT dispatch with the D-entry address book (the mini-agent data
// endpoints); the destination supervisor (the dest-agent in supervisor mode)
// serves the unit via the D mini-agents (each a local utility COPY on its
// segment) and combines their reports (XOR_D). The MiniLaunch carries the
// staging layout (the supervisor builds the distributed staging table on the
// destination coordinator) + the unit's shape (S/W for the mini's accept
// grid).
type unitMiniRequest struct {
	agent.OnSegmentReq // Mesh=true (Routed for S!=D); DestAddr = the D-entry address book
	// Launch is the destination supervisor's mini launch payload (staging
	// layout + S + W).
	Launch agent.MiniLaunch
	// D is the number of destination primary segments (== the address-book
	// entry count).
	D int
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
	// --port-range (local / single-host layout); nil = auto. The remote
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
	// destPeerAddr (1F): the dest agent's mesh peer listener address (the
	// version exchange; empty when the dest has no peer listener).
	destPeerAddr string
	// --- P3 production-dispatch (mesh): the D destination-segment mini-agents ---
	// supervisorReport is the dest-agent's (supervisor-mode) report listener
	// address (from its hello): the D minis dial it once for the run. Empty
	// outside supervisor mode.
	supervisorReport string
	// miniAddrs is the D-entry address book (seg order, comma-joined) from
	// the supervisor's MiniMap: the segreceivers' --dest-addr D form (the
	// source segreceiver dials mini d's endpoint for destination segment d).
	miniAddrs string
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
	alloc, _ := portalloc.ParseRange(cfg.PortRange) // Validate already checked it
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
	// Idempotent (Phase 2): Manager.Plan (the source-agent plan-build) calls
	// Start to bring the agents up, then Manager.Run calls it again before
	// the copy. A second Start on an already-started runner is a no-op (the
	// agents + control clients are already connected; the FR-02 snapshot is
	// already pinned by the first Start).
	if r.srcHandle != nil {
		return nil
	}
	if err := r.startSource(ctx); err != nil {
		return err
	}
	// A destination agent is only launched when a destination is configured.
	// The input-stream benchmark (bench.go) reads the source only and drains
	// to a local sink — it has no destination, so the source agent is all
	// that is needed. Launching the dest agent with an empty --dest conn
	// string would make it fail its pg pool before its control listener
	// opens, so the controller's dial would get "connection refused".
	if r.cfg.Dest != "" {
		if err := r.startDest(ctx); err != nil {
			// Surface the real startDest error (dial failure, launch
			// failure) — stopSource is cleanup, not the cause. Swallowing it
			// (the old `return r.stopSource()`) turned every dest-start
			// failure into the misleading "destination agent not started" at
			// stream time.
			stopErr := r.stopSource()
			if stopErr != nil {
				return fmt.Errorf("start dest: %w (source stop: %v)", err, stopErr)
			}
			return err
		}
	}
	return nil
}

// Plan (Phase 2, 2A) asks the SOURCE agent to build the copy plan over the
// control socket: the source establishes the persistent source->dest peer
// channel (the 1F version exchange), runs plan.Build with its LOCAL source
// pool + a remote destination querier (proxied over the peer channel), and
// returns the finished plan. The controller opens NO direct destination
// connection for planning. The destination agent's peer address comes from
// the dest hello (r.destPeerAddr, set by startDest in ALL modes).
func (r *processTwoAgentRunner) Plan(ctx context.Context) (*plan.Plan, error) {
	if r.ctrl == nil {
		return nil, fmt.Errorf("two-agent: source agent not started (call Start first)")
	}
	cfgBytes, err := json.Marshal(r.cfg)
	if err != nil {
		return nil, fmt.Errorf("two-agent: marshal config for build-plan: %w", err)
	}
	req := agent.PlanReq{
		Op:             "build-plan",
		PeerAddr:       r.destPeerAddr,
		SegBinary:      r.cfg.SegBinary,
		SegSSH:         r.sshUserPrefix(r.cfg.SourceAgentSSH),
		Config:         cfgBytes,
		TimeoutSeconds: int(r.cfg.Timeout / time.Second),
	}
	resp, err := r.ctrl.Plan(req)
	if err != nil {
		return nil, fmt.Errorf("two-agent: build-plan: %w", err)
	}
	if !resp.Ok {
		// A partial plan (timed out) is still returned (resp.Plan) — the
		// caller prints it as a forecast (FR-10 §6.6).
		if resp.Plan != nil && resp.Partial {
			return resp.Plan, fmt.Errorf("two-agent: build-plan (partial): %s", resp.Error)
		}
		return nil, fmt.Errorf("two-agent: build-plan: %s", resp.Error)
	}
	if resp.Plan == nil {
		return nil, fmt.Errorf("two-agent: build-plan: empty plan in response")
	}
	return resp.Plan, nil
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
	srcCtrlListen, srcCtrlDial, err := r.controlListen(r.agentHost(r.cfg.SourceAgentSSH))
	if err != nil {
		return err
	}
	srcArgs := []string{
		"--src", r.cfg.Source,
		"--snapshot", r.snapshotMode(),
		"--serve", srcCtrlListen,
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
	h, err := launch.Ensure(ctx, r.target("gpstream-source-agent", srcArgs, srcCtrlDial, r.cfg.SourceAgentSSH))
	if err != nil {
		return fmt.Errorf("two-agent: launch source agent: %w", err)
	}
	r.srcHandle = h
	client, err := dialControlRetry(ctx, srcCtrlDial, controlDialDeadline(r.cfg.SourceAgentSSH), r.controlTLS())
	if err != nil {
		h.Stop()
		r.srcHandle = nil
		return fmt.Errorf("two-agent: source control dial %s: %w (agent log: %s)\n%s", srcCtrlDial, err, agentLogPath("gpstream-source-agent"), tailLog(agentLogPath("gpstream-source-agent")))
	}
	r.ctrl = client
	return nil
}

// startDest brings up the long-lived dest agent and dials its control
// socket (reading the hello for the real listener addresses). Coordinator
// mode: one data + one control + one persist listener. Mesh mode
// (P3 production-dispatch): the dest-agent runs in SUPERVISOR mode — one
// control + one persist (the fallback coordinator path) + one report
// listener (the D minis dial it), the shared data listener is OFF — and the
// controller then launches the D destination-segment mini-agents (each
// gpstream-dest-agent --mini on its segment host) and waits for the
// MiniMap (the D-entry address book) to be ready.
func (r *processTwoAgentRunner) startDest(ctx context.Context) error {
	mesh := r.cfg.CopyMode == config.CopyMesh
	destCtrlListen, destCtrlDial, err := r.controlListen(r.agentHost(r.cfg.DestAgentSSH))
	if err != nil {
		return err
	}
	// Coordinator mode: the shared data listener (the source agent / the
	// segreceivers dial it). Mesh mode: OFF (the mini path uses the minis'
	// own data listeners; the fallback coordinator path uses the persist
	// listener) — 4+D port formula.
	var dataAddr string
	if !mesh {
		dataAddr, err = r.dataListen(r.agentHost(r.cfg.DestAgentSSH))
		if err != nil {
			return err
		}
	}
	// P1c: the dedicated persistent-coordinator listener (a third listener;
	// the source agent dials it ONCE per run and every coordinator unit
	// rides the same connection). Local agents get a pre-bound loopback
	// port; remote agents use auto :0 (the agent restricts it to
	// --port-range itself) — the hello reports the real address.
	persistListen, perr := r.dataListen(r.agentHost(r.cfg.DestAgentSSH))
	if perr != nil {
		return fmt.Errorf("two-agent: persist listen: %w", perr)
	}
	// Mesh mode: the supervisor's REPORT listener (the D minis dial it once
	// for the run; orphan-exit on drop). Architecture 08.10: the controller
	// does NOT read the destination catalog — the dest-agent (on the
	// destination coordinator) reads its OWN catalog, derives D, and
	// launches the D mini-agents itself. The controller only allocates the
	// report-listen port and hands the agent --supervisor + --report-listen.
	var reportListen string
	if mesh {
		var reportDial string
		reportListen, reportDial, err = r.controlListen(r.agentHost(r.cfg.DestAgentSSH))
		_ = reportDial
		if err != nil {
			return err
		}
	}
	// 1F/Phase 2: the PEER listener (the source agent dials it for the
	// persistent source->dest channel: the 1F version exchange + the read-only
	// dst-query loop for plan/resume/dry-run). Allocated in ALL modes (the
	// plan is built by the source agent in both coordinator and mesh modes).
	var peerListen string
	peerListen, _, err = r.controlListen(r.agentHost(r.cfg.DestAgentSSH))
	if err != nil {
		return err
	}
	// --dest comes FIRST so the agent stays identifiable by its conn string
	// (the crash-recovery test finds it via pgrep "<bin> --dest ").
	dargs := []string{"--dest", r.cfg.Dest}
	if mesh {
		// Supervisor mode: NO --serve / NO --data-listen (the shared data
		// listener is off; kind="mini" serves go to the minis, the fallback
		// coordinator path rides the persist listener). The agent reads its
		// OWN catalog for D + the segment hosts and launches the D
		// mini-agents itself (architecture 08.10); --mini-ssh-user (the
		// "user@" prefix of the dest-agent SSH target — the agent reaches
		// each segment host as user@<seg-host>) + --port-range let it reach
		// the segment hosts and bound their data ports (the 4+D formula).
		dargs = append(dargs,
			"--supervisor",
			"--report-listen", reportListen,
			"--mini-ssh-user", r.sshUserPrefix(r.cfg.DestAgentSSH),
			"--port-range", r.cfg.PortRange,
		)
	} else {
		dargs = append(dargs,
			"--serve",
			"--data-listen", dataAddr,
		)
	}
	// --peer-listen in BOTH modes (the source agent's persistent channel).
	dargs = append(dargs, "--peer-listen", peerListen)
	dargs = append(dargs,
		"--control-listen", destCtrlListen,
		"--persist-listen", persistListen,
		// P1d: size the destination pg pool for the concurrent persistent
		// serves (the run's data-worker count). In supervisor mode it is
		// the fallback coordinator path's pool (the user-confirmed
		// "уже реализовано для режима coordinator" mechanism).
		"--workers", strconv.Itoa(r.cfg.WorkersPerPair),
		"--accept-timeout", "5m",
		"--stream-timeout", "6h",
		"--frame-ack", r.cfg.FrameAckArg(),
		"--ack-timeout", r.cfg.AckTimeout,
		"--max-reconnects", strconv.Itoa(r.cfg.MaxReconnects),
		"--retransmit-buffer-max", strconv.FormatInt(r.cfg.RetransmitBufferMax, 10),
	)
	// --port-range matters only when a listener address is auto (":0"): the
	// remote/dial-first layout, where the agent binds its own data port and
	// the controller cannot pre-bind it. Local (same-host) agents get
	// pre-bound fixed ports from the controller's own allocator, so the range
	// is already applied there — passing it is a harmless no-op.
	if r.cfg.PortRange != "" {
		dargs = append(dargs, "--port-range", r.cfg.PortRange)
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
	h, err := launch.Ensure(ctx, r.target("gpstream-dest-agent", dargs, destCtrlDial, r.cfg.DestAgentSSH))
	if err != nil {
		return fmt.Errorf("two-agent: launch dest agent: %w", err)
	}
	r.destHandle = h
	client, err := dialDestControlRetry(ctx, destCtrlDial, controlDialDeadline(r.cfg.DestAgentSSH), r.controlTLS())
	if err != nil {
		h.Stop()
		r.destHandle = nil
		return fmt.Errorf("two-agent: dest control dial %s: %w (agent log: %s)\n%s", destCtrlDial, err, agentLogPath("gpstream-dest-agent"), tailLog(agentLogPath("gpstream-dest-agent")))
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
	// 1F: the mesh peer listener (the source agent dials it once before the
	// first mesh unit for the version exchange).
	r.destPeerAddr = client.Hello.Peer
	// Mesh mode (P3 production-dispatch, architecture 08.10): the DEST-AGENT
	// already launched the D mini-agents itself (in its constructor: it read
	// its own catalog and started the minis, which dial the supervisor's
	// report listener). The controller does NOT launch them — it only waits
	// for the MiniMap (the D-entry address book the dest-agent reports
	// through its control protocol; the segreceivers' --dest-addr).
	if mesh {
		addrs, err := r.MiniMap(ctx, 120*time.Second)
		if err != nil {
			r.Release(ctx)
			return fmt.Errorf("two-agent: mini-map: %w", err)
		}
		r.miniAddrs = addrs
	}
	return nil
}

// target builds a launch.Target for one agent. The binary is the packaged
// install name (resolved on PATH on whichever host it runs — the controller's
// own host for local, the agent host over SSH for remote).
func (r *processTwoAgentRunner) target(base string, args []string, controlAddr, sshHost string) launch.Target {
	bin := r.cfg.SourceAgentBinary
	if base == "gpstream-dest-agent" {
		bin = r.cfg.DestAgentBinary
	}
	if bin == "" {
		bin = base
	}
	return launch.Target{
		Binary:      bin,
		Args:        args,
		ControlAddr: controlAddr,
		SSHHost:     sshHost,
		LogPath:     agentLogPath(base),
		WaitTimeout: 30 * time.Second,
	}
}

// controlListen returns (listenAddr, dialAddr) for the agent's control
// socket. A local (same-host) agent gets a pre-bound loopback port for both.
// A remote agent gets a controller-allocated port: the agent binds
// "0.0.0.0:port" (listenAddr — it binds all its interfaces; the host in the
// flag may be a hostname it cannot bind) and the controller dials
// "host:port" (dialAddr).
func (r *processTwoAgentRunner) controlListen(host string) (listenAddr, dialAddr string, err error) {
	if host == "" || agent.IsLocalHost(host) {
		addr, e := r.prebindLocal()
		return addr, addr, e
	}
	port, e := r.allocRemoteControlPort(host)
	if e != nil {
		return "", "", fmt.Errorf("two-agent: allocate remote control port on %s: %w", host, e)
	}
	return net.JoinHostPort("0.0.0.0", strconv.Itoa(port)), net.JoinHostPort(host, strconv.Itoa(port)), nil
}

// dataListen returns the data address for the dest agent. A local agent gets a
// pre-bound loopback port (auto :0, reported in the hello); a remote agent
// uses "0.0.0.0:0" (auto :0 — the agent restricts it to --port-range itself,
// and the hello carries the real address).
func (r *processTwoAgentRunner) dataListen(host string) (string, error) {
	if host == "" || agent.IsLocalHost(host) {
		return r.prebindLocal()
	}
	return "0.0.0.0:0", nil
}

// prebindLocal learns a free loopback port for a local (same-host) agent and
// returns it as "127.0.0.1:port", honoring --port-range when set (the
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

// agentHost extracts the bare host from a --*-agent-ssh target
// ("user@host[:port]" / "host" / "" -> host or ""). The host decides the
// transport: local host = subprocess, remote host = SSH.
func (r *processTwoAgentRunner) agentHost(sshTarget string) string {
	host := sshTarget
	if i := strings.LastIndex(host, "@"); i >= 0 {
		host = host[i+1:]
	}
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	return host
}

// PeerInfo (1F) is the mesh version-exchange wiring: the destination agent's
// peer listener address (the source agent dials it once before the first
// mesh unit) + the "user@" prefix the source SSHes to its source segments to
// gather "segriver --version". Empty peer = no exchange (a dest without a
// peer listener / not in supervisor mode).
func (r *processTwoAgentRunner) PeerInfo() (peerAddr string, segSSH string) {
	return r.destPeerAddr, r.sshUserPrefix(r.cfg.SourceAgentSSH)
}

// sshUserPrefix extracts the "user@" prefix of an SSH target (the operator's
// --dest-agent-ssh user) so the dest-agent can reach each segment host as
// user@<seg-host>. Empty when the target has no user (no SSH / single-host).
func (r *processTwoAgentRunner) sshUserPrefix(sshTarget string) string {
	if i := strings.LastIndex(sshTarget, "@"); i >= 0 {
		return sshTarget[:i+1]
	}
	return ""
}

// allocRemoteControlPort picks a control port on a REMOTE agent host: within
// --port-range when set (the allocator probes via loopback, which is a
// local hint, not a remote probe — a remote collision is caught by the
// bounded control dial), else a kernel-picked loopback port as a fresh-port
// candidate.
func (r *processTwoAgentRunner) allocRemoteControlPort(host string) (int, error) {
	if r.portAlloc == nil {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return 0, err
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		return port, nil
	}
	port, err := r.portAlloc.Next("127.0.0.1")
	if err != nil {
		return 0, err
	}
	return port, nil
}

// tailLog returns the last ~10 non-empty lines of a log file ("" when absent
// or unreadable) for embedding in dial errors — the remote agent's stderr
// carries its master-check verdict.
func tailLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) > 10 {
		lines = lines[len(lines)-10:]
	}
	return "  " + strings.Join(lines, "\n  ")
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
// agent (the bytes it wrote to the socket). 2B-1: the verify + swap decision
// is the DESTINATION's (the source digest rides the unit's Trailer) — the
// runner returns the destination's verdict (swapped/verified) to the Manager.
func (r *processTwoAgentRunner) Stream(ctx context.Context, req unitStreamRequest) (int64, uint64, uint64, string, string, bool, bool, error) {
	if r.destCtrl == nil {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: destination agent not started")
	}
	// P1d: the persistent path is CONCURRENT (no streamMu): the dest agent's
	// control loop serves N units in flight, correlated by ReqID. The legacy
	// per-unit path keeps streamMu (the sequential data listener).
	if r.destPersistAddr != "" {
		return r.streamCoordinatorPersistent(ctx, req)
	}
	// 2B-1: the legacy per-unit path cannot delegate the verify + swap to the
	// destination (its FramedReader ends at the Trailer WITHOUT exposing the
	// source digest, so the destination could not compare) — the production
	// runner always uses the persistent channel, so this branch is
	// unreachable in production (only a misconfigured test could hit it).
	return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: legacy per-unit coordinator serve cannot carry the source digest to the destination (2B-1); use the persistent coordinator channel")
}

// streamCoordinatorPersistent (P1d) drives one coordinator unit over the
// dest's persistent channel POOL (no streamMu: the dest control loop serves
// N units in flight, correlated by ReqID; unit X rides channel X%N).
func (r *processTwoAgentRunner) streamCoordinatorPersistent(ctx context.Context, req unitStreamRequest) (int64, uint64, uint64, string, string, bool, bool, error) {
	ack, err := r.destCtrl.BeginServe(agent.DestServeReq{Kind: "coordinator", UnitID: req.UnitID, Journal: req.Journal})
	if err != nil {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %w", req.UnitID, err)
	}
	if !ack.Ok {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %s", req.UnitID, ack.Error)
	}
	sreq := req.StreamReq
	sreq.DestAddr = r.destPersistAddr
	sreq.Persistent = true
	resp, err := r.ctrl.Stream(sreq)
	if err != nil {
		_, _ = r.destCtrl.CancelUnit(req.UnitID)
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: stream unit %d: %w", req.UnitID, err)
	}
	if !resp.Ok {
		_, _ = r.destCtrl.CancelUnit(req.UnitID)
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: source agent stream unit %d: %s", req.UnitID, resp.Error)
	}
	done, err := r.destCtrl.WaitUnitDone(ack.ReqID)
	if err != nil {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: dest unit-done unit %d: %w", req.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: dest agent unit %d: %s", req.UnitID, done.Error)
	}
	// P9-E: return the inline content digests (both empty when the controller
	// did not request verify): the source's (from the stream reply) and the
	// destination's (from the unit-done report). 2B-1: the destination's
	// verdict (swapped = it performed the atomic rename itself; verified =
	// the SOURCE vs DEST digests matched, so the verify gate passed) — the
	// Manager skips its own verify + swap for the unit when the destination
	// handled them.
	return done.Rows, resp.Wire, done.Logical, resp.Digest, done.Digest, done.Swapped, done.Verified, nil
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
		UnitID:   req.UnitID,
	}
	if req.DirectWrite {
		kind = "direct"
		dreq = agent.DestServeReq{Kind: "direct", Direct: &req.DirectLaunch, UnitID: req.UnitID}
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
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, fmt.Errorf("two-agent: onsegment unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, fmt.Errorf("two-agent: source agent onsegment unit %d: %s", o.UnitID, oresp.Error)
	}
	// 3. Wait for the destination to finish (the unit-done report, correlated
	// by the ack's ReqID).
	done, err := r.destCtrl.WaitUnitDone(ack.ReqID)
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
	ack, err := r.destCtrl.BeginServe(agent.DestServeReq{Kind: "routed", Routed: &req.Launch, UnitID: req.UnitID})
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
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, fmt.Errorf("two-agent: onsegment (routed) unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, fmt.Errorf("two-agent: source agent onsegment (routed) unit %d: %s", o.UnitID, oresp.Error)
	}
	// 3. Wait for the destination to finish (the unit-done report, correlated
	// by the ack's ReqID).
	done, err := r.destCtrl.WaitUnitDone(ack.ReqID)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("two-agent: dest unit-done (routed) unit %d: %w", o.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, fmt.Errorf("two-agent: dest agent unit %d (routed): %s", o.UnitID, done.Error)
	}
	return done.Rows, done.Wire, done.Logical, nil
}

// Checksum asks the source agent (FR-02 snapshot) for the table's checksum.
// StreamMini (P3 production-dispatch, mesh) drives one unit through the MESH
// mini path: the destination supervisor serves it (kind="mini" — it builds
// the distributed staging table, broadcasts the unit to the D mini-agents,
// each does a local utility COPY on its segment, and the supervisor combines
// the D reports with XOR_D); the source agent triggers the ON SEGMENT
// dispatch with the D-entry address book (the segreceivers dial the
// mini-agents' data endpoints — neither cluster coordinator is in the data
// path). It returns the combined row count + wire + logical + (FR-02) the
// inline digests when verify is on.
func (r *processTwoAgentRunner) StreamMini(ctx context.Context, req unitMiniRequest) (int64, uint64, uint64, string, string, bool, bool, []string, error) {
	if r.destCtrl == nil {
		return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: destination agent not started")
	}
	if r.miniAddrs == "" {
		return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: no mini address book (the mesh MiniMap is not ready)")
	}
	r.streamMu.Lock() // serialize the whole exchange (see streamMu)
	defer r.streamMu.Unlock()
	// 1. Tell the supervisor to serve this unit (kind="mini"). It builds the
	//    distributed staging table, broadcasts the unit to the D minis, and
	//    returns the combined report (XOR_D) when the unit-done lands.
	ack, err := r.destCtrl.BeginServe(agent.DestServeReq{Kind: "mini", Mini: &req.Launch, UnitID: req.UnitID})
	if err != nil {
		return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: begin serve (mini) unit %d: %w", req.UnitID, err)
	}
	if !ack.Ok {
		return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: begin serve (mini) unit %d: %s", req.UnitID, ack.Error)
	}
	// 2. Instruct the source agent to run the ON SEGMENT dispatch; the
	//    segreceivers dial the D-entry address book (the mini data
	//    endpoints). Direct_1to1: each segreceiver s dials entry s. Routed
	//    (S!=D): it routes by cdbhash/JCH and fans (s,d) over W sub-streams
	//    to entry d.
	o := req.OnSegmentReq
	o.Mesh = true
	o.DestAddr = r.miniAddrs
	oresp, err := r.ctrl.OnSegment(o)
	if err != nil {
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: onsegment (mini) unit %d: %w", o.UnitID, err)
	}
	if !oresp.Ok {
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: source agent onsegment (mini) unit %d: %s", o.UnitID, oresp.Error)
	}
	// 3. Wait for the destination (supervisor) to finish (the unit-done
	//    report, correlated by the ack's ReqID).
	done, err := r.destCtrl.WaitUnitDone(ack.ReqID)
	if err != nil {
		return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: dest unit-done (mini) unit %d: %w", o.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, "", "", false, false, done.Events, fmt.Errorf("two-agent: dest agent unit %d (mini): %s", o.UnitID, done.Error)
	}
	return done.Rows, done.Wire, done.Logical, done.SrcDigest, done.Digest, done.Swapped, done.Verified, done.Events, nil
}

// MiniMap (P3 production-dispatch, mesh) polls the dest-agent (supervisor
// mode) until its D-entry address book is complete (all D minis have
// connected) and returns it comma-joined in seg order (the segreceivers'
// --dest-addr D form). It fails when the supervisor reports an accept error
// (a mini never connected) or the deadline elapses.
func (r *processTwoAgentRunner) MiniMap(ctx context.Context, deadline time.Duration) (string, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	for {
		mm, err := r.destCtrl.MiniMap()
		if err != nil {
			return "", fmt.Errorf("two-agent: mini-map: %w", err)
		}
		if mm.Ok {
			return mm.Addrs, nil
		}
		if mm.Error != "" && strings.Contains(mm.Error, "accept") {
			return "", fmt.Errorf("two-agent: mini-map: %s", mm.Error)
		}
		select {
		case <-deadlineCtx.Done():
			return "", fmt.Errorf("two-agent: mini-map not ready after %s (%s)", deadline, mm.Error)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

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

// DDL (2B-2, architecture 08.10) delegates one destination-DDL operation to
// the long-lived destination agent over its control socket. The agent runs it
// on a fresh LOCAL coordinator connection and reports the result; the
// controller never opens a direct destination connection.
func (r *processTwoAgentRunner) DDL(ctx context.Context, req agent.DDReq) (agent.DDResp, error) {
	resp, err := r.destCtrl.DDL(req)
	if err != nil {
		return resp, fmt.Errorf("two-agent: dest ddl %s %s.%s: %w", req.Action, req.Schema, req.Table, err)
	}
	if !resp.Ok && resp.Error != "" {
		return resp, fmt.Errorf("two-agent: dest ddl %s %s.%s: %s", req.Action, req.Schema, req.Table, resp.Error)
	}
	return resp, nil
}

// Catalog (2B-3, architecture 08.10) reads one source-catalog object on the
// long-lived source agent over its control socket (the controller never
// opens a direct source connection for the per-unit catalog ops).
func (r *processTwoAgentRunner) Catalog(ctx context.Context, req agent.CatalogReq) (agent.CatalogResp, error) {
	resp, err := r.ctrl.Catalog(req)
	if err != nil {
		return resp, fmt.Errorf("two-agent: source catalog %s %s.%s: %w", req.Action, req.Schema, req.Table, err)
	}
	if !resp.Ok && resp.Error != "" {
		return resp, fmt.Errorf("two-agent: source catalog %s %s.%s: %s", req.Action, req.Schema, req.Table, resp.Error)
	}
	return resp, nil
}

// TableExists (2B-5, architecture 08.10) asks the source agent whether the
// table exists (the zero-columns guard probe, on the agent's local pool).
func (r *processTwoAgentRunner) TableExists(ctx context.Context, schema, table string) bool {
	resp, err := r.ctrl.Catalog(agent.CatalogReq{Action: "table-exists", Schema: schema, Table: table})
	return err == nil && resp.Ok && resp.Exists
}

// DryRunChecks (2C-3, architecture 08.10) reads ONE side's dry-run values on
// that side's agent (its LOCAL database connection). side "source" routes to
// the source agent (the source pool, the on_segment probe when segBinary is
// set); side "dest" routes to the destination agent (a fresh LOCAL coordinator
// connection). The controller holds no direct database connection.
func (r *processTwoAgentRunner) DryRunChecks(ctx context.Context, req agent.DryRunChecksReq) (agent.DryRunChecksResp, error) {
	if req.Side == "dest" {
		if r.destCtrl == nil {
			return agent.DryRunChecksResp{}, fmt.Errorf("two-agent: destination agent not started (dry-run dest checks)")
		}
		resp, err := r.destCtrl.DryRunChecks(req)
		if err != nil {
			return resp, fmt.Errorf("two-agent: dest dry-run-checks: %w", err)
		}
		if !resp.Ok && resp.Error != "" {
			return resp, fmt.Errorf("two-agent: dest dry-run-checks: %s", resp.Error)
		}
		return resp, nil
	}
	if r.ctrl == nil {
		return agent.DryRunChecksResp{}, fmt.Errorf("two-agent: source agent not started (dry-run source checks)")
	}
	resp, err := r.ctrl.DryRunChecks(req)
	if err != nil {
		return resp, fmt.Errorf("two-agent: source dry-run-checks: %w", err)
	}
	if !resp.Ok && resp.Error != "" {
		return resp, fmt.Errorf("two-agent: source dry-run-checks: %s", resp.Error)
	}
	return resp, nil
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
// --port-range (r.portAlloc) when set. It returns the address plus a
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
			// P1d: source-agent control is concurrent (one conn, N ops in
			// flight) — start the response-reader that routes responses to
			// the waiting worker by ReqID.
			client.StartReader()
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
	portAlloc *portalloc.Allocator // --port-range for the pre-bound loopback ports (nil = auto)
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
	alloc, _ := portalloc.ParseRange(cfg.PortRange) // Validate already checked it
	return &InProcTwoAgentRunner{cfg: cfg, portAlloc: alloc}
}

// Start launches the in-process source agent (SourceServer) on a pre-bound
// loopback port and connects the control client to it.
func (r *InProcTwoAgentRunner) Start(ctx context.Context) error {
	// Idempotent (Phase 2): Manager.Plan (the source-agent plan-build) calls
	// Start, then Manager.Run calls it again. A second Start is a no-op.
	if r.srv != nil {
		return nil
	}
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
		// P1d: size the destination pg pool for the concurrent persistent
		// serves (the run's data-worker count).
		Workers:       r.cfg.WorkersPerPair,
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

// Plan (Phase 2, 2A, in-proc test double) builds the plan IN-PROCESS: the
// source agent's BuildPlan (its LOCAL source pool) + the destination catalog
// via a local pool on cfg.Dest (the test double has no cross-process peer
// channel — the production process runner exercises the real proxy). The
// Manager's unit-test doubles (fakeBenchRunner) implement Plan directly; this
// is the in-proc runner used by the pg6lab integration tests.
func (r *InProcTwoAgentRunner) Plan(ctx context.Context) (*plan.Plan, error) {
	if r.srv == nil {
		return nil, fmt.Errorf("two-agent: in-proc source agent not started (call Start first)")
	}
	var dst pg.Querier
	if r.cfg.Dest != "" {
		dp, err := pg.NewPool(ctx, r.cfg.Dest, r.cfg.SSLCert, r.cfg.SSLKey, r.cfg.SSLCA, r.cfg.SSLMinTLS, 2)
		if err != nil {
			return nil, fmt.Errorf("two-agent: in-proc plan: connect dest: %w", err)
		}
		defer dp.Close()
		dst = dp
	}
	timeout := r.cfg.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return r.srv.BuildPlan(ctx, timeout, r.cfg, dst)
}

// Stream drives one coordinator unit through the LONG-LIVED in-proc
// destination agent (P1c): BeginServe (the agent acks immediately and blocks
// on the persistent channel's next UnitBegin) -> the source agent streams the
// unit over the run's ONE persistent coordinator channel -> WaitUnitDone.
// The destination is the authoritative row-count source; the wire volume
// comes from the source agent. The swap is NOT performed (the Manager does it).
func (r *InProcTwoAgentRunner) Stream(ctx context.Context, req unitStreamRequest) (int64, uint64, uint64, string, string, bool, bool, error) {
	if r.dest == nil || r.destCtrl == nil {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: in-proc dest server not started")
	}
	// P1d: NO streamMu — the dest agent's control loop is concurrent (N
	// serves in flight on one control connection, correlated by ReqID), so
	// the run's N workers overlap their data phases. (The mesh serves still
	// serialize on the dest side — see StreamSegment/StreamRouted.)

	// 1. Tell the long-lived dest agent to serve this unit on the persistent
	//    channel. The ack returns immediately (the serve is registered and
	//    waiting for the unit's UnitBegin off its channel).
	ack, err := r.destCtrl.BeginServe(agent.DestServeReq{Kind: "coordinator", UnitID: req.UnitID, Journal: req.Journal})
	if err != nil {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %w", req.UnitID, err)
	}
	if !ack.Ok {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: begin serve (coordinator) unit %d: %s", req.UnitID, ack.Error)
	}

	// 2. Instruct the source agent to stream the unit over the persistent
	//    channel (the dest's persist address, from its hello). The source
	//    dials the channel pool on the first unit and keeps the connections
	//    for the run (unit X rides channel X%N).
	sreq := req.StreamReq
	sreq.DestAddr = r.dest.PersistAddr()
	sreq.Persistent = true
	resp, err := r.ctrl.Stream(sreq)
	if err != nil {
		// The unit's data will not arrive: cancel the in-flight serve (the
		// dest fails the serve / drains the partial channel) so the
		// unit-done report returns promptly instead of waiting for the run
		// ctx.
		_, _ = r.destCtrl.CancelUnit(req.UnitID)
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: stream unit %d: %w", req.UnitID, err)
	}
	if !resp.Ok {
		_, _ = r.destCtrl.CancelUnit(req.UnitID)
		_, _ = r.destCtrl.WaitUnitDone(ack.ReqID)
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: source agent stream unit %d: %s", req.UnitID, resp.Error)
	}

	// 3. Wait for the destination to finish the COPY (the unit-done report,
	//    correlated to THIS serve by the ack's ReqID).
	done, err := r.destCtrl.WaitUnitDone(ack.ReqID)
	if err != nil {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: dest unit-done unit %d: %w", req.UnitID, err)
	}
	if !done.Ok {
		return 0, 0, 0, "", "", false, false, fmt.Errorf("two-agent: dest agent unit %d: %s", req.UnitID, done.Error)
	}

	// Destination row count is authoritative; wire volume from the source.
	return done.Rows, resp.Wire, done.Logical, resp.Digest, done.Digest, done.Swapped, done.Verified, nil
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

// StreamMini (P3 mesh) is not implemented in the in-process runner: the
// mesh mini path needs the long-lived supervisor-mode dest-agent + the D
// mini-agents (a process layout). The in-process runner drives the
// coordinator + segment-direct + routed paths; a mesh-mode run uses the
// process runner (the production / live-e2e path).
func (r *InProcTwoAgentRunner) StreamMini(ctx context.Context, req unitMiniRequest) (int64, uint64, uint64, string, string, bool, bool, []string, error) {
	return 0, 0, 0, "", "", false, false, nil, fmt.Errorf("two-agent: StreamMini (mesh mini path) requires the process runner (the in-process runner does not run the supervisor + mini-agents)")
}

func (r *InProcTwoAgentRunner) Checksum(ctx context.Context, schema, table string) (string, error) {
	if r.srv == nil {
		return "", fmt.Errorf("two-agent: source agent not started")
	}
	return r.srv.Checksum(ctx, schema, table)
}

// DDL (2B-2, architecture 08.10) delegates one destination-DDL operation to
// the in-process destination agent over its control socket (mirrors the
// process runner; the in-proc DestServer runs it on a LOCAL coordinator
// connection).
func (r *InProcTwoAgentRunner) DDL(ctx context.Context, req agent.DDReq) (agent.DDResp, error) {
	if r.destCtrl == nil {
		return agent.DDResp{}, fmt.Errorf("two-agent: dest agent not started (ddl %s)", req.Action)
	}
	resp, err := r.destCtrl.DDL(req)
	if err != nil {
		return resp, fmt.Errorf("two-agent: dest ddl %s %s.%s: %w", req.Action, req.Schema, req.Table, err)
	}
	if !resp.Ok && resp.Error != "" {
		return resp, fmt.Errorf("two-agent: dest ddl %s %s.%s: %s", req.Action, req.Schema, req.Table, resp.Error)
	}
	return resp, nil
}

// Catalog (2B-3, architecture 08.10) reads one source-catalog object on the
// in-process source agent (mirrors the process runner; the in-proc
// SourceServer reads it on its LOCAL source pool).
func (r *InProcTwoAgentRunner) Catalog(ctx context.Context, req agent.CatalogReq) (agent.CatalogResp, error) {
	if r.srv == nil {
		return agent.CatalogResp{}, fmt.Errorf("two-agent: source agent not started (catalog %s)", req.Action)
	}
	resp, err := r.ctrl.Catalog(req)
	if err != nil {
		return resp, fmt.Errorf("two-agent: source catalog %s %s.%s: %w", req.Action, req.Schema, req.Table, err)
	}
	if !resp.Ok && resp.Error != "" {
		return resp, fmt.Errorf("two-agent: source catalog %s %s.%s: %s", req.Action, req.Schema, req.Table, resp.Error)
	}
	return resp, nil
}

// TableExists (2B-5, architecture 08.10) mirrors the process runner on the
// in-process source agent.
func (r *InProcTwoAgentRunner) TableExists(ctx context.Context, schema, table string) bool {
	if r.srv == nil {
		return false
	}
	return r.srv.TableExists(ctx, schema, table)
}

// DryRunChecks (2C-3, architecture 08.10) mirrors the process runner: it
// routes side "source" to the in-process source agent and side "dest" to the
// in-process destination agent over their (real, in-process) control sockets.
func (r *InProcTwoAgentRunner) DryRunChecks(ctx context.Context, req agent.DryRunChecksReq) (agent.DryRunChecksResp, error) {
	if req.Side == "dest" {
		if r.destCtrl == nil {
			return agent.DryRunChecksResp{}, fmt.Errorf("two-agent: in-proc dest agent not started (dry-run dest checks)")
		}
		resp, err := r.destCtrl.DryRunChecks(req)
		if err != nil {
			return resp, fmt.Errorf("two-agent: in-proc dest dry-run-checks: %w", err)
		}
		return resp, nil
	}
	if r.ctrl == nil {
		return agent.DryRunChecksResp{}, fmt.Errorf("two-agent: in-proc source agent not started (dry-run source checks)")
	}
	resp, err := r.ctrl.DryRunChecks(req)
	if err != nil {
		return resp, fmt.Errorf("two-agent: in-proc source dry-run-checks: %w", err)
	}
	return resp, nil
}

// Release ends the run (commits the source snapshot) and stops the agents.
func (r *InProcTwoAgentRunner) PeerInfo() (string, string) { return "", "" }

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
