// destserver.go — the LONG-LIVED destination agent server (design 04.10).
//
// One dest agent = ONE data listener + ONE control listener for the whole
// run (not one process/port per unit — the port-economy fix: Greenplum owns
// wide port ranges, so a run must reuse a SMALL set of ports). Units
// multiplex on the data listener: the controller sends a "serve-unit"
// control message, the server runs the matching serve (coordinator /
// segments-merge (c) / direct 1:1 (a) / routed (b)) against that unit's
// connections on the shared data listener, then reports "unit-done".
//
// The control loop is SEQUENTIAL (one unit at a time) — which matches the
// source side: the FR-02 snapshot lives on one pinned session, so the
// source issues one data COPY at a time. Table-level overlap comes from
// hiding the per-unit DDL/verify/swap/index work behind the previous unit's
// data transfer (the controller pipeline), not from concurrent dest units.
// A sequential dest also makes the data listener unambiguous: only the
// current unit's connections exist, so the serve accepts exactly its own
// count (1 / S / D / S×D×W).
//
// The agent speaks FIRST on the control connection: a "hello" message with
// its real bound addresses (auto :0 mode — the controller learns the data
// endpoint from it and folds it into the segreceivers' ProgramCommand and
// the source agent's stream instructions).
//
// Data-plane TLS: when enabled the shared data listener is wrapped ONCE at
// startup (tls.Listener + HandshakeFilter), so every unit's connections
// handshake without the serves wrapping it again.
package agent

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/portalloc"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// DestHello is the agent's first control message (its real bound addresses).
type DestHello struct {
	Op      string `json:"op"` // "hello"
	Data    string `json:"data"`
	Control string `json:"control"`
	// Persist (P1c) is the dedicated persistent-coordinator data listener's
	// address: the source agent dials it ONCE per run and keeps the
	// connection for every coordinator unit (the per-unit dial + UnitSpec
	// header + close are eliminated). Empty when the listener is off
	// (legacy builds / the one-shot single-unit modes, which have no
	// persistent channel).
	Persist string `json:"persist,omitempty"`
	// Report (P3 production-dispatch) is the supervisor's REPORT listener
	// address: the D destination-segment mini-agents dial it ONCE for the run
	// (a persistent connection, orphan-exit on drop). Empty when the agent is
	// not in supervisor mode. The controller reads it from the hello and
	// launches the D minis with --report-addr <Report>.
	Report string `json:"report,omitempty"`
	// MiniCount (P3 production-dispatch) is D, the number of destination
	// segments / mini-agents the supervisor coordinates. 0 = not in
	// supervisor mode.
	MiniCount int `json:"mini_count,omitempty"`
}

// DestServeReq is the controller's per-unit instruction to a long-lived
// dest agent. Kind selects the serve:
//
//	"coordinator" — one source-agent stream; the UnitSpec rides the data
//	connection (the serve reads it as it always has);
//	"segments"    — N source-segment slices -> SliceMux -> one coordinator
//	COPY (the merge sub-path (c));
//	"direct"      — D content-tagged slices -> D per-segment utility COPYs
//	(the 1:1 sub-path (a));
//	"routed"      — S×D×W sub-streams -> D per-segment utility COPYs (the
//	routed sub-path (b)).
//
// MiniLaunch is the per-unit instruction for the mesh mini path (P3
// production-dispatch, kind="mini"): the supervisor broadcasts a mini-unit
// request to EVERY mini-agent (each serves its own segment's slice of the
// unit), collects the D mini-unit-resp reports and combines them (XOR_D).
// The destination staging slice is created/truncated by the supervisor on
// the coordinator (replicated to the segments) BEFORE the broadcast.
type MiniLaunch struct {
	// Schema / Staging is the destination staging slice the mini loads.
	Schema  string `json:"schema,omitempty"`
	Staging string `json:"staging,omitempty"`
	// Cols / Dist / Part is the source table's layout (the supervisor
	// builds the DISTRIBUTED staging table from it on the destination
	// coordinator, mirroring ServeDirect — the DDL replicates to the
	// segments; the mini-agent only performs the local utility COPY into
	// its slice). A missing layout is an error (the staging cannot be
	// created).
	Cols []pg.Column      `json:"cols,omitempty"`
	Dist pg.Distribution  `json:"dist,omitempty"`
	Part pg.PartitionSpec `json:"part,omitempty"`
	// Compress is the FR-12 mode the source segreceivers used (the mini
	// decompresses).
	Compress string `json:"compress,omitempty"`
	// VerifyDigest asks each mini to tee the data-path digest (the
	// supervisor combines the per-segment accumulators).
	VerifyDigest bool `json:"verify_digest,omitempty"`
	// JobID / SourceSegments / Workers are the unit's shape (the mini's
	// data listener accepts S×W sub-streams per unit; 0/1 = direct: one
	// slice).
	JobID          uint32 `json:"job_id,omitempty"`
	SourceSegments int32  `json:"source_segments,omitempty"`
	Workers        int    `json:"workers,omitempty"`
}

type DestServeReq struct {
	Op   string `json:"op"` // "serve-unit"
	Kind string `json:"kind"`
	Swap bool   `json:"swap"`
	// UnitID (P1d) identifies the unit for the concurrent serve path: the
	// unit-done report echoes it so the controller correlates N serves in
	// flight on one control connection.
	UnitID uint32 `json:"unit_id,omitempty"`
	// ReqID (P1d) is the controller's per-request correlation id, echoed on
	// the ack and the unit-done (unit_id alone is not unique across
	// retries: the same unit can be served twice in one run).
	ReqID    uint64        `json:"req_id,omitempty"`
	Spec     *UnitSpec     `json:"spec,omitempty"`     // kind=coordinator
	Segments int           `json:"segments,omitempty"` // kind=segments
	Direct   *DirectLaunch `json:"direct,omitempty"`   // kind=direct
	Routed   *RoutedLaunch `json:"routed,omitempty"`   // kind=routed
	Mini     *MiniLaunch   `json:"mini,omitempty"`     // kind=mini (P3 mesh)
}

// DestAck is the agent's immediate reply to "serve-unit" (the serve is
// running in the background; its report arrives as a standalone unit-done).
type DestAck struct {
	Op string `json:"op"` // "ack"
	Ok bool   `json:"ok"`
	// ReqID (P1d) echoes DestServeReq.ReqID so the controller matches the
	// ack to the waiting worker (N serves in flight on one control conn).
	ReqID uint64 `json:"req_id,omitempty"`
	Error string `json:"error,omitempty"`
}

// DestUnitDone is the agent's report after a serve-unit finished.
type DestUnitDone struct {
	Op string `json:"op"` // "unit-done"
	Ok bool   `json:"ok"`
	// UnitID (P1d) echoes DestServeReq.UnitID (concurrent serves: the
	// controller maps the report to the unit by id).
	// ReqID (P1d) echoes DestServeReq.ReqID (the controller's correlation
	// id, robust across a unit's retries).
	ReqID   uint64 `json:"req_id,omitempty"`
	UnitID  uint32 `json:"unit_id,omitempty"`
	Rows    int64  `json:"rows,omitempty"`
	Logical uint64 `json:"logical,omitempty"`
	Wire    uint64 `json:"wire,omitempty"`
	Swapped bool   `json:"swapped,omitempty"`
	// Digest is the row-order-independent content digest the destination
	// computed on the data path (FR-02, P9-E) when the spec carried
	// VerifyDigest; empty otherwise. The controller compares it against the
	// source's inline digest (StreamResp.Digest) to verify without a re-read.
	Digest string `json:"digest,omitempty"`
	// SrcDigest (P3 mesh) is the combined SOURCE digest (XOR_D of the D
	// mini-agents' per-slice src-contribs) for a kind="mini" serve with
	// VerifyDigest — the mesh analogue of StreamResp.Digest (the mesh
	// source side is the segreceivers, whose per-slice digest rides the
	// slice trailer). Empty on the coordinator path (its source digest
	// rides StreamResp) and when verify is off.
	SrcDigest string `json:"src_digest,omitempty"`
	Error     string `json:"error,omitempty"`
}

// DestReleased is the agent's reply to "release".
type DestReleased struct {
	Op string `json:"op"` // "released"
	// ReqID (P1d) echoes the release's correlation id.
	ReqID uint64 `json:"req_id,omitempty"`
	Ok    bool   `json:"ok"`
}

// DestServerOptions configures one long-lived dest agent.
type DestServerOptions struct {
	// DataListen / ControlListen are the bind addresses ("host:0" = pick a
	// free port; the real addresses are reported in the hello).
	DataListen, ControlListen string
	// DestConn is the destination pg conn (coordinator: staging DDL, the
	// merge COPY, the swap). The per-segment DSNs ride the launch payloads.
	DestConn string
	// PG-plane TLS (the agent is a client of the destination DB).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Data-plane TLS for the data listener (the agent = server).
	DataTLSCert, DataTLSKey, DataTLSCA, DataTLSMinTLS string
	// ControlTLS for the control socket (this agent = server). CA present =
	// mTLS (require + verify the controller's client cert).
	ControlTLSCert, ControlTLSKey, ControlTLSCA string
	// AcceptTimeout bounds the wait for a unit's first connection;
	// StreamTimeout bounds the whole unit stream.
	AcceptTimeout, StreamTimeout time.Duration
	// PortAlloc, when set, restricts the data/control listener ports to the
	// operator's --port-range (strict firewall). It is consulted only
	// when a listener address is ":0" (auto-pick); a fixed port binds
	// verbatim. nil = legacy behavior (kernel picks, no range).
	PortAlloc *portalloc.Allocator
	// FrameAck enables the frame-level ACK + RST-resume transport (DESIGN
	// §4.6) on the data socket for the coordinator serve; ReliablePipe
	// carries its parameters.
	FrameAck     bool
	ReliablePipe transfer.ReliablePipeConfig
	// PersistListen (P1c) is the dedicated persistent-coordinator data
	// listener address (host:port; :0 = auto-pick, reported in the hello).
	// Empty = the listener is off (the legacy per-unit coordinator path is
	// used; the hello's Persist field is empty). When set, the listener
	// accepts the ONE persistent coordinator connection and serves its
	// units sequentially (ReceiveUnit -> staging -> COPY -> unit-done).
	// The data-plane TLS settings apply to it exactly as to the mesh data
	// listener.
	PersistListen string
	// Workers (P1d) is the run's data-worker count, used to size the
	// destination pg pool for the concurrent persistent serves
	// (Workers>1 = snapshot-epoch). 0/1 = strict: the single-serve sizing.
	Workers int
	// RunCtx, when set, is the base context every serve is bounded by (in
	// addition to AcceptTimeout/StreamTimeout). The IN-PROC runner sets it to
	// the controller's run context so a run abort / crash-simulation (a soft
	// ctx cancel) interrupts an in-flight unit's COPY exactly as the legacy
	// per-unit agent.Serve(ctx) did (the process runner leaves it nil: its
	// agent is a separate process whose serves are bounded by its own signal
	// ctx, and a crash is a hard kill, not a cancel).
	RunCtx context.Context
	// SupervisorListen (P3 production-dispatch) is the supervisor's REPORT
	// listener address (host:port; :0 = auto-pick, reported in the hello's
	// Report field). Empty = supervisor mode is OFF (the coordinator
	// dest-agent, no mini-agents). When set, MiniCount is D and the agent
	// ALSO coordinates the D destination-segment mini-agents: kind="mini"
	// serves are broadcast to the minis (each does a local utility COPY on
	// its segment) and combined (XOR_D), while the fallback coordinator path
	// (kind="coordinator", the non-routable / small-table units) keeps its
	// own pg pool + data listener on the same process (design: the
	// supervisor is a MODE of the dest-agent, one process on the dest
	// coordinator).
	SupervisorListen string
	// MiniCount (P3 production-dispatch) is D when SupervisorListen is set.
	MiniCount int
	// MiniHosts (P3 production-dispatch) are the per-segment hosts from the
	// catalog's DstDSNs (length D, seg order). The supervisor combines them
	// with each mini's hello data port to form the full data-listener
	// address the segreceivers dial. Empty value / empty slice = 127.0.0.1
	// (single-host layout / the local test stand).
	MiniHosts []string
}

// DestServer is the long-lived destination agent (one data + one control
// listener for the run; plus the P1c dedicated persistent-coordinator
// listener when enabled).
type DestServer struct {
	opts DestServerOptions
	// dataL is the PLAIN listener; tlsL (when data TLS is on) is the wrapped
	// one the serves accept from.
	dataL net.Listener
	tlsL  net.Listener
	ctrlL net.Listener
	// persistL (P1c): the dedicated persistent-coordinator listener (plain
	// when data TLS is off, TLS-wrapped when on). nil = off.
	persistL net.Listener
	// dq (P1c/P1d): the destination pg POOL the persistent serves use
	// (staging DDL, the COPY, the swap). nil when the persist listener is
	// off. P1d: a pool, not one conn — N persistent channels serve N units
	// concurrently, each with its own staging DDL + COPY on its own conn.
	dq *pg.Pool
	// runCtx: the base context every serve is bounded by (RunCtx when set,
	// else context.Background() — the process-runner behavior).
	runCtx context.Context
	// persConns (P1d): the live persistent coordinator connections (one per
	// channel the source dialed; the source dials Workers of them). Each
	// runs its own serve loop (servePersistentChannel); a loop removes its
	// connection on exit. persMu guards the slice.
	persMu    sync.Mutex
	persConns []*persConnW
	// persServes (P1d): tracks the per-channel serve loops, so Close can
	// wait for them (and their in-flight COPY) to finish before closing the
	// pg pool (F-02: the agent must outlive a running COPY so the staging
	// commits).
	persServes sync.WaitGroup
	// serveWG (P1d): tracks the serve-unit ops running on the concurrent
	// control loop (one goroutine per op). serveControl waits it before
	// exiting on control-conn loss / release, so an in-flight unit's COPY
	// finishes (and reports) before the agent exits.
	serveWG sync.WaitGroup
	// unitReg (P1d): maps a unit's UnitID to its in-flight serve's
	// registration. The per-channel serve loop signals it when the unit's
	// data arrives (started) and delivers the result (done); the serve op
	// (serveOne) waits on it. The registry is keyed by UnitID because the
	// channel loop sees the UnitBegin's unit-id (the spec's "unit-done
	// маппится по unit-id").
	unitMu  sync.Mutex
	unitReg map[uint32]*unitServe
	// meshMu (P1d): the mesh serves (segments / direct / routed) stay
	// serialized — their data-plane serves accept a variable number of
	// connections on the shared data listener, which is only unambiguous
	// one-at-a-time (the pre-P1d invariant). The coordinator persistent
	// path does NOT take it (that is the whole point of P1d).
	meshMu sync.Mutex
	// persAccepts (P1c): the number of persistent coordinator connections
	// accepted so far (the first unit's dial + one per re-dial after a
	// break) — a runtime signal that the persistent channel is in use.
	persAccepts atomic.Int64

	// ctrlConn (P1d): the accepted control connection, so the serve ops can
	// send their ack / unit-done lines on it (serialized by connMu —
	// concurrent serves must not interleave their response lines).
	ctrlConn net.Conn
	connMu   sync.Mutex
	ctrlDone chan struct{}
	stopOnce sync.Once
	// supervisor (P3 production-dispatch): the mini-agent orchestrator for
	// supervisor mode (SupervisorListen set). nil = coordinator mode.
	supervisor *Supervisor
	// miniMapReady (P3 production-dispatch): closed once AcceptMinis has
	// collected all D mini hellos (the MiniMap is complete). The controller
	// polls MiniMapReady before the first kind="mini" serve.
	miniMapMu    sync.Mutex
	miniMapReady bool
	// miniAcceptErr (P3 production-dispatch): set when AcceptMinis fails
	// (a mini failed to connect / the run aborted); the MiniMapReady op
	// reports it instead of blocking forever.
	miniAcceptErr error
	// miniHosts (P3 production-dispatch): the per-segment hosts from the
	// catalog's DstDSNs, applied to the MiniMap via SetMiniHost (the hello
	// carries the data port only). Empty value = 127.0.0.1 (single-host
	// layout / the local test stand).
	miniHosts []string
}

// unitServe (P1d) is the in-flight registration of one serve-unit op, keyed
// by the unit's UnitID in the DestServer.unitReg. The per-channel serve loop
// delivers the result on done (buffered 1: a cancelled serve op — a run
// abort — must not be blocked by an undelivered report). finished is set
// the moment a terminal report is delivered (success or failure), so a late
// failure (a channel break) cannot override a unit that already completed.
type unitServe struct {
	done     chan DestUnitDone
	finished atomic.Bool
}

// NewDestServer binds the data + control listeners and starts the control
// loop. The agent is ready when both listeners are up.
func NewDestServer(o DestServerOptions) (*DestServer, error) {
	if o.AcceptTimeout <= 0 {
		o.AcceptTimeout = 5 * time.Minute
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 6 * time.Hour
	}
	// P3 production-dispatch (supervisor mode): the shared mesh data listener
	// is UNUSED (the mini path uses the minis' own listeners; the fallback
	// coordinator path uses the persistent-coordinator listener). DataListen
	// empty = the data listener is off (supervisor mode). Coordinator mode
	// always binds it.
	var dataL net.Listener
	if o.DataListen != "" {
		var derr error
		dataL, derr = o.bindListener(o.DataListen)
		if derr != nil {
			return nil, fmt.Errorf("dest server: data listen %s: %w", o.DataListen, derr)
		}
	}
	ctrlL, err := o.bindListener(o.ControlListen)
	if err != nil {
		if dataL != nil {
			_ = dataL.Close()
		}
		return nil, fmt.Errorf("dest server: control listen %s: %w", o.ControlListen, err)
	}
	var persistL net.Listener
	if o.PersistListen != "" {
		persistL, err = o.bindListener(o.PersistListen)
		if err != nil {
			if dataL != nil {
				_ = dataL.Close()
			}
			_ = ctrlL.Close()
			return nil, fmt.Errorf("dest server: persist listen %s: %w", o.PersistListen, err)
		}
	}
	// P3 production-dispatch: supervisor mode REQUIRES the persistent
	// coordinator listener — it is the fallback coordinator path (the
	// non-routable / small-table units) that rides the P1d channel pool.
	if o.SupervisorListen != "" && o.PersistListen == "" {
		if dataL != nil {
			_ = dataL.Close()
		}
		_ = ctrlL.Close()
		return nil, fmt.Errorf("dest server: supervisor mode requires the persistent-coordinator listener (the fallback coordinator path)")
	}
	runCtx := context.Background()
	if o.RunCtx != nil {
		runCtx = o.RunCtx
	}
	s := &DestServer{opts: o, dataL: dataL, ctrlL: ctrlL, persistL: persistL, ctrlDone: make(chan struct{}), runCtx: runCtx}
	if transfer.TLSEnabled(o.ControlTLSCert, o.ControlTLSKey, o.ControlTLSCA) {
		ccfg, cerr := transfer.ServerTLSConfig(o.ControlTLSCert, o.ControlTLSKey, o.ControlTLSCA, "")
		if cerr != nil {
			s.Close()
			return nil, fmt.Errorf("dest server: control tls config: %w", cerr)
		}
		s.ctrlL = tls.NewListener(s.ctrlL, ccfg)
	}
	if dataL != nil && transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
		cfg, terr := transfer.ServerTLSConfig(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
		if terr != nil {
			s.Close()
			return nil, fmt.Errorf("dest server: data tls config: %w", terr)
		}
		s.tlsL = segstream.HandshakeFilter(tls.NewListener(dataL, cfg))
		if s.persistL != nil {
			// The persistent channel is the SAME data-plane TLS as the mesh
			// data listener (the source agent is the client either way).
			pcfg, perr := transfer.ServerTLSConfig(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
			if perr != nil {
				s.Close()
				return nil, fmt.Errorf("dest server: persist tls config: %w", perr)
			}
			s.persistL = tls.NewListener(s.persistL, pcfg)
		}
	}
	// P1c: the destination pg pool the persistent serves use (staging DDL,
	// the COPY, the swap) + the persistent-coordinator accept loop.
	if s.persistL != nil {
		// P1d: a POOL, not one connection — N persistent channels can
		// serve N units concurrently, each with its own staging DDL +
		// COPY. Sized for the run's workers plus headroom (0/1 = strict:
		// the old single connection plus one).
		n := o.Workers
		if n < 1 {
			n = 1
		}
		dq, derr := pg.NewPool(context.Background(), o.DestConn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS, n+2)
		if derr != nil {
			s.Close()
			return nil, fmt.Errorf("dest server: connect dest (persistent serve): %w", derr)
		}
		s.dq = dq
		s.unitReg = make(map[uint32]*unitServe)
		go s.acceptPersistent()
	}
	// P3 production-dispatch: the supervisor mode (SupervisorListen set). The
	// agent coordinates the D destination-segment mini-agents: bind the
	// report listener (the minis dial it once for the run) and start the
	// AcceptMinis goroutine (it blocks until all D hellos arrive, then the
	// MiniMap is complete and the controller may issue kind="mini" serves).
	// The fallback coordinator path (kind="coordinator") keeps its own data
	// listener + pg pool on this SAME process (the supervisor is a mode of
	// the dest-agent, one process on the dest coordinator).
	if o.SupervisorListen != "" {
		sup, serr := NewSupervisor(o.SupervisorListen, o.PortAlloc,
			o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
		if serr != nil {
			s.Close()
			return nil, fmt.Errorf("dest server: supervisor report listen: %w", serr)
		}
		s.supervisor = sup
		// Set the per-segment hosts BEFORE the AcceptMinis goroutine: it
		// applies them to the MiniMap once all D hellos arrive (the hello
		// carries the data port only).
		s.miniHosts = o.MiniHosts
		go func() {
			mm, aerr := sup.AcceptMinis(s.runCtx, o.MiniCount)
			if aerr != nil {
				// A mini failed to connect (or the run aborted): surface via
				// MiniMapReady (the controller's start waits on it).
				s.miniMapMu.Lock()
				s.miniAcceptErr = aerr
				s.miniMapMu.Unlock()
				return
			}
			// Apply the per-segment hosts (catalog DstDSNs) to the MiniMap:
			// the hello carries the data port only.
			for seg := 0; seg < o.MiniCount; seg++ {
				host := "127.0.0.1"
				if s.miniHosts != nil && seg < len(s.miniHosts) && s.miniHosts[seg] != "" {
					host = s.miniHosts[seg]
				}
				sup.SetMiniHost(seg, host)
			}
			_ = mm
			s.miniMapMu.Lock()
			s.miniMapReady = true
			s.miniMapMu.Unlock()
		}()
	}
	go s.serveControl(s.ctrlL) // the TLS-wrapped listener, when control TLS is on
	return s, nil
}

// bindListener binds addr, honoring the --port-range (o.PortAlloc) when
// the port is 0 (auto-pick) and a range is active. A fixed port, or no range,
// binds verbatim (the legacy behavior). The data and control listeners share
// the one allocator, so the two can never land on the same port.
func (o DestServerOptions) bindListener(addr string) (net.Listener, error) {
	if o.PortAlloc != nil {
		return o.PortAlloc.ListenAddr(addr)
	}
	return net.Listen("tcp", addr)
}

// Hello is the agent's first control message (its real bound addresses).
func (s *DestServer) Hello() DestHello {
	h := DestHello{Op: "hello", Data: s.DataAddr(), Control: s.ControlAddr()}
	if s.persistL != nil {
		h.Persist = s.PersistAddr()
	}
	if s.supervisor != nil {
		h.Report = s.supervisor.Addr()
		h.MiniCount = s.opts.MiniCount
	}
	return h
}

// PersistAccepts is the number of persistent coordinator connections the
// server has accepted (the first unit's dial + one per re-dial after a
// break). A test signal that the persistent channel is actually in use.
func (s *DestServer) PersistAccepts() int64 {
	if s.persistL == nil {
		return 0
	}
	return s.persAccepts.Load()
}

// PersistAddr is the dedicated persistent-coordinator listener's real bound
// address (routable, like DataAddr). Empty when the listener is off.
func (s *DestServer) PersistAddr() string {
	if s.persistL == nil {
		return ""
	}
	raw := s.persistL.Addr().String()
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return raw
	}
	if host != "0.0.0.0" && host != "::" && host != "[::]" {
		return raw
	}
	if ip, ierr := primaryIPv4(); ierr == nil {
		return net.JoinHostPort(ip, port)
	}
	return raw
}

// DataAddr / ControlAddr are the real bound listener addresses. DataAddr
// reports a ROUTABLE address: when the data listener was bound on a wildcard
// (0.0.0.0:0 / [::]:0 — the remote-launch auto mode, where the controller
// does not know the agent host's IP), the host part is replaced with the
// host's primary non-loopback IPv4, so the source agent / segreceivers can
// dial it across the network (the raw [::]:port bind address is not
// dialable from another host).
func (s *DestServer) DataAddr() string {
	if s.dataL == nil {
		return "" // P3 supervisor mode: the shared data listener is off
	}
	raw := s.dataL.Addr().String()
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return raw
	}
	if host != "0.0.0.0" && host != "::" && host != "[::]" {
		return raw
	}
	if ip, ierr := primaryIPv4(); ierr == nil {
		return net.JoinHostPort(ip, port)
	}
	return raw
}
func (s *DestServer) ControlAddr() string { return s.ctrlL.Addr().String() }

// primaryIPv4 returns the first non-loopback, global-unicast IPv4 address of
// this host (the same heuristic config.ResolveSegListen uses for the
// segreceivers' auto data address).
func primaryIPv4() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var candidate string
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLoopback() || !ipn.IP.To4().IsGlobalUnicast() {
			continue
		}
		if candidate == "" {
			candidate = ipn.IP.String()
		}
	}
	if candidate == "" {
		return "", fmt.Errorf("no non-loopback IPv4 address found")
	}
	return candidate, nil
}

// Done is closed when the control loop exits (the controller disconnected,
// the run was released, or the listeners failed).
func (s *DestServer) Done() <-chan struct{} { return s.ctrlDone }

// Kill simulates an EXTERNAL death of the agent's network plane (a SIGKILL
// analog for tests): the listeners (control + data + persistent) and the
// live persistent connection are closed hard, WITHOUT a graceful release and
// WITHOUT closing the destination pg connection (the in-process agent has no
// process to kill; the pg conn is closed when the test's cluster stops). An
// in-flight unit's data COPY breaks on the closed persistent connection, so
// the unit fails and the surviving partial staging is left for the resume
// (FR-07). Unlike Close, Kill never waits on the control loop (which may be
// blocked in a unit's serve) and never touches the pg conn (which the
// in-flight COPY is using — closing it would race).
func (s *DestServer) Kill() {
	s.stopOnce.Do(func() {
		_ = s.ctrlL.Close()
		_ = s.dataL.Close()
		if s.persistL != nil {
			_ = s.persistL.Close()
			s.persMu.Lock()
			for _, w := range s.persConns {
				if w != nil {
					_ = w.Conn.Close() // hard: the in-flight COPY's read breaks
				}
			}
			s.persConns = nil
			s.persMu.Unlock()
		}
	})
}

// Close shuts the listeners down (the agent's clean exit).
// SetMiniHosts (P3 production-dispatch) sets the per-segment hosts from the
// catalog's DstDSNs (the supervisor combines them with each mini's hello
// data port to form the full data-listener address the segreceivers dial).
// The controller passes them at start (after the hello); before the first
// kind="mini" serve the MiniMap is re-resolved against these hosts.
func (s *DestServer) SetMiniHosts(hosts []string) {
	s.miniMapMu.Lock()
	s.miniHosts = hosts
	s.miniMapMu.Unlock()
	if s.supervisor != nil {
		for seg := 0; seg < len(hosts); seg++ {
			if hosts[seg] != "" {
				s.supervisor.SetMiniHost(seg, hosts[seg])
			}
		}
	}
}

// MiniMapReady (P3 production-dispatch) reports whether the supervisor has
// collected all D mini hellos (the MiniMap is complete). The controller
// polls it before the first kind="mini" serve. ok is false on an accept
// failure (err carries the reason).
func (s *DestServer) MiniMapReady() (bool, error) {
	s.miniMapMu.Lock()
	defer s.miniMapMu.Unlock()
	return s.miniMapReady, s.miniAcceptErr
}

// MiniDataAddrs (P3 production-dispatch) returns the D-entry address book
// (seg ordinal 0..D-1 -> the mini's data-listener address), comma-joined in
// seg order (the segreceivers' --dest-addr D form, parsed by
// segstream.ParseDestAddrs). Empty when not in supervisor mode or the
// MiniMap is not ready.
func (s *DestServer) MiniDataAddrs() string {
	if s.supervisor == nil {
		return ""
	}
	s.miniMapMu.Lock()
	ready := s.miniMapReady
	s.miniMapMu.Unlock()
	if !ready {
		return ""
	}
	addrs := make([]string, s.opts.MiniCount)
	for seg := 0; seg < s.opts.MiniCount; seg++ {
		addrs[seg] = s.supervisor.MiniDataAddr(seg)
	}
	return strings.Join(addrs, ",")
}

// ensureMiniStaging (P3 production-dispatch, mesh): create the unit's
// distributed staging table on the destination coordinator if absent
// (or truncate a surviving one, so a resume re-copies from a clean table —
// F-06/F-07). It mirrors ServeDirect's coordinator-conn staging DDL: the
// CREATE TABLE replicates to the segments, so the D minis each find their
// local staging slice. The connection is a fresh LOCAL connection to the
// coordinator (the fallback coordinator path's DSN, design 04.10), held
// only for the DDL.
func (s *DestServer) ensureMiniStaging(ctx context.Context, ml *MiniLaunch) error {
	q, err := pg.NewConn(ctx, s.opts.DestConn, s.opts.SSLCert, s.opts.SSLKey, s.opts.SSLCA, s.opts.SSLMinTLS)
	if err != nil {
		return fmt.Errorf("kind=mini: connect coordinator (staging DDL): %w", err)
	}
	defer q.Close()
	da := dest.New(q)
	sctx, scancel := context.WithTimeout(ctx, 5*time.Minute)
	defer scancel()
	if err := da.EnsureSchema(sctx, ml.Schema); err != nil {
		return fmt.Errorf("kind=mini: ensure schema %s: %w", ml.Schema, err)
	}
	if da.Exists(sctx, ml.Schema, ml.Staging) {
		if terr := da.Truncate(sctx, ml.Schema, ml.Staging); terr != nil {
			return fmt.Errorf("kind=mini: truncate surviving staging %s.%s: %w", ml.Schema, ml.Staging, terr)
		}
		return nil
	}
	if err := da.CreateStaging(sctx, ml.Schema, ml.Cols, ml.Staging, ml.Dist, ml.Part); err != nil {
		return fmt.Errorf("kind=mini: create staging %s.%s: %w", ml.Schema, ml.Staging, err)
	}
	return nil
}

func (s *DestServer) Close() {
	s.stopOnce.Do(func() {
		// P3 production-dispatch: stop the supervisor first (send mini-done
		// to each mini, close the report listener) so the minis exit cleanly
		// before the agent's own teardown.
		if s.supervisor != nil {
			s.supervisor.Stop()
			s.supervisor.Close()
		}
		_ = s.ctrlL.Close()
		if s.dataL != nil {
			_ = s.dataL.Close()
		}
		if s.persistL != nil {
			_ = s.persistL.Close()
			s.persMu.Lock()
			for _, w := range s.persConns {
				if w != nil {
					_ = w.Close()
				}
			}
			s.persConns = nil
			s.persMu.Unlock()
		}
		// F-02: wait for the per-channel serve loops (and, with them, the
		// in-flight COPYs — the staging is one transaction: its commit is
		// what the post-kill assertions rely on) BEFORE closing the pg pool.
		s.persServes.Wait()
		s.serveWG.Wait()
		if s.dq != nil {
			s.dq.Close()
		}
	})
}

// serveControl accepts the ONE control connection (the controller) and runs
// the protocol: hello, then serve-unit* / release. P1d: CONCURRENT — each
// serve-unit op runs in its own goroutine (the controller's N workers issue
// N serves in flight on one connection), the responses (ack, unit-done) are
// serialized on the connection (connMu) and carry the op's ReqID so the
// controller matches them to the waiting worker. Release waits for the
// in-flight ops (serveWG) before replying — like the source agent's
// opsWg.Wait — so a running COPY finishes and reports before the agent
// exits.
func (s *DestServer) serveControl(l net.Listener) {
	defer close(s.ctrlDone)
	conn, err := l.Accept()
	if err != nil {
		return // no controller (orphan) — the process waits to be killed
	}
	defer conn.Close()
	s.ctrlConn = conn
	// The agent speaks first: report the real bound addresses (auto :0).
	if werr := SendLine(conn, s.Hello()); werr != nil {
		return
	}
	r := bufio.NewReader(conn)
	for {
		var req DestServeReq
		if rerr := recvLine(r, &req); rerr != nil {
			// Controller closed the control conn (run end / kill -9). WAIT
			// for the in-flight ops before exiting: the data path is
			// independent of the control conn (F-02), so a running COPY must
			// finish (the staging commits) BEFORE the agent exits and closes
			// the pg pool.
			s.serveWG.Wait()
			return
		}
		switch req.Op {
		case "serve-unit":
			ack := DestAck{Op: "ack", Ok: true, ReqID: req.ReqID}
			if req.Kind == "" {
				ack = DestAck{Op: "ack", Ok: false, ReqID: req.ReqID, Error: "serve-unit: missing kind"}
			}
			// P1d race guard: the persistent serve MUST be registered in
			// unitReg BEFORE the ack is sent. Once the ack returns, the
			// controller instructs the source to stream, and the source's
			// UnitBegin can reach the channel loop almost immediately —
			// before the serve op's goroutine is scheduled. If the unit is
			// not registered by then, the channel loop would drop it (data
			// already off the wire) and desync the channel. (Non-persistent
			// serves accept on the data listener and are not racy.)
			if ack.Ok && req.Kind == "coordinator" && s.persistL != nil {
				s.registerUnitServe(req.UnitID)
			}
			if serr := s.sendLine(conn, ack); serr != nil {
				s.serveWG.Wait()
				return
			}
			if !ack.Ok {
				continue
			}
			s.serveWG.Add(1)
			go func(r DestServeReq) {
				defer s.serveWG.Done()
				var done DestUnitDone
				if r.Kind == "coordinator" && s.persistL != nil {
					done = s.serveUnitPersistentWait(s.runCtx, &r)
				} else {
					// The mesh serves (segments / direct / routed) and the
					// legacy per-unit coordinator path stay serialized: their
					// data-plane serves accept a variable number of
					// connections on the shared data listener, which is only
					// unambiguous one-at-a-time.
					s.meshMu.Lock()
					done = s.serveOne(s.runCtx, &r)
					s.meshMu.Unlock()
				}
				// Correlate the report to the waiting worker (serveOne does
				// not set ReqID; serveUnitPersistentWait does).
				if done.ReqID == 0 {
					done.ReqID = r.ReqID
				}
				_ = s.sendLine(conn, done)
			}(req)
		case "cancel-unit":
			// The controller's source stream failed (the unit's data will
			// not arrive / is partial): fail the in-flight serve so the
			// controller's WaitUnitDone unblocks promptly instead of
			// waiting for the run ctx. A unit whose data already arrived is
			// left to complete (its report wins over the cancel).
			s.unitMu.Lock()
			us := s.unitReg[req.UnitID]
			s.unitMu.Unlock()
			if us != nil {
				s.failUnitServe(us, req.UnitID, fmt.Errorf("unit cancelled by the controller (source stream failed)"))
			}
			_ = s.sendLine(conn, DestAck{Op: "ack", Ok: true, ReqID: req.ReqID})
		case "mini-map":
			// P3 production-dispatch: the controller fetches the supervisor's
			// D-entry address book (the mini-agent data endpoints, seg order)
			// to fold into the segreceivers' ON SEGMENT ProgramCommand.
			if s.supervisor == nil {
				_ = s.sendLine(conn, MiniMapResp{Op: "mini-map-resp", ReqID: req.ReqID, Ok: false, Error: "not in supervisor mode"})
				continue
			}
			ready, aerr := s.MiniMapReady()
			if !ready {
				ee := "minis not ready (MiniMap incomplete)"
				if aerr != nil {
					ee = "minis not ready (accept: " + aerr.Error() + ")"
				}
				_ = s.sendLine(conn, MiniMapResp{Op: "mini-map-resp", ReqID: req.ReqID, Ok: false, Error: ee})
				continue
			}
			_ = s.sendLine(conn, MiniMapResp{Op: "mini-map-resp", ReqID: req.ReqID, Ok: true, Addrs: s.MiniDataAddrs()})
		case "release":
			s.serveWG.Wait() // in-flight ops must finish before the agent exits
			// P3 production-dispatch: stop the supervisor (mini-done to each
			// mini) so the minis exit cleanly at run end.
			if s.supervisor != nil {
				s.supervisor.Stop()
			}
			_ = s.sendLine(conn, DestReleased{Op: "released", Ok: true, ReqID: req.ReqID})
			return
		default:
			_ = s.sendLine(conn, DestUnitDone{Op: "unit-done", Ok: false, Error: "unknown op " + req.Op})
		}
	}
}

// sendLine writes one control response line, serialized (P1d: N serves in
// flight on one connection; the response lines must not interleave).
func (s *DestServer) sendLine(conn net.Conn, msg any) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return SendLine(conn, msg)
}

// serveOne runs one unit on the shared data listener and reports it. The
// control loop is sequential, so the data listener carries ONLY this
// unit's connections while it runs.
func (s *DestServer) serveOne(ctx context.Context, req *DestServeReq) DestUnitDone {
	var done DestUnitDone
	done.Op = "unit-done"
	switch req.Kind {
	case "coordinator":
		if s.persistL != nil {
			// P1d: the persistent path is served by the per-channel serve
			// loops (serveUnitPersistent, launched by the concurrent
			// control loop); this sequential serveOne branch is only
			// reached when the persist listener is OFF.
			done.Error = "persistent coordinator serve: served by the channel pool (serveUnitPersistent)"
			return done
		}
		// Legacy per-unit connection: the UnitSpec rides the data connection
		// (the source agent writes it as the first thing after dialing).
		res, err := Serve(ctx, DestOptions{
			SharedListen:  s.dataListener(),
			DestConn:      s.opts.DestConn,
			SSLCert:       s.opts.SSLCert,
			SSLKey:        s.opts.SSLKey,
			SSLCA:         s.opts.SSLCA,
			SSLMinTLS:     s.opts.SSLMinTLS,
			AcceptTimeout: s.opts.AcceptTimeout,
			StreamTimeout: s.opts.StreamTimeout,
			Swap:          req.Swap,
			FrameAck:      s.opts.FrameAck,
			ReliablePipe:  s.opts.ReliablePipe,
		})
		done.Rows, done.Logical, done.Swapped = res.Rows, res.Logical, res.Swapped
		done.Digest = res.Digest
		done.Ok = err == nil
		done.Error = errString(err)
	case "segments":
		if s.supervisor != nil {
			// P3 production-dispatch: in supervisor mode the mesh mini path
			// (kind="mini") replaces the old coordinator-side mesh serves
			// (the mini-agents load the slices locally on the segments).
			done.Error = "serve-unit kind=segments: not available in supervisor mode (use kind=mini)"
			return done
		}
		if req.Spec == nil {
			done.Error = "serve-unit kind=segments: missing spec"
			return done
		}
		res, err := ServeSegments(ctx, SegmentsOptions{
			SharedListen:  s.dataListener(),
			DestConn:      s.opts.DestConn,
			SSLCert:       s.opts.SSLCert,
			SSLKey:        s.opts.SSLKey,
			SSLCA:         s.opts.SSLCA,
			SSLMinTLS:     s.opts.SSLMinTLS,
			AcceptTimeout: s.opts.AcceptTimeout,
			StreamTimeout: s.opts.StreamTimeout,
			Spec:          *req.Spec,
			Segments:      req.Segments,
			Swap:          req.Swap,
			FrameAck:      s.opts.FrameAck,
			ReliablePipe:  s.opts.ReliablePipe,
		})
		done.Rows, done.Logical, done.Wire = res.Rows, res.Logical, res.Wire
		done.Ok = err == nil
		done.Error = errString(err)
	case "direct":
		if s.supervisor != nil {
			done.Error = "serve-unit kind=direct: not available in supervisor mode (use kind=mini)"
			return done
		}
		if req.Direct == nil {
			done.Error = "serve-unit kind=direct: missing direct launch"
			return done
		}
		res, err := ServeDirect(ctx, DirectOptions{
			SharedListen:  s.dataListener(),
			DestConn:      s.opts.DestConn,
			SSLCert:       s.opts.SSLCert,
			SSLKey:        s.opts.SSLKey,
			SSLCA:         s.opts.SSLCA,
			SSLMinTLS:     s.opts.SSLMinTLS,
			AcceptTimeout: s.opts.AcceptTimeout,
			StreamTimeout: s.opts.StreamTimeout,
			Launch:        *req.Direct,
			Swap:          req.Swap,
		})
		done.Rows, done.Logical, done.Wire = res.Rows, res.Logical, res.Wire
		done.Ok = err == nil
		done.Error = errString(err)
	case "routed":
		if s.supervisor != nil {
			done.Error = "serve-unit kind=routed: not available in supervisor mode (use kind=mini)"
			return done
		}
		if req.Routed == nil {
			done.Error = "serve-unit kind=routed: missing routed launch"
			return done
		}
		res, err := ServeRouted(ctx, RoutedOptions{
			SharedListen:  s.dataListener(),
			DestConn:      s.opts.DestConn,
			SSLCert:       s.opts.SSLCert,
			SSLKey:        s.opts.SSLKey,
			SSLCA:         s.opts.SSLCA,
			SSLMinTLS:     s.opts.SSLMinTLS,
			AcceptTimeout: s.opts.AcceptTimeout,
			StreamTimeout: s.opts.StreamTimeout,
			Launch:        *req.Routed,
			Swap:          req.Swap,
			FrameAck:      s.opts.FrameAck,
			ReliablePipe:  s.opts.ReliablePipe,
		})
		done.Rows, done.Logical, done.Wire = res.Rows, res.Logical, res.Wire
		done.Ok = err == nil
		done.Error = errString(err)
	case "mini":
		// P3 production-dispatch (mesh): the supervisor broadcasts the unit
		// to the D destination-segment mini-agents (each does a local
		// utility COPY on its segment) and combines the D reports (XOR_D).
		// The supervisor holds no data connection for this path (design
		// 04.10); the fallback coordinator path (kind="coordinator") is the
		// only one that uses this agent's pg pool.
		if s.supervisor == nil {
			done.Error = "serve-unit kind=mini: not in supervisor mode"
			return done
		}
		if req.Mini == nil {
			done.Error = "serve-unit kind=mini: missing mini launch"
			return done
		}
		if len(req.Mini.Cols) == 0 {
			done.Error = "serve-unit kind=mini: missing staging layout (cols) — the supervisor builds the staging DDL from it"
			return done
		}
		ready, aerr := s.MiniMapReady()
		if !ready {
			if aerr != nil {
				done.Error = "serve-unit kind=mini: minis not ready (accept: " + aerr.Error() + ")"
			} else {
				done.Error = "serve-unit kind=mini: minis not ready (MiniMap incomplete)"
			}
			return done
		}
		// Staging DDL on the destination coordinator (mirror ServeDirect):
		// the distributed staging table is built from the unit's layout
		// (the DDL replicates to the segments); a surviving staging (a
		// crashed/failed run) is truncated so a resume re-copies from a
		// clean table (F-06/F-07). The connection is LOCAL to the
		// coordinator (the fallback coordinator path's DSN, design 04.10) —
		// the supervisor holds no DATA connection for the mini path, only
		// this DDL class on the fallback coordinator connection.
		if derr := s.ensureMiniStaging(ctx, req.Mini); derr != nil {
			done.Error = derr.Error()
			return done
		}
		res, err := s.supervisor.ServeUnit(ctx, UnitReportReq{
			UnitID:         req.UnitID,
			JobID:          req.Mini.JobID,
			Schema:         req.Mini.Schema,
			Staging:        req.Mini.Staging,
			Compress:       req.Mini.Compress,
			VerifyDigest:   req.Mini.VerifyDigest,
			SourceSegments: req.Mini.SourceSegments,
			Workers:        req.Mini.Workers,
		})
		done.Rows, done.Logical, done.Wire = res.Rows, res.Logical, res.Wire
		done.Ok = err == nil
		done.Error = errString(err)
		// P9-E: the combined digests (XOR_D of the D per-segment
		// accumulators) — the mesh analogue of the inline content digest.
		if err == nil && req.Mini.VerifyDigest {
			done.Digest = res.DstDigest
			done.SrcDigest = res.SrcDigest
		}
	default:
		done.Error = "unknown serve kind " + req.Kind
	}
	return done
}

// P1c: the dedicated persistent-coordinator channel.
//
// The persist listener accepts the ONE persistent coordinator connection (the
// source agent's channel) and stores it. The control loop is SEQUENTIAL (one
// unit at a time) and drives the persistent serve SYNCHRONOUSLY: a
// serve-unit(kind=coordinator) on a persistent-capable server blocks in
// ReceiveUnit (the next UnitBegin, answering idle Pings) -> staging -> COPY,
// exactly like the legacy serve blocks in acceptOne+ReadUnitSpec. The
// connection is kept open between units (that is the point of P1c); on a
// break the unit fails and the connection is re-accepted for the next unit
// (the source re-dials; FR-07: the surviving staging is truncated + re-COPYd).

// persConnW wraps a persistent coordinator connection and signals on Close
// (the accept loop waits on it to know when to re-accept).
type persConnW struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *persConnW) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// acceptPersistent accepts the persistent coordinator connection(s) (P1d:
// the source dials Workers of them, one per channel) and gives each its own
// serve loop (servePersistentChannel). Each connection is kept for the run;
// a source re-dial after a break simply arrives as the next Accept and gets
// a fresh loop. Runs for the life of the server.
func (s *DestServer) acceptPersistent() {
	for {
		conn, err := s.persistL.Accept()
		if err != nil {
			return // listener closed (run end / stop)
		}
		w := &persConnW{Conn: conn, closed: make(chan struct{})}
		s.persAccepts.Add(1)
		s.persMu.Lock()
		s.persConns = append(s.persConns, w)
		s.persMu.Unlock()
		s.persServes.Add(1)
		go func() {
			defer s.persServes.Done()
			defer w.Close()
			s.servePersistentChannel(s.runCtx, w)
		}()
	}
}

// removePersConn drops a channel from the live set (its serve loop is
// exiting).
func (s *DestServer) removePersConn(w *persConnW) {
	s.persMu.Lock()
	for i, c := range s.persConns {
		if c == w {
			s.persConns = append(s.persConns[:i], s.persConns[i+1:]...)
			break
		}
	}
	s.persMu.Unlock()
}

// failUnitServe delivers an error report to the in-flight serve op for us
// (its channel died before the unit's data arrived, or the run is over), so
// the controller's WaitUnitDone unblocks with a failure instead of hanging.
// A unit that already completed (finished) is left alone: a late channel
// break must not override a successful report.
func (s *DestServer) failUnitServe(us *unitServe, unitID uint32, err error) {
	if us == nil || us.finished.Swap(true) {
		return
	}
	select {
	case us.done <- DestUnitDone{Op: "unit-done", Ok: false, UnitID: unitID, Error: errString(err)}:
	default:
	}
}

// servePersistentChannel is the destination half of ONE persistent channel
// (P1d): a loop that receives units off the connection (ReceiveUnit blocks
// until the source sends the next UnitBegin, answering idle Pings) and
// serves each — staging DDL -> COPY (+ inline digest) — on a connection
// from the destination pg pool, then delivers the result to the in-flight
// serve op (unitReg). The connection is NOT closed between units: the
// source keeps it for the run and routes further units to it (channel
// X%N). The loop exits when the connection breaks (the source tore the
// channel down / the run ended).
func (s *DestServer) servePersistentChannel(ctx context.Context, w *persConnW) {
	rcv := NewPersistentReceiver(w, 0)
	for {
		spec, data, err := rcv.ReceiveUnit(ctx)
		if err != nil {
			// The channel broke (source teardown / run end). Fail every
			// in-flight serve that is still waiting for a unit off THIS
			// channel (its data will never arrive), then exit the loop.
			s.failAllUnitServes(fmt.Errorf("persistent channel closed: %w", err))
			return
		}
		// The in-flight serve op for this unit (launched by the control loop
		// on its BeginServe) is waiting for the result.
		s.unitMu.Lock()
		us, ok := s.unitReg[spec.UnitID]
		s.unitMu.Unlock()
		if !ok {
			// No serve op is waiting for this unit (stale channel / a run
			// abort removed it before the data arrived). The UnitBegin +
			// data are already off the wire: DRAIN the unit to its Trailer
			// (io.EOF) so the channel stays in lock-step for the next unit,
			// then drop it.
			_, _ = io.Copy(io.Discard, data)
			continue
		}
		res := s.serveUnitData(ctx, spec, data)
		// Mark the unit terminal BEFORE delivering: a concurrent channel
		// break (failUnitServe) must not override a completed report. The
		// serve op consumes the report (buffered: if the op was already
		// cancelled — a run abort — the report is dropped, not lost).
		us.finished.Store(true)
		select {
		case us.done <- res:
		default:
		}
	}
}

// failAllUnitServes fails every in-flight serve (the run is over / the
// channel pool is being torn down).
func (s *DestServer) failAllUnitServes(err error) {
	s.unitMu.Lock()
	inflight := make([]*unitServe, 0, len(s.unitReg))
	for u, us := range s.unitReg {
		inflight = append(inflight, us)
		_ = u
	}
	s.unitMu.Unlock()
	for _, us := range inflight {
		s.failUnitServe(us, 0, err)
	}
}

// serveUnitData serves a unit whose data has already been received off the
// persistent channel (P1d): staging DDL + COPY (+ inline digest) on a
// connection from the destination pg pool. The swap is NOT performed (the
// Manager decides it, post-unit-done — the coordinator path's invariant).
func (s *DestServer) serveUnitData(ctx context.Context, spec UnitSpec, data *unitDataReader) DestUnitDone {
	var done DestUnitDone
	done.Op = "unit-done"
	done.UnitID = spec.UnitID
	if s.dq == nil {
		done.Error = "persistent serve: no destination pg pool"
		return done
	}
	q, release, aerr := s.dq.AcquireSnapshotConn(ctx)
	if aerr != nil {
		done.Error = fmt.Sprintf("persistent serve: acquire dest conn: %v", aerr)
		return done
	}
	defer release()
	res, err := ServeUnitData(ctx, q, dest.New(q), spec, data, false, s.opts.StreamTimeout)
	done.Rows, done.Logical, done.Swapped, done.Digest = res.Rows, res.Logical, res.Swapped, res.Digest
	done.Ok = err == nil
	done.Error = errString(err)
	return done
}

// registerUnitServe (P1d) registers an in-flight persistent serve in
// unitReg (keyed by the unit's UnitID). Called by the control loop BEFORE
// the ack is sent, so the channel loop can never see the unit's UnitBegin
// before its serve is registered (the race guard — see the serve-unit case).
func (s *DestServer) registerUnitServe(unitID uint32) {
	us := &unitServe{done: make(chan DestUnitDone, 1)}
	s.unitMu.Lock()
	// A retry of the same unit (a resume within the run) replaces the stale
	// registration: the old one's serve op has already been drained by the
	// controller (its WaitUnitDone failed on the earlier unit-done).
	s.unitReg[unitID] = us
	s.unitMu.Unlock()
}

// serveUnitPersistentWait (P1d) waits for the persistent serve the control
// loop already registered (registerUnitServe) for this unit: the per-channel
// serve loop delivers the result on the unitServe.done channel once the
// source's UnitBegin+data arrive and the COPY finishes. Bounded by the run
// ctx (a run abort interrupts the wait and fails the unit's report, so the
// channel loop is not stranded).
func (s *DestServer) serveUnitPersistentWait(ctx context.Context, req *DestServeReq) DestUnitDone {
	s.unitMu.Lock()
	us := s.unitReg[req.UnitID]
	s.unitMu.Unlock()
	defer func() {
		s.unitMu.Lock()
		if s.unitReg[req.UnitID] == us {
			delete(s.unitReg, req.UnitID)
		}
		s.unitMu.Unlock()
	}()
	if us == nil {
		return DestUnitDone{Op: "unit-done", Ok: false, UnitID: req.UnitID, ReqID: req.ReqID, Error: "serve: not registered"}
	}
	select {
	case res := <-us.done:
		res.ReqID = req.ReqID
		return res
	case <-ctx.Done():
		// Run abort: the source may still be (or about to be) streaming this
		// unit's data. Fail the unit's report (the channel loop then drains
		// the data to its Trailer and keeps the channel in lock-step); the
		// serve op returns the cancellation to the controller.
		s.failUnitServe(us, req.UnitID, ctx.Err())
		return DestUnitDone{Op: "unit-done", Ok: false, UnitID: req.UnitID, ReqID: req.ReqID, Error: "run context cancelled"}
	}
}

// dataListener is the listener the serves accept from (the TLS-wrapped one
// when data TLS is on, else the plain one).
func (s *DestServer) dataListener() net.Listener {
	if s.tlsL != nil {
		return s.tlsL
	}
	return s.dataL
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
