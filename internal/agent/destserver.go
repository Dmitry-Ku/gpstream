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
	"errors"
	"fmt"
	"io"
	"net"
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
type DestServeReq struct {
	Op   string `json:"op"` // "serve-unit"
	Kind string `json:"kind"`
	Swap bool   `json:"swap"`
	// UnitID (P1d) identifies the unit for the concurrent serve path: the
	// unit-done report echoes it so the controller correlates N serves in
	// flight on one control connection.
	UnitID   uint32        `json:"unit_id,omitempty"`
	Spec     *UnitSpec     `json:"spec,omitempty"`     // kind=coordinator
	Segments int           `json:"segments,omitempty"` // kind=segments
	Direct   *DirectLaunch `json:"direct,omitempty"`   // kind=direct
	Routed   *RoutedLaunch `json:"routed,omitempty"`   // kind=routed
}

// DestAck is the agent's immediate reply to "serve-unit" (the serve is
// running in the background; its report arrives as a standalone unit-done).
type DestAck struct {
	Op    string `json:"op"` // "ack"
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// DestUnitDone is the agent's report after a serve-unit finished.
type DestUnitDone struct {
	Op string `json:"op"` // "unit-done"
	Ok bool   `json:"ok"`
	// UnitID (P1d) echoes DestServeReq.UnitID (concurrent serves: the
	// controller maps the report to the unit by id).
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
	Error  string `json:"error,omitempty"`
}

// DestReleased is the agent's reply to "release".
type DestReleased struct {
	Op string `json:"op"` // "released"
	Ok bool   `json:"ok"`
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
	// operator's --data-port-range (strict firewall). It is consulted only
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
	// RunCtx, when set, is the base context every serve is bounded by (in
	// addition to AcceptTimeout/StreamTimeout). The IN-PROC runner sets it to
	// the controller's run context so a run abort / crash-simulation (a soft
	// ctx cancel) interrupts an in-flight unit's COPY exactly as the legacy
	// per-unit agent.Serve(ctx) did (the process runner leaves it nil: its
	// agent is a separate process whose serves are bounded by its own signal
	// ctx, and a crash is a hard kill, not a cancel).
	RunCtx context.Context
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
	// dq (P1c): the destination pg conn the persistent serve uses (nil when
	// the persist listener is off).
	dq *pg.Conn
	// runCtx: the base context every serve is bounded by (RunCtx when set,
	// else context.Background() — the process-runner behavior).
	runCtx context.Context
	// persConn (P1c): the live persistent coordinator connection (nil = none
	// up yet / broken). persWait is closed whenever a connection is stored
	// (takePersConn waits on it); a fresh unclosed one is created each time.
	persMu   sync.Mutex
	persConn *persConnW
	persWait chan struct{}
	// persAccepts (P1c): the number of persistent coordinator connections
	// accepted so far (the first unit's dial + one per re-dial after a
	// break) — a runtime signal that the persistent channel is in use.
	persAccepts atomic.Int64

	ctrlDone chan struct{}
	stopOnce sync.Once
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
	dataL, err := o.bindListener(o.DataListen)
	if err != nil {
		return nil, fmt.Errorf("dest server: data listen %s: %w", o.DataListen, err)
	}
	ctrlL, err := o.bindListener(o.ControlListen)
	if err != nil {
		_ = dataL.Close()
		return nil, fmt.Errorf("dest server: control listen %s: %w", o.ControlListen, err)
	}
	var persistL net.Listener
	if o.PersistListen != "" {
		persistL, err = o.bindListener(o.PersistListen)
		if err != nil {
			_ = dataL.Close()
			_ = ctrlL.Close()
			return nil, fmt.Errorf("dest server: persist listen %s: %w", o.PersistListen, err)
		}
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
	if transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
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
	// P1c: the destination pg connection the persistent serve uses (staging
	// DDL, the COPY, the swap) + the persistent-coordinator accept loop.
	if s.persistL != nil {
		dq, derr := pg.NewConn(context.Background(), o.DestConn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
		if derr != nil {
			s.Close()
			return nil, fmt.Errorf("dest server: connect dest (persistent serve): %w", derr)
		}
		s.dq = dq
		s.persWait = make(chan struct{})
		go s.acceptPersistent()
	}
	go s.serveControl(s.ctrlL) // the TLS-wrapped listener, when control TLS is on
	return s, nil
}

// bindListener binds addr, honoring the --data-port-range (o.PortAlloc) when
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
			if w := s.persConn; w != nil {
				_ = w.Conn.Close() // hard: the in-flight COPY's read breaks
				s.persConn = nil
			}
			s.persMu.Unlock()
		}
	})
}

// Close shuts the listeners down (the agent's clean exit).
func (s *DestServer) Close() {
	s.stopOnce.Do(func() {
		_ = s.ctrlL.Close()
		_ = s.dataL.Close()
		if s.persistL != nil {
			_ = s.persistL.Close()
			s.persMu.Lock()
			if w := s.persConn; w != nil {
				_ = w.Close()
				s.persConn = nil
			}
			s.persMu.Unlock()
		}
		if s.dq != nil {
			s.dq.Close()
		}
	})
}

// serveControl accepts the ONE control connection (the controller) and runs
// the sequential protocol: hello, then serve-unit* / release.
func (s *DestServer) serveControl(l net.Listener) {
	defer close(s.ctrlDone)
	conn, err := l.Accept()
	if err != nil {
		return // no controller (orphan) — the process waits to be killed
	}
	defer conn.Close()
	// The agent speaks first: report the real bound addresses (auto :0).
	if werr := SendLine(conn, s.Hello()); werr != nil {
		return
	}
	r := bufio.NewReader(conn)
	for {
		var req DestServeReq
		if rerr := recvLine(r, &req); rerr != nil {
			return // controller closed the control conn (run end / stop)
		}
		switch req.Op {
		case "serve-unit":
			// Acknowledge immediately (the serve runs in the background —
			// the controller instructs the SOURCE agent to stream AFTER
			// this ack), then report the result as a standalone unit-done.
			ack := DestAck{Op: "ack", Ok: true}
			if req.Kind == "" {
				ack = DestAck{Op: "ack", Ok: false, Error: "serve-unit: missing kind"}
			}
			if serr := SendLine(conn, ack); serr != nil {
				return
			}
			if !ack.Ok {
				continue
			}
			// Each unit is bounded by the run ctx (the in-proc runner's
			// run context, so a run abort / crash-simulation interrupts an
			// in-flight COPY) + the serve's own StreamTimeout.
			done := s.serveOne(s.runCtx, &req)
			if serr := SendLine(conn, done); serr != nil {
				return
			}
		case "release":
			_ = SendLine(conn, DestReleased{Op: "released", Ok: true})
			return
		default:
			_ = SendLine(conn, DestUnitDone{Op: "unit-done", Ok: false, Error: "unknown op " + req.Op})
		}
	}
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
			// P1c: the unit rides the run's ONE persistent coordinator
			// channel (the UnitBegin frame carries the spec; the connection
			// stays open between units). The control loop stays sequential:
			// ReceiveUnit blocks until the source sends the UnitBegin during
			// the Stream (after the ack), exactly like the legacy acceptOne
			// blocked for the source's dial.
			res, err := s.serveCoordinatorPersistent(ctx, req.Swap)
			done.Rows, done.Logical, done.Swapped = res.Rows, res.Logical, res.Swapped
			done.Digest = res.Digest
			done.Ok = err == nil
			done.Error = errString(err)
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

// acceptPersistent accepts the persistent coordinator connection(s) and keeps
// the latest one stored (s.persConn). It runs for the life of the server.
func (s *DestServer) acceptPersistent() {
	for {
		conn, err := s.persistL.Accept()
		if err != nil {
			return // listener closed (run end / stop)
		}
		w := &persConnW{Conn: conn, closed: make(chan struct{})}
		s.persAccepts.Add(1)
		s.persMu.Lock()
		old := s.persConn
		s.persConn = w
		ch := s.persWait
		s.persWait = make(chan struct{})
		s.persMu.Unlock()
		if old != nil {
			_ = old.Close() // a stale channel (the source re-dialed) — replace it
		}
		close(ch)  // wake waiters: a connection is now stored
		<-w.closed // wait for this connection to be torn down, then re-accept
	}
}

// takePersConn returns the live persistent coordinator connection, waiting
// (bounded by ctx) until one is accepted if none is up yet (the first unit,
// or a re-accept after a break).
func (s *DestServer) takePersConn(ctx context.Context) (net.Conn, error) {
	for {
		s.persMu.Lock()
		if c := s.persConn; c != nil {
			s.persMu.Unlock()
			return c.Conn, nil
		}
		ch := s.persWait
		s.persMu.Unlock()
		select {
		case <-ch:
			// A connection was stored: re-check (it may already be replaced).
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// dropPersConn tears down the persistent connection after a unit failure on
// a broken channel (the accept loop re-accepts the source's re-dial). A unit
// failure on a HEALTHY channel (e.g. a destination PG fault) keeps the
// connection: the next unit re-streams over it.
func (s *DestServer) dropPersConn(c net.Conn) {
	s.persMu.Lock()
	if w := s.persConn; w != nil && w.Conn == c {
		s.persConn = nil
	}
	s.persMu.Unlock()
	_ = c.Close()
}

// isConnError reports whether err is a data-connection failure (a broken
// socket / a clean EOF) rather than a destination-side fault: only the former
// requires re-accepting the persistent channel.
func isConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}

// serveCoordinatorPersistent serves ONE coordinator unit over the persistent
// channel: wait for the channel (bounded by AcceptTimeout, like the legacy
// acceptOne) -> ReceiveUnit (blocks on the unit's UnitBegin, answering idle
// Pings) -> staging -> COPY (+ inline digest) -> the swap, when requested.
func (s *DestServer) serveCoordinatorPersistent(ctx context.Context, swap bool) (DestResult, error) {
	waitCtx, wcancel := context.WithTimeout(ctx, s.opts.AcceptTimeout)
	conn, err := s.takePersConn(waitCtx)
	wcancel()
	if err != nil {
		return DestResult{}, fmt.Errorf("persistent serve: wait for channel: %w", err)
	}
	res, err := ServePersistentUnit(ctx, s.dq, dest.New(s.dq), NewPersistentReceiver(conn, 0), swap, s.opts.StreamTimeout)
	// Tear down the channel on a broken connection OR when the run context
	// was cancelled (a run abort / crash-simulation): the in-flight unit's
	// source-side COPY must then fail (its write hits the closed channel),
	// unblocking the source agent's control loop for the release. A unit
	// failure on a HEALTHY channel (a transient destination PG fault) keeps
	// the connection: the next unit re-streams over it.
	if err != nil && (isConnError(err) || ctx.Err() != nil) {
		s.dropPersConn(conn) // re-accept the source's re-dial (or the run is over)
	}
	return res, err
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
