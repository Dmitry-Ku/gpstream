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
	"net"
	"sync"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// DestHello is the agent's first control message (its real bound addresses).
type DestHello struct {
	Op      string `json:"op"` // "hello"
	Data    string `json:"data"`
	Control string `json:"control"`
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
	Op       string        `json:"op"` // "serve-unit"
	Kind     string        `json:"kind"`
	Swap     bool          `json:"swap"`
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
	Op      string `json:"op"` // "unit-done"
	Ok      bool   `json:"ok"`
	Rows    int64  `json:"rows,omitempty"`
	Logical uint64 `json:"logical,omitempty"`
	Wire    uint64 `json:"wire,omitempty"`
	Swapped bool   `json:"swapped,omitempty"`
	Error   string `json:"error,omitempty"`
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
}

// DestServer is the long-lived destination agent (one data + one control
// listener for the run).
type DestServer struct {
	opts DestServerOptions
	// dataL is the PLAIN listener; tlsL (when data TLS is on) is the wrapped
	// one the serves accept from.
	dataL net.Listener
	tlsL  net.Listener
	ctrlL net.Listener

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
	dataL, err := net.Listen("tcp", o.DataListen)
	if err != nil {
		return nil, fmt.Errorf("dest server: data listen %s: %w", o.DataListen, err)
	}
	ctrlL, err := net.Listen("tcp", o.ControlListen)
	if err != nil {
		_ = dataL.Close()
		return nil, fmt.Errorf("dest server: control listen %s: %w", o.ControlListen, err)
	}
	s := &DestServer{opts: o, dataL: dataL, ctrlL: ctrlL, ctrlDone: make(chan struct{})}
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
	}
	go s.serveControl(s.ctrlL) // the TLS-wrapped listener, when control TLS is on
	return s, nil
}

// Hello is the agent's first control message (its real bound addresses).
func (s *DestServer) Hello() DestHello {
	return DestHello{Op: "hello", Data: s.DataAddr(), Control: s.ControlAddr()}
}

// DataAddr / ControlAddr are the real bound listener addresses.
func (s *DestServer) DataAddr() string    { return s.dataL.Addr().String() }
func (s *DestServer) ControlAddr() string { return s.ctrlL.Addr().String() }

// Done is closed when the control loop exits (the controller disconnected,
// the run was released, or the listeners failed).
func (s *DestServer) Done() <-chan struct{} { return s.ctrlDone }

// Close shuts the listeners down (the agent's clean exit).
func (s *DestServer) Close() {
	s.stopOnce.Do(func() {
		_ = s.ctrlL.Close()
		_ = s.dataL.Close()
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
			// Each unit gets its own context (the control loop is
			// long-lived; the serve bounds itself with StreamTimeout).
			done := s.serveOne(context.Background(), &req)
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
		if req.Spec == nil {
			done.Error = "serve-unit kind=coordinator: missing spec"
			return done
		}
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
		})
		done.Rows, done.Logical, done.Swapped = res.Rows, res.Logical, res.Swapped
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
		})
		done.Rows, done.Logical, done.Wire = res.Rows, res.Logical, res.Wire
		done.Ok = err == nil
		done.Error = errString(err)
	default:
		done.Error = "unknown serve kind " + req.Kind
	}
	return done
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
