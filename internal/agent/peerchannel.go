// Package agent — SOURCE side of the Phase 2 source->dest peer channel.
//
// The source agent establishes a PERSISTENT multiplexed channel to the
// destination agent's peer listener (internal/agent/peerquery.go is the wire
// types; destserver.go servePeerSession is the destination side). The first
// message is the source's SourceVersion (the 1F cross-check); after the ack
// the channel multiplexes read-only destination-catalog queries (dst-query).
//
// Why: architecture 08.10 — the controller must open NO direct database
// connections at all; it delegates them to the agents. The SOURCE agent runs
// plan.Build (2A) / the resume lists (2E) / the dry-run checks (2C) with its
// LOCAL source pool + a remote destination querier (remoteQuerier) that
// proxies each read-only destination-catalog query to the destination agent,
// which runs it on its own LOCAL coordinator pool. The controller therefore
// opens NO direct destination connection.
package agent

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/version"
)

// peerChannel is the source agent's side of the persistent source->dest peer
// channel: one TCP connection to the destination's peer listener, the 1F
// version exchange as the first message, then a multiplexed dst-query loop
// (P1d-style ReqID correlation: one reader goroutine, one goroutine per
// query, responses serialized on the connection).
type peerChannel struct {
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
	// writeMu serializes request LINES on the connection (a request line is
	// never interleaved with another).
	writeMu sync.Mutex
	// pending: in-flight op correlation (keyed by ReqID). The single reader
	// routes each response line to the matching waiter.
	pendingMu sync.Mutex
	pending   map[uint64]chan []byte
	// dead is closed when the reader sees the connection end: every waiting
	// op fails fast.
	dead     chan struct{}
	deadOnce sync.Once
	nextID   atomic.Uint64
}

// errPeerDead is returned to every in-flight op when the peer connection
// drops.
var errPeerDead = errors.New("peer channel: connection closed")

// dialPeer establishes the persistent source->dest peer channel: dial the
// destination's peer listener, send the source's version report (the 1F
// cross-check), wait for the verdict, and (on a match) start the multiplexed
// dst-query reader loop. On a version mismatch the connection is closed and
// the error returned (no data / no plan flows — a mixed-version deployment
// must fail fast).
func dialPeer(ctx context.Context, peerAddr string, sv SourceVersion, dataCert, dataKey, dataCA string) (*peerChannel, error) {
	dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
	defer dcancel()
	d := net.Dialer{}
	conn, err := d.DialContext(dctx, "tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("peer: dial dest peer %s: %w", peerAddr, err)
	}
	// The destination wraps the peer listener in the data-plane TLS (it is a
	// data-plane channel: it carries the dst-query loop + the 1F version
	// exchange). When data TLS is on the source must be the TLS client — a
	// plaintext version report against a TLS server fails the handshake and
	// the destination closes the connection (BUG #12, same class as BUG #11:
	// the report-channel TLS the supervisor added was never matched on the
	// dialing side). The source presents its data-TLS keypair (the destination
	// runs mTLS: CA-set requires a client cert) and verifies the destination
	// against the CA.
	if transfer.TLSEnabled(dataCert, dataKey, dataCA) {
		host, herr := hostOf(peerAddr)
		if herr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("peer: bad peer addr %q: %w", peerAddr, herr)
		}
		ccfg, cerr := transfer.ClientTLSConfig(host, dataCert, dataKey, dataCA, "")
		if cerr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("peer: tls config: %w", cerr)
		}
		tc := tls.Client(conn, ccfg)
		hctx, hcanc := context.WithTimeout(ctx, 30*time.Second)
		if hserr := tc.HandshakeContext(hctx); hserr != nil {
			hcanc()
			_ = conn.Close()
			return nil, fmt.Errorf("peer: tls handshake with dest: %w", hserr)
		}
		hcanc()
		conn = tc
	}
	pc := &peerChannel{
		conn:    conn,
		w:       bufio.NewWriter(conn),
		r:       bufio.NewReader(conn),
		pending: make(map[uint64]chan []byte),
		dead:    make(chan struct{}),
	}
	// The 1F version exchange (first message, not multiplexed). The read
	// deadline bounds the handshake (the destination's verdict).
	if werr := pc.writeLine(sv); werr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("peer: send version: %w", werr)
	}
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	var ack PeerAck
	if rerr := recvLine(pc.r, &ack); rerr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("peer: read verdict: %w", rerr)
	}
	if !ack.Ok {
		_ = conn.Close()
		return nil, fmt.Errorf("peer: version exchange: %s", ack.Error)
	}
	_ = conn.SetReadDeadline(time.Time{}) // clear the handshake deadline (long-lived)
	go pc.readLoop()
	return pc, nil
}

// readLoop reads response lines for the channel's lifetime and routes each to
// the op that issued the matching ReqID (dst-query responses arrive in
// completion order — correlation by ReqID makes that safe).
func (pc *peerChannel) readLoop() {
	defer pc.markDead()
	for {
		line, err := pc.r.ReadBytes('\n')
		if len(line) > 0 {
			var env struct {
				ReqID uint64 `json:"req_id"`
			}
			if uerr := json.Unmarshal(line, &env); uerr == nil && env.ReqID != 0 {
				pc.pendingMu.Lock()
				ch, ok := pc.pending[env.ReqID]
				if ok {
					delete(pc.pending, env.ReqID)
				}
				pc.pendingMu.Unlock()
				if ok {
					select {
					case ch <- line:
					default: // already failed (dead) — drop
					}
					continue
				}
			}
			// A line we cannot correlate (should not happen). Drop it.
		}
		if err != nil {
			return // io.EOF (dest closed the channel) or a transport error
		}
	}
}

// markDead fails every waiting op once (connection loss).
func (pc *peerChannel) markDead() {
	pc.deadOnce.Do(func() {
		close(pc.dead)
		pc.pendingMu.Lock()
		for id := range pc.pending {
			delete(pc.pending, id)
		}
		pc.pendingMu.Unlock()
	})
}

// writeLine writes one request line (serialized).
func (pc *peerChannel) writeLine(msg any) error {
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()
	if err := SendLine(pc.w, msg); err != nil {
		return err
	}
	return pc.w.Flush()
}

// call issues one dst-query request and waits for the matching response
// (correlated by ReqID).
func (pc *peerChannel) call(ctx context.Context, req QueryReq, resp *QueryResp) error {
	id := pc.nextID.Add(1)
	req.ReqID = id
	ch := make(chan []byte, 1)
	pc.pendingMu.Lock()
	pc.pending[id] = ch
	pc.pendingMu.Unlock()
	defer func() {
		pc.pendingMu.Lock()
		if pc.pending[id] == ch {
			delete(pc.pending, id)
		}
		pc.pendingMu.Unlock()
	}()
	if werr := pc.writeLine(req); werr != nil {
		return werr
	}
	tctx, tcancel := context.WithTimeout(ctx, 60*time.Second)
	defer tcancel()
	select {
	case line := <-ch:
		if uerr := json.Unmarshal(line, resp); uerr != nil {
			return fmt.Errorf("peer: unmarshal response: %w", uerr)
		}
		return nil
	case <-pc.dead:
		return errPeerDead
	case <-tctx.Done():
		return tctx.Err()
	}
}

// close closes the peer connection.
func (pc *peerChannel) close() {
	if pc.conn != nil {
		_ = pc.conn.Close()
	}
}

// remoteQuerier is a pg.Querier that proxies read-only destination-catalog
// queries to the destination agent over the persistent peer channel (the
// destination runs them on its own LOCAL coordinator pool). plan.Build uses
// it as its `dst` argument, so the source agent never opens a direct
// destination connection. Only Query + Ping are supported (plan.Build never
// writes to the destination); Exec is an error.
type remoteQuerier struct {
	ch *peerChannel
}

// Query proxies one read-only destination query over the peer channel.
func (q *remoteQuerier) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	ra := make([]json.RawMessage, 0, len(args))
	for _, a := range args {
		b, err := json.Marshal(a)
		if err != nil {
			return nil, fmt.Errorf("peer querier: encode arg: %w", err)
		}
		ra = append(ra, b)
	}
	req := QueryReq{Op: "dst-query", SQL: sql, Args: ra}
	var resp QueryResp
	if err := q.ch.call(ctx, req, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, errors.New(resp.Error)
	}
	out := make([][]any, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		cells := make([]any, 0, len(row))
		for _, c := range row {
			cells = append(cells, c.decode())
		}
		out = append(out, cells)
	}
	return out, nil
}

// Ping verifies the destination pool is alive (a read-only probe over the
// channel; the version exchange does not touch the destination DB).
func (q *remoteQuerier) Ping(ctx context.Context) error {
	_, err := q.Query(ctx, "SELECT 1")
	return err
}

// Exec is unsupported: the source agent never writes to the destination
// (architecture 08.10 — the destination DB is touched only by the destination
// agent, and plan.Build is read-only against it).
func (q *remoteQuerier) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return 0, errors.New("peer querier is read-only: the source agent never writes to the destination")
}

// Close is a no-op (the channel is owned by the SourceServer, closed at run
// end).
func (q *remoteQuerier) Close() {}

// gatherSourceVersion builds the source's version report: its own identity +
// the number of source segment hosts (S) + the segreceiver versions it
// SSH-gathered from them (the 1F cross-check input). The source reads its own
// cluster's catalog locally (s.pool); a plain-PostgreSQL source (no
// gp_segment_configuration) reports S=0 (no segreceivers).
func (s *SourceServer) gatherSourceVersion(ctx context.Context, segBinary, segSSH string) (SourceVersion, error) {
	sv := SourceVersion{Op: "source-version", Version: version.String()}
	segHosts := []string{}
	if s.pool != nil {
		if rows, qerr := s.pool.Query(ctx, "SELECT hostname FROM gp_segment_configuration WHERE role = 'p' ORDER BY content"); qerr == nil {
			for _, row := range rows {
				if len(row) > 0 {
					segHosts = append(segHosts, fmt.Sprint(row[0]))
				}
			}
		}
		// A query error / empty result = a plain-PostgreSQL source (no
		// gp_segment_configuration): S=0 (no segreceivers) is the correct
		// report, not an error.
	}
	var segs []SegVersion
	for _, h := range segHosts {
		ssh := ""
		if segSSH != "" {
			ssh = segSSH + h
		}
		remote := "'" + segBinary + "' --version"
		v, gerr := RunRemoteOutput(ctx, ssh, remote)
		if gerr != nil {
			return sv, fmt.Errorf("version exchange: segriver --version on %s: %w", h, gerr)
		}
		// The segreceiver prints "<name> <ver (commit)>": drop the binary
		// name token so it compares against the destination's identity.
		fields := strings.Fields(v)
		if len(fields) < 2 {
			return sv, fmt.Errorf("version exchange: segriver --version on %s: empty (got %q)", h, v)
		}
		segs = append(segs, SegVersion{Host: h, Version: strings.Join(fields[1:], " ")})
	}
	sv.S = len(segHosts)
	sv.SegReceivers = segs
	return sv, nil
}

// ensurePeerChannel establishes (idempotently) the persistent source->dest
// peer channel: gather the source's version report, dial the destination's
// peer listener, run the 1F cross-check, and keep the connection open for the
// dst-query loop. It is guarded by s.peerOnce so concurrent callers (the
// plan-build + the first mesh unit) share one channel. It returns the channel
// (for a remoteQuerier) or the exchange error.
func (s *SourceServer) ensurePeerChannel(ctx context.Context, peerAddr, segBinary, segSSH string) (*peerChannel, error) {
	s.peerOnce.Do(func() {
		defer close(s.peerDone)
		// Bound the establishment (dial + the SSH segreceiver-version gather):
		// a hung segment SSH must not stall the plan / the first mesh unit.
		// The PERSISTENT connection itself has no deadline (it lives for the
		// run); only the handshake + version gather are bounded.
		ectx, ecancel := context.WithTimeout(ctx, 90*time.Second)
		defer ecancel()
		sv, gerr := s.gatherSourceVersion(ectx, segBinary, segSSH)
		if gerr != nil {
			s.peerErr = gerr
			return
		}
		c, derr := dialPeer(ectx, peerAddr, sv, s.opts.DataTLSCert, s.opts.DataTLSKey, s.opts.DataTLSCA)
		if derr != nil {
			s.peerErr = derr
			return
		}
		s.peerConn = c
	})
	select {
	case <-s.peerDone:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if s.peerErr != nil {
		return nil, s.peerErr
	}
	return s.peerConn, nil
}

// PeerQuerier returns a pg.Querier that proxies read-only destination-catalog
// queries to the destination agent over the persistent peer channel (nil if
// the channel is not established). The plan-build / resume / dry-run paths use
// it as the destination for plan.Build.
func (s *SourceServer) PeerQuerier() pg.Querier {
	if s.peerConn == nil {
		return nil
	}
	return &remoteQuerier{ch: s.peerConn}
}
