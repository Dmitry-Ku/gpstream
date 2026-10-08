package agent

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
)

// This file is the controller<->agent CONTROL protocol (ADR-002 plan, P3).
//
// The data path (framed + compressed bytes) flows source agent -> dest agent
// directly. The controller does NOT touch data; it drives the agents over
// short control messages: it tells the (long-lived) source agent which unit to
// stream and to which destination endpoint, and reads the source's report
// (rows + wire/logical) back. A newline-delimited JSON protocol over a TCP
// socket keeps it simple to implement, log and debug (one message per line).
//
// The source agent is long-lived because it owns the FR-02 snapshot: a
// REPEATABLE READ transaction lives on one server-side backend, so the same
// process must run every unit's data COPY under that one snapshot to keep the
// whole run consistent. The controller sends one "stream" message per unit and
// a final "release" (commit the snapshot) when the run ends.

// StreamReq is the controller's per-unit instruction to the source agent.
type StreamReq struct {
	Op        string `json:"op"` // "stream"
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	JobID     uint32 `json:"job_id"`
	UnitID    uint32 `json:"unit_id"`
	DestAddr  string `json:"dest"` // destination agent endpoint (the source dials it)
	Compress  string `json:"compress"`
	DstSchema string `json:"dst_schema"`
	DstTable  string `json:"dst_table"`
	Staging   string `json:"staging"`
	Overwrite bool   `json:"overwrite"`
	// VerifyDigest (FR-02, P9-E) asks the source agent to compute the
	// row-order-independent content digest of the streamed payload on the
	// data path (O(1) memory) and report it in StreamResp.Digest, so the
	// controller verifies against the destination's inline digest without
	// re-reading either table. Set by the manager when --verify is on.
	VerifyDigest bool `json:"verify_digest"`
	// Persistent (P1c) routes the unit over the persistent coordinator data
	// channel: the source holds ONE connection to the destination's
	// dedicated persistent-coordinator listener for the whole run and sends
	// the unit as a UnitBegin frame + framed data + Trailer (no per-unit
	// dial/UnitSpec/close). DestAddr is then the persistent-coordinator
	// listener address (from the destination hello's Persist field). False
	// (the one-shot bench / legacy per-unit path) dials DestAddr fresh and
	// sends the legacy UnitSpec header + framed stream.
	Persistent bool `json:"persistent,omitempty"`
	// ReqID (P1d) is the controller's per-request correlation id, echoed
	// in StreamResp so concurrent ops on one control connection are
	// matched to the waiting worker (unit_id alone is not unique across
	// retries: the same unit can be streamed twice in one run).
	ReqID uint64 `json:"req_id,omitempty"`
}

// OnSegmentReq is the controller's per-unit instruction for the P4
// on_segment data path: the source agent runs
// `COPY ... TO PROGRAM 'gpstream-segreceiver --listen <DestAddr> --compress
// <Compress> ...' ON SEGMENT` on its FR-02 snapshot connection. The kernel
// launches the segreceiver on every primary source segment; each one dials
// the destination agent (DestAddr) and streams its slice compressed. The
// source agent waits for the kernel's COPY to return (all segreceivers have
// streamed) and reports the kernel's row count; the authoritative count +
// the on-network wire volume come from the destination agent.
type OnSegmentReq struct {
	Op        string `json:"op"` // "onsegment"
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	JobID     uint32 `json:"job_id"`
	UnitID    uint32 `json:"unit_id"`
	DestAddr  string `json:"dest"`   // the destination agent's listen address (the segreceivers dial it)
	SegBinary string `json:"segbin"` // gpstream-segreceiver path on the source segment hosts
	// PeerAddr (1F) is the destination agent's PEER listener address: before
	// the first MESH unit, the source agent dials it ONCE and reports its own
	// version + the segreceiver versions it SSH-gathered from the S source
	// segments (the destination cross-checks them against its own + the mini
	// versions before any data flows). Empty = no version exchange (a
	// non-mesh unit / a dest without a peer listener).
	PeerAddr string `json:"peer_addr,omitempty"`
	// SegSSH (1F) is the "user@" prefix the source agent SSHes to its source
	// segment hosts to gather "segriver --version" (the dry-run A+ mechanism;
	// gpadmin has passwordless SSH to its segments). Empty = no SSH
	// (single-host layout: the source host IS the segment).
	SegSSH   string `json:"seg_ssh,omitempty"`
	Compress string `json:"compress"` // per-slice compression mode (the dest agent decompresses)
	// Data-plane TLS for the segment<->agent links (the segreceivers are the
	// clients; the destination agent serves). All empty = plain TCP.
	DataTLSCert string `json:"data_tls_cert,omitempty"`
	DataTLSKey  string `json:"data_tls_key,omitempty"`
	DataTLSCA   string `json:"data_tls_ca,omitempty"`
	// P9 routed mode (S≠D): when Routed, the segreceivers do not stream one
	// slice to a destination agent; they route every row to its destination
	// segment (cdbhash + jump-consistent-hash over RouteKey) and fan each
	// (source-seg,dest-seg) pair over RouteWorkers sub-streams, dialing
	// DestAddr (the destination agent) D×W times. DestAddr carries the
	// destination agent's listen address in both modes.
	Routed       bool   `json:"routed,omitempty"`
	RouteKey     string `json:"route_key,omitempty"` // "oid:pos,oid:pos"
	RouteD       int32  `json:"route_d,omitempty"`
	RouteWorkers int    `json:"route_workers,omitempty"`
	// Mesh (P3 production-dispatch, mesh direct_1to1): when set, DestAddr is
	// the D-entry ADDRESS BOOK (the comma-separated destination-segment
	// mini-agent data endpoints, seg order) and RouteD is the SOURCE segment
	// count (S). Each segreceiver (on source segment s) dials the entry s —
	// its own destination segment's mini-agent (S==D, equal hash keys) — and
	// streams its slice to that mini-agent's data listener. Neither cluster
	// coordinator is in the data path (design 04.10). The source agent builds
	// ProgramCommandMesh (not ProgramCommandFull) for the ON SEGMENT dispatch.
	Mesh bool `json:"mesh,omitempty"`
	// Inc3: frame-level ACK + RST-resume on the segment<->agent data links
	// (DESIGN §4.6). The source agent embeds the matching --frame-ack /
	// --ack-timeout / --max-reconnects / --retransmit-buffer-max flags in the
	// segreceiver program string; the destination agent is launched with the
	// same parameters by the controller.
	// ReqID (P1d) is the controller's per-request correlation id, echoed
	// in OnSegmentResp (concurrent ops on one control connection).
	ReqID         uint64 `json:"req_id,omitempty"`
	FrameAck      string `json:"frame_ack,omitempty"`   // on|off
	AckTimeout    string `json:"ack_timeout,omitempty"` // 15s|30s|adaptive
	MaxReconnects int    `json:"max_reconnects,omitempty"`
	RetransmitMax int64  `json:"retransmit_buffer_max,omitempty"`
}

// OnSegmentResp is the source agent's report after the ON SEGMENT COPY
// returned (all segreceivers have finished streaming). Rows is the kernel's
// COPY row count (a cross-check; the authoritative count is the destination
// agent's).
type OnSegmentResp struct {
	Ok     bool   `json:"ok"`
	UnitID uint32 `json:"unit_id,omitempty"` // P1d: echoes OnSegmentReq.UnitID
	ReqID  uint64 `json:"req_id,omitempty"`  // P1d: echoes OnSegmentReq.ReqID
	Rows   int64  `json:"rows,omitempty"`
	Error  string `json:"error,omitempty"`
}

// DirectReq is the controller's per-unit instruction for the P5 direct data
// path (R5 through the agents). The source agent reads the source table with
// `COPY ... TO STDOUT`, routes each row to one of D destination segments
// (jump-consistent-hash over the distribution key) and streams D per-segment
// COMPRESSED slices (content-tagged) to the destination agent, which performs
// D utility-mode per-segment COPYs. The destination's staging shape + the
// per-segment DSNs are handed to the DESTINATION agent at launch (DirectLaunch,
// --direct-spec), so the source only carries the routing key (KeyIdx/KeyOid/D)
// and the destination agent's data endpoint. The controller holds no data
// connection.
type DirectReq struct {
	Op       string `json:"op"` // "direct"
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	JobID    uint32 `json:"job_id"`
	UnitID   uint32 `json:"unit_id"`
	DestAddr string `json:"dest"` // the destination agent's listen address (the source dials it)
	Compress string `json:"compress"`
	// Routing key: distribution-key column positions (source COPY order),
	// their type OIDs, and the number of destination primary segments.
	KeyIdx []int    `json:"key_idx"`
	KeyOid []uint32 `json:"key_oid"`
	D      int32    `json:"d"`
	// Data-plane TLS for the agent<->agent socket (source = client).
	DataTLSCert string `json:"data_tls_cert,omitempty"`
	DataTLSKey  string `json:"data_tls_key,omitempty"`
	DataTLSCA   string `json:"data_tls_ca,omitempty"`
}

// DirectResp is the source agent's report after the direct stream finished
// (all D per-segment slices streamed). Rows is the count the router saw (a
// cross-check; the authoritative count is the destination agent's). PerSeg is
// the router's per-segment row distribution (the F-15 balance cross-check).
type DirectResp struct {
	Ok     bool    `json:"ok"`
	Rows   int64   `json:"rows,omitempty"`
	Wire   uint64  `json:"wire,omitempty"`
	PerSeg []int64 `json:"per_seg,omitempty"`
	Error  string  `json:"error,omitempty"`
}

// ReleaseReq tells the source agent to end the run (commit the snapshot and
// close the source connection).
type ReleaseReq struct {
	Op string `json:"op"` // "release"
	// ReqID (P1d) is the controller's per-request correlation id, echoed in
	// ReleaseResp. Release must be sent after every in-flight op has
	// completed (the client guarantees this), so the agent can wait for
	// ops to drain before committing the snapshot.
	ReqID uint64 `json:"req_id,omitempty"`
}

// StreamResp is the source agent's report after streaming a unit (the
// FR-04 volume + row count). Ok=false carries Error.
type StreamResp struct {
	Ok bool `json:"ok"`
	// UnitID (P1d) echoes StreamReq.UnitID so the controller's reader
	// correlates the report to the unit (concurrent ops, one control conn).
	UnitID uint32 `json:"unit_id,omitempty"`
	// ReqID echoes StreamReq.ReqID (concurrent-op correlation, P1d).
	ReqID   uint64 `json:"req_id,omitempty"`
	Rows    int64  `json:"rows,omitempty"`
	Wire    uint64 `json:"wire,omitempty"`
	Logical uint64 `json:"logical,omitempty"`
	// Digest is the row-order-independent content digest the source computed
	// on the data path (FR-02, P9-E) when StreamReq.VerifyDigest is set;
	// empty otherwise. The controller compares it against the destination's
	// inline digest (DestUnitDone.Digest) to verify without a re-read.
	Digest string `json:"digest,omitempty"`
	Error  string `json:"error,omitempty"`
}

// ReleaseResp acknowledges the release.
type ReleaseResp struct {
	Ok bool `json:"ok"`
	// ReqID (P1d) echoes ReleaseReq.ReqID (concurrent-op correlation).
	ReqID uint64 `json:"req_id,omitempty"`
	Error string `json:"error,omitempty"`
}

// ChecksumReq asks the source agent (which holds the FR-02 snapshot) for the
// content checksum of a table — the controller's --verify path compares it
// against the destination table's checksum. The source agent re-derives it on
// its pinned snapshot connection (the same snapshot the data COPY used).
type ChecksumReq struct {
	Op string `json:"op"` // "checksum"
	// ReqID (P1d) is the controller's per-request correlation id, echoed in
	// ChecksumResp (concurrent ops on one control connection).
	ReqID  uint64 `json:"req_id,omitempty"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// ChecksumResp is the source agent's checksum report.
type ChecksumResp struct {
	Ok bool `json:"ok"`
	// ReqID (P1d) echoes ChecksumReq.ReqID (concurrent-op correlation).
	ReqID    uint64 `json:"req_id,omitempty"`
	Checksum string `json:"checksum,omitempty"`
	Error    string `json:"error,omitempty"`
}

// SendLine writes one control message (JSON + newline) to w.
func SendLine(w io.Writer, msg any) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("agent control: marshal %T: %w", msg, err)
	}
	b = append(b, '\n')
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("agent control: write: %w", err)
	}
	return nil
}

// recvLine reads one newline-terminated control message from r and unmarshals
// it into out. It returns io.EOF at a clean end of the control stream.
func recvLine(r *bufio.Reader, out any) error {
	b, err := r.ReadBytes('\n')
	if len(b) > 0 {
		// Tolerate a trailing newline (json.Unmarshal ignores it).
		if uerr := json.Unmarshal(b, out); uerr != nil {
			return fmt.Errorf("agent control: unmarshal %q: %w", b, uerr)
		}
		return nil
	}
	if err != nil {
		return err // io.EOF or a transport error
	}
	return io.ErrUnexpectedEOF
}

// ControlTLS carries the controller's control-channel TLS client settings
// (design 04.10: the control channel crosses the WAN in the two-agent
// layout, so it must be encrypted like every other channel). CA is required
// to enable verification; cert/key (when set) present the controller's
// client certificate to an mTLS agent (agent --control-tls-ca set).
type ControlTLS struct {
	Cert, Key, CA string
}

// ControlClient is the controller's side of the agent control protocol (P3):
// it connects to the long-lived source agent's control socket and drives the
// run — one Stream per unit (the source dials the destination agent and
// streams) and a final Release (commit the snapshot). The controller never
// touches the data path; it only sends/receives these control messages.
//
// The source agent's control loop is strictly sequential (one op at a time,
// blocking), so the client serialises every op under mu: the run's worker
// pool may issue Stream/Checksum calls concurrently (FR-03) and each must
// wait for the previous op to complete before writing the next request.
type ControlClient struct {
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
	// writeMu (P1d) serializes request LINES on the single control
	// connection: two workers may have ops in flight, but a request line is
	// never interleaved with another (newline-delimited JSON).
	writeMu sync.Mutex
	// pending (P1d concurrent control): in-flight op correlation. Each
	// request carries a unique ReqID; the single reader goroutine routes the
	// matching response line back to the waiting worker (a response never
	// blocks on the next request). Keyed by ReqID.
	pendingMu sync.Mutex
	pending   map[uint64]chan []byte
	// dead is closed when the reader sees the connection end (the agent
	// exited): every waiting op fails fast instead of blocking forever.
	dead     chan struct{}
	deadOnce sync.Once
	nextID   atomic.Uint64
}

// errControlDead is returned to every in-flight op when the control
// connection drops (the reader goroutine closed dead).
var errControlDead = fmt.Errorf("control: connection closed")

// StartReader launches the single response-reader goroutine (P1d) for a
// source-agent control client. It is idempotent (a second call is a no-op).
// DialControl starts it automatically; the process runner's dialControlRetry
// (which dials with its own TLS settings) must call it after dialing. The
// DESTINATION control client does NOT use it (DestControlClient reads its
// own ack/unit-done lines sequentially).
func (c *ControlClient) StartReader() {
	if c.dead != nil {
		return // already started
	}
	c.dead = make(chan struct{})
	c.pending = make(map[uint64]chan []byte)
	go c.readLoop()
}

// readLoop reads response lines for the connection's lifetime and routes
// each to the op that issued the matching ReqID (concurrent control, P1d:
// the agent runs ops as goroutines, so responses arrive in COMPLETION order,
// not request order — correlation by ReqID is what makes that safe).
func (c *ControlClient) readLoop() {
	defer c.markDead()
	for {
		line, err := c.r.ReadBytes('\n')
		if len(line) > 0 {
			var env struct {
				ReqID uint64 `json:"req_id"`
			}
			if uerr := json.Unmarshal(line, &env); uerr == nil && env.ReqID != 0 {
				c.pendingMu.Lock()
				ch, ok := c.pending[env.ReqID]
				if ok {
					delete(c.pending, env.ReqID)
				}
				c.pendingMu.Unlock()
				if ok {
					select {
					case ch <- line:
					default: // already failed (dead) — drop
					}
					continue
				}
			}
			// A line we cannot correlate (should not happen: the agent
			// echoes ReqID on every response). Drop it rather than wedge.
		}
		if err != nil {
			return // io.EOF (agent exited) or a transport error
		}
	}
}

// markDead fails every waiting op once (connection loss).
func (c *ControlClient) markDead() {
	c.deadOnce.Do(func() {
		close(c.dead)
		c.pendingMu.Lock()
		for id := range c.pending {
			delete(c.pending, id)
		}
		c.pendingMu.Unlock()
	})
}

// call issues one request line and waits for the matching response line
// (correlated by ReqID). req must be a pointer to a request type carrying a
// ReqID field (set here); resp is unmarshaled from the response line.
func (c *ControlClient) call(req, resp any) error {
	id := c.nextID.Add(1)
	switch r := req.(type) {
	case *StreamReq:
		r.ReqID = id
	case *OnSegmentReq:
		r.ReqID = id
	case *ChecksumReq:
		r.ReqID = id
	case *ReleaseReq:
		r.ReqID = id
	}
	ch := make(chan []byte, 1)
	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		if c.pending[id] == ch {
			delete(c.pending, id)
		}
		c.pendingMu.Unlock()
	}()

	// Write the request line (serialized: one line at a time on the conn).
	c.writeMu.Lock()
	werr := SendLine(c.w, req)
	if werr == nil {
		werr = c.w.Flush()
	}
	c.writeMu.Unlock()
	if werr != nil {
		return werr
	}

	select {
	case line := <-ch:
		if uerr := json.Unmarshal(line, resp); uerr != nil {
			return fmt.Errorf("control: unmarshal response: %w", uerr)
		}
		return nil
	case <-c.dead:
		return errControlDead
	}
}

// DialControl connects the controller to a SOURCE agent's control socket.
// (plain TCP — the pre-redesign local layout). It starts the response-reader
// goroutine (P1d concurrent control): the source agent serves ops as
// goroutines, so responses arrive in completion order and are correlated
// to the waiting worker by ReqID. DO NOT use this for the DESTINATION
// control socket — that protocol (a leading hello, then sequential
// ack/unit-done) is read by DestControlClient itself; use
// DialDestControl for it.
func DialControl(ctx context.Context, addr string) (*ControlClient, error) {
	c, err := DialControlTLS(ctx, addr, ControlTLS{})
	if err != nil {
		return nil, err
	}
	c.StartReader()
	return c, nil
}

// DialControlTLS connects the controller to an agent control socket,
// optionally over TLS (the agent's --control-tls-* flags; the controller
// verifies the agent's certificate against CA — ServerName is the dial
// host, so the agent certificate must cover it).
func DialControlTLS(ctx context.Context, addr string, t ControlTLS) (*ControlClient, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("control: dial %s: %w", addr, err)
	}
	if t.CA != "" {
		host, _, herr := net.SplitHostPort(addr)
		if herr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("control: bad addr %q: %w", addr, herr)
		}
		ccfg, cerr := transfer.ClientTLSConfig(host, t.Cert, t.Key, t.CA, "")
		if cerr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("control: tls config %s: %w", addr, cerr)
		}
		hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
		defer hcancel()
		tc := tls.Client(conn, ccfg)
		if terr := tc.HandshakeContext(hctx); terr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("control: tls handshake %s: %w", addr, terr)
		}
		conn = tc
	}
	return &ControlClient{conn: conn, w: bufio.NewWriter(conn), r: bufio.NewReader(conn)}, nil
}

// Stream instructs the source agent to stream one unit to the given
// destination agent endpoint and waits for the source's report. P1d: the op
// runs concurrently with the other workers' ops (one control connection, N
// ops in flight, correlated by ReqID).
func (c *ControlClient) Stream(req StreamReq) (StreamResp, error) {
	var resp StreamResp
	if err := c.call(&req, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

// OnSegment instructs the source agent to run the ON SEGMENT dispatch for one
// unit (the P4 on_segment data path): it triggers the kernel to launch the
// segreceiver on every primary source segment, which stream each slice
// compressed to the destination agent. It blocks until the kernel's COPY
// returns (all segreceivers have streamed) and returns the kernel's row count.
func (c *ControlClient) OnSegment(req OnSegmentReq) (OnSegmentResp, error) {
	var resp OnSegmentResp
	if err := c.call(&req, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

// Checksum asks the source agent (which holds the FR-02 snapshot) for the
// content checksum of a table.
func (c *ControlClient) Checksum(schema, table string) (ChecksumResp, error) {
	var resp ChecksumResp
	req := ChecksumReq{Op: "checksum", Schema: schema, Table: table}
	if err := c.call(&req, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

// Release tells the source agent to end the run (commit the snapshot).
// P1d: the caller MUST have completed every in-flight op first (the manager
// sends it after the worker pool drains), so the agent can wait for its ops
// to settle before committing.
func (c *ControlClient) Release() (ReleaseResp, error) {
	var resp ReleaseResp
	req := ReleaseReq{Op: "release"}
	if err := c.call(&req, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

// Close closes the control connection.
func (c *ControlClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
