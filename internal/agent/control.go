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
	DestAddr  string `json:"dest"`     // the destination agent's listen address (the segreceivers dial it)
	SegBinary string `json:"segbin"`   // gpstream-segreceiver path on the source segment hosts
	Compress  string `json:"compress"` // per-slice compression mode (the dest agent decompresses)
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
	// Inc3: frame-level ACK + RST-resume on the segment<->agent data links
	// (DESIGN §4.6). The source agent embeds the matching --frame-ack /
	// --ack-timeout / --max-reconnects / --retransmit-buffer-max flags in the
	// segreceiver program string; the destination agent is launched with the
	// same parameters by the controller.
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
	Ok    bool   `json:"ok"`
	Rows  int64  `json:"rows,omitempty"`
	Error string `json:"error,omitempty"`
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
}

// StreamResp is the source agent's report after streaming a unit (the
// FR-04 volume + row count). Ok=false carries Error.
type StreamResp struct {
	Ok      bool   `json:"ok"`
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
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// ChecksumReq asks the source agent (which holds the FR-02 snapshot) for the
// content checksum of a table — the controller's --verify path compares it
// against the destination table's checksum. The source agent re-derives it on
// its pinned snapshot connection (the same snapshot the data COPY used).
type ChecksumReq struct {
	Op     string `json:"op"` // "checksum"
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// ChecksumResp is the source agent's checksum report.
type ChecksumResp struct {
	Ok       bool   `json:"ok"`
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
	mu   sync.Mutex
}

// DialControl connects the controller to a source agent's control socket
// (plain TCP — the pre-redesign local layout).
func DialControl(ctx context.Context, addr string) (*ControlClient, error) {
	return DialControlTLS(ctx, addr, ControlTLS{})
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
// destination agent endpoint and waits for the source's report.
func (c *ControlClient) Stream(req StreamReq) (StreamResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var resp StreamResp
	if err := SendLine(c.w, req); err != nil {
		return resp, err
	}
	if err := c.w.Flush(); err != nil {
		return resp, fmt.Errorf("control: flush: %w", err)
	}
	if err := recvLine(c.r, &resp); err != nil {
		return resp, fmt.Errorf("control: read stream response: %w", err)
	}
	return resp, nil
}

// OnSegment instructs the source agent to run the ON SEGMENT dispatch for one
// unit (the P4 on_segment data path): it triggers the kernel to launch the
// segreceiver on every primary source segment, which stream each slice
// compressed to the destination agent. It blocks until the kernel's COPY
// returns (all segreceivers have streamed) and returns the kernel's row count.
func (c *ControlClient) OnSegment(req OnSegmentReq) (OnSegmentResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var resp OnSegmentResp
	if err := SendLine(c.w, req); err != nil {
		return resp, err
	}
	if err := c.w.Flush(); err != nil {
		return resp, fmt.Errorf("control: flush: %w", err)
	}
	if err := recvLine(c.r, &resp); err != nil {
		return resp, fmt.Errorf("control: read onsegment response: %w", err)
	}
	return resp, nil
}

// Checksum asks the source agent (which holds the FR-02 snapshot) for the
// content checksum of a table.
func (c *ControlClient) Checksum(schema, table string) (ChecksumResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var resp ChecksumResp
	if err := SendLine(c.w, ChecksumReq{Op: "checksum", Schema: schema, Table: table}); err != nil {
		return resp, err
	}
	if err := c.w.Flush(); err != nil {
		return resp, fmt.Errorf("control: flush: %w", err)
	}
	if err := recvLine(c.r, &resp); err != nil {
		return resp, fmt.Errorf("control: read checksum response: %w", err)
	}
	return resp, nil
}

// Release tells the source agent to end the run (commit the snapshot).
func (c *ControlClient) Release() (ReleaseResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var resp ReleaseResp
	if err := SendLine(c.w, ReleaseReq{Op: "release"}); err != nil {
		return resp, err
	}
	if err := c.w.Flush(); err != nil {
		return resp, fmt.Errorf("control: flush: %w", err)
	}
	if err := recvLine(c.r, &resp); err != nil {
		return resp, fmt.Errorf("control: read release response: %w", err)
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
