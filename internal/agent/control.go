package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
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
	Error   string `json:"error,omitempty"`
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

// ControlClient is the controller's side of the agent control protocol (P3):
// it connects to the long-lived source agent's control socket and drives the
// run — one Stream per unit (the source dials the destination agent and
// streams) and a final Release (commit the snapshot). The controller never
// touches the data path; it only sends/receives these control messages.
type ControlClient struct {
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
}

// DialControl connects the controller to a source agent's control socket.
func DialControl(ctx context.Context, addr string) (*ControlClient, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("control: dial %s: %w", addr, err)
	}
	return &ControlClient{conn: conn, w: bufio.NewWriter(conn), r: bufio.NewReader(conn)}, nil
}

// Stream instructs the source agent to stream one unit to the given
// destination agent endpoint and waits for the source's report.
func (c *ControlClient) Stream(req StreamReq) (StreamResp, error) {
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
