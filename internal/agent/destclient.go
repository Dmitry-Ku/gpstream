// destclient.go — the controller's side of the LONG-LIVED dest agent
// control protocol (design 04.10).
//
// The controller dials the dest agent's control address (the operator's
// config, or — in auto mode — the address the remote launch learned from
// the agent's hello) and reads the hello FIRST (the agent speaks first:
// its real bound data + control addresses). From then on it is the same
// sequential request/response discipline as the source ControlClient: one
// ServeUnit per unit (blocking until the unit's load is done), a final
// Release.
package agent

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
)

// DestControlClient is the controller's side of the long-lived dest agent.
type DestControlClient struct {
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
	mu   sync.Mutex
	// Hello is the agent's real bound addresses (auto :0 mode).
	Hello DestHello
}

// DialDestControl connects to a long-lived dest agent's control socket and
// reads its hello (the agent speaks first: its real bound addresses). In
// auto :0 mode the hello's Data field is the endpoint the segreceivers /
// source agent must dial.
func DialDestControl(ctx context.Context, addr string) (*DestControlClient, error) {
	return DialDestControlTLS(ctx, addr, ControlTLS{})
}

// DialDestControlTLS is DialDestControl with control-channel TLS (the
// controller verifies the agent certificate against CA; ServerName = the
// dial host).
func DialDestControlTLS(ctx context.Context, addr string, t ControlTLS) (*DestControlClient, error) {
	base, err := DialControlTLS(ctx, addr, t)
	if err != nil {
		return nil, err
	}
	c := &DestControlClient{conn: base.conn, w: base.w, r: base.r}
	var hello DestHello
	if err := recvLine(c.r, &hello); err != nil {
		_ = c.conn.Close()
		return nil, fmt.Errorf("dest control: read hello from %s: %w", addr, err)
	}
	if hello.Op != "hello" {
		_ = c.conn.Close()
		return nil, fmt.Errorf("dest control: %s: first message is %q, want hello", addr, hello.Op)
	}
	c.Hello = hello
	return c, nil
}

// BeginServe instructs the agent to serve one unit on its shared data
// listener. It returns once the agent ACKs (the serve is running in the
// background, accepting on the data listener) — the controller now
// instructs the SOURCE agent to stream the unit, then calls WaitUnitDone.
func (c *DestControlClient) BeginServe(req DestServeReq) (DestAck, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	req.Op = "serve-unit"
	var ack DestAck
	if err := SendLine(c.w, req); err != nil {
		return ack, err
	}
	if err := c.w.Flush(); err != nil {
		return ack, fmt.Errorf("dest control: flush: %w", err)
	}
	// The ack is immediate (the serve is running in the background).
	if err := recvLine(c.r, &ack); err != nil {
		return ack, fmt.Errorf("dest control: read ack: %w", err)
	}
	return ack, nil
}

// WaitUnitDone blocks until the in-flight serve reports (the unit's load
// finished or failed). It pairs with BeginServe.
func (c *DestControlClient) WaitUnitDone() (DestUnitDone, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var resp DestUnitDone
	if err := recvLine(c.r, &resp); err != nil {
		return resp, fmt.Errorf("dest control: read unit-done: %w", err)
	}
	return resp, nil
}

// Release tells the agent to end the run (close listeners, exit).
func (c *DestControlClient) Release() (DestReleased, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var resp DestReleased
	if err := SendLine(c.w, DestReleased{Op: "release"}); err != nil {
		return resp, err
	}
	if err := c.w.Flush(); err != nil {
		return resp, fmt.Errorf("dest control: flush: %w", err)
	}
	_ = recvLine(c.r, &resp) // best effort: the agent may close first
	return resp, nil
}

// Close drops the control connection (the agent's control loop exits and
// the agent becomes an orphan waiting for its supervisor to kill it).
func (c *DestControlClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}
