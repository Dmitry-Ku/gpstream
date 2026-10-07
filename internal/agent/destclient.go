// destclient.go — the controller's side of the LONG-LIVED dest agent
// control protocol (design 04.10).
//
// The controller dials the dest agent's control address (the operator's
// config, or — in auto mode — the address the remote launch learned from
// the agent's hello) and reads the hello FIRST (the agent speaks first:
// its real bound data + control addresses).
//
// P1d: CONCURRENT. The run's N data workers issue N serves in flight on
// ONE control connection (BeginServe is acked immediately; each serve
// reports its unit-done when the destination finishes the COPY). The
// responses arrive in completion order, not request order, so a single
// reader goroutine routes each line to the waiting worker by its ReqID
// (the controller's per-request correlation id, echoed on the ack and the
// unit-done). This mirrors the source agent's ControlClient.
package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// DestControlClient is the controller's side of the long-lived dest agent.
type DestControlClient struct {
	conn    net.Conn
	w       *bufio.Writer
	r       *bufio.Reader
	writeMu sync.Mutex // serializes request lines (one at a time on the conn)
	// Hello is the agent's real bound addresses (auto :0 mode).
	Hello DestHello

	// concurrent control (P1d): a single reader goroutine routes responses
	// to the waiting worker by ReqID. nextID is atomic (concurrent
	// BeginServe/CancelUnit/Release) and starts at 1 via Add(1): ReqID 0 is
	// indistinguishable from "absent" once the JSON tag is omitempty, and
	// the reader drops uncorrelatable (req_id==0) lines — the first op
	// would deadlock on its own ack.
	nextID    atomic.Uint64
	pendMu    sync.Mutex
	ackCh     map[uint64]chan DestAck
	doneCh    map[uint64]chan DestUnitDone
	releaseCh map[uint64]chan DestReleased
	// lateDone: a unit-done can arrive BEFORE its waiter registers (a fast
	// serveOne — e.g. an unknown kind — finishes before the caller gets to
	// WaitUnitDone). The reader buffers the raw line; WaitUnitDone picks it
	// up. (acks/releases cannot be early: synchronous replies, waiter
	// registered before the write.)
	lateMu   sync.Mutex
	lateDone map[uint64][]byte
	dead     chan struct{}
	deadOnce sync.Once
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
	c := &DestControlClient{
		conn:      base.conn,
		w:         base.w,
		r:         base.r,
		ackCh:     make(map[uint64]chan DestAck),
		doneCh:    make(map[uint64]chan DestUnitDone),
		releaseCh: make(map[uint64]chan DestReleased),
		lateDone:  make(map[uint64][]byte),
		dead:      make(chan struct{}),
	}
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
	go c.readLoop()
	return c, nil
}

// markDead closes dead (once) and fails every pending waiter, so a worker
// blocked in BeginServe/WaitUnitDone/Release unblocks when the connection
// dies (agent exit / run end) instead of hanging.
func (c *DestControlClient) markDead() {
	c.deadOnce.Do(func() { close(c.dead) })
	c.pendMu.Lock()
	defer c.pendMu.Unlock()
	for id, ch := range c.ackCh {
		select {
		case ch <- DestAck{Op: "ack", Ok: false, Error: "dest control connection closed"}:
		default:
		}
		delete(c.ackCh, id)
	}
	for id, ch := range c.doneCh {
		select {
		case ch <- DestUnitDone{Op: "unit-done", Ok: false, Error: "dest control connection closed"}:
		default:
		}
		delete(c.doneCh, id)
	}
	for id, ch := range c.releaseCh {
		select {
		case ch <- DestReleased{Op: "released", Ok: false}:
		default:
		}
		delete(c.releaseCh, id)
	}
}

// readLoop is the single response-reader goroutine (P1d). It routes each
// line to the waiting worker by its ReqID (acks, unit-dones, and the
// release reply). Lines it cannot correlate are dropped (should not happen:
// the agent echoes ReqID on every response).
func (c *DestControlClient) readLoop() {
	defer c.markDead()
	for {
		line, err := c.r.ReadBytes('\n')
		if len(line) > 0 {
			var env struct {
				Op    string `json:"op"`
				ReqID uint64 `json:"req_id"`
			}
			if uerr := json.Unmarshal(line, &env); uerr == nil && env.ReqID != 0 {
				c.pendMu.Lock()
				switch env.Op {
				case "ack":
					if ch, ok := c.ackCh[env.ReqID]; ok {
						var a DestAck
						_ = json.Unmarshal(line, &a)
						delete(c.ackCh, env.ReqID)
						c.pendMu.Unlock()
						ch <- a
						continue
					}
				case "unit-done":
					if ch, ok := c.doneCh[env.ReqID]; ok {
						var d DestUnitDone
						_ = json.Unmarshal(line, &d)
						delete(c.doneCh, env.ReqID)
						c.pendMu.Unlock()
						ch <- d
						continue
					}
					// No waiter registered yet: the serve finished before the
					// caller reached WaitUnitDone. Buffer the raw line there.
					c.lateMu.Lock()
					if len(c.lateDone) < 64 { // bound: a leaky agent must not grow us
						c.lateDone[env.ReqID] = append([]byte(nil), line...)
					}
					c.lateMu.Unlock()
				case "released":
					if ch, ok := c.releaseCh[env.ReqID]; ok {
						var rl DestReleased
						_ = json.Unmarshal(line, &rl)
						delete(c.releaseCh, env.ReqID)
						c.pendMu.Unlock()
						ch <- rl
						continue
					}
				}
				c.pendMu.Unlock()
			}
		}
		if err != nil {
			return // io.EOF (agent exited) or a transport error
		}
	}
}

// BeginServe instructs the agent to serve one unit. It returns once the
// agent ACKs (the serve is running in the background). The ReqID is the
// correlation id for the matching WaitUnitDone (N serves in flight on one
// connection). The controller now instructs the SOURCE agent to stream the
// unit, then calls WaitUnitDone(req.ReqID).
func (c *DestControlClient) BeginServe(req DestServeReq) (DestAck, error) {
	id := c.nextID.Add(1)
	req.Op = "serve-unit"
	req.ReqID = id
	ch := make(chan DestAck, 1)
	c.pendMu.Lock()
	c.ackCh[id] = ch
	c.pendMu.Unlock()
	defer func() {
		c.pendMu.Lock()
		if c.ackCh[id] == ch {
			delete(c.ackCh, id)
		}
		c.pendMu.Unlock()
	}()
	c.writeMu.Lock()
	werr := SendLine(c.w, req)
	if werr == nil {
		werr = c.w.Flush()
	}
	c.writeMu.Unlock()
	if werr != nil {
		return DestAck{}, werr
	}
	select {
	case a := <-ch:
		return a, nil
	case <-c.dead:
		return DestAck{}, errControlDead
	}
}

// CancelUnit tells the agent to fail the in-flight serve for unitID (the
// controller's source stream failed, so the unit's data will not arrive):
// the matching WaitUnitDone(ack.ReqID) then returns promptly with a failure
// instead of waiting for the run ctx. A no-op when the unit is not in
// flight (or already completed — its report wins).
func (c *DestControlClient) CancelUnit(unitID uint32) (DestAck, error) {
	id := c.nextID.Add(1)
	ch := make(chan DestAck, 1)
	c.pendMu.Lock()
	c.ackCh[id] = ch
	c.pendMu.Unlock()
	defer func() {
		c.pendMu.Lock()
		if c.ackCh[id] == ch {
			delete(c.ackCh, id)
		}
		c.pendMu.Unlock()
	}()
	c.writeMu.Lock()
	werr := SendLine(c.w, DestServeReq{Op: "cancel-unit", UnitID: unitID, ReqID: id})
	if werr == nil {
		werr = c.w.Flush()
	}
	c.writeMu.Unlock()
	if werr != nil {
		return DestAck{}, werr
	}
	select {
	case a := <-ch:
		return a, nil
	case <-c.dead:
		return DestAck{}, errControlDead
	}
}

// WaitUnitDone blocks until the serve with the given ReqID reports (the
// unit's load finished or failed). It pairs with the matching BeginServe.
func (c *DestControlClient) WaitUnitDone(reqID uint64) (DestUnitDone, error) {
	// An early unit-done (the serve finished before this call) is already
	// buffered by the reader: return it without waiting.
	c.lateMu.Lock()
	if raw, ok := c.lateDone[reqID]; ok {
		delete(c.lateDone, reqID)
		c.lateMu.Unlock()
		var d DestUnitDone
		if uerr := json.Unmarshal(raw, &d); uerr == nil {
			return d, nil
		}
	} else {
		c.lateMu.Unlock()
	}
	ch := make(chan DestUnitDone, 1)
	c.pendMu.Lock()
	c.doneCh[reqID] = ch
	c.pendMu.Unlock()
	defer func() {
		c.pendMu.Lock()
		if c.doneCh[reqID] == ch {
			delete(c.doneCh, reqID)
		}
		c.pendMu.Unlock()
	}()
	select {
	case d := <-ch:
		return d, nil
	case <-c.dead:
		return DestUnitDone{}, errControlDead
	}
}

// Release tells the agent to end the run (close listeners, exit). It blocks
// until the agent acknowledges (the agent waits for its in-flight serves
// before replying, so a running COPY finishes before the agent exits).
func (c *DestControlClient) Release() (DestReleased, error) {
	id := c.nextID.Add(1)
	ch := make(chan DestReleased, 1)
	c.pendMu.Lock()
	c.releaseCh[id] = ch
	c.pendMu.Unlock()
	defer func() {
		c.pendMu.Lock()
		if c.releaseCh[id] == ch {
			delete(c.releaseCh, id)
		}
		c.pendMu.Unlock()
	}()
	c.writeMu.Lock()
	werr := SendLine(c.w, DestReleased{Op: "release", ReqID: id})
	if werr == nil {
		werr = c.w.Flush()
	}
	c.writeMu.Unlock()
	if werr != nil {
		return DestReleased{}, werr
	}
	select {
	case rl := <-ch:
		return rl, nil
	case <-c.dead:
		// Best effort: the agent may have closed the control conn on exit.
		return DestReleased{Op: "released"}, nil
	}
}

// Close drops the control connection (the agent's control loop exits and
// the agent becomes an orphan waiting for its supervisor to kill it).
func (c *DestControlClient) Close() error {
	c.markDead()
	return c.conn.Close()
}
