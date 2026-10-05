// reliablepipe_net.go — the TCP transport for ReliablePipe (Inc3).
//
// NewReliablePipePair wires the two ends over an in-memory bidirectional pipe
// (a shared *rpLink coordinates the re-accept). This file is the network
// variant: the two ends talk over a REAL TCP connection (a net.Conn satisfies
// the same dupConn contract), and the RST-resume re-accept is driven by the
// receiver's listener — the exact shape of the production data path, where the
// segreceiver / source-agent (sender) dials the destination agent's data
// listener and, on a break, re-dials the SAME address while the destination
// agent re-accepts.
//
// Coordination over TCP (replacing the in-memory link's shared state):
//   - the receiver owns a re-accept loop on the listener: each Accept() after
//     the first is a resume; it swaps the fresh conn into the receiver and
//     advances swapGen so the receiver's receiveLoop (which is blocked on the
//     broken conn) wakes and continues on the new transport. The swap is
//     ordered swapConn THEN swapGen++/broadcast, so a woken receiveLoop always
//     sees the fresh conn (the link mutex is the happens-before edge).
//   - the sender's reconnect closes its old conn (the in-memory design's
//     sever), which delivers a FIN/RST that unblocks the receiver's read on
//     the broken transport even in the silent-break case.
//   - a clean end-of-stream is the ctrlFin control frame (in-band), NOT a TCP
//     EOF, so "the sender is done" is unambiguous. A hard sender death (no
//     ctrlFin, no re-dial) leaves the receiver bounded by the unit's stream
//     timeout (RESILIENCE: budget exhausted -> unit failed -> resume).

package transfer

import (
	"errors"
	"net"
	"sync"
	"time"
)

// NewReliablePipeTCPPair builds a frame-ACK'd ReliablePipe pair over a real
// TCP connection: the sender end S (which dials addr and re-dials on a break)
// and the receiver end R (which re-accepts on l for RST-resume). It performs
// the initial dial+accept up front, so both ends are connected on return. l
// must already be listening; it is NOT closed by this function (the receiver
// owns its lifetime via Close / the re-accept loop).
//
// This is the in-process form (both ends, and thus the coordinating *rpLink,
// live in one process) used to validate the protocol over real sockets. The
// production cross-process form (sender and receiver in different processes)
// reuses the same per-end logic; the receiver's re-accept loop is the
// destination agent's data-listener accept loop.
func NewReliablePipeTCPPair(cfg ReliablePipeConfig, l net.Listener) (s, r *ReliablePipe, err error) {
	if cfg.MaxReconnects <= 0 {
		cfg.MaxReconnects = 20
	}
	if cfg.RetransmitBufferMax <= 0 {
		cfg.RetransmitBufferMax = 1 << 30
	}
	if cfg.AckByteInterval <= 0 {
		cfg.AckByteInterval = 1 << 20
	}
	if cfg.AckTimeInterval <= 0 {
		cfg.AckTimeInterval = 100 * time.Millisecond
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 30 * time.Second
	}

	addr := l.Addr().String()
	dialer := func() (dupConn, error) { return net.Dial("tcp", addr) }

	link := &rpLink{}
	link.swapCv = sync.NewCond(&link.mu)
	link.doneOnce.Do(func() { link.done = make(chan struct{}) })

	// Initial connection: accept on the listener, dial from the sender side.
	acceptCh := make(chan dupConn, 1)
	go func() {
		c, aerr := l.Accept()
		if aerr != nil {
			acceptCh <- nil
			return
		}
		acceptCh <- c
	}()
	sc, derr := dialer()
	if derr != nil {
		return nil, nil, &tcpPairError{op: "dial", err: derr}
	}
	rc := <-acceptCh
	if rc == nil {
		_ = sc.Close()
		return nil, nil, &tcpPairError{op: "accept", err: errors.New("listener accept failed")}
	}

	s = newReliablePipe(rpSender, cfg, link, sc)
	r = newReliablePipe(rpReceiver, cfg, link, rc)
	link.s, link.r = s, r
	s.dialer = dialer

	// Re-accept loop: each Accept() after the first is a resume (the sender
	// re-dialed after a break). Swap the fresh conn in, THEN signal.
	go func() {
		for {
			select {
			case <-link.done:
				return
			default:
			}
			nc, aerr := l.Accept()
			if aerr != nil {
				// Listener closed (receiver app-level Close) -> sender gone.
				r.linkGone()
				return
			}
			r.swapConn(nc) // fresh conn first
			link.mu.Lock()
			link.swapGen++
			link.swapCv.Broadcast()
			link.mu.Unlock()
		}
	}()

	s.wg.Add(2)
	r.wg.Add(2)
	go s.receiveLoop()
	go s.watchdogLoop()
	go r.receiveLoop()
	go r.ackLoop()
	return s, r, nil
}

// tcpPairError wraps a dial/accept failure during pair setup.
type tcpPairError struct {
	op  string
	err error
}

func (e *tcpPairError) Error() string { return "reliablepipe tcp " + e.op + ": " + e.err.Error() }
