package manifest

// BUG #13 regression: StreamMini must CancelUnit (dest) when the source
// ON SEGMENT dispatch fails, then WaitUnitDone returns promptly with the
// cancel's failure. Without the CancelUnit the dest supervisor waits for
// data that never arrives and never emits unit-done, so WaitUnitDone blocks
// forever HOLDING streamMu — the second manager worker then blocks on the
// mutex and the whole run deadlocks (INC8, 1221 tables, workers=2: the
// source COPY failed with "forbidden to COPY to or from an external program
// in Yandex Cloud" on a YC Greenplum build, the controller hung for minutes
// with both workers parked: one in WaitUnitDone, one on streamMu.Lock()).
//
// This test drives the REAL DestControlClient/ControlClient over loopback
// against two fake agents:
//   - the source agent answers onsegment with Ok=false (the COPY failure),
//   - the dest agent answers serve-unit with an ack and emits unit-done for
//     the serve ONLY when it receives cancel-unit (a real supervisor does
//     not complete a serve whose data never arrives — that is the premise
//     of the bug).
//
// With the fix: StreamMini returns within the test deadline with the
// onsegment error and the dest fake observed the cancel-unit.
// Without the fix: no cancel-unit reaches the dest fake, no unit-done is
// ever emitted, StreamMini parks in WaitUnitDone, and the deadline fires
// (RED).

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
)

func TestStreamMiniCancelsDestOnSourceOnSegmentFailure(t *testing.T) {
	const unitID = uint32(42)

	// --- fake source agent: onsegment -> Ok=false (the COPY failure) ---
	srcL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen source fake: %v", err)
	}
	go func() {
		conn, aerr := srcL.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		for {
			line, rerr := r.ReadBytes('\n')
			if rerr != nil {
				return
			}
			var req agent.OnSegmentReq
			if uerr := json.Unmarshal(line, &req); uerr != nil {
				continue
			}
			if req.Op != "onsegment" {
				continue
			}
			resp := agent.OnSegmentResp{
				Ok:     false,
				UnitID: req.UnitID,
				ReqID:  req.ReqID,
				Error:  "COPY ON SEGMENT: forbidden to COPY to or from an external program in Yandex Cloud",
			}
			b, _ := json.Marshal(resp)
			w.Write(append(b, '\n'))
			w.Flush()
		}
	}()

	// --- fake dest agent: hello, then serve-unit -> ack; cancel-unit ->
	// ack + unit-done (the serve fails only on the cancel, mirroring a real
	// supervisor whose minis never receive the unit's data) ---
	dstL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen dest fake: %v", err)
	}
	var cancels int32
	go func() {
		conn, aerr := dstL.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		// The dest agent speaks first (the hello).
		hb, _ := json.Marshal(agent.DestHello{Op: "hello"})
		w.Write(append(hb, '\n'))
		w.Flush()
		serveReqID := map[uint32]uint64{}
		for {
			line, rerr := r.ReadBytes('\n')
			if rerr != nil {
				return
			}
			var env struct {
				Op     string `json:"op"`
				ReqID  uint64 `json:"req_id"`
				UnitID uint32 `json:"unit_id"`
			}
			if uerr := json.Unmarshal(line, &env); uerr != nil {
				continue
			}
			switch env.Op {
			case "serve-unit":
				serveReqID[env.UnitID] = env.ReqID
				ab, _ := json.Marshal(agent.DestAck{Op: "ack", Ok: true, ReqID: env.ReqID})
				w.Write(append(ab, '\n'))
				w.Flush()
			case "cancel-unit":
				atomic.AddInt32(&cancels, 1)
				ab, _ := json.Marshal(agent.DestAck{Op: "ack", Ok: true, ReqID: env.ReqID})
				w.Write(append(ab, '\n'))
				w.Flush()
				if sid, ok := serveReqID[env.UnitID]; ok {
					db, _ := json.Marshal(agent.DestUnitDone{
						Op:     "unit-done",
						Ok:     false,
						ReqID:  sid,
						UnitID: env.UnitID,
						Error:  "unit cancelled by the controller (source stream failed)",
					})
					w.Write(append(db, '\n'))
					w.Flush()
				}
			}
		}
	}()
	defer srcL.Close()
	defer dstL.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srcCtrl, err := agent.DialControl(ctx, srcL.Addr().String())
	if err != nil {
		t.Fatalf("dial source fake: %v", err)
	}
	dstCtrl, err := agent.DialDestControl(ctx, dstL.Addr().String())
	if err != nil {
		t.Fatalf("dial dest fake: %v", err)
	}
	defer srcCtrl.Close()
	defer dstCtrl.Close()

	r := &processTwoAgentRunner{ctrl: srcCtrl, destCtrl: dstCtrl, miniAddrs: "127.0.0.1:1"}
	req := unitMiniRequest{
		OnSegmentReq: agent.OnSegmentReq{Op: "onsegment", UnitID: unitID, DestAddr: "ignored"},
		D:            3,
	}

	// StreamMini must return promptly (the cancel unblocks WaitUnitDone).
	// Without the fix it parks in WaitUnitDone forever (holding streamMu) —
	// the deadline below is the RED signal.
	done := make(chan error, 1)
	go func() {
		_, _, _, _, _, _, _, _, err := r.StreamMini(ctx, req)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("StreamMini: expected an error (the source onsegment failed), got nil")
		}
		if !strings.Contains(err.Error(), "onsegment") {
			t.Fatalf("StreamMini: expected the onsegment failure to surface, got: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("BUG #13: StreamMini hung in WaitUnitDone after the source onsegment failed " +
			"(no CancelUnit sent to the dest) — the dest never emits unit-done and streamMu is held forever")
	}

	if got := atomic.LoadInt32(&cancels); got == 0 {
		t.Fatal("BUG #13: the dest agent never received cancel-unit after the source onsegment failure")
	}
}
