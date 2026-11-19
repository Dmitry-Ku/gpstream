//go:build gp6lab

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/segstream"
)

// TestTwoAgentMeshRoutedFrameAckResume is the Inc3.3b regression: the
// production mini path (supervisor mode) routes the S×W sub-stream
// connections of a routed unit over ONE shared data listener through the
// ResumeRouter — by the identity HELLO (StreamID: job/unit/src/dst/worker) —
// so BOTH the initial connections and the RST-resume re-dials reach their
// own receiver pipe.
//
// Before the router, every sub-stream pipe owned its OWN re-accept loop on
// the shared listener: when 2+ sub-streams broke within one ResumeTimeout
// window, the OS could hand a re-dial to a SIBLING pipe's Accept(). The
// sibling saw a foreign HELLO and failed, taking the whole unit down into a
// re-copy (no corruption — the digest catches it — but a spurious unit
// failure under concurrent WAN breaks).
//
// Shape: S=2 source "segments" × W=2 workers into ONE destination mini
// (D=1): 4 frame-ack pipes on the mini's single data listener. The test
// severs TWO of the four transports (w=0 of each source) SIMULTANEOUSLY,
// mid-stream, and asserts:
//
//   - the unit COMPLETES (no spurious failure — deterministic with identity
//     routing; a flaky race without it),
//   - rows + both digests == the buffered reference of the whole table,
//   - the severed senders reconnected (Stats().Reconnects >= 1) AND actually
//     replayed un-ACKed frames (Retransmitted >= 1 — the ring replay ran,
//     not just a re-dial),
//   - the two untouched senders did NOT reconnect (a sibling's break must
//     not disturb the other sub-streams),
//   - the on-wire volume is the compressed one and NON-ZERO (the router
//     counts the raw network bytes, FR-04 — the old per-pipe receiver
//     reported 0 on the frame-ack path).
//
// Timing is made deterministic, not racy:
//   - rows=80000 -> each sub-stream is ~700KB logical > the 256KB coalesce
//     chunk, so each pipe writes SEVERAL frames (the source is paced by
//     slowReader: 16KB per 100 ms -> frames at ~3.2/6.4/9 s).
//   - AckTimeInterval=5s on BOTH ends (WAN-like): the first ACK tick lands
//     at ~5 s (after frame 0, written ~3.2 s), and the break at ~7 s finds
//     frame 1 (written ~6.4 s) un-ACKed in the ring — so the retransmit
//     ring is non-empty at the break by construction and the resume MUST
//     replay it (Retransmitted >= 1). (Breaking before the first ACK would
//     leave lastAck at its INITIAL 0, which the replay treats as
//     "frame 0 already ACKed" — the frame is still re-sent by the Write
//     loop, so the data is safe, but the replay counter would not move.)
//   - the break is released by a barrier AFTER all four senders report up,
//     so it lands while all four streams are flowing.
//
// Shape note: D=1 is the minimal routed shape (each destination mini
// receives S×W sub-streams regardless of D — the per-mini perspective is the
// production one); S=2×W=2 gives 4 concurrent pipes, enough for the
// multi-tenant routing to matter.
func TestTwoAgentMeshRoutedFrameAckResume(t *testing.T) {
	const (
		rows    = 80000
		srcPort = uint32(5811)
		dstPort = uint32(5812)
		jobID   = uint32(0x6104)
		unitID  = uint32(4)
		S       = 2 // two source "segments"
		D       = 1 // one destination "segment" (one mini)
		W       = 2 // sub-streams per (source-seg, dest-seg) pair
		staging = "_gs_cmr_4"
		table   = "cmr_orders"
	)
	// The two sub-streams severed simultaneously (w=0 of each source — the
	// concurrent-break condition); (s,1) stays untouched.
	broken := map[[2]int]bool{{0, 0}: true, {1, 0}: true}

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "cmr_src")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	dstCl, err := pgtest.Start(dstPort, "cmr_dst")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	defer dstCl.Stop()

	srcConn, err := pg.NewConn(ctx, srcCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcConn.Close()
	dstConn, err := pg.NewConn(ctx, dstCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect dest: %v", err)
	}
	defer dstConn.Close()

	mustExec(t, srcConn, `DROP TABLE IF EXISTS `+table)
	mustExec(t, srcConn, `CREATE TABLE `+table+` (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO `+table+` SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, table, rows)

	// S source slices (round-robin partition, the kernel's per-segment shape).
	slices := splitBinarySlices(t, full, S)

	mustExec(t, dstConn, "DROP TABLE IF EXISTS public."+staging)
	mustExec(t, dstConn, "CREATE TABLE public."+staging+" (id integer, customer text, amount numeric)")

	refAcc, refRows, err := transfer.DigestAccumulator(full)
	if err != nil {
		t.Fatalf("reference DigestAccumulator: %v", err)
	}
	if refRows != int64(rows) {
		t.Fatalf("reference rows %d != seeded %d", refRows, rows)
	}
	refDigest := transfer.CombinedDigestString(refAcc, int64(rows))

	// WAN-like ACK pacing on BOTH ends: frames stay un-ACKed for ~5 s, so
	// the retransmit ring is non-empty at the break (deterministic replay).
	rpCfg := transfer.NewReliablePipeConfig("30s", 20, 1<<30)
	rpCfg.AckTimeInterval = 5 * time.Second
	rpCfg.AckByteInterval = 1 << 30

	// Supervisor + the one real mini-agent, FRAME-ACK ON (the production
	// default; launchDestMinis forwards the supervisor's --frame-ack).
	sup, err := agent.NewSupervisor("127.0.0.1:0", nil, "", "", "", "")
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	defer sup.Close()
	reportAddr := sup.Addr()
	mmCh := make(chan agent.MiniMap, 1)
	mmErr := make(chan error, 1)
	go func() {
		mm, err := sup.AcceptMinis(ctx, D)
		if err != nil {
			mmErr <- err
			return
		}
		mmCh <- mm
	}()
	go func() {
		_, _ = agent.ServeMini(ctx, agent.MiniOptions{
			Seg:           0,
			DataListen:    "127.0.0.1:0",
			PrimaryDSN:    dstCl.URL(),
			ReportAddr:    reportAddr,
			AcceptTimeout: 30 * time.Second,
			StreamTimeout: 120 * time.Second,
			FrameAck:      true,
			ReliablePipe:  rpCfg,
		})
	}()
	var mm agent.MiniMap
	select {
	case e := <-mmErr:
		t.Fatalf("supervisor accept minis: %v", e)
	case mm = <-mmCh:
	case <-time.After(30 * time.Second):
		t.Fatalf("supervisor did not accept %d minis in time", D)
	}
	if len(mm) != D {
		t.Fatalf("MiniMap has %d entries, want %d (mm=%v)", len(mm), D, mm)
	}
	sup.SetMiniHost(0, "127.0.0.1")
	if sup.MiniDataAddr(0) == "" {
		t.Fatalf("mini 0 has no data address (MiniMap=%v)", mm)
	}
	addr := sup.MiniDataAddr(0)

	// Drive the ROUTED unit (S=2, W=2) on the supervisor (concurrently).
	supCh := make(chan agent.CombinedUnitResult, 1)
	supErr := make(chan error, 1)
	go func() {
		res, err := sup.ServeUnit(ctx, agent.UnitReportReq{
			ReqID:          1,
			UnitID:         unitID,
			JobID:          jobID,
			Schema:         "public",
			Staging:        staging,
			Compress:       "zstd",
			VerifyDigest:   true,
			SourceSegments: int32(S),
			Workers:        W,
		})
		if err != nil {
			supErr <- err
			return
		}
		supCh <- res
	}()

	// The S fake segreceivers (the REAL mesh form: each sub-stream dials the
	// mini's data listener and rides a frame-ack ReliablePipe with its own
	// StreamID identity HELLO — cmd/gpstream-segreceiver main.go routed path).
	sends := make([][]*transfer.ReliablePipe, S)
	ready := make(chan int, S)
	breakBarrier := make(chan struct{})
	errCh := make(chan error, S)
	for s := 0; s < S; s++ {
		go func(s int) {
			conns := make([][]transfer.Pipe, D)
			sends[s] = make([]*transfer.ReliablePipe, W)
			for d := 0; d < D; d++ {
				conns[d] = make([]transfer.Pipe, W)
				for w := 0; w < W; w++ {
					c, derr := dialRetry(addr, 30*time.Second)
					if derr != nil {
						errCh <- fmt.Errorf("fake segreceiver %d dial (d%d w%d): %w", s, d, w, derr)
						return
					}
					dialOnce := func() (transfer.DupConn, error) {
						return net.DialTimeout("tcp", addr, 10*time.Second)
					}
					rp, err := transfer.NewReliablePipeSenderOnConnID(rpCfg, c, dialOnce,
						transfer.StreamID{JobID: jobID, UnitID: unitID, Content: int16(s), DstContent: int16(d), Worker: int32(w)})
					if err != nil {
						errCh <- fmt.Errorf("frame-ack sender %d (d%d w%d): %w", s, d, w, err)
						return
					}
					sends[s][w] = rp
					conns[d][w] = rp
				}
			}
			sender := segstream.NewRoutedSender(ctx, &slowReader{src: bytes.NewReader(slices[s]), chunk: 16 * 1024, pause: 100 * time.Millisecond}, []int{0}, []uint32{23}, int32(D), W,
				uint32(jobID), uint32(unitID), int16(s), compress.ModeZstd, 3, true, conns)
			runDone := make(chan error, 1)
			go func() {
				_, e := sender.Run()
				runDone <- e
			}()
			ready <- s
			if broken[[2]int{s, 0}] {
				// Wait for the global barrier (all senders up + streams
				// flowing), then sever this source's w=0 transport
				// mid-stream — the concurrent-break condition.
				<-breakBarrier
				sends[s][0].CloseTransport()
			}
			if e := <-runDone; e != nil {
				errCh <- fmt.Errorf("fake segreceiver %d run: %w", s, e)
				return
			}
			errCh <- nil
		}(s)
	}

	// All senders up; let the streams flow past the first ACK tick (5 s) so
	// frame 1 is in the ring un-ACKed, then release the concurrent break
	// at ~7 s.
	for i := 0; i < S; i++ {
		select {
		case <-ready:
		case e := <-errCh:
			t.Fatalf("fake segreceiver: %v", e)
		case <-time.After(60 * time.Second):
			t.Fatalf("a fake segreceiver did not come up in time")
		}
	}
	time.Sleep(7000 * time.Millisecond)
	for si := 0; si < S; si++ {
		for wi := 0; wi < W; wi++ {
			st := sends[si][wi].Stats()
			t.Logf("at break: sender (%d,%d): lastSent=%d lastAck=%d ring=%d",
				si, wi, st.LastSent, st.LastAck, st.RingBytes)
		}
	}
	close(breakBarrier)
	for i := 0; i < S; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("fake segreceiver: %v", e)
		}
	}

	var res agent.CombinedUnitResult
	select {
	case e := <-supErr:
		t.Fatalf("supervisor ServeUnit (concurrent-break mesh): %v", e)
	case res = <-supCh:
	case <-time.After(120 * time.Second):
		t.Fatalf("supervisor did not finish the unit in time")
	}
	t.Logf("concurrent-break mesh: rows=%d srcDigest=%q dstDigest=%q wire=%d logical=%d",
		res.Rows, res.SrcDigest, res.DstDigest, res.Wire, res.Logical)
	for si := 0; si < S; si++ {
		for wi := 0; wi < W; wi++ {
			st := sends[si][wi].Stats()
			t.Logf("sender (%d,%d): reconnects=%d retransmitted=%d lastSent=%d lastAck=%d ring=%d",
				si, wi, st.Reconnects, st.Retransmitted, st.LastSent, st.LastAck, st.RingBytes)
		}
	}

	// The severed senders must have reconnected (the test proves the resume
	// path ran; without it the unit would have failed spuriously) AND must
	// have replayed un-ACKed frames (the ring replay, not just a re-dial).
	for _, pair := range [][2]int{{0, 0}, {1, 0}} {
		st := sends[pair[0]][pair[1]].Stats()
		if st.Reconnects < 1 {
			t.Fatalf("severed sender (%d,%d) reconnected %d times, want >= 1", pair[0], pair[1], st.Reconnects)
		}
		if st.Retransmitted < 1 {
			t.Fatalf("severed sender (%d,%d) replayed %d frames, want >= 1 (the ring must be non-empty at the break)", pair[0], pair[1], st.Retransmitted)
		}
	}
	// The untouched senders must not have reconnected (a sibling's break
	// must not disturb the other sub-streams).
	if got := sends[0][1].Stats().Reconnects; got != 0 {
		t.Fatalf("untouched sender (0,1) reconnected %d times, want 0", got)
	}
	if got := sends[1][1].Stats().Reconnects; got != 0 {
		t.Fatalf("untouched sender (1,1) reconnected %d times, want 0", got)
	}

	// The unit completed with the full table, exactly once, digests intact.
	if res.Rows != int64(rows) {
		t.Fatalf("combined rows %d != seeded %d (perSeg=%v)", res.Rows, rows, res.PerSeg)
	}
	if res.SrcDigest != refDigest {
		t.Fatalf("srcDigest %q != reference %q", res.SrcDigest, refDigest)
	}
	if res.DstDigest != refDigest {
		t.Fatalf("dstDigest %q != reference %q", res.DstDigest, refDigest)
	}
	// FR-04: the router counts the raw network bytes — non-zero now (the old
	// per-pipe receiver reported 0 on the frame-ack path) and compressed.
	if res.Wire == 0 {
		t.Fatalf("on-wire volume 0: the router must count the raw network bytes (FR-04)")
	}
	if res.Wire >= uint64(len(full)) {
		t.Fatalf("on-wire %d >= logical %d: not compressed on the network", res.Wire, len(full))
	}
	if got := mustCount(t, dstConn, "SELECT count(*) FROM public."+staging); got != int64(rows) {
		t.Fatalf("staging count = %d, want %d", got, rows)
	}
}

// slowReader releases at most `chunk` bytes per Read and pauses `pause`
// after each — it stretches the 80k-row source over ~9 s, so the concurrent
// break lands MID-STREAM by construction (a CloseTransport on an already
// ended pipe would be a vacuous no-reconnect and the test could pass green
// without exercising the resume path).
type slowReader struct {
	src   *bytes.Reader
	chunk int
	pause time.Duration
}

func (s *slowReader) Read(b []byte) (int, error) {
	n := len(b)
	if n > s.chunk {
		n = s.chunk
	}
	k, err := s.src.Read(b[:n])
	if k > 0 {
		time.Sleep(s.pause)
	}
	return k, err
}
