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
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// TestTwoAgentRoutedFrameAckServe is the Inc3 acceptance for the routed (S≠D)
// on_segment data path: the destination agent serves one routed unit over the
// frame-ack transport (each of the S×D×W sub-streams = its own ReliablePipe,
// routed by its identity HELLO src/dst/worker to its own receiver-pipe), and
// the S fake segreceivers run REAL RoutedSenders whose (d,w) sub-streams ride
// ReliablePipe senders. The destination merges the per-segment sub-streams and
// loads D utility-mode COPYs — neither cluster coordinator in the data path.
func TestTwoAgentRoutedFrameAckServe(t *testing.T) {
	const (
		srcPort  = 5636
		dstPort  = 5637
		rows     = 20000
		jobID    = 0x9abd
		unitID   = 11
		finalTab = "routed_orders_fa"
		staging  = "_gs_testjob_fa11"
		S        = 2
		W        = 2
		D        = 1
	)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	defer srcCl.Stop()
	dstCl, err := pgtest.Start(dstPort, "dstdb")
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

	mustExec(t, srcConn, `DROP TABLE IF EXISTS routed_orders_fa`)
	mustExec(t, srcConn, `CREATE TABLE routed_orders_fa (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO routed_orders_fa SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "routed_orders_fa", rows)

	sa := source.New(srcConn)
	cols, err := sa.Columns(ctx, "public", finalTab)
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	dist := sa.Distribution(ctx, "public", finalTab)
	part, _ := pg.CapturePartitionSpec(srcConn, ctx, "public", finalTab)

	slices := splitBinarySlices(t, full, S)

	// Destination agent: serve the routed unit over the frame-ack transport.
	listen := freePort(t)
	dresCh := make(chan agent.RoutedResult, 1)
	derrCh := make(chan error, 1)
	dsn := dstCl.URL()
	go func() {
		res, err := agent.ServeRouted(ctx, agent.RoutedOptions{
			Listen:   listen,
			DestConn: dsn,
			Launch: agent.RoutedLaunch{
				DirectLaunch: agent.DirectLaunch{
					UnitID:    uint32(unitID),
					Schema:    "public",
					Table:     finalTab,
					Staging:   staging,
					Overwrite: true,
					Compress:  "zstd",
					Cols:      cols,
					Dist:      dist,
					Part:      part,
					D:         int32(D),
					DstDSNs:   []string{dsn},
				},
				SourceSegments: int32(S),
				Workers:        W,
			},
			Swap:          true,
			AcceptTimeout: 30 * time.Second,
			StreamTimeout: 90 * time.Second,
			FrameAck:      true,
			ReliablePipe:  transfer.DefaultReliablePipeConfig(),
		})
		if err != nil {
			derrCh <- err
			return
		}
		dresCh <- res
	}()

	// S fake segreceivers: each runs a REAL RoutedSender over its slice; its
	// (d,w) sub-streams ride ReliablePipe senders (identity HELLO:
	// src=s, dst=d, worker=w).
	time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
	cfg := transfer.DefaultReliablePipeConfig()
	cfg.ResumeTimeout = 10 * time.Second
	dial := func() (transfer.DupConn, error) {
		return net.DialTimeout("tcp", listen, 5*time.Second)
	}
	errCh := make(chan error, S)
	for s := 0; s < S; s++ {
		go func(s int) {
			conns := make([][]transfer.Pipe, D)
			for d := 0; d < D; d++ {
				conns[d] = make([]transfer.Pipe, W)
				for w := 0; w < W; w++ {
					c, derr := net.Dial("tcp", listen)
					if derr != nil {
						errCh <- fmt.Errorf("fake segreceiver %d dial (d%d w%d): %w", s, d, w, derr)
						return
					}
					id := transfer.StreamID{JobID: jobID, UnitID: unitID, Content: int16(s), DstContent: int16(d), Worker: int32(w)}
					rp, perr := transfer.NewReliablePipeSenderOnConnID(cfg, c, dial, id)
					if perr != nil {
						errCh <- fmt.Errorf("fake segreceiver %d pipe (d%d w%d): %w", s, d, w, perr)
						return
					}
					conns[d][w] = rp
				}
			}
			sender := segstream.NewRoutedSender(ctx, bytes.NewReader(slices[s]), []int{0}, []uint32{23}, D, W,
				uint32(jobID), uint32(unitID), int16(s), compress.ModeZstd, 3, conns)
			if _, err := sender.Run(); err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d run: %w", s, err)
				return
			}
			errCh <- nil
		}(s)
	}
	for i := 0; i < S; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("fake segreceiver: %v", e)
		}
	}
	logical := uint64(len(full))

	var dres agent.RoutedResult
	select {
	case serr := <-derrCh:
		t.Fatalf("dest agent serve routed (frame-ack): %v", serr)
	case dres = <-dresCh:
		t.Logf("serve routed (frame-ack): rows=%d trailer=%d wire=%d logical=%d swapped=%v perseg=%v",
			dres.Rows, dres.TrailerRows, dres.Wire, dres.Logical, dres.Swapped, dres.PerSeg)
		if dres.Rows != rows || dres.TrailerRows != rows {
			t.Fatalf("row count mismatch: dst=%d trailer=%d want=%d", dres.Rows, dres.TrailerRows, rows)
		}
		if !dres.Swapped {
			t.Fatalf("dest agent did not swap")
		}
		if dres.Wire == 0 {
			t.Fatalf("on-wire volume is 0 (router wire counter not wired)")
		}
		if dres.Wire >= logical {
			t.Fatalf("on-wire %d >= logical %d: not compressed on the network", dres.Wire, logical)
		}
	}

	if got := mustCount(t, dstConn, `SELECT count(*) FROM public.routed_orders_fa`); got != rows {
		t.Fatalf("dest routed_orders_fa count = %d, want %d", got, rows)
	}
	wantDigest := mustText(t, srcConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM routed_orders_fa r`)
	if got := mustText(t, dstConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM public.routed_orders_fa r`); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q", got, wantDigest)
	}
	t.Logf("Inc3 routed frame-ack ok: rows=%d wire=%d logical=%d ratio=%.1fx",
		rows, dres.Wire, logical, float64(logical)/float64(dres.Wire))
}
