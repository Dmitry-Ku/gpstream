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

// TestTwoAgentOnSegmentFrameAckServe is the Inc3 acceptance for the on_segment
// data path: the destination agent serves one on_segment unit over the
// frame-ack transport (each slice = its own ReliablePipe, routed by identity
// HELLO to its own receiver-pipe), and the N fake segreceivers stream their
// slices COMPRESSED over real TCP through ReliablePipe senders (identity
// HELLO: job/unit/content). The destination merges the N slices and loads a
// real pgtest destination. This exercises the production on_segment frame-ack
// path end-to-end: dynamic pool binding (the dest does not know the source
// content ids), the identity HELLO routing, and the merged COPY.
func TestTwoAgentOnSegmentFrameAckServe(t *testing.T) {
	const (
		srcPort  = 5626
		dstPort  = 5627
		rows     = 20000
		jobID    = 0x5679
		unitID   = 9
		finalTab = "seg_orders_fa"
		staging  = "_gs_testjob_fa9"
		nSeg     = 2
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

	mustExec(t, srcConn, `DROP TABLE IF EXISTS seg_orders_fa`)
	mustExec(t, srcConn, `CREATE TABLE seg_orders_fa (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO seg_orders_fa SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryFrameAck(t, srcConn, rows)

	sa := source.New(srcConn)
	cols, err := sa.Columns(ctx, "public", finalTab)
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	dist := sa.Distribution(ctx, "public", finalTab)
	part, _ := pg.CapturePartitionSpec(srcConn, ctx, "public", finalTab)

	slices := splitBinarySlices(t, full, nSeg)

	// Destination agent: serve the on_segment unit over the frame-ack
	// transport (each slice its own ReliablePipe, routed by identity HELLO).
	listen := freePort(t)
	sresCh := make(chan agent.SegmentsResult, 1)
	serrCh := make(chan error, 1)
	go func() {
		res, err := agent.ServeSegments(ctx, agent.SegmentsOptions{
			Listen:   listen,
			DestConn: dstCl.URL(),
			Spec: agent.UnitSpec{
				JobID: jobID, UnitID: unitID,
				Schema: "public", Table: finalTab, Staging: staging, Overwrite: true,
				Compress: "zstd",
				Cols:     cols, Dist: dist, Part: part,
			},
			Segments:      nSeg,
			Swap:          true,
			AcceptTimeout: 30 * time.Second,
			StreamTimeout: 60 * time.Second,
			FrameAck:      true,
			ReliablePipe:  transfer.DefaultReliablePipeConfig(),
		})
		if err != nil {
			serrCh <- err
			return
		}
		sresCh <- res
	}()

	// N fake segreceivers: each streams its slice COMPRESSED over real TCP
	// through a ReliablePipe sender (identity HELLO: job/unit/content).
	time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
	cfg := transfer.DefaultReliablePipeConfig()
	cfg.ResumeTimeout = 10 * time.Second
	errCh := make(chan error, nSeg)
	for i := 0; i < nSeg; i++ {
		go func(i int) {
			conn, err := net.Dial("tcp", listen)
			if err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d dial: %w", i, err)
				return
			}
			id := transfer.StreamID{JobID: jobID, UnitID: unitID, Content: int16(i)}
			dial := func() (transfer.DupConn, error) {
				return net.DialTimeout("tcp", listen, 5*time.Second)
			}
			rp, err := transfer.NewReliablePipeSenderOnConnID(cfg, conn, dial, id)
			if err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d pipe: %w", i, err)
				return
			}
			// Stream the slice through the pipe (the pipe owns the conn).
			if _, err := segstream.SendCompressed(ctx, rp, bytes.NewReader(slices[i]), jobID, unitID, int16(i), compress.ModeZstd, 3); err != nil {
				errCh <- fmt.Errorf("fake segreceiver %d send: %w", i, err)
				return
			}
			_ = rp.Close() // clean FIN
			errCh <- nil
		}(i)
	}
	for i := 0; i < nSeg; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("fake segreceiver: %v", e)
		}
	}
	logical := uint64(len(full))

	var sres agent.SegmentsResult
	select {
	case serr := <-serrCh:
		t.Fatalf("dest agent serve segments (frame-ack): %v", serr)
	case sres = <-sresCh:
		t.Logf("serve segments (frame-ack): rows=%d trailer=%d wire=%d logical=%d swapped=%v",
			sres.Rows, sres.TrailerRows, sres.Wire, sres.Logical, sres.Swapped)
		if sres.Rows != rows || sres.TrailerRows != rows {
			t.Fatalf("row count mismatch: dst=%d trailer=%d want=%d", sres.Rows, sres.TrailerRows, rows)
		}
		if !sres.Swapped {
			t.Fatalf("dest agent did not swap")
		}
		// The on-wire volume is the COMPRESSED bytes (+ framing) the router
		// read from the network: it must be less than the logical (raw)
		// volume (the bytes were compressed on the wire).
		if sres.Wire == 0 {
			t.Fatalf("on-wire volume is 0 (router wire counter not wired)")
		}
		if sres.Wire >= logical {
			t.Fatalf("on-wire %d >= logical %d: not compressed on the network", sres.Wire, logical)
		}
	}

	if got := mustCount(t, dstConn, `SELECT count(*) FROM public.seg_orders_fa`); got != rows {
		t.Fatalf("dest seg_orders_fa count = %d, want %d", got, rows)
	}
	wantDigest := mustText(t, srcConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM seg_orders_fa r`)
	if got := mustText(t, dstConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM public.seg_orders_fa r`); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q", got, wantDigest)
	}
	t.Logf("Inc3 on_segment frame-ack ok: rows=%d wire=%d logical=%d ratio=%.1fx",
		rows, sres.Wire, logical, float64(logical)/float64(sres.Wire))
}

// readCopyBinaryFrameAck reads a binary COPY stream fully from the source.
// (A thin wrapper so the frame-ack test is self-contained; the plain test has
// its own readCopyBinary.)
func readCopyBinaryFrameAck(t *testing.T, q *pg.Conn, rows int) []byte {
	t.Helper()
	var buf bytes.Buffer
	n, err := source.New(q).CopyToStream(context.Background(), "public", "seg_orders_fa", &buf)
	if err != nil {
		t.Fatalf("read binary copy: %v", err)
	}
	if n != int64(rows) {
		t.Fatalf("binary copy rows = %d, want %d", n, rows)
	}
	return buf.Bytes()
}
