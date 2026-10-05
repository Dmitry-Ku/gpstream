//go:build gp6lab

package integration

import (
	"bytes"
	"context"
	"fmt"
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

// TestTwoAgentRoutedServe is the P9 acceptance: a REAL destination agent
// (agent.ServeRouted) accepts S×D×W sub-stream connections (each a
// self-contained binary-COPY stream, content-tagged with its destination
// segment), merges the per-segment sub-streams, and loads D utility-mode COPYs
// — with NEITHER cluster coordinator in the data path. The fake segreceivers
// are REAL RoutedSenders: they route a slice's rows by the distribution key
// (cdbhash + JCH, here int4 → content 0 on plain PG) and fan each (s,d) pair
// over W sub-streams — exactly the wire form the kernel-launched segreceivers
// use. S=2 source "segments", D=1 destination "segment" (plain PG: one
// connection, the coordinator), W=2 workers → 4 sub-stream connections.
func TestTwoAgentRoutedServe(t *testing.T) {
	const (
		srcPort  = 5626
		dstPort  = 5627
		rows     = 20000
		jobID    = 0x9abc
		unitID   = 9
		finalTab = "routed_orders"
		staging  = "_gs_testjob_9"
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

	// Seed a source table (single int4 key column — the routing key) and
	// read its binary-COPY stream.
	mustExec(t, srcConn, `DROP TABLE IF EXISTS routed_orders`)
	mustExec(t, srcConn, `CREATE TABLE routed_orders (id integer, customer text, amount numeric)`)
	mustExec(t, srcConn,
		`INSERT INTO routed_orders SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	full := readCopyBinaryTable(t, srcConn, "routed_orders", rows)

	// The unit shape, captured from the source.
	sa := source.New(srcConn)
	cols, err := sa.Columns(ctx, "public", finalTab)
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	dist := sa.Distribution(ctx, "public", finalTab)
	part, _ := pg.CapturePartitionSpec(srcConn, ctx, "public", finalTab) // non-fatal (plain PG)

	// Split the stream into S slices (the kernel's per-segment shape).
	slices := splitBinarySlices(t, full, S)

	// Destination agent: serve the routed unit (with the swap). On plain PG
	// the single per-segment DSN is the coordinator.
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
		})
		if err != nil {
			derrCh <- err
			return
		}
		dresCh <- res
	}()

	// S fake segreceivers: each runs a REAL RoutedSender over its slice
	// (route by the int4 key + fan each (s,d) pair over W sub-streams). The
	// destination segment is content 0 (plain PG), so every sub-stream is
	// content-tagged 0.
	time.Sleep(50 * time.Millisecond) // let the dest agent bind its listener
	errCh := make(chan error, S)
	var logical uint64
	for s := 0; s < S; s++ {
		go func(s int) {
			// Dial D×W connections (one per (d, w) sub-stream).
			conns := make([][]transfer.Pipe, D)
			for d := 0; d < D; d++ {
				conns[d] = make([]transfer.Pipe, W)
				for w := 0; w < W; w++ {
					c, derr := dialRetry(listen, 90*time.Second)
					if derr != nil {
						errCh <- fmt.Errorf("fake segreceiver %d dial (d%d w%d): %w", s, d, w, derr)
						return
					}
					conns[d][w] = c // net.Conn satisfies transfer.Pipe
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
	logical = uint64(len(full))

	select {
	case serr := <-derrCh:
		t.Fatalf("dest agent serve routed: %v", serr)
	case dres := <-dresCh:
		t.Logf("serve routed: rows=%d trailer=%d wire=%d logical=%d swapped=%v perseg=%v",
			dres.Rows, dres.TrailerRows, dres.Wire, dres.Logical, dres.Swapped, dres.PerSeg)
		// 1. The destination wrote exactly the seeded row count
		// (disjoint-and-complete: no duplication, no loss across the
		// S×D×W sub-streams).
		if dres.Rows != rows || dres.TrailerRows != rows {
			t.Fatalf("row count mismatch: dst=%d trailer=%d want=%d", dres.Rows, dres.TrailerRows, rows)
		}
		if !dres.Swapped {
			t.Fatalf("dest agent did not swap")
		}
		// 2. ADR-002 acceptance: the network carried the COMPRESSED volume.
		if dres.Wire >= logical {
			t.Fatalf("on-wire %d >= logical %d: not compressed on the network", dres.Wire, logical)
		}
	}

	// 3. The final table has the source's content (order-independent digest).
	if got := mustCount(t, dstConn, `SELECT count(*) FROM public.routed_orders`); got != rows {
		t.Fatalf("dest routed_orders count = %d, want %d", got, rows)
	}
	wantDigest := mustText(t, srcConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM routed_orders r`)
	if got := mustText(t, dstConn,
		`SELECT md5(string_agg(row_to_json(r)::text, chr(10) ORDER BY r.id)) FROM public.routed_orders r`); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q", got, wantDigest)
	}
}
