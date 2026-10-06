//go:build gp6lab

package integration

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/agent"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
	"github.com/gpstream/gpstream/internal/transfer/source"
)

// TestPersistentChannelDestServe is the P1c dest-half acceptance: a REAL
// destination agent serves a unit over the PERSISTENT coordinator channel
// (PersistentReceiver + ServePersistentUnit) into a REAL pgtest destination.
// The source side is the PersistentSender over a real net.Pipe: UnitBegin
// (the spec) + framed zstd data + Trailer (the inline digest + row count).
// Two units ride the ONE connection (the channel stays open between units),
// each lands in its own final table (swap), and the destination's inline
// digest (VerifyDigest on) must equal the reference digest of the source
// rows (P9-E inline verify over the persistent channel).
func TestPersistentChannelDestServe(t *testing.T) {
	const (
		dstPort = 5651
		jobID   = 0x7788
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	defer dstCl.Stop()

	dq, err := pg.NewConn(ctx, dstCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect dest: %v", err)
	}
	defer dq.Close()
	da := dest.New(dq)

	// The persistent channel over a real net.Pipe (a=sender, b=receiver).
	a, b := net.Pipe()
	snd := agent.NewPersistentSender(a, jobID)
	rcv := agent.NewPersistentReceiver(b, jobID)
	defer snd.Close()
	defer b.Close()

	comp := compress.New(compress.ModeZstd, 3)

	// Two units of different shapes, streamed over the ONE connection.
	units := []struct {
		unitID uint32
		table  string
		staging string
		rows   int
	}{
		{1, "pers_t1", "_gs_pers_1", 5000},
		{2, "pers_t2", "_gs_pers_2", 3000},
	}
	// Build each unit's binary-COPY payload on a scratch source (the same
	// shape the source agent would stream), and its reference digest.
	srcCl, err := pgtest.Start(dstPort+1, "srcdb")
	if err != nil {
		t.Fatalf("start scratch source: %v", err)
	}
	defer srcCl.Stop()
	srcConn, err := pg.NewConn(ctx, srcCl.URL(), "", "", "", "")
	if err != nil {
		t.Fatalf("connect scratch source: %v", err)
	}
	defer srcConn.Close()

	var (
		payloads   [][]byte
		wantDigest []string
		wantRows   []int64
		sa         = source.New(srcConn)
	)
	for _, u := range units {
		mustExec(t, srcConn, `DROP TABLE IF EXISTS scratch_t`)
		mustExec(t, srcConn, `CREATE TABLE scratch_t (id integer, customer text, amount numeric)`)
		mustExec(t, srcConn,
			`INSERT INTO scratch_t SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(u.rows)+`) g`)
		var buf bytes.Buffer
		n, cerr := sa.CopyToStream(ctx, "public", "scratch_t", &buf)
		if cerr != nil {
			t.Fatalf("copy scratch: %v", cerr)
		}
		if n != int64(u.rows) {
			t.Fatalf("scratch rows = %d, want %d", n, u.rows)
		}
		payloads = append(payloads, buf.Bytes())
		want, derr := transfer.ContentDigest(buf.Bytes())
		if derr != nil {
			t.Fatalf("reference digest: %v", derr)
		}
		wantDigest = append(wantDigest, want)
		wantRows = append(wantRows, n)
	}

	// The destination's staging shape (cols/dist/part) comes from the source
	// table the spec references (what the controller would capture).
	cols, err := sa.Columns(ctx, "public", "scratch_t")
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	dist := sa.Distribution(ctx, "public", "scratch_t")
	part, _ := pg.CapturePartitionSpec(srcConn, ctx, "public", "scratch_t") // non-fatal (plain PG)

	// The destination serves both units off the ONE persistent connection.
	servDone := make(chan struct{})
	go func() {
		defer close(servDone)
		for i := range units {
			res, err := agent.ServePersistentUnit(ctx, dq, da, rcv, true /*swap*/, 60*time.Second)
			if err != nil {
				t.Errorf("unit %d: serve: %v", units[i].unitID, err)
				return
			}
			if res.Rows != wantRows[i] {
				t.Errorf("unit %d: rows = %d, want %d", units[i].unitID, res.Rows, wantRows[i])
			}
			if !res.Swapped {
				t.Errorf("unit %d: not swapped", units[i].unitID)
			}
			// P9-E inline verify over the persistent channel: the destination's
			// on-the-data-path digest must equal the source's reference digest.
			if res.Digest != wantDigest[i] {
				t.Errorf("unit %d: dst digest %q != reference %q", units[i].unitID, res.Digest, wantDigest[i])
			}
		}
	}()

	// The source sends both units over the ONE connection (sequentially — the
	// strict worker pool serializes the data path).
	for i := range units {
		spec := agent.UnitSpec{
			JobID: jobID, UnitID: units[i].unitID,
			Schema: "public", Table: units[i].table, Staging: units[i].staging, Overwrite: true,
			Compress: "zstd:3", Cols: cols, Dist: dist, Part: part,
			VerifyDigest: true,
		}
		// The source's raw digest accumulator (P0): the XOR of one SHA-256 per
		// row, derived from the same reference digest the destination computes.
		acc, rows, aerr := transfer.DigestAccumulator(payloads[i])
		if aerr != nil {
			t.Fatalf("unit %d: source digest accumulator: %v", units[i].unitID, aerr)
		}
		if _, _, err := snd.SendUnit(ctx, spec, bytes.NewReader(payloads[i]), comp, acc, rows); err != nil {
			t.Fatalf("unit %d: send: %v", units[i].unitID, err)
		}
	}
	// The channel stays open; close it once both units are sent (the serve
	// loop, having served both, returns; the conn close is cleanup).
	snd.Close()
	<-servDone

	// Both final tables hold the source's rows (the swap landed).
	for i := range units {
		if got := mustCount(t, dq, `SELECT count(*) FROM public.`+units[i].table); got != wantRows[i] {
			t.Fatalf("unit %d: dest %s = %d rows, want %d", units[i].unitID, units[i].table, got, wantRows[i])
		}
	}
	t.Logf("persistent dest-serve ok: 2 units over 1 connection, inline digests verified")
}
