//go:build gp6lab

package integration

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestTwoAgent2B6DestOwnsJournal is the 2B-6 acceptance (architecture 08.10):
// the COORDINATOR two-agent path's gpstream_units DB row is owned by the
// DESTINATION agent (the coordinator analogue of the mesh mini path, 1D). The
// destination copies, verifies and swaps the unit, so it writes the DB row
// itself (copying at the serve start, swapped/failed at the serve end); the
// controller records ONLY the file mirror for the unit (it skips its own DB
// writes — the SetState !c.DestJournal guard) and delegates the FR-17
// index_done drain write to the destination agent. The test drives the
// production (default) coordinator data path end to end:
//
//  1. a source table with a PK and a SECONDARY index is copied through the
//     two-agent coordinator data path (staging + swap);
//  2. the PRIMARY journal (gpstream_units) holds the unit's row with the
//     DESTINATION's terminal state (swapped) + its on-the-data-path checksum
//     + the row count — the row the destination agent wrote (the controller
//     never writes the DB for a DestJournal unit, so the terminal state can
//     only have come from the destination);
//  3. the FR-17 background pool builds the secondary index and the pool drain
//     delegates index_done=true to the destination agent (the row is
//     index_done=true in the PRIMARY journal, mirroring the file mirror);
//  4. the file mirror (the controller's durable resume source) carries the
//     same terminal state + index_done — both planes agree.
func TestTwoAgent2B6DestOwnsJournal(t *testing.T) {
	const (
		srcPort = 5771
		dstPort = 5772
		rows    = 2000
	)
	ctx := context.Background()

	srcCl, err := pgtest.Start(srcPort, "srcdb")
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { srcCl.Stop() })
	dstCl, err := pgtest.Start(dstPort, "dstdb")
	if err != nil {
		t.Fatalf("start dest: %v", err)
	}
	t.Cleanup(func() { dstCl.Stop() })

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 4)
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	t.Cleanup(func() { dst.Close() })

	// Source: PK + one secondary index (the FR-17 pool's target; the PK is the
	// constraint pass's, excluded from the pool by design).
	mustExec(t, src, `CREATE TABLE public.j2b6 (
		id integer PRIMARY KEY, customer text, amount numeric)`)
	mustExec(t, src, `CREATE INDEX j2b6_c_idx ON public.j2b6 (customer)`)
	mustExec(t, src, `INSERT INTO public.j2b6
		SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5
		FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	if n := mustCount(t, src, `SELECT count(*) FROM public.j2b6`); n != rows {
		t.Fatalf("j2b6 seeded %d, want %d", n, rows)
	}

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	cfg.ConflictAction = config.Overwrite // staging + swap (the two-agent path)
	cfg.Verify = config.VerifyOn          // exercise the agent-side verify
	cfg.IndexWorkers = 2
	cfg.Source = srcCl.URL()
	cfg.Dest = dstCl.URL()

	jdir := t.TempDir()
	mirrorPath := filepath.Join(jdir, "journal.jsonl")
	j := manifest.NewJournal(dst, mirrorPath)
	mgr := newTwoAgentManager(t, &cfg, j, srcCl.URL(), dstCl.URL())

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) == 0 {
		t.Fatalf("plan has no units")
	}
	if err := runWithTimeout(t, mgr, p, 180*time.Second); err != nil {
		t.Fatalf("manager.Run (2B-6): %v", err)
	}

	// 1. Data landed + the secondary index was built by the FR-17 pool.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.j2b6`); got != rows {
		t.Fatalf("dest j2b6 count = %d, want %d", got, rows)
	}
	sec := mustCount(t, dst,
		`SELECT count(*) FROM pg_indexes
		 WHERE schemaname = 'public' AND tablename = 'j2b6' AND indexname <> 'j2b6_pkey'`)
	if sec != 1 {
		t.Fatalf("secondary indexes on final j2b6 = %d, want 1 (FR-17 pool)", sec)
	}

	// 2. PRIMARY journal: the unit's row is in the DESTINATION's terminal
	//    state (swapped) with its on-the-data-path checksum + row count — the
	//    row the destination agent wrote (the controller skips its own DB
	//    writes for a DestJournal unit, so the terminal state can only have
	//    come from the destination).
	if got := mustCount(t, dst, `SELECT count(*) FROM gpstream_units`); got != 1 {
		t.Fatalf("gpstream_units rows = %d, want 1", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM gpstream_units WHERE state = 'swapped'`); got != 1 {
		t.Fatalf("gpstream_units state=swapped rows = %d, want 1 (destination's terminal state)", got)
	}
	if got := mustCount(t, dst,
		`SELECT count(*) FROM gpstream_units WHERE row_count = `+strconv.Itoa(rows)); got != 1 {
		t.Fatalf("gpstream_units row_count=%d rows = %d, want 1", rows, got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM gpstream_units WHERE checksum IS NOT NULL AND checksum <> ''`); got != 1 {
		t.Fatalf("gpstream_units rows with a checksum = %d, want 1 (destination's content digest)", got)
	}

	// 3. FR-17: the pool drain delegated index_done=true to the destination
	//    agent (the row is index_done=true in the PRIMARY journal).
	if got := mustCount(t, dst, `SELECT count(*) FROM gpstream_units WHERE index_done`); got != 1 {
		t.Fatalf("gpstream_units index_done=true rows = %d, want 1 (FR-17 delegate)", got)
	}

	// 4. File mirror (the controller's durable resume source): the LAST record
	//    for the unit carries the terminal state + index_done — both planes
	//    agree (the destination's DB row and the controller's mirror).
	last, ok := lastMirrorUnit(mirrorPath, 1)
	if !ok {
		t.Fatalf("mirror has no record for unit 1")
	}
	if last.State != manifest.StateSwapped {
		t.Fatalf("mirror unit 1 state=%s, want swapped", last.State)
	}
	if !last.IndexDone {
		t.Fatalf("mirror unit 1 index_done=false after pool drain, want true")
	}
	if last.DestJournal == false {
		t.Fatalf("mirror unit 1 dest_journal=false, want true (the controller must know the destination owns the DB row for a resume)")
	}
	t.Logf("2B-6 ok: destination owns the coordinator gpstream_units row (swapped + digest + index_done); controller keeps the file mirror")
}
