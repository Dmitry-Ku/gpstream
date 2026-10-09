//go:build gp6lab

package integration

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestFR17IndexDoneJournal is the close-out for the 10K stress finding
// ("index_done: 0" after a 54-minute run). It verifies the full FR-17
// background-index chain end to end against real PG clusters:
//
//  1. a source table with a PK and a SECONDARY index is copied through the
//     two-agent data path (staging + swap);
//  2. the background pool (FR-17) builds the secondary index on the final
//     table (the PK is built by the post-swap constraint pass, not the pool);
//  3. the pool drain marks the unit index_done in BOTH journal planes:
//     the file mirror (last record index_done=true) and the PRIMARY journal
//     (gpstream_units.index_done = true — the column the stress run never
//     had, which is why its index_done read as 0);
//  4. a second run of the same operation (T5 resume) KEEPS the unit (no
//     re-copy) and does NOT re-enqueue/re-REINDEX (the IndexDone guard),
//     proving the flag is what makes the resume idempotent.
func TestFR17IndexDoneJournal(t *testing.T) {
	const (
		srcPort = 5701
		dstPort = 5702
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

	// Source: PK + one secondary index (the pool's target; the PK is the
	// constraint pass's, excluded from the pool by design).
	mustExec(t, src, `CREATE TABLE public.idx_t (
		id integer PRIMARY KEY, customer text, amount numeric)`)
	mustExec(t, src, `CREATE INDEX idx_t_c_idx ON public.idx_t (customer)`)
	mustExec(t, src, `INSERT INTO public.idx_t
		SELECT g, 'cust_' || (g % 97), (g % 100) * 10.5
		FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	if n := mustCount(t, src, `SELECT count(*) FROM public.idx_t`); n != rows {
		t.Fatalf("idx_t seeded %d, want %d", n, rows)
	}

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	cfg.ConflictAction = config.Overwrite
	cfg.IndexWorkers = 2

	jdir := t.TempDir()
	mirrorPath := jdir + "/journal.jsonl"
	j := manifest.NewJournal(dst, mirrorPath)
	mgr := newTwoAgentManager(t, &cfg, j, srcCl.URL(), dstCl.URL())

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) == 0 {
		t.Fatalf("plan has no units")
	}
	if err := runWithTimeout(t, mgr, p, 120*time.Second); err != nil {
		t.Fatalf("manager.Run (FR-17): %v", err)
	}

	// 1. Data landed.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.idx_t`); got != rows {
		t.Fatalf("dest idx_t count = %d, want %d", got, rows)
	}
	// 2. The SECONDARY index was built on the final table by the pool
	//    (exactly one: idx_t_c_idx; the PK is a constraint, not a pool job).
	sec := mustCount(t, dst,
		`SELECT count(*) FROM pg_indexes
		 WHERE schemaname = 'public' AND tablename = 'idx_t'
		   AND indexname <> 'idx_t_pkey'`)
	if sec != 1 {
		t.Fatalf("secondary indexes on final idx_t = %d, want 1 (FR-17 pool)", sec)
	}
	// 3a. File mirror: the LAST record for unit 1 carries index_done=true.
	last, ok := lastMirrorUnit(mirrorPath, 1)
	if !ok {
		t.Fatalf("mirror has no record for unit 1")
	}
	if !last.IndexDone {
		t.Fatalf("mirror unit 1 index_done=false after pool drain, want true")
	}
	if last.State != manifest.StateSwapped {
		t.Fatalf("mirror unit 1 state=%s, want swapped", last.State)
	}
	// 3b. PRIMARY journal: gpstream_units.index_done = true (the column the
	//     10K stress journal never had).
	if got := mustCount(t, dst, `SELECT count(*) FROM gpstream_units`); got != 1 {
		t.Fatalf("gpstream_units rows = %d, want 1", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM gpstream_units WHERE index_done`); got != 1 {
		t.Fatalf("gpstream_units index_done=true rows = %d, want 1 (FR-17 journal)", got)
	}

	// 4. Idempotent resume: a second run of the same operation keeps the unit
	//    (no re-copy) and does not re-enqueue the index build (IndexDone
	//    guard in requeueIndexes) — the data and index counts are unchanged.
	mgr2 := newTwoAgentManager(t, &cfg, j, srcCl.URL(), dstCl.URL())
	p2 := buildPlan(t, cfg, src, dst)
	if err := runWithTimeout(t, mgr2, p2, 120*time.Second); err != nil {
		t.Fatalf("manager.Run (FR-17 resume): %v", err)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.idx_t`); got != rows {
		t.Fatalf("resume re-copied: dest idx_t count = %d, want %d (keep rule)", got, rows)
	}
	if got := mustCount(t, dst,
		`SELECT count(*) FROM pg_indexes
		 WHERE schemaname = 'public' AND tablename = 'idx_t' AND indexname <> 'idx_t_pkey'`); got != 1 {
		t.Fatalf("resume changed secondary index count = %d, want 1 (IndexDone guard)", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM gpstream_units WHERE index_done`); got != 1 {
		t.Fatalf("resume flipped gpstream_units index_done rows = %d, want 1", got)
	}
	t.Logf("FR-17 ok: secondary index built, index_done in mirror+pg-journal, resume idempotent")
}

// lastMirrorUnit re-reads the append-only file mirror and returns the LAST
// record for unitID (LoadFromMirror keeps the last record per unit; this is
// the same rule, exposed for the assertion).
func lastMirrorUnit(path string, unitID int64) (manifest.Unit, bool) {
	f, err := os.Open(path)
	if err != nil {
		return manifest.Unit{}, false
	}
	defer f.Close()
	var last manifest.Unit
	found := false
	dec := json.NewDecoder(f)
	for dec.More() {
		var u manifest.Unit
		if err := dec.Decode(&u); err != nil {
			return last, found
		}
		if u.UnitID == unitID {
			last = u
			found = true
		}
	}
	return last, found
}
