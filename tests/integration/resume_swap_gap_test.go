//go:build gp6lab

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestResumeCompletesUnswappedUnit closes the resume gap: a unit a previous
// (crashed) run left in the "completed" state — data copied and verified into
// the staging table, but the atomic rename to the final name never ran — used
// to be skipped as terminal on re-run, leaving the destination WITHOUT the
// table while the journal said "done". A re-run must now finish the swap
// (FR-06/FR-07: "resume completes to the end").
//
// The crash window is reproduced by taking a normal run to its end, then
// putting the destination back into the pre-swap state (final table dropped,
// staging kept) and the journal back to "completed".
func TestResumeCompletesUnswappedUnit(t *testing.T) {
	const rows = 4

	// makeCrashed: run a fresh copy to completion, then roll both the
	// destination and the journal mirror back to the "completed, not swapped"
	// state. Returns the journal mirror path and the staging table name.
	makeCrashed := func(t *testing.T, src, dstQ *pg.Pool) (mirror, staging string) {
		t.Helper()
		mustExec(t, src, `DROP TABLE IF EXISTS public.orders`)
		mustExec(t, src, `CREATE TABLE public.orders (id integer, customer text, amount numeric)`)
		mustExec(t, src, `INSERT INTO public.orders VALUES (1,'alice',10.50),(2,'bob',20.25),(3,'carol',30.00),(4,'dave',40.75)`)
		mustExec(t, dstQ, `DROP TABLE IF EXISTS public.orders`)

		cfg := config.Default()
		p := buildPlan(t, cfg, src, dstQ)
		mirror = filepath.Join(t.TempDir(), "journal.jsonl")
		mgr := newTwoAgentManager(t, &cfg, manifest.NewJournal(nil, mirror), src.DSN(), dstQ.DSN())
		if err := mgr.Run(context.Background(), p); err != nil {
			t.Fatalf("initial run: %v", err)
		}
		if n := mustCount(t, dstQ, `SELECT count(*) FROM public.orders`); n != rows {
			t.Fatalf("initial run left %d rows, want %d", n, rows)
		}
		staging = dest.StagingName(p.Hash, 1)
		// Roll the destination back into the crash window: the swap renamed
		// staging -> orders, so invert it (orders -> staging). Now the staging
		// table holds the verified data and the final table does not exist —
		// exactly the pre-swap state a crash between the "completed" journal
		// write and the swap leaves behind.
		mustExec(t, dstQ, `ALTER TABLE public.orders RENAME TO `+staging)
		// Roll the journal back: swapped -> completed.
		mirror = rewriteMirrorState(t, mirror, 1, "completed")
		if n := mustCount(t, dstQ, `SELECT count(*) FROM public.`+staging); n != rows {
			t.Fatalf("crashed staging table has %d rows, want %d", n, rows)
		}
		return mirror, staging
	}

	t.Run("case1_staging_present_swap_completed", func(t *testing.T) {
		src, dstQ, _, _ := startPGPair(t, 5821, 5822)
		defer func() { src.Close(); dstQ.Close() }()
		mirror, staging := makeCrashed(t, src, dstQ)

		// Destination is in the crashed state: final table gone, staging kept.
		if mustCount(t, dstQ, `SELECT count(*) FROM pg_class WHERE relname = 'orders'`) != 0 {
			t.Fatal("crashed state: final table exists")
		}

		cfg := config.Default()
		p := buildPlan(t, cfg, src, dstQ)
		mgr := newTwoAgentManager(t, &cfg, manifest.NewJournal(nil, mirror), src.DSN(), dstQ.DSN())
		if err := mgr.Run(context.Background(), p); err != nil {
			t.Fatalf("re-run: %v", err)
		}
		// The gap is closed: the destination has the table with the data.
		if n := mustCount(t, dstQ, `SELECT count(*) FROM public.orders`); n != rows {
			t.Fatalf("after re-run destination has %d rows, want %d (unswapped unit not completed!)", n, rows)
		}
		if v := poolText(t, dstQ, `SELECT customer FROM public.orders WHERE id=3`); v != "carol" {
			t.Fatalf("row id=3 customer=%q, want carol", v)
		}
		// No leftover staging table (the rename consumed it).
		if mustCount(t, dstQ, `SELECT count(*) FROM pg_class WHERE relname = '`+staging+`'`) != 0 {
			t.Fatal("staging table left behind after the recovered swap")
		}
		// The journal now says swapped (the re-run counted the unit as
		// completed work, not as a skip).
		if u := unitFromMirror(t, mirror, 1); u.State != manifest.StateSwapped {
			t.Fatalf("unit state after re-run = %v, want swapped", u.State)
		}
		if mgr.Units != 1 {
			t.Fatalf("re-run Units=%d, want 1 (the recovered unit must count as completed, not skipped)", mgr.Units)
		}
		if mgr.Skipped != 0 {
			t.Fatalf("re-run Skipped=%d, want 0 (the completed unit must be finished, not skipped)", mgr.Skipped)
		}
	})

	t.Run("case2_final_present_state_recorded", func(t *testing.T) {
		src, dstQ, _, _ := startPGPair(t, 5823, 5824)
		defer func() { src.Close(); dstQ.Close() }()
		mirror, staging := makeCrashed(t, src, dstQ)
		// Case 2: the rename already landed but the "swapped" journal write
		// was lost — final table present, staging gone, journal says
		// completed.
		mustExec(t, dstQ, `ALTER TABLE public.`+staging+` RENAME TO orders`)

		cfg := config.Default()
		p := buildPlan(t, cfg, src, dstQ)
		mgr := newTwoAgentManager(t, &cfg, manifest.NewJournal(nil, mirror), src.DSN(), dstQ.DSN())
		if err := mgr.Run(context.Background(), p); err != nil {
			t.Fatalf("re-run: %v", err)
		}
		if n := mustCount(t, dstQ, `SELECT count(*) FROM public.orders`); n != rows {
			t.Fatalf("after re-run %d rows, want %d", n, rows)
		}
		if u := unitFromMirror(t, mirror, 1); u.State != manifest.StateSwapped {
			t.Fatalf("unit state after re-run = %v, want swapped", u.State)
		}
	})

	t.Run("case3_both_absent_data_lost", func(t *testing.T) {
		src, dstQ, _, _ := startPGPair(t, 5825, 5826)
		defer func() { src.Close(); dstQ.Close() }()
		mirror, staging := makeCrashed(t, src, dstQ)
		// Case 3: the verified staging data was removed (e.g. operator
		// cleanup) and the rename never ran — both tables gone. The re-run
		// must NOT claim success: it fails the unit (re-copy required) and
		// reports the anomaly.
		mustExec(t, dstQ, `DROP TABLE IF EXISTS public.`+staging)

		cfg := config.Default()
		p := buildPlan(t, cfg, src, dstQ)
		mgr := newTwoAgentManager(t, &cfg, manifest.NewJournal(nil, mirror), src.DSN(), dstQ.DSN())
		if err := mgr.Run(context.Background(), p); err == nil {
			t.Fatal("re-run with lost staging data returned nil, want an error (data must not be claimed copied)")
		}
		if u := unitFromMirror(t, mirror, 1); u.State != manifest.StateFailed {
			t.Fatalf("unit state after re-run = %v, want failed (re-copy required)", u.State)
		}
		if n := mustCount(t, dstQ, `SELECT count(*) FROM pg_class WHERE relname = 'orders'`); n != 0 {
			t.Fatalf("final table exists after a data-loss recovery (want none; the unit must be left for a re-copy)")
		}
	})
}

// rewriteMirrorState rewrites the journal file mirror, forcing unit id to the
// given state (simulating the journal state a crashed run left behind).
func rewriteMirrorState(t *testing.T, mirror string, id int64, state string) string {
	t.Helper()
	in, err := os.ReadFile(mirror)
	if err != nil {
		t.Fatalf("read mirror: %v", err)
	}
	var out []byte
	sc := bufio.NewScanner(strings.NewReader(string(in)))
	found := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var u manifest.Unit
		if err := json.Unmarshal([]byte(line), &u); err != nil {
			out = append(out, []byte(line+"\n")...)
			continue
		}
		if u.UnitID == id {
			u.State = manifest.State(state)
			found = true
		}
		b, _ := json.Marshal(&u)
		out = append(out, append(b, '\n')...)
	}
	if !found {
		t.Fatalf("mirror has no record for unit %d", id)
	}
	if err := os.WriteFile(mirror, out, 0o600); err != nil {
		t.Fatalf("write mirror: %v", err)
	}
	return mirror
}

// poolText runs a single-row text query against a *pg.Pool (mustText only
// accepts *pg.Conn, but startPGPair returns pools).
func poolText(t *testing.T, c *pg.Pool, sql string) string {
	t.Helper()
	rows, err := c.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatalf("query %q: no rows", sql)
	}
	switch v := rows[0][0].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		t.Fatalf("query %q: unexpected type %T", sql, rows[0][0])
		return ""
	}
}

// unitFromMirror returns the latest journal record for the given unit id.
func unitFromMirror(t *testing.T, mirror string, id int64) *manifest.Unit {
	t.Helper()
	in, err := os.ReadFile(mirror)
	if err != nil {
		t.Fatalf("read mirror: %v", err)
	}
	var last *manifest.Unit
	sc := bufio.NewScanner(strings.NewReader(string(in)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var u manifest.Unit
		if err := json.Unmarshal([]byte(line), &u); err != nil {
			continue
		}
		if u.UnitID == id {
			last = &u
		}
	}
	if last == nil {
		t.Fatalf("mirror has no record for unit %d", id)
	}
	return last
}
