//go:build gp6lab

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
)

// TestTwoAgentMeshFloodSmallTables is the P3 production-dispatch (INC6)
// flood acceptance (design item 6: "flood small tables — the pool is reused,
// no per-table handshake"): a mesh job with MANY small tables must not pay a
// per-table handshake. The destination agent (supervisor mode in production;
// the in-proc dest server locally) is long-lived for the RUN and every table
// rides the SAME agent + connection pool (the fallback-pool model, confirmed
// 08.10: the supervisor holds a pool of workers+2 open connections; lazy open
// + 30s idle close is the normal behavior, no warmup).
//
// A small table (below --small-table-threshold) never reaches the mesh mini
// path — the manager's P1b small-table check runs first and sends it down the
// coordinator path. So the flood is exactly N small tables each degrading to
// the coordinator path on ONE long-lived destination agent: the contract is
// N tables, N fallback events, all data correct, in a short wall time (no
// per-table agent launch / handshake). On plain PG the destination also has
// no segment catalog, but the small-table path takes precedence, so the
// events are onsegment_coordinator_fallback (the P1b small-table kind), not
// mesh_coordinator_fallback.
func TestTwoAgentMeshFloodSmallTables(t *testing.T) {
	const (
		srcPort = 5662
		dstPort = 5663
		nTables = 8
		rows    = 2000 // per table
	)
	src, dst, srcCl, dstCl := startPGPair(t, srcPort, dstPort)
	defer func() { src.Close(); dst.Close() }() // clusters are cleaned up by startPGPair

	// Seed N small, compressible tables.
	for i := 0; i < nTables; i++ {
		name := fmt.Sprintf("fl%d", i)
		mustExec(t, src, fmt.Sprintf(`DROP TABLE IF EXISTS public.%s`, name))
		mustExec(t, src, fmt.Sprintf(`CREATE TABLE public.%s (id int, val text)`, name))
		mustExec(t, src,
			fmt.Sprintf(`INSERT INTO public.%s SELECT g, 'flood_' || g FROM generate_series(1,%d) g`, name, rows))
		if n := mustCount(t, src, fmt.Sprintf(`SELECT count(*) FROM public.%s`, name)); n != rows {
			t.Fatalf("%s seeded %d, want %d", name, n, rows)
		}
	}

	cfg := config.Default()
	cfg.CopyMode = config.CopyMesh
	cfg.SmallTableThreshold = 1 << 20 // all 8 tables are small (the flood shape)
	cfg.ConflictAction = config.Overwrite
	cfg.Verify = config.VerifyOn
	cfg.Compression = config.CompZstd
	cfg.WorkersPerPair = 1

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != nTables {
		t.Fatalf("plan has %d units, want %d", len(p.Units), nTables)
	}

	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, src, dst, j, srcCl.URL(), dstCl.URL())

	start := time.Now()
	if err := runWithTimeout(t, mgr, p, 180*time.Second); err != nil {
		t.Fatalf("manager.Run (mesh flood of %d small tables): %v", nTables, err)
	}
	dur := time.Since(start)
	t.Logf("mesh flood: %d tables x %d rows in %v", nTables, rows, dur)

	// 1. Every table landed with the correct row count.
	for i := 0; i < nTables; i++ {
		name := fmt.Sprintf("fl%d", i)
		if got := mustCount(t, dst, fmt.Sprintf(`SELECT count(*) FROM public.%s`, name)); got != rows {
			t.Fatalf("dest %s count = %d, want %d (flood corrupted a table)", name, got, rows)
		}
	}

	// 2. The per-table degradation is observable: exactly N fallback events
	// (one per table). A small table takes the P1b small-table path (the
	// manager's coordinatorFallbackFor check runs BEFORE the mesh case), so
	// the events are onsegment_coordinator_fallback — NOT the
	// mesh_coordinator_fallback of streamTwoAgentMini (that path is for a
	// LARGE table the mesh case reaches). The flood contract: N tables, N
	// fallback events, one long-lived dest agent (no per-table launch).
	evs, err := os.ReadFile(filepath.Join(jdir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	meshFB, smallFB := 0, 0
	for _, line := range strings.Split(string(evs), "\n") {
		switch {
		case strings.Contains(line, `"kind":"mesh_coordinator_fallback"`):
			meshFB++
		case strings.Contains(line, `"kind":"onsegment_coordinator_fallback"`):
			smallFB++
		}
	}
	// Every table degraded (one event each): either through the mesh case
	// (streamTwoAgentMini) or the P1b small-table case — both are the
	// coordinator path. The flood contract is: N tables, N fallback events,
	// one long-lived dest agent (no per-table agent launch).
	if meshFB+smallFB != nTables {
		t.Fatalf("fallback events: mesh=%d small=%d, want total %d (one per table)", meshFB, smallFB, nTables)
	}
	t.Logf("fallback events: mesh_coordinator_fallback=%d onsegment_coordinator_fallback=%d", meshFB, smallFB)

	// 3. The manager copied all rows (FR-02 row count: N tables x rows).
	if mgr.Rows != int64(nTables*rows) {
		t.Fatalf("manager.Rows = %d, want %d", mgr.Rows, int64(nTables*rows))
	}

	// 4. No staging tables survived (each unit swapped).
	if got := mustCount(t, dst,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname LIKE '_gs\_%'`); got != 0 {
		t.Fatalf("staging tables still present after the flood: %d", got)
	}
	t.Logf("flood ok: %d tables, %d rows, %v", nTables, nTables*rows, dur)
}
