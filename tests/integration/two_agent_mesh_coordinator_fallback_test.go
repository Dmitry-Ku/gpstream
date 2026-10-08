//go:build gp6lab

package integration

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
)

// TestTwoAgentMeshCoordinatorFallbackPlainPG is the P3 production-dispatch
// INC4 acceptance for the MANAGER-LEVEL mesh degradation: a LARGE table under
// --copy-mode mesh on a plain-PostgreSQL stand (no Greenplum segment catalog)
// cannot be placed segment-to-segment — the destination has no
// gp_segment_configuration, so the mesh mini path is unavailable. The P4
// design degrades PER TABLE (not per job) to the coordinator data path, and the
// decision is recorded in events.jsonl as a mesh_coordinator_fallback event.
//
// Because the segment-direct mini path is unavailable on plain PG, a
// SUCCESSFUL run (correct count + content digest) proves the table took the
// coordinator path through the new streamTwoAgentMini degradation branch —
// the manager's CopyMesh case no longer hard-fails on a non-Greenplum side.
// (The segment-to-segment mini DATA path itself is exercised live on the GP6
// dev matrix A/D/B; the write side is also covered by the P5 fake-segreceiver
// mesh tests.)
func TestTwoAgentMeshCoordinatorFallbackPlainPG(t *testing.T) {
	src, dst, srcCl, dstCl := startPGPair(t, 5660, 5661)
	defer func() { src.Close(); dst.Close() }() // clusters are cleaned up by startPGPair

	// A LARGE, compressible table: above the small-table threshold (so it is
	// NOT the P1b small-table fallback) and large enough that a successful
	// run is meaningful. Repetitive text keeps it compressible (the
	// coordinator path's wire volume stays under the logical volume).
	const rows = 20000
	mustExec(t, src, `DROP TABLE IF EXISTS public.orders`)
	mustExec(t, src, `CREATE TABLE public.orders (id integer, customer text, note text, amount numeric)`)
	mustExec(t, src,
		`INSERT INTO public.orders SELECT g, 'cust_' || (g % 97), 'repeatable note body for compression padding', (g % 100) * 10.5 FROM generate_series(1, `+strconv.Itoa(rows)+`) g`)
	if n := mustCount(t, src, `SELECT count(*) FROM public.orders`); n != rows {
		t.Fatalf("orders seeded %d, want %d", n, rows)
	}
	wantDigest := digest(t, src, "public.orders")

	cfg := config.Default()
	cfg.CopyMode = config.CopyMesh // the mode under test: the mesh mini path
	cfg.SmallTableThreshold = 1 << 20
	cfg.ConflictAction = config.Overwrite
	cfg.Verify = config.VerifyOn
	cfg.Compression = config.CompZstd
	cfg.WorkersPerPair = 1

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 1 {
		t.Fatalf("plan has %d units, want 1", len(p.Units))
	}
	// Premise: the table is LARGE (above the threshold), so it is NOT the
	// P1b small-table fallback — it must reach streamTwoAgentMini.
	if p.Units[0].Bytes <= cfg.SmallTableThreshold {
		t.Fatalf("orders size %d not above threshold %d (premise broken: this would be the small-table fallback, not the mesh degradation)",
			p.Units[0].Bytes, cfg.SmallTableThreshold)
	}

	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, src, dst, j, srcCl.URL(), dstCl.URL())

	start := time.Now()
	if err := runWithTimeout(t, mgr, p, 180*time.Second); err != nil {
		t.Fatalf("manager.Run (mesh on plain PG -> coordinator fallback): %v", err)
	}
	t.Logf("mesh-coordinator-fallback run: %v", time.Since(start))

	// 1. The destination table is correct (the coordinator path landed the
	//    data; the mini path was unavailable on plain PG).
	if got := mustCount(t, dst, `SELECT count(*) FROM public.orders`); got != rows {
		t.Fatalf("dest orders count = %d, want %d (mesh degradation corrupted the copy)", got, rows)
	}
	if got := digest(t, dst, "public.orders"); got != wantDigest {
		t.Fatalf("dest content digest %q != source %q (mesh degradation corrupted content)", got, wantDigest)
	}

	// 2. The manager copied all rows (FR-02 row count) through the fallback.
	if mgr.Rows != rows {
		t.Fatalf("manager.Rows = %d, want %d", mgr.Rows, rows)
	}

	// 3. The degradation decision is observable: exactly ONE
	// mesh_coordinator_fallback event, for this table. (NOT the P1b
	// onsegment_coordinator_fallback small-table event — the table is large.)
	evs, err := os.ReadFile(filepath.Join(jdir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	n := 0
	for _, line := range strings.Split(string(evs), "\n") {
		if !strings.Contains(line, `"kind":"mesh_coordinator_fallback"`) {
			continue
		}
		n++
		if !strings.Contains(line, `"table":"public.orders"`) {
			t.Fatalf("mesh_coordinator_fallback event is for the wrong table: %s", line)
		}
	}
	if n != 1 {
		t.Fatalf("mesh_coordinator_fallback events: count=%d, want exactly 1 for public.orders", n)
	}
	// And it must NOT be the small-table fallback (a different event kind).
	if strings.Contains(string(evs), `"kind":"onsegment_coordinator_fallback"`) {
		t.Fatalf("a large table must degrade via mesh_coordinator_fallback, not the small-table onsegment_coordinator_fallback")
	}

	// 4. The staging table is gone (the controller's swap landed).
	if got := mustCount(t, dst,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname LIKE '_gs\_%'`); got != 0 {
		t.Fatalf("staging tables still present after the mesh-fallback run: %d", got)
	}

	t.Logf("INC4 ok: large mesh table on plain PG degraded to the coordinator path, rows=%d, 1 mesh_coordinator_fallback event", rows)
}
