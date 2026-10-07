//go:build gp6lab

package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
)

// P1b: small-table → coordinator fallback under --copy-mode mesh.
//
// One knob (--small-table-threshold) drives both small-table behaviors: a
// table for which per-slice compression is not worth the encoder cost (F-12)
// is also one for which the segment-direct pipeline (S segreceivers + S
// destination connections) is not worth it. Such a table takes the
// coordinator data path instead, which also carries the inline content digest
// (P9-E), so it verifies without a re-read for free.

// TestOnSegmentSmallTableCoordinatorFallback is the LOCAL acceptance: with
// --copy-mode mesh, a SINGLE small table (below the threshold) must be
// copied through the COORDINATOR path (the segment-direct path is unavailable
// on plain PG and would fail, so a successful run proves the fallback). The
// fallback event must be recorded, the data must land correct, and the
// coordinator path's inline digest must have verified it (--verify on).
func TestOnSegmentSmallTableCoordinatorFallback(t *testing.T) {
	src, dst, _, _ := startPGPair(t, 5645, 5646)
	defer func() { src.Close(); dst.Close() }()

	// A single TINY table (tens of KB, well under the 1 MiB threshold).
	mustExec(t, src, `CREATE TABLE public.small (id int, val text)`)
	mustExec(t, src, `INSERT INTO public.small SELECT g, 'v' || g FROM generate_series(1,500) g`)

	cfg := config.Default()
	cfg.CopyMode = config.CopyMesh    // the mode the fallback overrides for small tables
	cfg.SmallTableThreshold = 1 << 20 // 1 MiB default
	cfg.Verify = config.VerifyOn      // inline-digest verify (coordinator path, P9-E)
	cfg.Compression = config.CompZstd
	cfg.ConflictAction = config.Overwrite
	cfg.WorkersPerPair = 1

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 1 {
		t.Fatalf("plan has %d units, want 1", len(p.Units))
	}
	if p.Units[0].Bytes >= cfg.SmallTableThreshold {
		t.Fatalf("small size %d not below threshold %d (premise broken)",
			p.Units[0].Bytes, cfg.SmallTableThreshold)
	}

	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, src, dst, j, src.DSN(), dst.DSN())
	if err := runWithTimeout(t, mgr, p, 120*time.Second); err != nil {
		t.Fatalf("manager.Run (on_segment, small): %v", err)
	}

	// 1. The data landed correct (the coordinator path copied it).
	if got := mustCount(t, dst, `SELECT count(*) FROM public.small`); got != 500 {
		t.Fatalf("small: %d rows, want 500 (coordinator fallback corrupted)", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.small WHERE id=7 AND val='v7'`); got != 1 {
		t.Fatalf("small id=7 val=v7 missing (data corrupted in the coordinator fallback)")
	}

	// 2. The fallback decision is observable: exactly one
	// onsegment_coordinator_fallback event, for the small table.
	evs, err := os.ReadFile(filepath.Join(jdir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	n, hit := 0, false
	for _, line := range strings.Split(string(evs), "\n") {
		if !strings.Contains(line, `"kind":"onsegment_coordinator_fallback"`) {
			continue
		}
		n++
		if strings.Contains(line, `"table":"public.small"`) {
			hit = true
		}
	}
	if n != 1 || !hit {
		t.Fatalf("onsegment_coordinator_fallback events: count=%d hitSmall=%v, want exactly 1 for public.small", n, hit)
	}
}

// TestOnSegmentCoordinatorFallbackLive is the LIVE acceptance (GP6 cluster,
// only when GP6_LAB_CLUSTERS is set): in the SAME run, a small table falls
// back to the coordinator (fallback event) and a large table stays on the
// segment-direct (mesh) path (no fallback event). Both must land correct.
func TestOnSegmentCoordinatorFallbackLive(t *testing.T) {
	if !pgtest.LiveMode() {
		t.Skip("live mode off (set GP6_LAB_CLUSTERS to run against the GP6 cluster)")
	}
	src, dst, _, _ := startPGPair(t, 5647, 5648)
	defer func() { src.Close(); dst.Close() }()

	// small: tens of KB (< 1 MiB threshold). large: several MB (> threshold).
	mustExec(t, src, `CREATE TABLE public.p1b_small (id int, val text)`)
	mustExec(t, src, `INSERT INTO public.p1b_small SELECT g, 'v' || g FROM generate_series(1,500) g`)
	mustExec(t, src, `CREATE TABLE public.p1b_large (id int, val text)`)
	mustExec(t, src, `INSERT INTO public.p1b_large SELECT g, 'v' || g FROM generate_series(1,300000) g`)

	cfg := config.Default()
	cfg.CopyMode = config.CopyMesh
	cfg.SmallTableThreshold = 1 << 20
	cfg.Verify = config.VerifyOn
	cfg.Compression = config.CompZstd
	cfg.ConflictAction = config.Overwrite
	cfg.WorkersPerPair = 1

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 2 {
		t.Fatalf("plan has %d units, want 2", len(p.Units))
	}
	byName := map[string]int64{}
	for _, u := range p.Units {
		byName[u.Name] = u.Bytes
	}
	if byName["p1b_small"] >= cfg.SmallTableThreshold {
		t.Fatalf("p1b_small %d not below threshold %d (premise broken)", byName["p1b_small"], cfg.SmallTableThreshold)
	}
	if byName["p1b_large"] <= cfg.SmallTableThreshold {
		t.Fatalf("p1b_large %d not above threshold %d (premise broken)", byName["p1b_large"], cfg.SmallTableThreshold)
	}

	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, src, dst, j, src.DSN(), dst.DSN())
	if err := runWithTimeout(t, mgr, p, liveDeadline(300*time.Second)); err != nil {
		t.Fatalf("manager.Run (on_segment, small+large): %v", err)
	}

	if got := mustCount(t, dst, `SELECT count(*) FROM public.p1b_small`); got != 500 {
		t.Fatalf("p1b_small: %d rows, want 500", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.p1b_large`); got != 300000 {
		t.Fatalf("p1b_large: %d rows, want 300000", got)
	}

	// Exactly ONE fallback event, and it is for the SMALL table (the large
	// table stayed on the segment-direct path).
	evs, err := os.ReadFile(filepath.Join(jdir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	n, small, large := 0, false, false
	for _, line := range strings.Split(string(evs), "\n") {
		if !strings.Contains(line, `"kind":"onsegment_coordinator_fallback"`) {
			continue
		}
		n++
		if strings.Contains(line, `"table":"public.p1b_small"`) {
			small = true
		}
		if strings.Contains(line, `"table":"public.p1b_large"`) {
			large = true
		}
	}
	if n != 1 || !small || large {
		t.Fatalf("fallback events: count=%d small=%v large=%v, want exactly 1 for p1b_small (large must stay on mesh)", n, small, large)
	}
}
