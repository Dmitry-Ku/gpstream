//go:build gp6lab

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
)

// TestSmallTableCompressionAutoDisable — F-12: "for very small tables
// compression is disabled automatically (overhead)" + acceptance "data
// correctness after compression (checksums)".
//
// One run copies a TINY table (below --small-table-threshold) and a LARGE
// table (above it) together, with zstd on by default. The F-12 decision is
// per unit, so in the SAME run the small table rides the transport with
// compression OFF and the large one with zstd ON. Both must land byte-correct
// (checksum verify is on in these tests), and events.jsonl must
// record a compression_off event for the small table only.
//
// This is the live acceptance: it exercises the real data path (source COPY ->
// framed pipe -> destination COPY) in both per-unit modes at once, proving the
// mode swap does not corrupt either stream.
func TestSmallTableCompressionAutoDisable(t *testing.T) {
	src, dst, _, _ := startPGPair(t, 5671, 5672)
	defer func() { src.Close(); dst.Close() }()

	ctx := context.Background()
	// tiny: a few hundred rows (~tens of KB, well under 1 MiB).
	mustExec(t, src, `CREATE TABLE public.tiny (id int, val text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.tiny SELECT g, 'v' || g FROM generate_series(1,500) g`); err != nil {
		t.Fatalf("seed tiny: %v", err)
	}
	// big: ~200k short rows (several MB, well above 1 MiB).
	mustExec(t, src, `CREATE TABLE public.big (id int, val text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.big SELECT g, 'v' || g FROM generate_series(1,200000) g`); err != nil {
		t.Fatalf("seed big: %v", err)
	}

	cfg := config.Default()
	cfg.Compression = config.CompZstd // real mode, so the swap is observable
	cfg.SmallTableThreshold = 1 << 20 // 1 MiB default
	cfg.Verify = config.VerifyOn      // checksum verify on every unit
	cfg.WorkersPerPair = 1            // serial: deterministic event order

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 2 {
		t.Fatalf("plan has %d units, want 2", len(p.Units))
	}
	// Confirm the size split the test depends on: tiny below, big above.
	byName := map[string]int64{}
	for _, u := range p.Units {
		byName[u.Name] = u.Bytes
	}
	if byName["tiny"] >= cfg.SmallTableThreshold {
		t.Fatalf("tiny size %d not below threshold %d (test premise broken)",
			byName["tiny"], cfg.SmallTableThreshold)
	}
	if byName["big"] <= cfg.SmallTableThreshold {
		t.Fatalf("big size %d not above threshold %d (test premise broken)",
			byName["big"], cfg.SmallTableThreshold)
	}

	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, j, src.DSN(), dst.DSN())
	if err := mgr.Run(ctx, p); err != nil {
		t.Fatalf("manager.Run: %v", err)
	}

	// Data correctness in BOTH modes: checksum verify already ran inside
	// Run (--verify on); the counts below are the independent cross-check.
	if got := mustCount(t, dst, `SELECT count(*) FROM public.tiny`); got != 500 {
		t.Fatalf("tiny: %d rows, want 500 (compression-off path corrupted)", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.big`); got != 200000 {
		t.Fatalf("big: %d rows, want 200000 (zstd path corrupted)", got)
	}
	// Spot-check the actual values survived (not just the count).
	if got := mustCount(t, dst, `SELECT count(*) FROM public.tiny WHERE id=7 AND val='v7'`); got != 1 {
		t.Fatalf("tiny id=7 val=v7 missing (data corrupted in off-mode)")
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.big WHERE id=199999 AND val='v199999'`); got != 1 {
		t.Fatalf("big id=199999 val=v199999 missing (data corrupted in zstd mode)")
	}

	// The F-12 decision is observable: exactly ONE compression_off event, and
	// it is for the tiny table (the large table keeps zstd).
	evs, err := os.ReadFile(filepath.Join(jdir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	offs := 0
	offTiny, offBig := false, false
	for _, line := range splitLines(string(evs)) {
		if !contains(line, `"kind":"compression_off"`) {
			continue
		}
		offs++
		if contains(line, `"table":"public.tiny"`) {
			offTiny = true
		}
		if contains(line, `"table":"public.big"`) {
			offBig = true
		}
	}
	if offs != 1 {
		t.Fatalf("got %d compression_off events, want 1 (tiny only)", offs)
	}
	if !offTiny {
		t.Fatalf("compression_off event must be for the tiny table")
	}
	if offBig {
		t.Fatalf("compression_off event wrongly recorded for the big table (it keeps zstd)")
	}
}

func splitLines(s string) []string { return strings.Split(s, "\n") }
func contains(s, sub string) bool  { return strings.Contains(s, sub) }
