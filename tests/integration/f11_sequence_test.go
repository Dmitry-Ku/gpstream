//go:build gp6lab

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
)

// TestSequencePostLoadSync — F-11: "sequences (values are synchronized
// post-load)".
//
// A source table with a serial primary key (id serial -> a real sequence
// public.seq_t_id_seq) is copied end-to-end. The destination table is
// created from the source's column layout, so its id column carries the
// verbatim serial default nextval('public.seq_t_id_seq'::regclass); the
// sequence itself must be recreated on the destination AND resynchronised
// to the source last_value, so that:
//
//  1. the destination sequence exists (the column default resolves),
//  2. its last_value equals the source last_value (100 after the seed),
//  3. the NEXT nextval returns 101 — the post-load state matches the
//     source (no id collision with the copied rows),
//  4. events.jsonl records a sequence_synced event (F-04).
//
// The data path must be unaffected: full checksum verify passes and the
// counts match.
func TestSequencePostLoadSync(t *testing.T) {
	src, dst, _, _ := startPGPair(t, 5681, 5682)
	defer func() { src.Close(); dst.Close() }()

	ctx := context.Background()
	mustExec(t, src, `CREATE TABLE public.seq_t (id serial PRIMARY KEY, val text)`)
	// Insert via the serial default so the sequence advances with the ids.
	if _, err := src.Exec(ctx,
		`INSERT INTO public.seq_t (val) SELECT 'v' || g FROM generate_series(1,100) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A few nextval calls so the sequence is ahead of the rows' ids: the
	// source last_value is 105 (100 seeded + 5 nextval), so the destination
	// resync must land on 105, not on the max row id (100).
	if _, err := src.Exec(ctx,
		`SELECT nextval('public.seq_t_id_seq') FROM generate_series(1,5)`); err != nil {
		t.Fatalf("advance sequence: %v", err)
	}
	srcLast := mustCount(t, src, `SELECT last_value FROM public.seq_t_id_seq`)
	if srcLast != 105 {
		t.Fatalf("source last_value = %d, want 105 (test premise broken)", srcLast)
	}

	cfg := config.Default()
	cfg.WorkersPerPair = 1

	p := buildPlan(t, cfg, src, dst)
	if len(p.Units) != 1 {
		t.Fatalf("plan has %d units, want 1", len(p.Units))
	}
	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, src, dst, j, src.DSN(), dst.DSN())
	if err := runWithTimeout(t, mgr, p, 60*time.Second); err != nil {
		t.Fatalf("manager.Run: %v", err)
	}

	// Data correctness (the sequence sync must not touch the data path).
	if got := mustCount(t, dst, `SELECT count(*) FROM public.seq_t`); got != 100 {
		t.Fatalf("seq_t: %d rows, want 100", got)
	}
	if got := mustCount(t, dst, `SELECT count(*) FROM public.seq_t WHERE id=1 AND val='v1'`); got != 1 {
		t.Fatalf("seq_t id=1 val=v1 missing (data corrupted)")
	}

	// The sequence exists on the destination under its source name.
	if got := mustCount(t, dst, `SELECT count(*) FROM pg_class c JOIN pg_namespace n
		ON n.oid = c.relnamespace WHERE c.relname='seq_t_id_seq' AND n.nspname='public'
		AND c.relkind='S'`); got != 1 {
		t.Fatalf("destination sequence seq_t_id_seq missing")
	}
	// Its value is resynchronised to the source last_value.
	if got := mustCount(t, dst, `SELECT last_value FROM public.seq_t_id_seq`); got != srcLast {
		t.Fatalf("destination last_value = %d, want %d (source last_value)", got, srcLast)
	}
	// The next nextval continues from there — no id collision with the
	// copied rows, exactly like on the source.
	if got := mustCount(t, dst, `SELECT nextval('public.seq_t_id_seq')`); got != srcLast+1 {
		t.Fatalf("next nextval = %d, want %d", got, srcLast+1)
	}
	// The destination column default resolves to the sequence (the serial
	// default was copied verbatim and the sequence exists to back it).
	if got := mustCount(t, dst, `SELECT count(*) FROM public.seq_t`); got != 100 {
		t.Fatalf("seq_t rows changed after nextval probe: %d", got)
	}

	// events.jsonl records the sync (F-04).
	evs := readEventLines(t, jdir)
	if !evsContain(evs, `"kind":"sequence_synced"`) || !evsContain(evs, `"sequence":"public.seq_t_id_seq"`) {
		t.Fatalf("events.jsonl missing sequence_synced for seq_t_id_seq: %v", evs)
	}
}

// TestSequenceSyncResumeIdempotent — F-11 + F-06/FR-07: a re-run over an
// already-synced destination must not corrupt the sequence (CREATE SEQUENCE
// IF NOT EXISTS + setval are idempotent): the value stays at the source
// last_value, no duplicate rows.
func TestSequenceSyncResumeIdempotent(t *testing.T) {
	src, dst, _, _ := startPGPair(t, 5683, 5684)
	defer func() { src.Close(); dst.Close() }()

	ctx := context.Background()
	mustExec(t, src, `CREATE TABLE public.seq_t (id serial PRIMARY KEY, val text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.seq_t (val) SELECT 'v' || g FROM generate_series(1,50) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	srcLast := mustCount(t, src, `SELECT last_value FROM public.seq_t_id_seq`)
	if srcLast != 50 {
		t.Fatalf("source last_value = %d, want 50 (test premise broken)", srcLast)
	}

	cfg := config.Default()
	cfg.WorkersPerPair = 1
	cfg.ConflictAction = config.Overwrite // the second run rewrites the table
	p := buildPlan(t, cfg, src, dst)

	jdir := t.TempDir()
	j := manifest.NewJournal(dst, filepath.Join(jdir, "journal.jsonl"))
	mgr := newTwoAgentManager(t, &cfg, src, dst, j, src.DSN(), dst.DSN())
	if err := runWithTimeout(t, mgr, p, 60*time.Second); err != nil {
		t.Fatalf("first run: %v", err)
	}
	// A second run over the same destination (the table + sequence exist).
	p2 := buildPlan(t, cfg, src, dst)
	if err := runWithTimeout(t, mgr, p2, 60*time.Second); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if got := mustCount(t, dst, `SELECT count(*) FROM public.seq_t`); got != 50 {
		t.Fatalf("seq_t: %d rows after re-run, want 50 (duplicates?)", got)
	}
	if got := mustCount(t, dst, `SELECT last_value FROM public.seq_t_id_seq`); got != srcLast {
		t.Fatalf("last_value after re-run = %d, want %d (idempotent)", got, srcLast)
	}
}

// readEventLines reads every line of the run's events.jsonl (a missing file
// is an empty list — the run emitted nothing).
func readEventLines(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read events.jsonl: %v", err)
	}
	return splitLines(string(b))
}

// evsContain reports whether any line contains sub.
func evsContain(evs []string, sub string) bool {
	for _, l := range evs {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}
