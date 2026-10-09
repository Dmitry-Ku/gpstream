//go:build gp6lab

package integration

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"debug/buildinfo"

	"github.com/gpstream/gpstream/internal/config"
	"github.com/gpstream/gpstream/internal/manifest"
	"github.com/gpstream/gpstream/internal/pgtest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// raceEnabled reports whether this test binary was built with -race (the
// detector slows the whole binary ~3x, so the throughput floor is relaxed).
func raceEnabled() bool {
	exe, err := os.Executable()
	if err != nil {
		return false
	}
	bi, err := buildinfo.ReadFile(exe)
	if err != nil {
		return false
	}
	for _, s := range bi.Settings {
		if s.Key == "-race" && s.Value == "true" {
			return true
		}
	}
	return false
}

// parseSize parses a "512M"/"1G" size for the A/B override.
func parseSize(s string) int64 {
	s = strings.TrimSpace(s)
	mult := int64(1)
	if s != "" {
		switch strings.ToLower(string(s[len(s)-1])) {
		case "k":
			mult = 1 << 10
			s = s[:len(s)-1]
		case "m":
			mult = 1 << 20
			s = s[:len(s)-1]
		case "g":
			mult = 1 << 30
			s = s[:len(s)-1]
		case "t":
			mult = 1 << 40
			s = s[:len(s)-1]
		}
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n * mult
}

// T7 (DESIGN §6): speed soak — sustained-throughput characterization of
// the coordinator data path on real PG 9.4.26 clusters (the GP6 kernel).
//
// This is a CHARACTERIZATION test, not a 100 Gbit/s gate (that needs the GP6
// self-hosted runner — see RC-CHECKLIST.md): it copies a ~600 MB table
// (15M rows) with 4 parallel stream workers (cmdRun shape, no rate limit)
// and asserts
//
//  1. the run completes with exact row counts and digests (no corruption
//     under sustained load), and
//  2. the observed end-to-end throughput (plan+copy+verify+swap on one
//     local box, single PG instance per side) clears the single-box floor.
//     The GP6 T0-4 numbers (~110–133 MB/s) come from a DISTRIBUTED source
//     (segments read in parallel) and do not bound a single local box; the
//     measured baseline here is ~45 MB/s. The floor is deliberately low so
//     the test is stable on a loaded CI box; it exists to catch a data-path
//     regression that turns the copy into a trickle (e.g. serialization or
//     a full re-read per chunk).
func TestSoakThroughputLive(t *testing.T) {
	const (
		nRows   = 15_000_000 // ~600 MB heap of (id int, v text)
		maxTime = 600 * time.Second
	)
	minMBps := 25.0 // single-box floor, see above
	// The -race detector slows the ENTIRE binary (both PG instances + data
	// path) to roughly a third of its non-race speed; a clean-HEAD race run
	// measures ~13 MB/s here, so the floor is unachievable under race by
	// construction. Keep the assertion for the production (non-race) build,
	// where it catches a real data-path regression; under race assert
	// correctness only (row counts + digest) and log the throughput.
	if raceEnabled() {
		t.Logf("soak: -race build — throughput floor skipped (race detector slows the whole binary ~3x); asserting correctness only")
		minMBps = 0
	}

	ctx := context.Background()
	srcCl, err := pgtest.Start(5551, "srcdb")
	if err != nil {
		t.Fatalf("start src: %v", err)
	}
	t.Cleanup(srcCl.Stop)
	dstCl, err := pgtest.Start(5552, "dstdb")
	if err != nil {
		t.Fatalf("start dst: %v", err)
	}
	t.Cleanup(dstCl.Stop)

	src, err := pg.NewPool(ctx, srcCl.URL(), "", "", "", "", 8)
	if err != nil {
		t.Fatalf("src pool: %v", err)
	}
	dst, err := pg.NewPool(ctx, dstCl.URL(), "", "", "", "", 8)
	if err != nil {
		src.Close()
		t.Fatalf("dst pool: %v", err)
	}
	defer func() { src.Close(); dst.Close() }()

	mustExec(t, src, `CREATE TABLE public.soak (id int, v text)`)
	if _, err := src.Exec(ctx,
		`INSERT INTO public.soak SELECT g, 'v' || g FROM generate_series(1,`+itoa(nRows)+`) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Source digest at the (static) snapshot point — no concurrent writer.
	srcDigest := mustDigest(t, src, "public.soak")

	cfg := config.Default()
	cfg.WorkersPerPair = 4 // cmdRun default shape: parallel stream workers
	// A/B override for data-path experiments (e.g. GPSTREAM_FRAME_ACK=off).
	if v := os.Getenv("GPSTREAM_FRAME_ACK"); v != "" {
		cfg.FrameAck = v
	}
	if v := os.Getenv("GPSTREAM_RETRANSMIT_BUFFER_MAX"); v != "" {
		cfg.RetransmitBufferMax = parseSize(v)
	}
	p := buildPlan(t, cfg, src, dst)
	j := manifest.NewJournal(dst, t.TempDir()+"/journal.jsonl")
	mgr := newTwoAgentManager(t, &cfg, j, src.DSN(), dst.DSN())

	start := time.Now()
	if err := runWithTimeout(t, mgr, p, maxTime); err != nil {
		t.Fatalf("soak run: %v", err)
	}
	elapsed := time.Since(start)

	if got := mustCount(t, dst, `SELECT count(*) FROM public.soak`); got != nRows {
		t.Fatalf("soak: dest has %d rows, want %d", got, nRows)
	}
	dstDigest := mustDigest(t, dst, "public.soak")
	if dstDigest != srcDigest {
		t.Fatalf("soak: digest mismatch after sustained copy")
	}

	// Heap byte count of the destination table — an honest wire-size proxy
	// for the coordinator path (the COPY stream carries the same row data).
	var relBytes int64
	rows, err := dst.Query(ctx, `SELECT pg_relation_size('public.soak')`)
	if err != nil {
		t.Fatalf("pg_relation_size: %v", err)
	}
	switch v := rows[0][0].(type) {
	case int64:
		relBytes = v
	case int32:
		relBytes = int64(v)
	default:
		t.Fatalf("pg_relation_size: unexpected type %T", rows[0][0])
	}

	mbps := float64(relBytes) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("soak: %d rows, %.1f MB heap, %s end-to-end (plan+copy+verify+swap), ~%.0f MB/s",
		nRows, float64(relBytes)/(1024*1024), elapsed.Round(time.Millisecond), mbps)
	if mbps < minMBps {
		t.Fatalf("soak throughput %.1f MB/s is below the %.0f MB/s floor (data-path regression?)", mbps, minMBps)
	}
}
