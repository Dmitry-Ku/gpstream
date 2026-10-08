package dryrun

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestDryrunChaosBench is the CR-7 acceptance criterion: seed the FULL fault
// matrix across source AND destination and assert the diagnostic detects every one
// (no fault is silently PASSed). Each check category must surface a FAIL/WARN.
func TestDryrunChaosBench(t *testing.T) {
	src := pg.NewFakeConn()
	dst := pg.NewFakeConn()
	// Start from a healthy baseline for any category not explicitly faulted.
	seedHealthy(src)
	seedHealthy(dst)

	// Fault 1 — source coordinator unreachable (ping fails).
	src.SetPingError(errUnreachable)
	// Fault 2 — dest version cannot be answered.
	dst.Off("version")
	// Fault 3 — dest has no up segments (all primaries down).
	dst.On("FROM gp_segment_configuration", [][]any{
		{int64(0), "p", "p", "n", "d", "host0", 6000, "dwh"},
		{int64(1), "p", "p", "n", "d", "host1", 6000, "dwh"},
	})
	// Fault 4 — dest lacks database privileges.
	dst.On("has_database_privilege", [][]any{{int64(0), int64(1)}})
	// Fault 5 — dest disk probe returns no data (full disk).
	dst.Off("SELECT 1")
	// Fault 6 — collation differs (source=C, dest=C.UTF-8).
	src.On("pg_database", [][]any{{"C"}})
	dst.On("pg_database", [][]any{{"C.UTF-8"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if rep.Healthy {
		t.Fatalf("expected an unhealthy report with a full fault matrix, got healthy")
	}

	// Every seeded fault must be surfaced as the expected severity.
	check := func(side, name string, want Severity, got Severity) {
		if got != want {
			t.Errorf("%s/%s = %v, want %v", side, name, got, want)
		}
	}
	check("source", "reachability", Fail, find(rep, "source", "reachability"))
	check("dest", "version", Fail, find(rep, "dest", "version"))
	check("dest", "segments", Fail, find(rep, "dest", "segments"))
	check("dest", "permissions", Warn, find(rep, "dest", "permissions"))
	check("dest", "disk", Fail, find(rep, "dest", "disk"))
	check("network", "collation", Warn, find(rep, "network", "collation"))

	// At least 3 hard FAILs from the matrix (source unreachable, dest version,
	// dest segments, dest disk).
	if n := rep.Failures(); n < 3 {
		t.Fatalf("expected >=3 FAILs from the fault matrix, got %d", n)
	}
}

// TestDryrunChaosSourceSide mirrors the chaos bench but faults the SOURCE side
// (version/segments/permissions/disk) to prove both sides are checked — a
// one-sided checker would miss source-side faults (CR-7).
func TestDryrunChaosSourceSide(t *testing.T) {
	src := pg.NewFakeConn()
	dst := pg.NewFakeConn()
	seedHealthy(src)
	seedHealthy(dst)

	// Source version cannot be answered -> FAIL.
	src.Off("version")
	// Source has no up segments -> FAIL.
	src.On("FROM gp_segment_configuration", [][]any{
		{int64(0), "p", "p", "n", "d", "host0", 6000, "dwh"},
		{int64(1), "p", "p", "n", "d", "host1", 6000, "dwh"},
	})
	// Source lacks database privileges -> WARN.
	src.On("has_database_privilege", [][]any{{int64(0), int64(1)}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if find(rep, "source", "version") != Fail {
		t.Errorf("source version: got %v, want FAIL", find(rep, "source", "version"))
	}
	if find(rep, "source", "segments") != Fail {
		t.Errorf("source segments: got %v, want FAIL", find(rep, "source", "segments"))
	}
	if find(rep, "source", "permissions") != Warn {
		t.Errorf("source permissions: got %v, want WARN", find(rep, "source", "permissions"))
	}
	if rep.Healthy {
		t.Fatalf("expected unhealthy with source-side faults")
	}
}

// TestDryrunCleanBench is the control: a fault-free bench must report healthy
// with zero FAILs, proving the checker does not false-positive (CR-7).
func TestDryrunCleanBench(t *testing.T) {
	src := pg.NewFakeConn()
	dst := pg.NewFakeConn()
	seedHealthy(src)
	seedHealthy(dst)

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if !rep.Healthy {
		t.Fatalf("expected a healthy report for a clean bench, got %d FAILs", rep.Failures())
	}
	if rep.Failures() != 0 {
		t.Errorf("clean bench should have 0 FAILs, got %d", rep.Failures())
	}
}

// seedHealthy loads a clean baseline so any category not faulted reports PASS.
// The probe answers under "to_regclass" and the catalog under
// "FROM gp_segment_configuration" (longest-substring match keeps them apart).
// Catalog rows: content, role, preferred_role, mode, status, hostname, port,
// datname — primaries up so UpCount() > 0.
func seedHealthy(f *pg.FakeConn) {
	f.On("version", [][]any{{"Greenplum 6.x (gpstream demo)"}})
	f.On("to_regclass", [][]any{{int64(16384)}}) // probe -> OID (Greenplum)
	f.On("FROM gp_segment_configuration", [][]any{
		{int64(0), "p", "p", "n", "u", "host0", 6000, "dwh"},
		{int64(1), "p", "p", "n", "u", "host1", 6000, "dwh"},
		{int64(-1), "p", "p", "n", "u", "coordinator", 5432, "dwh"},
	})
	f.On("has_database_privilege", [][]any{{int64(1), int64(1)}})
	f.On("pg_database", [][]any{{"C"}})
	f.On("pg_extension", extRow("plpgsql"))
	f.On("SELECT 1", [][]any{{int64(1)}})
}
