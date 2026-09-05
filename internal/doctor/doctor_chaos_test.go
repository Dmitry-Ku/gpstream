package doctor

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/pg"
)

// TestDoctorChaosBench is the CР-7 acceptance criterion: seed the FULL fault
// matrix across source AND destination and assert the doctor detects every one
// (no fault is silently PASSed). Each check category must surface a FAIL/WARN.
func TestDoctorChaosBench(t *testing.T) {
	src := pg.NewFakeConn()
	dst := pg.NewFakeConn()
	// Start from a healthy baseline for any category not explicitly faulted.
	seedHealthy(src)
	seedHealthy(dst)

	// Fault 1 — source coordinator unreachable (ping fails).
	src.SetPingError(errUnreachable)
	// Fault 2 — dest version cannot be answered.
	dst.Off("version")
	// Fault 3 — dest has no up segments (count 0).
	dst.On("gp_segment_configuration", [][]any{{int64(0)}})
	// Fault 4 — dest missing gp_toolkit EXECUTE.
	dst.On("has_privilege", [][]any{{int64(0)}})
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

// TestDoctorChaosSourceSide mirrors the chaos bench but faults the SOURCE side
// (version/segments/permissions/disk) to prove both sides are checked — a
// one-sided checker would miss source-side faults (CР-7).
func TestDoctorChaosSourceSide(t *testing.T) {
	src := pg.NewFakeConn()
	dst := pg.NewFakeConn()
	seedHealthy(src)
	seedHealthy(dst)

	// Source version cannot be answered -> FAIL.
	src.Off("version")
	// Source has no up segments -> FAIL.
	src.On("gp_segment_configuration", [][]any{{int64(0)}})
	// Source missing privilege -> WARN.
	src.On("has_privilege", [][]any{{int64(0)}})

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

// TestDoctorCleanBench is the control: a fault-free bench must report healthy
// with zero FAILs, proving the checker does not false-positive (CР-7).
func TestDoctorCleanBench(t *testing.T) {
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
func seedHealthy(f *pg.FakeConn) {
	f.On("version", [][]any{{"Greenplum 6.x (gpstream demo)"}})
	f.On("gp_segment_configuration", [][]any{{int64(4)}})
	f.On("has_privilege", [][]any{{int64(1)}})
	f.On("pg_database", [][]any{{"C"}})
	f.On("SELECT 1", [][]any{{int64(1)}})
}
