package doctor

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// healthy returns a fake that answers every doctor query with healthy values.
// FakeConn matches by longest substring, so the probe is registered under
// "to_regclass" (the probe SQL does NOT contain the catalog key) and the
// catalog under "FROM gp_segment_configuration" (the probe SQL does not
// contain "FROM " + the table name) — the two queries resolve disjointly.
// Catalog rows: content, role, preferred_role, mode, status, hostname, port,
// datname.
func healthy() *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("version", [][]any{{"Greenplum 6.x (gpstream demo)"}})
	f.On("to_regclass", [][]any{{int64(16384)}}) // probe -> OID (Greenplum)
	f.On("FROM gp_segment_configuration", [][]any{
		{int64(0), "p", "p", "n", "u", "host0", 6000, "dwh"},
		{int64(1), "p", "p", "n", "u", "host1", 6000, "dwh"},
		{int64(0), "m", "s", "n", "u", "host2", 7000, "dwh"},
		{int64(1), "m", "s", "n", "u", "host3", 7000, "dwh"},
		{int64(-1), "p", "p", "n", "u", "coordinator", 5432, "dwh"},
	})
	f.On("has_database_privilege", [][]any{{int64(1), int64(1)}})
	f.On("pg_database", [][]any{{"C"}})
	f.On("SELECT 1", [][]any{{int64(1)}})
	return f
}

// TestDoctorHealthy verifies a clean bench reports PASS with no FAIL.
func TestDoctorHealthy(t *testing.T) {
	rep := Run(context.Background(), healthy(), healthy(), Options{MinTLS: "1.2"})
	if !rep.Healthy {
		t.Fatalf("expected healthy, got %d FAILs", rep.Failures())
	}
}

// plainPG returns a fake modeling plain PostgreSQL 9.4.26 (the GP6 kernel) —
// no Greenplum catalogs exist. The to_regclass probe answers an empty row,
// which Row/RowStr surface as NULL: plain-PG single-node mode.
func plainPG() *pg.FakeConn {
	f := pg.NewFakeConn()
	f.On("version", [][]any{{"PostgreSQL 9.4.26 on x86_64-unknown-linux-gnu"}})
	f.On("gp_segment_configuration", [][]any{{}}) // probe -> NULL (catalog absent)
	f.On("has_database_privilege", [][]any{{int64(1), int64(1)}})
	f.On("pg_database", [][]any{{"C"}})
	f.On("SELECT 1", [][]any{{int64(1)}})
	return f
}

// TestDoctorPlainPGMode is the M0 regression: against plain PostgreSQL (no
// gp_segment_configuration) the doctor must degrade to WARN single-node mode
// and stay healthy — never a hard FAIL on a working GP6-kernel instance.
func TestDoctorPlainPGMode(t *testing.T) {
	rep := Run(context.Background(), plainPG(), plainPG(), Options{MinTLS: "1.2"})
	if !rep.Healthy {
		t.Fatalf("plain PG (GP6 kernel) must be healthy, got %d FAILs", rep.Failures())
	}
	for _, side := range []string{"source", "dest"} {
		if found := find(rep, side, "segments"); found != Warn {
			t.Errorf("%s segments on plain PG: got %v, want WARN", side, found)
		}
	}
}

// TestDoctorDetectsFaults is the CР-7 criterion: the doctor must detect every
// seeded fault. We inject distinct faults across source/dest and assert each
// is surfaced (no fault is silently passed).
func TestDoctorDetectsFaults(t *testing.T) {
	src := healthy()
	dst := healthy()

	// Fault 1: source unreachable (ping fails)
	src.SetPingError(errUnreachable)
	// Fault 2: dest version query cannot be answered -> FAIL
	dst.Off("version")
	// Fault 3: dest has no up segments (all primaries down) -> FAIL
	dst.On("FROM gp_segment_configuration", [][]any{
		{int64(0), "p", "p", "n", "d", "host0", 6000, "dwh"},
		{int64(1), "p", "p", "n", "d", "host1", 6000, "dwh"},
	})
	// Fault 4: dest missing gp_toolkit EXECUTE -> WARN
	dst.On("has_database_privilege", [][]any{{int64(0), int64(1)}})
	// Fault 5: dest disk probe cannot be answered -> FAIL
	dst.Off("SELECT 1")
	// Fault 6: collation differs (source=C, dest=C.UTF-8) -> WARN
	src.On("pg_database", [][]any{{"C"}})
	dst.On("pg_database", [][]any{{"C.UTF-8"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if rep.Healthy {
		t.Fatalf("expected unhealthy with injected faults")
	}
	// Each injected fault must appear as a FAIL/WARN finding (not a PASS).
	if found := find(rep, "source", "reachability"); found != Fail {
		t.Errorf("source unreachable: got %v, want FAIL", found)
	}
	if found := find(rep, "dest", "version"); found != Fail {
		t.Errorf("dest version: got %v, want FAIL", found)
	}
	if found := find(rep, "dest", "segments"); found != Fail {
		t.Errorf("dest segments: got %v, want FAIL", found)
	}
	if found := find(rep, "dest", "permissions"); found != Warn {
		t.Errorf("dest permissions: got %v, want WARN", found)
	}
	if found := find(rep, "dest", "disk"); found != Fail {
		t.Errorf("dest disk: got %v, want FAIL", found)
	}
	if found := find(rep, "network", "collation"); found != Warn {
		t.Errorf("collation difference: got %v, want WARN", found)
	}
	if rep.Failures() < 3 {
		t.Fatalf("expected >=3 FAILs from injected faults, got %d", rep.Failures())
	}
}

// find returns the severity of the first check with the given side+name.
func find(r *Report, side, name string) Severity {
	for _, c := range r.Checks {
		if c.Side == side && c.Name == name {
			return c.Sev
		}
	}
	return Pass
}

// errUnreachable simulates an unreachable cluster.
var errUnreachable = context.Canceled
