package dryrun

import (
	"context"
	"testing"
)

// TestFromSidesCopyToProgramGUC: the mesh on_segment data path launches the
// segreceivers via `COPY ... TO PROGRAM ... ON SEGMENT`, which the Yandex GP
// fork gates behind the ycmdb.yc_allow_copy_to_program GUC (PGC_POSTMASTER).
// The dry-run must surface a PRESENT-but-off GUC as a HARD BLOCKER for
// --copy-mode mesh (the run would otherwise die on the first unit with
// "forbidden to COPY to or from an external program" — the INC8 live run,
// where D was the source and the GUC was off by default). A MISSING GUC
// (a non-Yandex build, where COPY TO PROGRAM is allowed by default) must NOT
// block; the coordinator path (COPY ... TO STDOUT) is unaffected either way.
func TestFromSidesCopyToProgramGUC(t *testing.T) {
	baseSrc := func(guc string) *SideValues {
		return &SideValues{Side: "source", Reachable: true, Version: "Greenplum 6.x",
			Greenplum: true, UpCount: 3, HasCreate: true, DiskOK: true,
			Collation: "C", CollationSig: "sig", Encoding: "UTF8",
			Extensions: []string{"plpgsql"}, CopyToProgramGUC: guc}
	}
	dst := &SideValues{Side: "dest", Reachable: true, Version: "Greenplum 6.x",
		Greenplum: true, UpCount: 3, HasCreate: true, DiskOK: true,
		Collation: "C", CollationSig: "sig", Encoding: "UTF8",
		Extensions: []string{"plpgsql"}}

	t.Run("guc on + mesh = PASS, healthy", func(t *testing.T) {
		rep := FromSides(baseSrc("on"), dst, Options{CopyMode: "mesh"})
		if !rep.Healthy {
			t.Fatalf("GUC=on must not block: %d FAILs: %+v", rep.Failures(), rep.Sorted())
		}
		if got := find(rep, "source", "copy_to_program_guc"); got != Pass {
			t.Errorf("GUC=on: got %v, want PASS", got)
		}
	})

	t.Run("guc off + mesh = FAIL (blocker), unhealthy", func(t *testing.T) {
		rep := FromSides(baseSrc("off"), dst, Options{CopyMode: "mesh"})
		if rep.Healthy {
			t.Fatalf("GUC=off + mesh must be UNHEALTHY (hard blocker)")
		}
		if got := find(rep, "source", "copy_to_program_guc"); got != Fail {
			t.Errorf("GUC=off + mesh: got %v, want FAIL", got)
		}
		// The recommendation must point at the fix (GUC in postgresql.conf +
		// restart) and the coordinator fallback.
		for _, c := range rep.Sorted() {
			if c.Name == "copy_to_program_guc" && c.Sev == Fail {
				if c.Rec == "" {
					t.Errorf("FAIL finding must carry a recommendation (how to fix)")
				}
			}
		}
	})

	t.Run("guc off + coordinator = WARN (not a blocker), healthy", func(t *testing.T) {
		rep := FromSides(baseSrc("off"), dst, Options{CopyMode: "coordinator"})
		if !rep.Healthy {
			t.Fatalf("GUC=off + coordinator must stay healthy (the coordinator path uses COPY TO STDOUT): %+v", rep.Sorted())
		}
		if got := find(rep, "source", "copy_to_program_guc"); got != Warn {
			t.Errorf("GUC=off + coordinator: got %v, want WARN", got)
		}
	})

	t.Run("guc absent (non-Yandex build) + mesh = no finding, healthy", func(t *testing.T) {
		rep := FromSides(baseSrc(""), dst, Options{CopyMode: "mesh"})
		if !rep.Healthy {
			t.Fatalf("GUC absent must not block a mesh run (non-Yandex build allows COPY TO PROGRAM): %+v", rep.Sorted())
		}
		// No copy_to_program_guc finding at all (find() defaults to Pass when
		// absent, so check the report directly).
		for _, c := range rep.Checks {
			if c.Name == "copy_to_program_guc" {
				t.Errorf("GUC absent: unexpected finding %v: %s", c.Sev, c.Msg)
			}
		}
	})
}

// TestGatherSideCollectsCopyToProgramGUC proves the SOURCE-side gather reads
// the GUC from pg_settings (the dest side does not need it) and records both
// the value and its source.
func TestGatherSideCollectsCopyToProgramGUC(t *testing.T) {
	q := &scriptedQuerier{
		queries: map[string][][]any{
			"SELECT version();": {{`Greenplum 6.x`}},
			// pg_settings: the GUC row (present, off — the INC8 case).
			"SELECT setting, source FROM pg_settings WHERE name = 'ycmdb.yc_allow_copy_to_program'": {{"off", "default"}},
			"SHOW statement_timeout;":                                               {{"0"}},
			"SHOW idle_in_transaction_session_timeout;":                             {{"0"}},
			"SHOW max_connections;":                                                 {{"250"}},
			"SELECT datcollate FROM pg_database WHERE datname = current_database()": {{"C"}},
			`SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()`: {{"UTF8"}},
			"SELECT 1": {{"1"}},
			"SELECT has_database_privilege(current_database(), 'CREATE'), has_database_privilege(current_database(), 'CONNECT')": {{1, 1}},
		},
	}
	sv := GatherSide(context.Background(), q, "source", 2)
	if sv.CopyToProgramGUC != "off" {
		t.Fatalf("GUC value: got %q, want %q", sv.CopyToProgramGUC, "off")
	}
	if sv.CopyToProgramGUCSource != "default" {
		t.Errorf("GUC source: got %q, want %q", sv.CopyToProgramGUCSource, "default")
	}

	// A build without the GUC (the pg_settings query returns no rows): the
	// field stays empty (distinguishable from "off").
	q2 := &scriptedQuerier{queries: map[string][][]any{
		"SELECT version();": {{`PostgreSQL 9.4`}},
		"SELECT setting, source FROM pg_settings WHERE name = 'ycmdb.yc_allow_copy_to_program'": {},
		"SHOW statement_timeout;":                                               {{"0"}},
		"SHOW idle_in_transaction_session_timeout;":                             {{"0"}},
		"SHOW max_connections;":                                                 {{"100"}},
		"SELECT datcollate FROM pg_database WHERE datname = current_database()": {{"C"}},
		`SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()`: {{"UTF8"}},
		"SELECT 1": {{"1"}},
		"SELECT has_database_privilege(current_database(), 'CREATE'), has_database_privilege(current_database(), 'CONNECT')": {{1, 1}},
	}}
	sv2 := GatherSide(context.Background(), q2, "source", 2)
	if sv2.CopyToProgramGUC != "" {
		t.Fatalf("absent GUC: got %q, want empty", sv2.CopyToProgramGUC)
	}
}

// scriptedQuerier answers Query() from a map (the dryrun GatherSide contract:
// exact-match SQL -> rows; unknown SQL -> empty).
type scriptedQuerier struct {
	queries map[string][][]any
}

func (q *scriptedQuerier) Query(ctx context.Context, sql string, args ...any) ([][]any, error) {
	if rows, ok := q.queries[sql]; ok {
		return rows, nil
	}
	return nil, nil
}
func (q *scriptedQuerier) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return 0, nil
}
func (q *scriptedQuerier) Ping(ctx context.Context) error { return nil }
func (q *scriptedQuerier) Close()                         {}
