package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// behaviorFakes returns src/dst fakes with the given pg_database rows and
// behavioral collation probe answers (the CollationProbeSQL result as text).
func behaviorFakes(srcDB, dstDB, srcSig, dstSig string) (*pg.FakeConn, *pg.FakeConn) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{srcDB, "en_US.utf8", "en_US.utf8"}})
	src.On("array_agg(x ORDER BY x)", [][]any{{srcSig}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{dstDB, "en_US.utf8", "en_US.utf8"}})
	dst.On("array_agg(x ORDER BY x)", [][]any{{dstSig}})
	return src, dst
}

// TestDetectCollationsBehavioralDiffSameLocaleName is the Fix-2 core case
// (lab, 29.09): two clusters with the SAME LC_COLLATE name
// ("en_US.utf8") but DIFFERENT glibc — the name comparison alone is a false
// negative, the behavioral probe (CollationProbeSQL) catches it. The dry-run
// plan must report a DB-level collation diff.
func TestDetectCollationsBehavioralDiffSameLocaleName(t *testing.T) {
	src, dst := behaviorFakes("srcdb", "dstdb", `{1,"1",2}`, `{"1",1,2}`)
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/dstdb?sslmode=disable")
	if len(out) != 1 {
		t.Fatalf("got %d diffs, want 1 (behavioral glibc diff): %+v", len(out), out)
	}
	d := out[0]
	if d.IsColumn() {
		t.Fatalf("expected a DB-level diff, got %+v", d)
	}
	if d.DB != "srcdb" {
		t.Fatalf("diff.DB = %q, want srcdb", d.DB)
	}
	// Same locale name, so the message must say the names match but the
	// behavior differs (the operator's signal to check glibc).
	if !strings.Contains(d.LCCollate, "en_US.utf8 -> en_US.utf8") ||
		!strings.Contains(d.LCCollate, "DIFFERENT glibc sort behavior") {
		t.Fatalf("LCCollate = %q, want same-name + glibc-behavior annotation", d.LCCollate)
	}
}

// TestDetectCollationsBehavioralAndNameDiff verifies that a name diff AND a
// behavioral diff produce ONE combined DB-level finding (not two).
func TestDetectCollationsBehavioralAndNameDiff(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"srcdb", "C", "C"}})
	src.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"dstdb", "en_US.utf8", "en_US.utf8"}})
	dst.On("array_agg(x ORDER BY x)", [][]any{{`{"1",1,2}`}})
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/dstdb?sslmode=disable")
	if len(out) != 1 {
		t.Fatalf("got %d diffs, want 1 combined: %+v", len(out), out)
	}
	if out[0].IsColumn() || out[0].DB != "srcdb" {
		t.Fatalf("expected one DB-level diff for srcdb, got %+v", out)
	}
	if !strings.Contains(out[0].LCCollate, "C -> en_US.utf8") {
		t.Fatalf("LCCollate = %q, want the name pair", out[0].LCCollate)
	}
	if !strings.Contains(out[0].LCCollate, "behavioral probe") {
		t.Fatalf("LCCollate = %q, want the behavioral annotation", out[0].LCCollate)
	}
}

// TestDetectCollationsNoBehavioralDiff verifies that same locale name AND the
// same behavioral signature produce no finding (the probe is a detector, not
// a source of noise).
func TestDetectCollationsNoBehavioralDiff(t *testing.T) {
	src, dst := behaviorFakes("srcdb", "dstdb", `{1,"1",2}`, `{1,"1",2}`)
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/dstdb?sslmode=disable")
	if len(out) != 0 {
		t.Fatalf("got %d diffs, want 0 (same name + same behavior): %+v", len(out), out)
	}
}

// TestDetectCollationsProbeFaultDegrades verifies that a probe fault on
// either side degrades to the name-only comparison (never a plan failure,
// and never a spurious behavioral finding).
func TestDetectCollationsProbeFaultDegrades(t *testing.T) {
	// src answers the probe, dst has no canned answer -> probe errors on dst.
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"srcdb", "en_US.utf8", "en_US.utf8"}})
	src.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2}`}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"dstdb", "en_US.utf8", "en_US.utf8"}})
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/dstdb?sslmode=disable")
	if len(out) != 0 {
		t.Fatalf("probe fault must degrade to name-only (no diff), got %+v", out)
	}
}
