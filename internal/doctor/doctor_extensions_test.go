package doctor

import (
	"context"
	"strings"
	"testing"
)

// extensionRow models the single-column pg_extension result (extname).
func extRow(names ...string) [][]any {
	rows := make([][]any, 0, len(names))
	for _, n := range names {
		rows = append(rows, []any{n})
	}
	return rows
}

// TestDoctorExtensionsMissing is the FR-05/FR-11 criterion: an extension
// present in the source database but absent from the destination (the classic
// MADlib/PostGIS/orafce restore trap) must be surfaced as a WARN naming the
// exact missing extension(s), without failing the doctor.
func TestDoctorExtensionsMissing(t *testing.T) {
	src := healthy()
	dst := healthy()
	// Source DB has plpgsql + postgis; dest only plpgsql.
	src.On("pg_extension", extRow("plpgsql", "postgis"))
	dst.On("pg_extension", extRow("plpgsql"))

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if !rep.Healthy {
		t.Fatalf("missing extension must not FAIL the doctor, got %d FAILs", rep.Failures())
	}
	if find(rep, "network", "extensions") != Warn {
		t.Fatalf("extensions: got %v, want WARN", find(rep, "network", "extensions"))
	}
	for _, c := range rep.Checks {
		if c.Name == "extensions" && c.Side == "network" {
			if !strings.Contains(c.Msg, "postgis") {
				t.Errorf("missing-extension finding must name postgis: %q", c.Msg)
			}
			if !strings.Contains(c.Rec, "CREATE EXTENSION") {
				t.Errorf("recommendation must say CREATE EXTENSION: %q", c.Rec)
			}
		}
	}
}

// TestDoctorExtensionsAllPresent: no source extension missing on the dest ->
// PASS, no WARN.
func TestDoctorExtensionsAllPresent(t *testing.T) {
	src := healthy()
	dst := healthy()
	src.On("pg_extension", extRow("plpgsql", "madlib"))
	dst.On("pg_extension", extRow("plpgsql", "madlib"))

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if find(rep, "network", "extensions") != Pass {
		t.Fatalf("all-present extensions: got %v, want PASS", find(rep, "network", "extensions"))
	}
	if !rep.Healthy {
		t.Fatalf("expected healthy, got %d FAILs", rep.Failures())
	}
}

// TestDoctorExtensionsQueryFault: an unreadable pg_extension degrades to WARN
// (with the error), never FAIL, and never blocks the rest of the report.
func TestDoctorExtensionsQueryFault(t *testing.T) {
	src := healthy()
	dst := healthy()
	src.On("pg_extension", extRow("plpgsql"))
	dst.Off("pg_extension") // dest cannot answer the extension query

	// Off() removes the canned result, but the extension SQL contains
	// "pg_extension" — ensure no other substring also matches it. The
	// "to_regclass" probe and catalog queries are disjoint.
	_ = src
	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if find(rep, "network", "extensions") != Warn {
		t.Fatalf("extension query fault: got %v, want WARN", find(rep, "network", "extensions"))
	}
	if !rep.Healthy {
		t.Fatalf("extension query fault must not FAIL the doctor")
	}
}
