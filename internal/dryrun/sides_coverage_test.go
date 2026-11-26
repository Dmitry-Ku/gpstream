package dryrun

import (
	"strings"
	"testing"
)

// healthySides returns a matching fault-free pair (the clean-bench baseline).
func healthySides() (*SideValues, *SideValues) {
	src := &SideValues{Side: "source", Reachable: true,
		Version: "Greenplum 6.x", Greenplum: true, UpCount: 3,
		HasCreate: true, DiskOK: true, Collation: "C", CollationSig: "sig",
		Encoding: "UTF8", Extensions: []string{"plpgsql"},
	}
	dst := &SideValues{Side: "dest", Reachable: true,
		Version: "Greenplum 6.x", Greenplum: true, UpCount: 3,
		HasCreate: true, DiskOK: true, Collation: "C", CollationSig: "sig",
		Encoding: "UTF8", Extensions: []string{"plpgsql"},
		DestCollationsInstalled: []string{"default", "C"},
	}
	return src, dst
}

// TestFromSidesCollationBehaviorDiff verifies (F-16) that two clusters with
// the SAME locale name but different glibc sort behavior (the behavioral
// probe signature differs) surface a "collation-behavior" WARN — the name
// comparison alone is a false negative (e.g. EL7 vs EL8 en_US.utf8).
func TestFromSidesCollationBehaviorDiff(t *testing.T) {
	src, dst := healthySides()
	src.CollationSig = "glibc-7"
	dst.CollationSig = "glibc-8"
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if found := find(rep, "network", "collation-behavior"); found != Warn {
		t.Fatalf("behavior diff: got %v, want WARN", found)
	}
	// The DB-level name comparison still passes (same name).
	if found := find(rep, "network", "collation"); found != Pass {
		t.Errorf("same name: got %v, want PASS", found)
	}
}

// TestFromSidesCollationBehaviorProbeFault verifies a probe fault on either
// side degrades to NO finding (never blocks the run).
func TestFromSidesCollationBehaviorProbeFault(t *testing.T) {
	src, dst := healthySides()
	src.CollationSigErr = "query canceled"
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if hasCheck(rep, "network", "collation-behavior") {
		t.Fatalf("probe fault must not raise a collation-behavior finding")
	}
	if !rep.Healthy {
		t.Fatalf("probe fault must not make the report unhealthy")
	}
}

// TestFromSidesCollationBehaviorSame verifies identical signatures raise no
// behavioral finding.
func TestFromSidesCollationBehaviorSame(t *testing.T) {
	src, dst := healthySides()
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if hasCheck(rep, "network", "collation-behavior") {
		t.Fatalf("matching signatures must raise no collation-behavior finding")
	}
}

// TestFromSidesColumnCollationInstalled verifies (F-16) that no
// column-collation WARN is raised when every source collation is installed
// on the destination.
func TestFromSidesColumnCollationInstalled(t *testing.T) {
	src, dst := healthySides()
	src.SourceCollationsUsed = []string{"C.utf8"}
	dst.DestCollationsInstalled = []string{"default", "C", "C.utf8"}
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if hasCheck(rep, "network", "column-collation") {
		t.Fatalf("installed collation must raise no column-collation finding")
	}
}

// TestFromSidesColumnCollationNone verifies a source with no explicit column
// collation raises no column-collation finding.
func TestFromSidesColumnCollationNone(t *testing.T) {
	src, dst := healthySides()
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if hasCheck(rep, "network", "column-collation") {
		t.Fatalf("no explicit collation must raise no column-collation finding")
	}
}

// TestFromSidesExtensionsMissing is the FR-05/FR-11 criterion: an extension
// present in the source database but absent from the destination must be
// surfaced as a WARN naming the exact missing extension(s), without failing
// the diagnostic.
func TestFromSidesExtensionsMissing(t *testing.T) {
	src, dst := healthySides()
	src.Extensions = []string{"plpgsql", "postgis"}
	dst.Extensions = []string{"plpgsql"}
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if !rep.Healthy {
		t.Fatalf("missing extension must not FAIL the diagnostic, got %d FAILs", rep.Failures())
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

// TestFromSidesEncodingSame verifies no encoding finding when the encodings
// match.
func TestFromSidesEncodingSame(t *testing.T) {
	src, dst := healthySides()
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if hasCheck(rep, "network", "encoding") {
		t.Fatalf("no encoding finding expected for matching encodings")
	}
}

// TestFromSidesEncodingUndetermined verifies an undetermined encoding (empty
// on either side) degrades to no finding.
func TestFromSidesEncodingUndetermined(t *testing.T) {
	src, dst := healthySides()
	src.Encoding = ""
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if hasCheck(rep, "network", "encoding") {
		t.Fatalf("undetermined encoding must raise no finding")
	}
	if !rep.Healthy {
		t.Fatalf("undetermined encoding must not make the report unhealthy")
	}
}
