package dryrun

import "testing"

// TestFromSidesDiffIsPure (2C-3, architecture 08.10) proves the controller-side
// diff (FromSides) is PURE: it reads two sides' gathered values and produces
// the same findings as the legacy controller-side diagnostic — without any
// database access (the controller holds no direct connection; the agents
// gathered the values). The diff checks (collation, encoding, extensions,
// route) are the ones that need BOTH sides.
func TestFromSidesDiffIsPure(t *testing.T) {
	// Two matching Greenplum sides: healthy, no diff findings.
	src := &SideValues{Side: "source", Reachable: true, Version: "Greenplum 6.x",
		Greenplum: true, UpCount: 3, HasCreate: true, DiskOK: true,
		Collation: "C", CollationSig: "sig", Encoding: "UTF8",
		Extensions: []string{"plpgsql", "postgis"}}
	dst := &SideValues{Side: "dest", Reachable: true, Version: "Greenplum 6.x",
		Greenplum: true, UpCount: 3, HasCreate: true, DiskOK: true,
		Collation: "C", CollationSig: "sig", Encoding: "UTF8",
		Extensions: []string{"plpgsql", "postgis"}}
	rep := FromSides(src, dst, Options{MinTLS: "1.2", Workers: 1})
	if !rep.Healthy {
		t.Fatalf("matching sides must be healthy, got %d FAILs: %+v", rep.Failures(), rep.Sorted())
	}
	if found := find(rep, "network", "collation"); found != Pass {
		t.Errorf("matching collation: got %v, want PASS", found)
	}
	if found := find(rep, "network", "extensions"); found != Pass {
		t.Errorf("all extensions installed: got %v, want PASS", found)
	}
	if found := find(rep, "src->dst", "route"); found != Pass {
		t.Errorf("route resolvable: got %v, want PASS", found)
	}

	// Mismatches: collation name differs, encoding differs, an extension is
	// missing on the dest, the dest has a column-collation the source uses.
	src2 := &SideValues{Side: "source", Reachable: true, Version: "v",
		Greenplum: true, UpCount: 2, HasCreate: true, DiskOK: true,
		Collation: "C", Encoding: "SQL_ASCII",
		Extensions:           []string{"plpgsql", "postgis"},
		SourceCollationsUsed: []string{"mycoll"}}
	dst2 := &SideValues{Side: "dest", Reachable: true, Version: "v",
		Greenplum: true, UpCount: 2, HasCreate: true, DiskOK: true,
		Collation: "C.utf8", Encoding: "UTF8",
		Extensions:              []string{"plpgsql"},
		DestCollationsInstalled: []string{"default"}}
	rep2 := FromSides(src2, dst2, Options{MinTLS: "1.2"})
	if found := find(rep2, "network", "collation"); found != Warn {
		t.Errorf("collation name differs: got %v, want WARN", found)
	}
	if found := find(rep2, "network", "encoding"); found != Warn {
		t.Errorf("encoding differs: got %v, want WARN", found)
	}
	if found := find(rep2, "network", "extensions"); found != Warn {
		t.Errorf("missing extension: got %v, want WARN", found)
	}
	if found := find(rep2, "network", "column-collation"); found != Warn {
		t.Errorf("missing column collation: got %v, want WARN", found)
	}
}

// TestFromSidesUnreachable (2C-3) proves the reachability FAIL short-circuits
// the side's other checks (an unreachable cluster is the only finding that
// side reports) — the same semantics as the legacy diagnostic.
func TestFromSidesUnreachable(t *testing.T) {
	src := &SideValues{Side: "source", Reachable: false, ReachErr: "connection refused"}
	dst := &SideValues{Side: "dest", Reachable: true, Version: "v",
		Greenplum: true, UpCount: 1, HasCreate: true, DiskOK: true}
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if rep.Healthy {
		t.Fatalf("an unreachable source must be unhealthy")
	}
	if found := find(rep, "source", "reachability"); found != Fail {
		t.Errorf("source reachability: got %v, want FAIL", found)
	}
	// The source's version check must NOT also fire (the side short-circuits).
	if hasCheck(rep, "source", "version") {
		t.Errorf("source version should be skipped when unreachable, but it is present")
	}
	// The dest is still checked.
	if found := find(rep, "dest", "reachability"); found != Pass {
		t.Errorf("dest reachability: got %v, want PASS", found)
	}
}

// TestFromSidesPlainPG (2C-3) proves the plain-PostgreSQL (no Greenplum
// catalog) side degrades to the single-node WARN, as the legacy diagnostic
// does (M0 regression on the GP6 kernel).
func TestFromSidesPlainPG(t *testing.T) {
	src := &SideValues{Side: "source", Reachable: true, Version: "PostgreSQL 9.4.26",
		Greenplum: false, HasCreate: true, DiskOK: true, Collation: "C", Encoding: "UTF8"}
	dst := &SideValues{Side: "dest", Reachable: true, Version: "PostgreSQL 9.4.26",
		Greenplum: false, HasCreate: true, DiskOK: true, Collation: "C", Encoding: "UTF8"}
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if !rep.Healthy {
		t.Fatalf("plain PG must be healthy, got %d FAILs", rep.Failures())
	}
	for _, side := range []string{"source", "dest"} {
		if found := find(rep, side, "segments"); found != Warn {
			t.Errorf("%s segments (plain PG): got %v, want WARN", side, found)
		}
	}
	// No Greenplum on either side -> the route is not resolvable (WARN).
	if found := find(rep, "src->dst", "route"); found != Warn {
		t.Errorf("route (plain PG): got %v, want WARN", found)
	}
}

// hasCheck reports whether a (side, name) check is present in the report
// (find returns Pass for both "present as PASS" and "absent", so absence is
// checked separately).
func hasCheck(r *Report, side, name string) bool {
	for _, c := range r.Checks {
		if c.Side == side && c.Name == name {
			return true
		}
	}
	return false
}
