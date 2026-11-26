package dryrun

import "testing"

// find returns the severity of the first check with the given side+name.
func find(r *Report, side, name string) Severity {
	for _, c := range r.Checks {
		if c.Side == side && c.Name == name {
			return c.Sev
		}
	}
	return Pass
}

// TestFromSidesDetectsFaults is the CR-7 criterion on the production path:
// seeded faults across source and destination must each be surfaced (no fault
// is silently passed). The faults are SideValues the agents would report.
func TestFromSidesDetectsFaults(t *testing.T) {
	src := &SideValues{Side: "source", Reachable: false, ReachErr: "connection refused"}
	dst := &SideValues{Side: "dest", Reachable: true,
		Version: "",        // version query cannot be answered -> FAIL
		Greenplum: true, UpCount: 0, // no up segments -> FAIL
		HasCreate: false,   // no database privileges -> WARN
		DiskOK: false, DiskErr: "disk probe returned no data",
		Collation: "C.UTF-8",
		Extensions: []string{"plpgsql"},
	}
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if rep.Healthy {
		t.Fatalf("expected unhealthy with injected faults")
	}
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
	if n := rep.Failures(); n < 3 {
		t.Fatalf("expected >=3 FAILs from injected faults, got %d", n)
	}
}

// TestFromSidesDetectsSourceSideFaults mirrors the fault matrix on the SOURCE
// side to prove both sides are checked — a one-sided checker would miss
// source-side faults (CR-7).
func TestFromSidesDetectsSourceSideFaults(t *testing.T) {
	src := &SideValues{Side: "source", Reachable: true,
		Version: "",        // FAIL
		Greenplum: true, UpCount: 0, // FAIL
		HasCreate: false,   // WARN
		DiskOK: true,
		Collation: "C",
	}
	dst := &SideValues{Side: "dest", Reachable: true,
		Version: "Greenplum 6.x", Greenplum: true, UpCount: 3,
		HasCreate: true, DiskOK: true, Collation: "C",
	}
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
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

// TestFromSidesCleanBench is the control: a fault-free pair of sides must
// report healthy with zero FAILs (no false positives, CR-7).
func TestFromSidesCleanBench(t *testing.T) {
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
	rep := FromSides(src, dst, Options{MinTLS: "1.2"})
	if !rep.Healthy {
		t.Fatalf("expected a healthy report for a clean bench, got %d FAILs: %v", rep.Failures(), rep.Sorted())
	}
	if rep.Failures() != 0 {
		t.Errorf("clean bench should have 0 FAILs, got %d", rep.Failures())
	}
}
