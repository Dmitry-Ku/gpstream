package dryrun

import (
	"strings"
	"testing"
)

// hasNameResolution reports whether the report carries a name-resolution
// finding at all (find() cannot distinguish "absent" from "Pass").
func hasNameResolution(r *Report) bool {
	for _, c := range r.Checks {
		if c.Name == "name-resolution" {
			return true
		}
	}
	return false
}

func nameResolutionCheck(r *Report) (Check, bool) {
	for _, c := range r.Checks {
		if c.Name == "name-resolution" {
			return c, true
		}
	}
	return Check{}, false
}

// gpSide is a minimal healthy Greenplum side for the FromSides name
// resolution tests.
func gpSide(side string, up int, hosts ...string) *SideValues {
	return &SideValues{Side: side, Reachable: true, Version: "Greenplum 6.x",
		Greenplum: true, UpCount: up, HasCreate: true, DiskOK: true,
		Collation: "C", CollationSig: "sig", Encoding: "UTF8",
		Extensions:   []string{"plpgsql"},
		SegmentHosts: hosts}
}

// TestFromSidesNameResolution (mesh preflight, 14.10) proves the report
// gate: mesh + a dest with segment hostnames + an unresolvable pair = FAIL
// (the run as configured cannot succeed) with the /etc/hosts recommendation;
// all probes clean = Pass; no source segments = not-applicable Pass;
// non-mesh mode and a plain-PG destination produce NO finding at all.
func TestFromSidesNameResolution(t *testing.T) {
	// 1. Mesh, all names resolve -> Pass with the probe count.
	src := gpSide("source", 3, "vm3")
	src.NameChecksProbed = 2 // 1 host x 2 dest hostnames
	dst := gpSide("dest", 2, "vm5", "vm6")
	rep := FromSides(src, dst, Options{MinTLS: "1.2", Workers: 1, CopyMode: "mesh"})
	c, ok := nameResolutionCheck(rep)
	if !ok || c.Sev != Pass {
		t.Fatalf("mesh all-resolve: finding = %+v (ok=%v), want Pass", c, ok)
	}
	if !strings.Contains(c.Msg, "2 destination segment hostname(s)") || !strings.Contains(c.Msg, "2 probes") {
		t.Errorf("mesh all-resolve: message missing the counts: %s", c.Msg)
	}

	// 2. Mesh, one name unresolvable -> FAIL + /etc/hosts recommendation,
	// and the report is unhealthy (the dry-run exits non-zero).
	src2 := gpSide("source", 3, "vm3")
	src2.NameChecksProbed = 2
	src2.NameCheckFailures = []string{"vm5 (from vm3)"}
	rep2 := FromSides(src2, dst, Options{MinTLS: "1.2", Workers: 1, CopyMode: "mesh"})
	c2, ok := nameResolutionCheck(rep2)
	if !ok || c2.Sev != Fail {
		t.Fatalf("mesh unresolvable: finding = %+v (ok=%v), want FAIL", c2, ok)
	}
	if !strings.Contains(c2.Msg, "vm5 (from vm3)") {
		t.Errorf("mesh unresolvable: message missing the failing pair: %s", c2.Msg)
	}
	if !strings.Contains(c2.Rec, "/etc/hosts") {
		t.Errorf("mesh unresolvable: recommendation missing the /etc/hosts fix: %s", c2.Rec)
	}
	if rep2.Healthy {
		t.Errorf("mesh unresolvable: the report must be unhealthy (a FAIL blocks the dry-run)")
	}

	// 3. Mesh, name checks requested but NO agent ran them (mixed versions:
	// an older source agent without the name probe) -> WARN "not checked",
	// NOT a FAIL (unverified, not failed), and the report stays healthy.
	src3u := gpSide("source", 3, "vm3")
	src3u.NameChecksUnsupported = true
	rep3u := FromSides(src3u, dst, Options{MinTLS: "1.2", Workers: 1, CopyMode: "mesh"})
	c3u, ok := nameResolutionCheck(rep3u)
	if !ok || c3u.Sev != Warn || !strings.Contains(c3u.Msg, "NOT checked") {
		t.Errorf("mesh mixed-version: finding = %+v (ok=%v), want Warn NOT checked", c3u, ok)
	}
	if !rep3u.Healthy {
		t.Errorf("mesh mixed-version: an unverified check must not make the report unhealthy")
	}

	// 4. Mesh, source has no segments of its own (plain-PG source): the
	// check did not run (0 probes) -> not-applicable Pass.
	src4 := &SideValues{Side: "source", Reachable: true, Version: "PostgreSQL",
		HasCreate: true, DiskOK: true, Collation: "C", CollationSig: "sig",
		Encoding: "UTF8", Extensions: []string{"plpgsql"}}
	rep4 := FromSides(src4, dst, Options{MinTLS: "1.2", Workers: 1, CopyMode: "mesh"})
	c4, ok := nameResolutionCheck(rep4)
	if !ok || c4.Sev != Pass || !strings.Contains(c4.Msg, "not applicable") {
		t.Errorf("mesh plain-PG source: finding = %+v (ok=%v), want not-applicable Pass", c4, ok)
	}

	// 5. Coordinator mode: no finding at all (the mode dials no dest
	// segment names — the 14.10 E1/E2 contrast).
	rep5 := FromSides(src, dst, Options{MinTLS: "1.2", Workers: 1, CopyMode: "coordinator"})
	if hasNameResolution(rep5) {
		t.Errorf("coordinator mode must not produce a name-resolution finding")
	}

	// 6. Mesh, plain-PG destination (no segment hostnames): no finding.
	dst6 := &SideValues{Side: "dest", Reachable: true, Version: "PostgreSQL",
		HasCreate: true, DiskOK: true, Collation: "C", CollationSig: "sig",
		Encoding: "UTF8", Extensions: []string{"plpgsql"}}
	rep6 := FromSides(src, dst6, Options{MinTLS: "1.2", Workers: 1, CopyMode: "mesh"})
	if hasNameResolution(rep6) {
		t.Errorf("plain-PG destination must not produce a name-resolution finding")
	}
}
