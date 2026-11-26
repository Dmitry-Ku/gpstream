// Package dryrun implements the self-diagnostic (FR-05) that reports
// PASS/FAIL/WARN for the source and destination clusters with actionable
// recommendations. It is invoked by `gpstream dry-run` before the plan is
// rendered.
//
// 2C (architecture 08.10): the controller holds NO direct database
// connection. Each side's AGENT gathers its own dry-run values on its LOCAL
// database connection (GatherSide) over the control plane; the controller
// collects both sides' SideValues and computes the diff in memory
// (FromSides). The agent-side SSH checks (on_segment, mini-readiness, mesh
// name resolution) live in dryrun_checks.go.
package dryrun

import (
	"sort"
	"strings"
	"time"
)

// Severity of a single check result.
type Severity int

const (
	Pass Severity = iota // green
	Warn                 // yellow: usable but attention needed
	Fail                 // red: blocks the job
)

func (s Severity) String() string {
	switch s {
	case Pass:
		return "PASS"
	case Warn:
		return "WARN"
	case Fail:
		return "FAIL"
	default:
		return "?"
	}
}

// Check is a single diagnostic finding.
type Check struct {
	Name string
	Side string // "source", "dest" or "network"
	Sev  Severity
	Msg  string
	Rec  string // recommendation; empty if none
}

// Report is the aggregated diagnostic output.
type Report struct {
	Checks  []Check
	Healthy bool // true only if no FAIL
}

// Add appends a check and (re)marks overall health.
func (r *Report) Add(c Check) {
	r.Checks = append(r.Checks, c)
	if c.Sev == Fail {
		r.Healthy = false
	}
}

// Fail returns the number of FAIL checks.
func (r *Report) Failures() int {
	n := 0
	for _, c := range r.Checks {
		if c.Sev == Fail {
			n++
		}
	}
	return n
}

// Sorted returns checks grouped by severity (Fail first) for stable output.
func (r *Report) Sorted() []Check {
	out := make([]Check, len(r.Checks))
	copy(out, r.Checks)
	rank := func(s Severity) int {
		switch s {
		case Fail:
			return 0
		case Warn:
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if rank(out[i].Sev) != rank(out[j].Sev) {
			return rank(out[i].Sev) < rank(out[j].Sev)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Options control which checks run.
type Options struct {
	// SpeedTest bounds the throughput measurement; zero disables it.
	TimeoutMS int
	// MinTLSSupported is the required minimum TLS version (FR-13).
	MinTLS string
	// SegmentBinary is the absolute path the gpstream-segreceiver binary
	// will have on the SOURCE segment hosts (it is a required component of
	// the segment-direct capability; empty disables the on_segment check).
	SegmentBinary string
	// Workers is the run's --workers (the host-pair worker pool / the
	// snapshot-epoch N). The diagnostic uses it to size the source DB's
	// max_connections against the run's connection demand (P1d). 0 disables
	// the connection-sizing check.
	Workers int
	// CopyMode is the run's --copy-mode ("mesh" / "coordinator"). "mesh"
	// makes the source's Yandex-fork COPY TO PROGRAM gate a hard requirement
	// (the on_segment data path launches the segreceivers via COPY ... TO
	// PROGRAM ... ON SEGMENT); otherwise the check is informational.
	CopyMode string
	// (P7) the data plane is always two-agent: the source/dest agents carry
	// the data and the controller holds no data connections. There is no
	// topology to select, so the check below is a constant confirmation.
}

// isNonzeroDuration reports whether a SHOW duration value is non-zero.
// "0", "", "0s" are zero; anything else (e.g. "5s", "1min") is non-zero.
func isNonzeroDuration(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return false
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d > 0
	}
	// Postgres durations like "1min", "00:00:05" that time.ParseDuration
	// rejects: treat a non-"0" string as set.
	return s != "0"
}

// boolStr returns yes when cond, else no.
func boolStr(cond bool, yes, no string) string {
	if cond {
		return " — " + yes
	}
	return " — " + no
}

// orUnknown renders an empty locale name as a placeholder for operator-facing
// messages (the behavioral finding is reported even when datcollate is blank).
func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
