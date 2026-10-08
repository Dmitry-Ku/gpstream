package segstream

import (
	"context"
	"fmt"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// CheckOnSegment probes the source cluster for the segment-direct
// capability (DESIGN §1.3: dry-run sets capability on_segment; planner fixes
// copy_mode; the operator can override with --copy-mode). All probes run on
// the coordinator (gpstream never connects to segments directly). binPath is
// the absolute path the segreceiver binary will have on the segment hosts
// (the controller installs it there; the kernel runs it by absolute path).
//
// Probe order (cheapest first):
//
//  1. GUC — SELECT current_setting('ycmdb.yc_allow_copy_to_program'). A
//     missing GUC (non-Yandex build) fails the query: capability off, with
//     the build-specific hint.
//  2. Dispatch — COPY (SELECT 1) TO PROGRAM 'true <SEGID>' ON SEGMENT.
//     Exercises the full kernel path (grammar + MangleCopyFileName +
//     per-segment exec) with a program that cannot fail. Fails on
//     non-Greenplum (syntax error: "ON SEGMENT") and when the GUC is off
//     ("... is not allowed").
//  3. Binary — COPY (SELECT 1) TO PROGRAM 'binPath --check --content
//     <SEGID>' ON SEGMENT. A clean exit proves the binary is on ALL segment
//     hosts (one missing binary fails the whole COPY via
//     DF_CANCEL_ON_ERROR).
//
// The probe is read-only and idempotent (no files, no table changes).
func CheckOnSegment(ctx context.Context, q pg.Querier, binPath string) *OnSegmentProbe {
	p := &OnSegmentProbe{}

	// 1. GUC.
	p.GUCValue = pg.RowStr(ctx, q, "SELECT current_setting('"+GUCName+"')")
	p.GUC = p.GUCValue == "on"
	if p.GUCValue == "" {
		p.GUC = false
	}

	// 2. Dispatch (a no-op program; the <SEGID> substitution proves the
	// kernel path end to end).
	_, derr := q.Exec(ctx, "COPY (SELECT 1) TO PROGRAM 'true <SEGID>' ON SEGMENT")
	if derr != nil {
		p.Dispatch = derr.Error()
	} else {
		p.DispatchOK = true
		p.Dispatch = "ok"
	}

	// 3. Binary on segment hosts (segreceiver --check exits 0, prints the
	// marker to the segment server log).
	_, berr := q.Exec(ctx, "COPY (SELECT 1) TO PROGRAM '"+binPath+" --check --content <SEGID>' ON SEGMENT")
	if berr != nil {
		p.Binary = berr.Error()
	} else {
		p.BinaryOK = true
		p.Binary = "ok"
	}

	return p
}

// Summary renders the probe for the dry-run report (one line per facet).
func (p *OnSegmentProbe) Summary() []string {
	out := make([]string, 0, 3)
	if p.GUC {
		out = append(out, "GUC ycmdb.yc_allow_copy_to_program = on")
	} else {
		out = append(out, fmt.Sprintf("GUC %s = %q (want \"on\"; set in postgresql.conf of EVERY data-dir, then gpstop -ar)", GUCName, p.GUCValue))
	}
	if p.DispatchOK {
		out = append(out, "ON SEGMENT dispatch: ok")
	} else {
		out = append(out, "ON SEGMENT dispatch: FAIL — "+p.Dispatch)
	}
	if p.BinaryOK {
		out = append(out, "gpstream-segreceiver present on all segment hosts")
	} else {
		out = append(out, "gpstream-segreceiver: FAIL — "+p.Binary+" (install the binary on the segment hosts)")
	}
	return out
}

// OK reports the full capability (GUC + dispatch + binary).
func (p *OnSegmentProbe) OK() bool { return p.GUC && p.DispatchOK && p.BinaryOK }
