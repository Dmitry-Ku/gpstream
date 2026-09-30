package segstream

// CheckOnSegment capability probe (doctor on_segment): the three probe
// facets (GUC / dispatch / binary) and their Summary/OK rendering, on the
// fake querier (the live path is covered by the doctor on GP6).

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

func TestCheckOnSegmentAllOK(t *testing.T) {
	q := pg.NewFakeConn().On("current_setting", [][]any{{"on"}})
	p := CheckOnSegment(context.Background(), q, "/tmp/gpstream-segreceiver")
	if !p.GUC || !p.DispatchOK || !p.BinaryOK {
		t.Fatalf("probe = %+v, want all OK", p)
	}
	if !p.OK() {
		t.Error("OK() = false, want true")
	}
	for _, line := range p.Summary() {
		if strings.Contains(line, "FAIL") {
			t.Errorf("summary line %q must not contain FAIL", line)
		}
	}
	if len(p.Summary()) != 3 {
		t.Errorf("summary = %d lines, want 3", len(p.Summary()))
	}
}

func TestCheckOnSegmentGUCAbsent(t *testing.T) {
	// Non-Yandex build: current_setting fails -> GUC off, hint in summary.
	q := pg.NewFakeConn() // no canned result for current_setting -> query error
	p := CheckOnSegment(context.Background(), q, "/tmp/gpstream-segreceiver")
	if p.GUC {
		t.Error("GUC must be off when current_setting fails")
	}
	if p.GUCValue != "" {
		t.Errorf("GUCValue = %q, want empty", p.GUCValue)
	}
	joined := strings.Join(p.Summary(), "\n")
	if !strings.Contains(joined, "want") || !strings.Contains(joined, GUCName) {
		t.Errorf("summary must carry the GUC hint: %q", joined)
	}
	if p.OK() {
		t.Error("OK() must be false with GUC off")
	}
}

func TestCheckOnSegmentDispatchFails(t *testing.T) {
	// GUC on, but the dispatch COPY is rejected (non-Greenplum or GUC off at
	// the kernel): the probe must record the error, not panic.
	q := pg.NewFakeConn().
		On("current_setting", [][]any{{"on"}}).
		SetExecError(errors.New("syntax error at or near \"ON\""))
	p := CheckOnSegment(context.Background(), q, "/tmp/gpstream-segreceiver")
	if !p.GUC {
		t.Error("GUC should still be on")
	}
	if p.DispatchOK || p.BinaryOK {
		t.Errorf("dispatch/binary must be off on exec error: %+v", p)
	}
	if !strings.Contains(p.Dispatch, "syntax error") {
		t.Errorf("Dispatch = %q, want the exec error", p.Dispatch)
	}
	if !strings.Contains(p.Summary()[1], "FAIL") {
		t.Errorf("summary line 2 must be a FAIL: %q", p.Summary()[1])
	}
	if p.OK() {
		t.Error("OK() must be false with dispatch failing")
	}
}

// TestOnSegmentProbeSummaryFailureLines renders the failure facets directly
// (the fake Exec cannot distinguish dispatch from binary probe strings).
func TestOnSegmentProbeSummaryFailureLines(t *testing.T) {
	p := &OnSegmentProbe{
		GUC:      false,
		GUCValue: "",
		Dispatch: "COPY TO PROGRAM is not allowed",
		Binary:   "segment 1: /bin/sh: 1: /tmp/gpstream-segreceiver: not found",
	}
	lines := p.Summary()
	if len(lines) != 3 {
		t.Fatalf("summary = %d lines, want 3", len(lines))
	}
	if !strings.Contains(lines[0], GUCName) {
		t.Errorf("line 0 = %q, want GUC hint", lines[0])
	}
	if !strings.Contains(lines[1], "FAIL") || !strings.Contains(lines[1], "not allowed") {
		t.Errorf("line 1 = %q, want dispatch FAIL", lines[1])
	}
	if !strings.Contains(lines[2], "FAIL") || !strings.Contains(lines[2], "install the binary") {
		t.Errorf("line 2 = %q, want binary FAIL", lines[2])
	}
	if p.OK() {
		t.Error("OK() = true on a fully-failed probe")
	}
}

// TestHandshakeFilterAddrClose: the filter must forward Addr() to the
// wrapped listener and Close() must close it (Receive's cleanup path).
func TestHandshakeFilterAddrClose(t *testing.T) {
	plain, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := HandshakeFilter(plain)
	if f.Addr().String() != plain.Addr().String() {
		t.Errorf("Addr = %v, want %v (must forward)", f.Addr(), plain.Addr())
	}
	if err := f.Close(); err != nil {
		t.Errorf("Close = %v, want nil", err)
	}
}
