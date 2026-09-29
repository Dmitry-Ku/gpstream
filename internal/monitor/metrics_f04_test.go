package monitor

import (
	"strings"
	"testing"
	"time"
)

func TestRowsAndBytesPerTable(t *testing.T) {
	m := New()
	m.RecordRows("s.t1", 100)
	m.RecordRows("s.t1", 50)
	m.RecordRows("s.t2", 7)
	if m.RowsTotal() != 157 {
		t.Errorf("rows = %d, want 157", m.RowsTotal())
	}
	m.RecordBytes("s.t1", 2048, 4096)
	m.RecordBytes("s.t2", 100, 300)
	if m.Bytes != 2148 {
		t.Errorf("wire bytes = %d, want 2148", m.Bytes)
	}
	if m.BytesDecoded != 4396 {
		t.Errorf("logical bytes = %d, want 4396", m.BytesDecoded)
	}
	rows := map[string]uint64{}
	wire := map[string]uint64{}
	for _, r := range m.PerTable() {
		rows[r.Table] = r.Rows
		wire[r.Table] = r.Wire
	}
	if rows["s.t1"] != 150 || rows["s.t2"] != 7 {
		t.Errorf("per-table rows = %v", rows)
	}
	if wire["s.t1"] != 2048 || wire["s.t2"] != 100 {
		t.Errorf("per-table wire = %v", wire)
	}
}

func TestUnitStateGauges(t *testing.T) {
	m := New()
	// Two units start pending, one goes copying, then verifies, then completes.
	m.SetUnitState("s.a", "pending")
	m.SetUnitState("s.b", "pending")
	if got := m.TablesByStatus()["pending"]; got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	m.SetUnitState("s.a", "copying")
	if m.ActiveFlowsCount() != 1 {
		t.Fatalf("active flows = %d, want 1", m.ActiveFlowsCount())
	}
	st := m.TablesByStatus()
	if st["pending"] != 1 || st["copying"] != 1 {
		t.Fatalf("by-status = %v", st)
	}
	m.SetUnitState("s.a", "verifying")
	if m.ActiveFlowsCount() != 0 {
		t.Fatalf("active flows after verify = %d, want 0", m.ActiveFlowsCount())
	}
	m.SetUnitState("s.a", "completed")
	st = m.TablesByStatus()
	if st["completed"] != 1 || st["copying"] != 0 {
		t.Fatalf("by-status = %v", st)
	}
	// Per-table state follows the unit.
	found := false
	for _, r := range m.PerTable() {
		if r.Table == "s.a" && r.State != "completed" {
			t.Errorf("s.a state = %q, want completed", r.State)
		}
		if r.Table == "s.a" {
			found = true
		}
	}
	if !found {
		t.Error("s.a missing from per-table")
	}
	// A re-entry into copying increments again.
	m.SetUnitState("s.a", "pending")
	m.SetUnitState("s.a", "copying")
	if m.ActiveFlowsCount() != 1 {
		t.Fatalf("active flows re-copy = %d, want 1", m.ActiveFlowsCount())
	}
}

func TestETACounter(t *testing.T) {
	m := New()
	// A realistic run: 100 MB planned. With ~400 KiB moved in ~20 ms the
	// implied throughput is tens of MB/s, so the ETA for the remaining
	// ~99.6 MB must be a positive number of seconds.
	m.SetTotalBytes(100_000_000)
	if m.ETASeconds() != 0 {
		t.Fatal("eta before any data must be 0 (no throughput yet)")
	}
	// Simulate 200 KiB logical done with a known throughput window.
	m.RecordBytes("", 200*1024, 200*1024)
	// Throughput needs >=2 samples spanning time; force a second sample.
	time.Sleep(20 * time.Millisecond)
	m.RecordBytes("", 200*1024, 200*1024)
	if m.BytesDecoded != 400*1024 {
		t.Fatalf("decoded = %d, want %d", m.BytesDecoded, 400*1024)
	}
	if m.ETASeconds() <= 0 {
		t.Fatal("eta must be > 0 with throughput and remaining volume")
	}
}

func TestChecksumAndRecoveryCounters(t *testing.T) {
	m := New()
	m.RecordChecksumError()
	m.RecordChecksumError()
	m.RecordAutoRecovery()
	out := m.Prometheus()
	if !strings.Contains(out, "gpstream_checksum_errors_total 2") {
		t.Errorf("checksum errors missing:\n%s", out)
	}
	if !strings.Contains(out, "gpstream_auto_recovery_total 1") {
		t.Errorf("auto recovery missing:\n%s", out)
	}
}

func TestPrometheusNewFamilies(t *testing.T) {
	m := New()
	m.SetPair("vm1:5432 -> vm4:7432")
	m.SetTotalBytes(1000)
	m.RecordRows("s.t1", 10)
	m.RecordBytes("s.t1", 100, 120)
	m.SetUnitState("s.t1", "swapped")
	out := m.Prometheus()
	for _, want := range []string{
		"gpstream_rows_total 10",
		"gpstream_active_flows 0",
		"gpstream_eta_seconds 0",
		`gpstream_tables_by_status{status="swapped"} 1`,
		`gpstream_table_rows{table="s.t1"} 10`,
		`gpstream_table_bytes_wire{table="s.t1"} 100`,
		`gpstream_table_bytes_logical{table="s.t1"} 120`,
		`gpstream_table_state{table="s.t1",status="swapped"} 1`,
		`gpstream_pair_up{pair="vm1:5432 -> vm4:7432"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("prometheus output missing %q:\n%s", want, out)
		}
	}
}

func TestPrometheusValidOrdering(t *testing.T) {
	// HELP/TYPE must precede their samples (Prometheus textformat rule).
	m := New()
	m.SetUnitState("s.t1", "copying")
	m.SetUnitState("s.t2", "pending")
	out := m.Prometheus()
	lines := strings.Split(out, "\n")
	index := func(prefix string) int {
		for i, l := range lines {
			if strings.HasPrefix(l, prefix) {
				return i
			}
		}
		return -1
	}
	helpIdx := index("# HELP gpstream_tables_by_status")
	typeIdx := index("# TYPE gpstream_tables_by_status")
	firstSample := index("gpstream_tables_by_status{")
	if helpIdx < 0 || typeIdx < 0 || firstSample < 0 {
		t.Fatalf("missing tables_by_status family:\n%s", out)
	}
	if !(helpIdx < typeIdx && typeIdx < firstSample) {
		t.Fatalf("bad ordering: help=%d type=%d sample=%d\n%s", helpIdx, typeIdx, firstSample, out)
	}
}
