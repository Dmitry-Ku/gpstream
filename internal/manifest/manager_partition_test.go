package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/plan"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestManagerPartitionGuardFailsEarly verifies (Fix 1) that a unit carrying a
// non-empty PartitionGuard (a string-keyed partitioned parent without a
// DEFAULT partition on a collation-different destination) fails EARLY — before
// any DDL or data is moved — with the guard diagnostic and a partition_guard
// event. This is the run-time enforcement of the dry-run warning: without it
// the whole COPY would abort mid-stream on "no partition for partitioning
// key" (lab, 29.09).
func TestManagerPartitionGuardFailsEarly(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)

	p := &plan.Plan{Units: []plan.Table{{
		DB: "db", Schema: "public", Name: "events", DstSchema: "public", DstName: "events",
		Bytes: 100,
		PartitionGuard: "public.events is partitioned by a string key with no DEFAULT partition and the destination collation differs",
	}}}
	err := m.Run(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "partition guard") {
		t.Fatalf("run err = %v, want a partition-guard failure", err)
	}
	u := j.Units()[0]
	if u.State != StateFailed {
		t.Fatalf("unit state = %s, want %s", u.State, StateFailed)
	}
	if !eventsContaining(t, dir, "partition_guard") {
		t.Fatalf("expected a partition_guard event, events:\n%s", readFile(t, dir+"/events.jsonl"))
	}
	// The guard fires BEFORE any DDL: no CREATE TABLE was issued.
	for _, q := range dst.Queried() {
		if strings.Contains(q, "CREATE TABLE") {
			t.Fatalf("guard must fail before DDL, but a CREATE was issued: %v", dst.Queried())
		}
	}
}

// TestManagerPartitionSpecInStagingDDL verifies (Fix 1) that a partitioned
// unit's spec is reproduced in the destination staging DDL, so the destination
// re-creates the same leaf layout and re-routes the copied rows by its own
// collation.
func TestManagerPartitionSpecInStagingDDL(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)

	spec := pg.ParsePartitionSpec(
		`PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, DEFAULT PARTITION z )`)
	p := &plan.Plan{Units: []plan.Table{{
		DB: "db", Schema: "public", Name: "events", DstSchema: "public", DstName: "events",
		Bytes: 100,
		Partition: spec,
	}}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	// The staging CREATE must carry the PARTITION BY spec (and the default).
	found := false
	for _, q := range dst.Queried() {
		if strings.Contains(q, "CREATE TABLE IF NOT EXISTS") && strings.Contains(q, "PARTITION BY RANGE") &&
			strings.Contains(q, "DEFAULT PARTITION z") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("staging DDL missing the partition spec, got %v", dst.Queried())
	}
}
