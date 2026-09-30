package manifest

import (
	"context"
	"strings"
	"testing"

	"github.com/gpstream/gpstream/internal/config"
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
		Bytes:          100,
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
		Bytes:     100,
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

// TestManagerPartitionedUnitReadsParent pins the data-path contract that the
// segment-direct transports (on_segment / direct) depend on (lab e2e,
// 29.09): a partitioned unit's SOURCE data read targets the PARENT name —
// `COPY <schema>.<parent> TO STDOUT` — whose single stream carries every
// leaf's rows. It must NOT read a leaf partition directly (a leaf relfilenode
// holds only that leaf's rows; reading leaves separately would drop the others
// and, under a differing glibc, route by the wrong collation). The planner
// already guarantees the unit's Name is the parent (TestPlannerLeafExclusionOnGP);
// this pins that the read honours that Name.
func TestManagerPartitionedUnitReadsParent(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	m := NewManager(configDefault(), src, dst, j)

	spec := pg.ParsePartitionSpec(
		`PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, DEFAULT PARTITION z )`)
	p := &plan.Plan{Units: []plan.Table{{
		DB: "db", Schema: "public", Name: "events", DstSchema: "public", DstName: "events",
		Bytes:     100,
		Partition: spec, // a partitioned parent (2 leaves a/z)
	}}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("run: %v", err)
	}
	// The source data read must target the parent (events), via COPY ... TO STDOUT.
	parentRead, leafRead := false, false
	for _, q := range src.Queried() {
		if strings.Contains(q, "TO STDOUT") {
			if strings.Contains(q, `COPY "public"."events" TO STDOUT`) {
				parentRead = true
			}
			// A leaf partition is named <parent>_<N>_prt_<leaf> on GP6; a read
			// targeting it would be the degradation this test guards against.
			if strings.Contains(q, `COPY "public"."events_`) {
				leafRead = true
			}
		}
	}
	if !parentRead {
		t.Fatalf("partitioned unit must READ the parent via COPY, source reads were: %v", src.Queried())
	}
	if leafRead {
		t.Fatalf("partitioned unit must NOT read a leaf partition directly, source reads were: %v", src.Queried())
	}
}

// TestManagerDirectModeDegradesPartitionedToCoordinator pins the GP6 kernel
// limitation found live (29.09): utility-mode COPY FROM is not supported on
// partition-hierarchy roots (SQLSTATE 0A000), so --copy-mode direct must
// degrade a partitioned unit to the coordinator path (parent-COPY) and SUCCEED
// — not fail the unit. The operator sees the decision in events.jsonl
// (direct_degraded).
func TestManagerDirectModeDegradesPartitionedToCoordinator(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	src, dst := newPair(t, payload, payload)
	j := NewJournal(pg.NewFakeConn(), dir+"/journal.jsonl")
	cfg := configDefault()
	cfg.CopyMode = config.CopyDirect
	m := NewManager(cfg, src, dst, j)

	spec := pg.ParsePartitionSpec(
		`PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, DEFAULT PARTITION z )`)
	p := &plan.Plan{Units: []plan.Table{{
		DB: "db", Schema: "public", Name: "events", DstSchema: "public", DstName: "events",
		Bytes:     100,
		Partition: spec,
	}}}
	if err := m.Run(context.Background(), p); err != nil {
		t.Fatalf("direct mode must degrade a partitioned unit to the coordinator path and succeed, got: %v", err)
	}
	u := j.Units()[0]
	if u.State != StateSwapped {
		t.Fatalf("unit state = %s, want %s", u.State, StateSwapped)
	}
	// The degradation decision must be operator-visible AND partition-specific
	// (not the generic non-GP-destination fallback).
	if !eventsContaining(t, dir, "direct_degraded") {
		t.Fatalf("expected a direct_degraded event, events:\n%s", readFile(t, dir+"/events.jsonl"))
	}
	if !eventsContaining(t, dir, "partitioned parent") {
		t.Fatalf("direct_degraded must cite the partition reason, events:\n%s", readFile(t, dir+"/events.jsonl"))
	}
	// And the data must have been read via the parent's coordinator COPY.
	found := false
	for _, q := range src.Queried() {
		if strings.Contains(q, `COPY "public"."events" TO STDOUT`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("degraded run must read the parent via coordinator COPY, source reads were: %v", src.Queried())
	}
}
