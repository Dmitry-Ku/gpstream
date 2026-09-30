package manifest

// Pure-helper coverage for the segment-direct data path (R5): the catalog
// enumeration + DSN derivation that copyStreamDirect/copyStreamSegmentDirect
// build on. The full data paths are live-only (GP6 gp_segment_configuration +
// pgxpool utility sessions + network listeners, covered by the lab e2e); here
// we pin the driver-agnostic decode and the URL/segment arithmetic, which the
// fakes can reach.

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

func TestToInt(t *testing.T) {
	cases := map[any]int64{
		int64(7):  7,
		int32(7):  7,
		uint32(7): 7,
		uint64(7): 7,
		"42":      42,
		"0":       0,
		"not-a-n": 0, // Sscanf fails -> 0
		nil:       0,
		3.9:       3, // fmt.Sprint(3.9)="3.9" -> Sscanf reads 3
	}
	for in, want := range cases {
		if got := toInt(in); got != want {
			t.Errorf("toInt(%v) = %d, want %d", in, got, want)
		}
	}
}

func TestSegmentDSN(t *testing.T) {
	seg := destSegment{Content: 1, Hostname: "seg1", Port: 20001}
	got, err := segmentDSN("postgres://gp@10.0.0.11:6432/dstdb?sslmode=verify-full", seg)
	if err != nil {
		t.Fatalf("segmentDSN: %v", err)
	}
	// host:port must be swapped to the segment's, and gp_session_role=utility
	// must be present (required to connect to a primary segment, R5 spike).
	want := "postgres://gp@seg1:20001/dstdb?gp_session_role=utility&sslmode=verify-full"
	if got != want {
		t.Errorf("segmentDSN = %q, want %q", got, want)
	}

	// A keyword/value (non-URL) DSN must be rejected: direct mode needs the
	// URL form so the host/port can be swapped.
	if _, err := segmentDSN("host=10.0.0.11 port=6432 user=gp dbname=dstdb", seg); err == nil {
		t.Error("keyword DSN: want error (direct needs URL form)")
	}
	// A bad scheme is rejected too.
	if _, err := segmentDSN("http://gp@h:1/db", seg); err == nil {
		t.Error("http scheme: want error")
	}
}

func TestPrimarySegmentCount(t *testing.T) {
	ctx := context.Background()

	// A Greenplum source reports the distinct primary contents.
	src := pg.NewFakeConn().On("gp_segment_configuration", [][]any{{int64(4)}})
	dst := pg.NewFakeConn()
	m := NewManager(configDefault(), src, dst, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	n, err := m.primarySegmentCount(ctx)
	if err != nil || n != 4 {
		t.Fatalf("primarySegmentCount = %d err %v, want 4", n, err)
	}

	// A plain-PostgreSQL source: the query returns no rows -> RowStr "" ->
	// the caller degrades to the coordinator path.
	src2 := pg.NewFakeConn() // no canned result -> query error -> ""
	m2 := NewManager(configDefault(), src2, dst, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	if _, err := m2.primarySegmentCount(ctx); err == nil {
		t.Error("plain PG: want error (cannot enumerate segments)")
	}
}

func TestDestSegments(t *testing.T) {
	ctx := context.Background()

	// Dense 0..D-1 contents in order: the happy path.
	dst := pg.NewFakeConn().On("gp_segment_configuration", [][]any{
		{int32(0), "seg0", int32(20000)},
		{int32(1), "seg1", int32(20001)},
		{int32(2), "seg2", int32(20002)},
	})
	m := NewManager(configDefault(), pg.NewFakeConn(), dst, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	segs, err := m.destSegments(ctx)
	if err != nil {
		t.Fatalf("destSegments: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("got %d segments, want 3", len(segs))
	}
	for i, s := range segs {
		if s.Content != int32(i) || s.Hostname != "seg"+string(rune('0'+i)) || s.Port != 20000+i {
			t.Errorf("seg %d = %+v, want content=%d host=seg%d port=%d", i, s, i, i, 20000+i)
		}
	}

	// A query fault (plain-PG destination) degrades to an error.
	m2 := NewManager(configDefault(), pg.NewFakeConn(), pg.NewFakeConn(), NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	if _, err := m2.destSegments(ctx); err == nil {
		t.Error("plain PG dest: want error")
	}

	// An empty result (no up primaries) is an error.
	dstEmpty := pg.NewFakeConn().On("gp_segment_configuration", [][]any{})
	m3 := NewManager(configDefault(), pg.NewFakeConn(), dstEmpty, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	if _, err := m3.destSegments(ctx); err == nil {
		t.Error("no up primaries: want error")
	}

	// Non-dense contents (a missing content) break slice coverage -> error.
	dstGap := pg.NewFakeConn().On("gp_segment_configuration", [][]any{
		{int32(0), "seg0", int32(20000)},
		{int32(2), "seg2", int32(20002)}, // content 1 missing
	})
	m4 := NewManager(configDefault(), pg.NewFakeConn(), dstGap, NewJournal(pg.NewFakeConn(), t.TempDir()+"/j.jsonl"))
	if _, err := m4.destSegments(ctx); err == nil {
		t.Error("non-dense contents: want error")
	}
}
