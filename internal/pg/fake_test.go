package pg

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestFakeConnPingDelay covers the delay+cancel branch of Ping (FR-10 --timeout
// bounds the plan-formation phase; a hung segment must yield on cancel).
func TestFakeConnPingDelay(t *testing.T) {
	f := NewFakeConn()
	f.SetPingDelay(5 * time.Second)
	// A cancelled context must surface the cancel (not block until the delay).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.Ping(ctx); err != context.Canceled {
		t.Fatalf("ping(cancelled) = %v, want context.Canceled", err)
	}
	// A short delay with an alive context must complete and return nil.
	f.SetPingDelay(1 * time.Millisecond)
	if err := f.Ping(context.Background()); err != nil {
		t.Fatalf("ping(untimed) = %v, want nil", err)
	}
}

// TestFakeConnExec covers the Exec path and that it records the query.
func TestFakeConnExec(t *testing.T) {
	f := NewFakeConn()
	n, err := f.Exec(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if n != 1 {
		t.Fatalf("exec returned %d, want 1", n)
	}
	if got := f.Queried(); len(got) != 1 || got[0] != "SELECT 1" {
		t.Fatalf("queried = %v, want [SELECT 1]", got)
	}
}

// TestFakeConnPanicQuery covers SetPanicQuery: a panic in the query path is the
// CR-7/FR-06 fault a worker-pool recovery test relies on.
func TestFakeConnPanicQuery(t *testing.T) {
	f := NewFakeConn()
	f.SetPanicQuery("boom")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic from SetPanicQuery")
		}
	}()
	_, _ = f.Query(context.Background(), "select boom")
}

// TestFakeConnPanicCopy covers SetPanicCopy (every copy panics) and
// SetPanicCopyTimes (only the N-th copy panics, then it succeeds).
func TestFakeConnPanicCopy(t *testing.T) {
	// Every copy panics.
	f := NewFakeConn()
	f.OnReadCopy("TO STDOUT", []byte("x"))
	f.SetPanicCopy()
	panicked := func() (r any) {
		defer func() { r = recover() }()
		_, _ = f.ReadCopy(context.Background(), bytes.NewBuffer(nil), "COPY x TO STDOUT")
		return nil
	}()
	if panicked == nil {
		t.Fatal("SetPanicCopy should make ReadCopy panic")
	}

	// Only the N-th copy panics, then it succeeds.
	g := NewFakeConn()
	g.OnReadCopy("TO STDOUT", []byte("payload"))
	g.SetPanicCopyTimes(1)
	// First call (readCopyN==1) panics.
	second := func() (r any) {
		defer func() { r = recover() }()
		_, _ = g.ReadCopy(context.Background(), bytes.NewBuffer(nil), "COPY x TO STDOUT")
		return nil
	}()
	if second == nil {
		t.Fatal("SetPanicCopyTimes(1) should panic on the first call")
	}
	// The call after the panic fires must succeed.
	var buf bytes.Buffer
	n, err := g.ReadCopy(context.Background(), &buf, "COPY x TO STDOUT")
	if err != nil {
		t.Fatalf("post-panic read copy: %v", err)
	}
	if n != 1 {
		t.Fatalf("post-panic read copy rows = %d, want 1", n)
	}
	if buf.String() != "payload" {
		t.Fatalf("post-panic payload = %q, want %q", buf.String(), "payload")
	}
}

// TestFakeConnReadCopyUnknown covers the "no canned COPY payload" error branch.
func TestFakeConnReadCopyUnknown(t *testing.T) {
	f := NewFakeConn()
	if _, err := f.ReadCopy(context.Background(), bytes.NewBuffer(nil), "COPY nope TO STDOUT"); err == nil {
		t.Fatal("expected an error for an unregistered COPY")
	}
}

// TestFakeConnWriteCopyError covers the "no COPY sink" error branch and that a
// registered sink records the written bytes.
func TestFakeConnWriteCopyError(t *testing.T) {
	f := NewFakeConn()
	if _, err := f.WriteCopy(context.Background(), bytes.NewReader([]byte("x")), "COPY y FROM STDIN"); err == nil {
		t.Fatal("expected an error for an unregistered COPY sink")
	}
	f.OnWriteCopy("FROM STDIN")
	n, err := f.WriteCopy(context.Background(), bytes.NewReader([]byte("row")), "COPY y FROM STDIN")
	if err != nil {
		t.Fatalf("registered write copy: %v", err)
	}
	if n != 1 {
		t.Fatalf("write copy rows = %d, want 1", n)
	}
	if got := f.WrittenCopy("FROM STDIN"); string(got) != "row" {
		t.Fatalf("written copy = %q, want %q", got, "row")
	}
}

// TestFakeConnClose covers Close and that a closed fake still answers queries
// (the fake does not enforce a closed state on Query, matching a driver that
// can be queried before Close in some paths).
func TestFakeConnClose(t *testing.T) {
	f := NewFakeConn()
	f.On("hello", [][]any{{int64(1)}})
	f.Close()
	if _, err := f.Query(context.Background(), "select hello"); err != nil {
		t.Fatalf("query after close: %v", err)
	}
}
