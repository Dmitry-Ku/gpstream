package pg

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// TestReadCopy exercises the fake's StreamingQuerier COPY ... TO STDOUT path
// (FR-01 source side) directly at the pg layer.
func TestReadCopy(t *testing.T) {
	f := NewFakeConn()
	payload := []byte("BINARY-PAYLOAD")
	f.OnReadCopy("TO STDOUT", payload)
	var buf bytes.Buffer
	n, err := f.ReadCopy(context.Background(), &buf, `COPY public.orders TO STDOUT (FORMAT binary)`)
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero rows for a non-empty payload")
	}
	if buf.String() != string(payload) {
		t.Fatalf("got %q, want %q", buf.String(), string(payload))
	}
	// It must be logged as a query.
	if !containsLast(f.Queried(), "COPY") {
		t.Fatalf("read copy not logged: %v", f.Queried())
	}
}

// TestReadCopyUnregistered returns an error for an unregistered COPY (models a
// "cannot determine" fault, CR-7).
func TestReadCopyUnregistered(t *testing.T) {
	f := NewFakeConn()
	var buf bytes.Buffer
	if _, err := f.ReadCopy(context.Background(), &buf, `COPY foo TO STDOUT`); err == nil {
		t.Fatal("expected error for unregistered COPY")
	}
}

// TestReadCopyEmpty returns row count 0 for an empty payload.
func TestReadCopyEmpty(t *testing.T) {
	f := NewFakeConn()
	f.OnReadCopy("TO STDOUT", []byte{})
	var buf bytes.Buffer
	n, err := f.ReadCopy(context.Background(), &buf, `COPY foo TO STDOUT`)
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if n != 0 {
		t.Fatalf("empty payload should yield 0 rows, got %d", n)
	}
}

// TestWriteCopy records the streamed payload (FR-01 destination side).
func TestWriteCopy(t *testing.T) {
	f := NewFakeConn()
	f.OnWriteCopy("FROM STDIN")
	n, err := f.WriteCopy(context.Background(),
		strings.NewReader("ROW-BATCH"), `COPY public.orders FROM STDIN (FORMAT binary)`)
	if err != nil {
		t.Fatalf("write copy: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero rows for a non-empty payload")
	}
	if got := f.WrittenCopy("FROM STDIN"); string(got) != "ROW-BATCH" {
		t.Fatalf("recorded %q, want ROW-BATCH", got)
	}
	if !containsLast(f.Queried(), "COPY") {
		t.Fatalf("write copy not logged: %v", f.Queried())
	}
}

// TestWriteCopyUnregistered returns an error for an unregistered COPY sink.
func TestWriteCopyUnregistered(t *testing.T) {
	f := NewFakeConn()
	if _, err := f.WriteCopy(context.Background(), strings.NewReader("x"), `COPY foo FROM STDIN`); err == nil {
		t.Fatal("expected error for unregistered COPY sink")
	}
}

// TestClose marks the fake closed (Querier contract).
func TestClose(t *testing.T) {
	f := NewFakeConn()
	f.Close()
	if !f.closed {
		t.Fatal("expected closed=true")
	}
}

func containsLast(q []string, s string) bool {
	if len(q) == 0 {
		return false
	}
	return strings.Contains(q[len(q)-1], s)
}
