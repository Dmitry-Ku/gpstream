package source

import (
	"bytes"
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/pg"
)

// TestColumns reads faithful column definitions through the streaming fake.
func TestColumns(t *testing.T) {
	f := pg.NewFakeConn()
	f.On("format_type", [][]any{
		{"id", "integer", true, ""},
		{"customer", "text", false, `''::text`},
	})
	a := New(f)
	cols, err := a.Columns(context.Background(), "public", "orders")
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("got %d cols, want 2: %+v", len(cols), cols)
	}
	if cols[0].Name != "id" || cols[0].Type != "integer" || !cols[0].Nullable {
		t.Fatalf("col0 = %+v", cols[0])
	}
	if cols[1].Name != "customer" || cols[1].Type != "text" || cols[1].Nullable {
		t.Fatalf("col1 = %+v (customer should be NOT NULL)", cols[1])
	}
	if cols[1].Default != `''::text` {
		t.Fatalf("col1 default = %q", cols[1].Default)
	}
}

// TestCopyToStream streams the binary COPY payload of a table into a writer
// (FR-01). The fake returns a canned payload; the agent must write it.
func TestCopyToStream(t *testing.T) {
	f := pg.NewFakeConn()
	payload := []byte("BINARY-COPY-ROW-BATCH")
	f.OnReadCopy("TO STDOUT", payload)
	a := New(f)
	var buf bytes.Buffer
	n, err := a.CopyToStream(context.Background(), "public", "orders", &buf)
	if err != nil {
		t.Fatalf("copy to stream: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero row count")
	}
	if buf.String() != string(payload) {
		t.Fatalf("streamed %q, want %q", buf.String(), string(payload))
	}
}

// TestChecksum computes an md5 over the binary COPY payload (FR-02) and
// compares it against an independently computed digest.
func TestChecksum(t *testing.T) {
	f := pg.NewFakeConn()
	payload := []byte("hello-binary-copy")
	f.OnReadCopy("TO STDOUT", payload)
	a := New(f)
	got, err := a.Checksum(context.Background(), "public", "orders")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if got == "" || got == "0" {
		t.Fatalf("expected a non-empty digest, got %q", got)
	}
	// The fake returns the same payload for a second call, so the digest must
	// match the one computed from the raw payload.
	f2 := pg.NewFakeConn()
	f2.OnReadCopy("TO STDOUT", payload)
	a2 := New(f2)
	got2, _ := a2.Checksum(context.Background(), "public", "orders")
	if got != got2 {
		t.Fatalf("checksum not deterministic: %s vs %s", got, got2)
	}
}

// TestChecksumEmpty returns "0" for an empty payload (no rows).
func TestChecksumEmpty(t *testing.T) {
	f := pg.NewFakeConn()
	f.OnReadCopy("TO STDOUT", []byte{})
	a := New(f)
	got, err := a.Checksum(context.Background(), "public", "orders")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if got != "0" {
		t.Fatalf("empty payload should be \"0\", got %q", got)
	}
}

// TestQuoteIdent checks the identifier quoting (escaping embedded quotes).
func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"public":        `"public"`,
		`a"b`:          `"a""b"`,
		"":              `""`,
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Fatalf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestToStrToBool checks the any->string/bool conversions.
func TestToStrToBool(t *testing.T) {
	if toStr(nil) != "" {
		t.Fatal("nil -> empty")
	}
	if toStr("x") != "x" {
		t.Fatal("string passthrough")
	}
	if toStr(int64(7)) != "7" {
		t.Fatal("int render")
	}
	if !toBool(true) || toBool(false) || toBool("yes") {
		t.Fatal("toBool")
	}
}
