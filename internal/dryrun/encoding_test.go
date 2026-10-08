package dryrun

import (
	"context"
	"testing"
)

// TestDryrunEncodingDiff verifies (F-16) that the diagnostic reports a WARN when
// the source and destination databases use different character encodings
// (data-destructive: string bytes are reinterpreted on the destination).
func TestDryrunEncodingDiff(t *testing.T) {
	src := healthy()
	dst := healthy()
	src.On("pg_encoding_to_char", [][]any{{"SQL_ASCII"}})
	dst.On("pg_encoding_to_char", [][]any{{"UTF8"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	if find(rep, "network", "encoding") != Warn {
		t.Fatalf("expected an encoding WARN, got %v (checks %+v)", find(rep, "network", "encoding"), rep.Checks)
	}
}

// TestDryrunEncodingSame verifies no encoding finding when the encodings
// match.
func TestDryrunEncodingSame(t *testing.T) {
	src := healthy()
	dst := healthy()
	src.On("pg_encoding_to_char", [][]any{{"UTF8"}})
	dst.On("pg_encoding_to_char", [][]any{{"UTF8"}})

	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	for _, c := range rep.Checks {
		if c.Name == "encoding" {
			t.Fatalf("no encoding finding expected for matching encodings, got %+v", c)
		}
	}
}

// TestDryrunEncodingProbeFault verifies a resolution fault degrades to no
// finding (never a WARN, never an unhealthy diagnostic).
func TestDryrunEncodingProbeFault(t *testing.T) {
	src := healthy()
	dst := healthy()
	// no pg_encoding_to_char answer on either side -> RowStr "" -> no check.
	rep := Run(context.Background(), src, dst, Options{MinTLS: "1.2"})
	for _, c := range rep.Checks {
		if c.Name == "encoding" {
			t.Fatalf("probe fault must not raise an encoding finding, got %+v", c)
		}
	}
	if !rep.Healthy {
		t.Fatalf("probe fault must not make the diagnostic unhealthy")
	}
}
