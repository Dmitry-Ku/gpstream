package pg

import (
	"context"
	"testing"
)

// TestParsePartitionSpec extracts the partition-key columns and the default
// flag from a pg_get_partition_def string (Fix 1). GP6 has no
// pg_get_partkeydef, so the key must come out of the spec text.
func TestParsePartitionSpec(t *testing.T) {
	// GP6 RANGE spec with an explicit default partition (live shape, 29.09).
	s := ParsePartitionSpec(`PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, DEFAULT PARTITION z )`)
	if s.Spec == "" {
		t.Fatal("spec must be preserved")
	}
	if len(s.KeyColumns) != 1 || s.KeyColumns[0] != "day" {
		t.Fatalf("key columns = %v, want [day]", s.KeyColumns)
	}
	if !s.HasDefault {
		t.Fatal("HasDefault = false, want true")
	}

	// No default partition.
	s2 := ParsePartitionSpec(`PARTITION BY RANGE (d) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, PARTITION b START ('02') INCLUSIVE END ('03') EXCLUSIVE)`)
	if s2.HasDefault {
		t.Fatal("HasDefault = true, want false")
	}
	if len(s2.KeyColumns) != 1 || s2.KeyColumns[0] != "d" {
		t.Fatalf("key columns = %v, want [d]", s2.KeyColumns)
	}

	// LIST partitioning.
	s3 := ParsePartitionSpec(`PARTITION BY LIST (r) (PARTITION north VALUES('n'), PARTITION south VALUES('s', 'e'))`)
	if len(s3.KeyColumns) != 1 || s3.KeyColumns[0] != "r" {
		t.Fatalf("LIST key columns = %v, want [r]", s3.KeyColumns)
	}
	if s3.HasDefault {
		t.Fatal("LIST HasDefault = true, want false")
	}

	// Multi-key RANGE.
	s4 := ParsePartitionSpec(`PARTITION BY RANGE (a, b) (PARTITION x START (1, 'n') INCLUSIVE END (2, 's') EXCLUSIVE)`)
	if len(s4.KeyColumns) != 2 || s4.KeyColumns[0] != "a" || s4.KeyColumns[1] != "b" {
		t.Fatalf("multi-key columns = %v, want [a b]", s4.KeyColumns)
	}

	// Quoted key column.
	s5 := ParsePartitionSpec(`PARTITION BY RANGE ("day") (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE)`)
	if len(s5.KeyColumns) != 1 || s5.KeyColumns[0] != "day" {
		t.Fatalf("quoted key columns = %v, want [day]", s5.KeyColumns)
	}

	// A plain (non-partitioned) table: pg_get_partition_def yields NULL ->
	// an empty string -> a zero spec.
	if z := ParsePartitionSpec(""); z.Spec != "" || z.KeyColumns != nil || z.HasDefault {
		t.Fatalf("empty input must yield a zero PartitionSpec, got %+v", z)
	}
}

// TestCapturePartitionSpec reads pg_get_partition_def and parses it; a plain
// table (NULL result) yields a zero spec without error.
func TestCapturePartitionSpec(t *testing.T) {
	f := NewFakeConn()
	f.On("pg_get_partition_def", [][]any{
		{`PARTITION BY RANGE (day) (PARTITION a START ('01') INCLUSIVE END ('02') EXCLUSIVE, DEFAULT PARTITION z )`},
	})
	spec, err := CapturePartitionSpec(f, context.Background(), "public", "events")
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if spec.Spec == "" || len(spec.KeyColumns) != 1 || spec.KeyColumns[0] != "day" || !spec.HasDefault {
		t.Fatalf("spec = %+v", spec)
	}

	// Plain table: NULL -> zero spec, no error.
	f2 := NewFakeConn()
	f2.On("pg_get_partition_def", [][]any{{nil}})
	plain, err := CapturePartitionSpec(f2, context.Background(), "public", "plain")
	if err != nil {
		t.Fatalf("capture plain: %v", err)
	}
	if plain.Spec != "" || plain.KeyColumns != nil || plain.HasDefault {
		t.Fatalf("plain table must yield a zero spec, got %+v", plain)
	}
}

// TestCollationSignature is the behavioral collation fingerprint: the probe
// sorts a fixed set of collation-sensitive string literals, so two databases
// with the SAME locale name but different glibc (CentOS7 vs CentOS8) return
// different signatures (lab, 29.09: A and C both en_US.utf8, but
// {1,"1",2,...} vs {"1",1,2,...}).
func TestCollationSignature(t *testing.T) {
	f := NewFakeConn()
	f.On("array_agg(x ORDER BY x)", [][]any{{`{1,"1",2,a,"a","a",A,e,é,z,Z}`}})
	sig, err := CollationSignature(f, context.Background())
	if err != nil {
		t.Fatalf("signature: %v", err)
	}
	if sig != `{1,"1",2,a,"a","a",A,e,é,z,Z}` {
		t.Fatalf("signature = %q", sig)
	}

	// A different glibc -> a different signature for the same locale name.
	f2 := NewFakeConn()
	f2.On("array_agg(x ORDER BY x)", [][]any{{`{"1",1,2,"a",a,"a",A,e,é,z,Z}`}})
	sig2, _ := CollationSignature(f2, context.Background())
	if sig == sig2 {
		t.Fatal("signatures must differ across glibc")
	}
}
