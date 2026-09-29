package plan

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// encodingFakes returns src/dst fakes whose pg_database rows carry the given
// datencoding OIDs (4th column) and identical locales (no locale/behavioral
// diff), so the only possible finding is the encoding one. The
// pg_encoding_to_char($1) resolution answers are registered per side.
func encodingFakes(srcDB, dstDB string, srcOID, dstOID int64, srcName, dstName string) (*pg.FakeConn, *pg.FakeConn) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{srcDB, "en_US.utf8", "en_US.utf8", srcOID}})
	src.On("pg_encoding_to_char", [][]any{{srcName}})
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{dstDB, "en_US.utf8", "en_US.utf8", dstOID}})
	dst.On("pg_encoding_to_char", [][]any{{dstName}})
	return src, dst
}

// TestDetectCollationsEncodingDiff verifies (F-16) that a database-encoding
// mismatch (same locale, different datencoding) is reported as an
// encoding-level finding with the resolved char names.
func TestDetectCollationsEncodingDiff(t *testing.T) {
	src, dst := encodingFakes("srcdb", "dstdb", 6, 0, "SQL_ASCII", "UTF8")
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/dstdb?sslmode=disable")
	if len(out) != 1 {
		t.Fatalf("got %d diffs, want 1 (encoding): %+v", len(out), out)
	}
	if !out[0].IsEncoding() {
		t.Fatalf("expected an encoding-level diff, got %+v", out[0])
	}
	if out[0].DB != "srcdb" || out[0].Encoding != "SQL_ASCII -> UTF8" {
		t.Fatalf("encoding diff = %q/%q, want srcdb / SQL_ASCII -> UTF8", out[0].DB, out[0].Encoding)
	}
}

// TestDetectCollationsEncodingSame verifies that a matching datencoding
// produces no finding (the probe is a detector, not a source of noise).
func TestDetectCollationsEncodingSame(t *testing.T) {
	src, dst := encodingFakes("srcdb", "dstdb", 6, 6, "UTF8", "UTF8")
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/dstdb?sslmode=disable")
	if len(out) != 0 {
		t.Fatalf("got %d diffs, want 0 (same encoding): %+v", len(out), out)
	}
}

// TestDetectCollationsEncodingResolutionFault verifies that a fault resolving
// the encoding name degrades to the raw OID (the finding is still reported).
func TestDetectCollationsEncodingResolutionFault(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_database", [][]any{{"srcdb", "en_US.utf8", "en_US.utf8", int64(6)}})
	// no pg_encoding_to_char answer on src -> resolution fault -> raw OID.
	dst := pg.NewFakeConn()
	dst.On("pg_database", [][]any{{"dstdb", "en_US.utf8", "en_US.utf8", int64(1)}})
	dst.On("pg_encoding_to_char", [][]any{{"UTF8"}})
	tabs := []Table{{DB: "srcdb", Schema: "public", Name: "t1", DstSchema: "public", DstName: "t1"}}
	out := detectCollations(context.Background(), tabs, src, dst,
		"postgres://gpadmin@127.0.0.1:7432/dstdb?sslmode=disable")
	if len(out) != 1 || !out[0].IsEncoding() {
		t.Fatalf("got %+v, want one encoding diff", out)
	}
	if out[0].Encoding != "6 -> UTF8" {
		t.Fatalf("encoding = %q, want the raw source OID '6 -> UTF8'", out[0].Encoding)
	}
}
