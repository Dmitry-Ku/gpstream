package transfer

import (
	"encoding/binary"
	"strings"
	"testing"
)

// buildCopyStream builds a valid PostgreSQL binary COPY payload (signature +
// header + rows + end marker) from rows of raw field values. A nil field is
// encoded as NULL (len -1).
func buildCopyStream(t *testing.T, rows [][][]byte) []byte {
	t.Helper()
	var b []byte
	b = append(b, []byte(copySignature)...)
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], 0) // no extension block
	binary.BigEndian.PutUint32(hdr[4:8], 0) // flags
	b = append(b, hdr[:]...)
	for _, row := range rows {
		var fc [2]byte
		binary.BigEndian.PutUint16(fc[:], uint16(len(row)))
		b = append(b, fc[:]...)
		for _, f := range row {
			var ln [4]byte
			if f == nil {
				binary.BigEndian.PutUint32(ln[:], 0xFFFFFFFF)
			} else {
				binary.BigEndian.PutUint32(ln[:], uint32(len(f)))
			}
			b = append(b, ln[:]...)
			b = append(b, f...)
		}
	}
	b = append(b, 0xFF, 0xFF) // int16 -1 end-of-stream marker
	return b
}

// TestContentDigestOrderIndependent is the whole point of the digest (FR-02,
// E1 e2e 27.09): two byte streams holding the SAME rows in DIFFERENT orders
// must produce the SAME digest, while a genuinely different row set must not.
func TestContentDigestOrderIndependent(t *testing.T) {
	a, b := [][]byte{[]byte("alpha"), []byte("1")}, [][]byte{[]byte("beta"), []byte("2")}
	c, d := [][]byte{[]byte("gamma"), []byte("3")}, [][]byte{nil, []byte("4")}

	ab := buildCopyStream(t, [][][]byte{a, b})
	cd := buildCopyStream(t, [][][]byte{c, d})
	ba := buildCopyStream(t, [][][]byte{b, a})
	dc := buildCopyStream(t, [][][]byte{d, c})

	gab, err := ContentDigest(ab)
	if err != nil {
		t.Fatalf("digest ab: %v", err)
	}
	gcd, _ := ContentDigest(cd)
	gba, _ := ContentDigest(ba)
	gdc, _ := ContentDigest(dc)

	if gab != gba {
		t.Fatalf("order-dependent: %s (a,b) vs %s (b,a)", gab, gba)
	}
	if gcd != gdc {
		t.Fatalf("order-dependent: %s (c,d) vs %s (d,c)", gcd, gdc)
	}
	if gab == gcd {
		t.Fatalf("different row sets produced the same digest %s", gab)
	}
	// Row count is folded into the digest: rows:hex.
	if !strings.HasPrefix(gab, "2:") {
		t.Fatalf("digest %q should start with row count 2:", gab)
	}
}

// TestContentDigestNULLAndEmpty covers NULL fields and the empty table.
func TestContentDigestNULLAndEmpty(t *testing.T) {
	if got, err := ContentDigest(nil); err != nil || got != "0:" {
		t.Fatalf("empty digest = %q (err %v), want 0:", got, err)
	}
	withNull := buildCopyStream(t, [][][]byte{{[]byte("x"), nil}})
	withoutNull := buildCopyStream(t, [][][]byte{{[]byte("x"), []byte("")}})
	gn, err := ContentDigest(withNull)
	if err != nil {
		t.Fatalf("null digest: %v", err)
	}
	ge, _ := ContentDigest(withoutNull)
	if gn == ge {
		t.Fatalf("NULL field and empty field produced the same digest %s", gn)
	}
	// Permutation with a NULL field is still order-independent.
	reordered := buildCopyStream(t, [][][]byte{{[]byte("y"), []byte("9")}, {[]byte("x"), nil}})
	gn2, _ := ContentDigest(buildCopyStream(t, [][][]byte{{[]byte("x"), nil}, {[]byte("y"), []byte("9")}}))
	g3, _ := ContentDigest(reordered)
	if gn2 != g3 {
		t.Fatalf("order-dependent with NULL: %s vs %s", gn2, g3)
	}
}

// TestContentDigestOpaqueBlob: a payload that is not a binary COPY stream (the
// test-fake case) is hashed as an opaque blob and is deterministic.
func TestContentDigestOpaqueBlob(t *testing.T) {
	g1, err := ContentDigest([]byte("not-a-copy-stream"))
	if err != nil {
		t.Fatalf("blob digest: %v", err)
	}
	g2, _ := ContentDigest([]byte("not-a-copy-stream"))
	if g1 != g2 {
		t.Fatalf("blob digest not deterministic: %s vs %s", g1, g2)
	}
	if !strings.HasPrefix(g1, "blob:") {
		t.Fatalf("opaque payload should yield a blob digest, got %q", g1)
	}
}

// TestContentDigestTruncated: a truncated stream is an error, not a silent
// (wrong) digest.
func TestContentDigestTruncated(t *testing.T) {
	full := buildCopyStream(t, [][][]byte{{[]byte("hello")}})
	if _, err := ContentDigest(full[:len(full)-1]); err == nil {
		t.Fatal("truncated stream should error")
	}
}
