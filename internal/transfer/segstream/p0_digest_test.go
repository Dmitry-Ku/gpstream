package segstream

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// xorAcc folds an accumulator into acc (XOR-composition of per-slice digests).
func xorAcc(acc, b *[32]byte) {
	for i := range acc {
		acc[i] ^= b[i]
	}
}

// sliceAcc computes the raw per-row XOR accumulator of a binary-COPY slice
// (independent of the sender path: it re-serializes the rows through a
// ContentDigestWriter) — the reference value the trailer must carry.
func sliceAcc(t *testing.T, slice []byte) [32]byte {
	t.Helper()
	d := transfer.NewContentDigest()
	if _, err := d.Write(slice); err != nil {
		t.Fatalf("reference digest: %v", err)
	}
	acc, rows, err := d.Accumulator()
	if err != nil {
		t.Fatalf("reference accumulator: %v", err)
	}
	if rows == 0 {
		t.Fatalf("reference digest: 0 rows")
	}
	return acc
}

// TestSendDigestTrailer verifies that with digest=true the trailer carries the
// raw per-row XOR accumulator (matching the reference), and with digest=false
// the Digest field is zero (the pre-P0 wire, byte-identical for digest off).
func TestSendDigestTrailer(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	slice := genSlice(rnd, 120)
	want := sliceAcc(t, slice)

	for _, digest := range []bool{true, false} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ch := make(chan error, 1)
		go func() {
			conn, err := net.Dial("tcp", l.Addr().String())
			if err != nil {
				ch <- err
				return
			}
			_, err = Send(context.Background(), conn, bytes.NewReader(slice), 1, 1, 0, digest)
			ch <- err
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		results, err := Receive(ctx, l, 1, nil)
		if err != nil {
			t.Fatalf("digest=%v receive: %v", digest, err)
		}
		if serr := <-ch; serr != nil {
			t.Fatalf("digest=%v send: %v", digest, serr)
		}
		if len(results) != 1 || results[0].Trailer == nil {
			t.Fatalf("digest=%v: no trailer", digest)
		}
		got := results[0].Digest
		if digest {
			if got != want {
				t.Errorf("digest=%v: trailer digest %x != reference %x", digest, got[:4], want[:4])
			}
		} else {
			if got != [32]byte{} {
				t.Errorf("digest=false: trailer digest must be zero, got %x", got[:4])
			}
		}
	}
}

// TestSendCompressedDigestTrailer is the same invariant for the compressed
// (P4) path: the digest is over the UNcompressed payload, so it matches the
// reference regardless of compression.
func TestSendCompressedDigestTrailer(t *testing.T) {
	rnd := rand.New(rand.NewSource(9))
	slice := genSlice(rnd, 90)
	want := sliceAcc(t, slice)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan error, 1)
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			ch <- err
			return
		}
		_, err = SendCompressed(context.Background(), conn, bytes.NewReader(slice), 1, 1, 0, compress.ModeZstd, 3, true)
		ch <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := ReceiveCompressed(ctx, l, 1, compress.ModeZstd, nil)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if serr := <-ch; serr != nil {
		t.Fatalf("send: %v", serr)
	}
	if len(results) != 1 || results[0].Trailer == nil {
		t.Fatal("no trailer")
	}
	if results[0].Digest != want {
		t.Errorf("compressed trailer digest %x != reference %x", results[0].Digest[:4], want[:4])
	}
}

// TestDigestXORAcrossSlices verifies the core P0 invariant: the XOR of the
// per-slice trailer digests of a unit is independent of how the unit's rows
// are partitioned into slices. The same row multiset split S ways on the
// source must XOR to the same value as split D ways on the destination
// (S != D) — this is what lets the destination XOR its D segment digests and
// compare against the source's S segment digests.
func TestDigestXORAcrossSlices(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	// Generate N independent single rows so we can regroup them freely into
	// different partitionings.
	total := 240                    // divisible by 3 and 4 so every split uses all rows
	single := make([][]byte, total) // each = header+1row+EOF
	for i := 0; i < total; i++ {
		s := genSlice(rnd, 1)
		// strip header (19B) and EOF (2B) -> the single row's wire bytes
		row := s[19 : len(s)-2]
		single[i] = row
	}

	// For each split, compute the XOR of per-slice digests (each partition is
	// one self-contained slice).
	xorOf := func(k int) [32]byte {
		var acc [32]byte
		per := total / k
		for p := 0; p < k; p++ {
			b := bcopyHeader()
			for i := p * per; i < (p+1)*per; i++ {
				b = append(b, single[i]...)
			}
			b = bcopyEOF(b)
			ref := sliceAcc(t, b)
			xorAcc(&acc, &ref)
		}
		return acc
	}
	a := xorOf(3) // source split
	b := xorOf(4) // destination split (different)
	if a != b {
		t.Errorf("XOR of 3-way split %x != XOR of 4-way split %x (row multiset identical)", a[:4], b[:4])
	}
}

// TestDigestReSerializationVerbatim verifies the routed invariant: re-reading a
// slice row-by-row (RowSplitter) and re-emitting each row verbatim into new
// per-destination BinaryCopyStream slices yields per-slice digests whose XOR
// equals the original slice's digest — i.e. routing (which re-serializes the
// rows) does not change the row content, so the digest is preserved.
func TestDigestReSerializationVerbatim(t *testing.T) {
	rnd := rand.New(rand.NewSource(11))
	slice := genSlice(rnd, 80)
	want := sliceAcc(t, slice)

	// Re-serialize: split into rows, then regroup into 2 destination slices
	// (simulating S=1 source segment routed to D=2 destination segments) and
	// XOR the destination per-slice digests.
	split := transfer.NewRowSplitter(bytes.NewReader(slice))
	var destSlices [][]byte
	var cur [2][]byte
	n := 0
	for {
		row, err := split.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("split: %v", err)
		}
		d := n % 2
		if cur[d] == nil {
			cur[d] = bcopyHeader()
		}
		cur[d] = append(cur[d], row...)
		n++
	}
	for d := 0; d < 2; d++ {
		if cur[d] == nil {
			cur[d] = bcopyHeader()
		}
		destSlices = append(destSlices, bcopyEOF(cur[d]))
	}
	var acc [32]byte
	for _, ds := range destSlices {
		ref := sliceAcc(t, ds)
		xorAcc(&acc, &ref)
	}
	if acc != want {
		t.Errorf("routed re-serialization digest %x != original %x", acc[:4], want[:4])
	}
}

// TestDigestNegativeOneBit verifies that flipping a single byte in a row
// changes the digest (corruption is detectable).
func TestDigestNegativeOneBit(t *testing.T) {
	// Build a known slice: header + one row with one 64-byte data field + EOF,
	// so we can flip a byte INSIDE the field data (not a structural byte).
	slice := bcopyHeader()
	field := make([]byte, 64)
	for i := range field {
		field[i] = 'A'
	}
	slice = bcopyRow(slice, field)
	slice = bcopyEOF(slice)
	want := sliceAcc(t, slice)

	corrupt := append([]byte(nil), slice...)
	// The field data starts at 19 (header) + 2 (fieldCount) + 4 (len) = 25.
	corrupt[25] ^= 0x01
	got := sliceAcc(t, corrupt)
	if got == want {
		t.Error("flipping a byte did not change the digest")
	}
}

// TestDigestEmptySlice verifies the trailer digest of an empty (0-row) slice
// is the zero accumulator (XOR identity) with 0 rows.
func TestDigestEmptySlice(t *testing.T) {
	slice := bcopyEOF(bcopyHeader())
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan error, 1)
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			ch <- err
			return
		}
		_, err = Send(context.Background(), conn, bytes.NewReader(slice), 1, 1, 0, true)
		ch <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := Receive(ctx, l, 1, nil)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if serr := <-ch; serr != nil {
		t.Fatalf("send: %v", serr)
	}
	if results[0].Trailer == nil {
		t.Fatal("no trailer")
	}
	if results[0].Rows != 0 {
		t.Errorf("rows = %d, want 0", results[0].Rows)
	}
	if results[0].Digest != [32]byte{} {
		t.Errorf("empty-slice digest = %x, want zero", results[0].Digest[:4])
	}
}
