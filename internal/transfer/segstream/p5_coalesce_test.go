package segstream

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// p5Row is one binary-COPY row (~40 bytes): 3 fields, like genCompressibleSlice.
func p5Row(i int) []byte {
	var b []byte
	var fc [2]byte
	binary.BigEndian.PutUint16(fc[:], 3)
	b = append(b, fc[:]...)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], 8)
	b = append(b, l[:]...)
	b = append(b, []byte("row-000-")...)
	binary.BigEndian.PutUint32(l[:], 1)
	b = append(b, l[:]...)
	b = append(b, byte(i%256))
	binary.BigEndian.PutUint32(l[:], 16)
	b = append(b, l[:]...)
	b = append(b, []byte("0123456789abcdef")...)
	return b
}

// countFrames drains a framed stream and returns the number of DATA frames
// (the trailer is not counted).
func countFrames(conn net.Conn) (int, error) {
	n := 0
	for {
		f, err := transfer.ReadFrame(context.Background(), conn)
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
		if f.Flags&transfer.FlagTrailer != 0 {
			return n, nil
		}
		n++
	}
}

// TestSendCompressedCoalescesPipedRows (P5 regression): the base direct path
// feeds SendCompressed from an unbuffered io.Pipe that yields one binary-COPY
// row per Read. Before the fix, SendCompressed framed each row as its own
// frame and re-initialised a zstd encoder per frame (~13MB allocation each) —
// at 300k rows the source agent livelocked in GC (2 CPU pinned, 0 rows on the
// destination, P5). The fix coalesces reads up to chunkSize, so the frame
// count is bounded by payload/chunkSize, not by the row count.
func TestSendCompressedCoalescesPipedRows(t *testing.T) {
	const nRows = 15000 // ~600KB of payload
	pr, pw := io.Pipe()
	defer pr.Close()

	// Feed the slice row by row through the unbuffered pipe — the exact
	// shape of the P5 direct path (server.go: COPY -> io.Pipe -> SendCompressed).
	go func() {
		var err error
		_, err = pw.Write(bcopyHeader())
		if err == nil {
			for i := 0; i < nRows; i++ {
				if _, err = pw.Write(p5Row(i)); err != nil {
					break
				}
			}
		}
		if err == nil {
			_, err = pw.Write([]byte{0xFF, 0xFF}) // int16 -1 end-of-stream marker
		}
		if cerr := pw.CloseWithError(err); cerr != nil && err != nil {
			_ = cerr
		}
	}()

	pc, sc := net.Pipe()
	defer pc.Close()
	defer sc.Close()

	done := make(chan error, 1)
	go func() {
		_, e := SendCompressed(context.Background(), pc, pr, 1, 1, 0, compress.ModeZstd, 3)
		done <- e
	}()

	frames, ferr := countFrames(sc)
	if e := <-done; e != nil {
		t.Fatalf("SendCompressed: %v", e)
	}
	if ferr != nil {
		t.Fatalf("countFrames: %v", ferr)
	}
	// ~600KB / 256KB = 3 full frames + a small tail. The buggy code produced
	// one frame per row (15000). Assert the coalesced count is bounded by
	// payload/chunkSize (with slack) and far below the row count.
	if frames > 4 {
		t.Fatalf("data frames = %d for %d rows — SendCompressed framed rows individually (P5 regression); want <= 4", frames, nRows)
	}
	t.Logf("P5 coalesce ok: rows=%d dataFrames=%d", nRows, frames)
}

// TestSendCoalescesPipedRows is the uncompressed Send twin of the P5
// regression: row-by-row pipe input must still yield chunkSize-sized frames,
// not one frame per row.
func TestSendCoalescesPipedRows(t *testing.T) {
	const nRows = 15000
	pr, pw := io.Pipe()
	defer pr.Close()
	go func() {
		var err error
		_, err = pw.Write(bcopyHeader())
		if err == nil {
			for i := 0; i < nRows; i++ {
				if _, err = pw.Write(p5Row(i)); err != nil {
					break
				}
			}
		}
		if err == nil {
			_, err = pw.Write([]byte{0xFF, 0xFF})
		}
		_ = pw.CloseWithError(err)
	}()

	pc, sc := net.Pipe()
	defer pc.Close()
	defer sc.Close()
	done := make(chan error, 1)
	go func() {
		_, e := Send(context.Background(), pc, pr, 1, 1, 0)
		done <- e
	}()
	frames, ferr := countFrames(sc)
	if e := <-done; e != nil {
		t.Fatalf("Send: %v", e)
	}
	if ferr != nil {
		t.Fatalf("countFrames: %v", ferr)
	}
	if frames > 4 {
		t.Fatalf("data frames = %d for %d rows — Send framed rows individually (P5 regression); want <= 4", frames, nRows)
	}
	t.Logf("P5 coalesce (uncompressed) ok: rows=%d dataFrames=%d", nRows, frames)
}
