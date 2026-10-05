package segstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// routRow renders one binary-COPY row: int16 fieldCount + (int32 len, data)*.
func routRow(fields ...[]byte) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(len(fields)))
	for _, f := range fields {
		if f == nil {
			b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(f)))
		b = append(b, f...)
	}
	return b
}

func routInt4(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

// routErr is a simple error for test assertions.
type routErr struct{ msg string }

func (e *routErr) Error() string { return e.msg }

// TestRoutedSenderSplitsDisjointAndComplete is the P9 source-side acceptance:
// one source slice (S=1 here) with N rows is routed to D destination segments
// over W sub-streams each. Asserts:
//  1. every sub-stream is a well-formed binary-COPY stream (header + rows +
//     EOF),
//  2. the split is DISJOINT AND COMPLETE: the multiset of rows across all
//     D×W sub-streams == the source multiset (no duplication, no loss),
//  3. routing matches the kernel's placement: every row with key k lands in a
//     sub-stream of segment d == RouteKeySegment(k, D),
//  4. within a segment the rows are spread round-robin over the W
//     sub-streams (max-min spread <= 1),
//  5. each sub-stream's trailer row count == its row count.
func TestRoutedSenderSplitsDisjointAndComplete(t *testing.T) {
	const (
		nRows = 200
		D     = 2
		W     = 2
	)
	// Build the source slice: header + rows + EOF. Column 0 = int4 key.
	var src bytes.Buffer
	src.Write(BinaryHeaderBytes())
	keys := make([]int32, nRows)
	for i := 0; i < nRows; i++ {
		keys[i] = int32(i * 7 % 997)
		src.Write(routRow(routInt4(keys[i]), []byte("payload-")))
	}
	var eof [2]byte
	binary.BigEndian.PutUint16(eof[:], 0xFFFF)
	src.Write(eof[:])

	// The connection grid: net.Pipe pairs, one per (d, w). net.Pipe is
	// unbuffered, so the senders block until the drain goroutines read — the
	// drain MUST be running before Run() is called (mirrors production, where
	// the destination agent reads concurrently).
	type end struct {
		sender net.Conn
		reader net.Conn
	}
	grid := make([][]end, D)
	for d := 0; d < D; d++ {
		grid[d] = make([]end, W)
		for w := 0; w < W; w++ {
			a, b := net.Pipe()
			grid[d][w] = end{sender: a, reader: b}
		}
	}
	conns := make([][]transfer.Pipe, D)
	for d := 0; d < D; d++ {
		conns[d] = make([]transfer.Pipe, W)
		for w := 0; w < W; w++ {
			conns[d][w] = grid[d][w].sender // net.Conn satisfies transfer.Pipe
		}
	}

	// Start the drain goroutines FIRST (they read the reader ends).
	type sub struct {
		rows    [][]byte
		trailer int64
	}
	subs := make([][]*sub, D)
	drainErrs := make(chan error, D*W)
	for d := 0; d < D; d++ {
		subs[d] = make([]*sub, W)
		for w := 0; w < W; w++ {
			subs[d][w] = &sub{}
		}
	}
	for d := 0; d < D; d++ {
		for w := 0; w < W; w++ {
			d, w := d, w
			go func() {
				c := grid[d][w].reader
				defer c.Close()
				sawHeader := false
				var derr error
				defer func() { drainErrs <- derr }()
				for {
					f, rerr := transfer.ReadFrame(context.Background(), c)
					if rerr != nil {
						return // clean EOF after the last frame (or cancel)
					}
					if f.Flags&transfer.FlagTrailer != 0 {
						tr, terr := f.Trailer()
						if terr != nil {
							derr = terr
							return
						}
						subs[d][w].trailer = tr.Rows
						continue
					}
					if !sawHeader {
						if len(f.Data) < 19 || !bytes.Equal(f.Data[:11], []byte("PGCOPY\n\xff\r\n\x00")) {
							derr = &routErr{msg: "sub(" + itoa(d) + "," + itoa(w) + "): bad stream header"}
							return
						}
						sawHeader = true
						rest := f.Data[19:]
						subs[d][w].rows = append(subs[d][w].rows, routParseRows(&rest)...)
						continue
					}
					if bytes.HasSuffix(f.Data, eof[:]) {
						body := f.Data[:len(f.Data)-2]
						subs[d][w].rows = append(subs[d][w].rows, routParseRows(&body)...)
						continue
					}
					subs[d][w].rows = append(subs[d][w].rows, routParseRows(&f.Data)...)
				}
			}()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sender := NewRoutedSender(ctx, &src, []int{0}, []uint32{23}, D, W, 1, 1, 0, compress.ModeNone, 0, conns)
	rows, err := sender.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rows != int64(nRows) {
		t.Fatalf("routed rows = %d, want %d", rows, nRows)
	}

	// Wait for all drain goroutines (they exit on the clean connection EOF).
	var firstDrainErr error
	for i := 0; i < D*W; i++ {
		if e := <-drainErrs; e != nil && firstDrainErr == nil {
			firstDrainErr = e
		}
	}
	if firstDrainErr != nil {
		t.Fatalf("drain: %v", firstDrainErr)
	}

	// 2+3: disjoint-and-complete + correct placement.
	total := 0
	perSeg := map[int]int{}
	wantPerSeg := map[int]int{}
	for i := range keys {
		d, herr := transfer.RouteKeySegment([]int{0}, []uint32{23}, D,
			[][]byte{routInt4(keys[i])}, []bool{false})
		if herr != nil {
			t.Fatalf("reference hash: %v", herr)
		}
		wantPerSeg[int(d)]++
	}
	seen := map[string]int{}
	for d := 0; d < D; d++ {
		for w := 0; w < W; w++ {
			// 1: the trailer row count == the rows actually in the stream.
			if subs[d][w].trailer != int64(len(subs[d][w].rows)) {
				t.Errorf("sub(%d,%d): trailer %d != rows %d", d, w, subs[d][w].trailer, len(subs[d][w].rows))
			}
			for _, r := range subs[d][w].rows {
				total++
				perSeg[d]++
				seen[string(r)]++
			}
		}
	}
	if total != nRows {
		t.Fatalf("total rows across sub-streams = %d, want %d (disjoint-and-complete)", total, nRows)
	}
	for d := 0; d < D; d++ {
		if perSeg[d] != wantPerSeg[d] {
			t.Errorf("segment %d: got %d rows, want %d (kernel placement)", d, perSeg[d], wantPerSeg[d])
		}
	}
	srcMultiset := map[string]int{}
	for i := 0; i < nRows; i++ {
		srcMultiset[string(routRow(routInt4(keys[i]), []byte("payload-")))]++
	}
	for r, n := range seen {
		if srcMultiset[r] != n {
			t.Fatalf("row multiplicity mismatch: got %d, want %d (disjoint-and-complete violated)", n, srcMultiset[r])
		}
	}

	// 4: round-robin spread within each segment: max-min <= 1.
	for d := 0; d < D; d++ {
		min, max := -1, -1
		for w := 0; w < W; w++ {
			n := len(subs[d][w].rows)
			if min < 0 || n < min {
				min = n
			}
			if n > max {
				max = n
			}
		}
		if min >= 0 && max-min > 1 {
			t.Errorf("segment %d: sub-stream spread %d..%d (want balanced round-robin, max-min <= 1)", d, min, max)
		}
	}
}

// itoa is a tiny int->string for error messages (avoids importing strconv).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// routParseRows parses the row region of a binary-COPY payload into complete
// rows (the payload contains no header/EOF — those are handled by the
// caller).
func routParseRows(b *[]byte) [][]byte {
	var out [][]byte
	off := 0
	for off+2 <= len(*b) {
		fc := int(int16(binary.BigEndian.Uint16((*b)[off : off+2])))
		if fc < 0 {
			break
		}
		off += 2
		start := off - 2
		for j := 0; j < fc; j++ {
			if off+4 > len(*b) {
				return out
			}
			ln := int(int32(binary.BigEndian.Uint32((*b)[off : off+4])))
			off += 4
			if ln >= 0 {
				off += ln
			}
		}
		out = append(out, (*b)[start:off])
	}
	return out
}
