package segstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
)

// --- binary COPY stream helpers (real PG 9.4.26 layout) ---

func bcopyHeader() []byte {
	b := []byte("PGCOPY\n\xff\r\n\x00")
	b = append(b, 0, 0, 0, 0) // int32 remainingHeaderSize = 0
	b = append(b, 0, 0, 0, 0) // int32 flags = 0
	return b
}

func bcopyRow(dst []byte, fields ...[]byte) []byte {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(fields)))
	dst = append(dst, n[:]...)
	for _, f := range fields {
		var l [4]byte
		if f == nil {
			binary.BigEndian.PutUint32(l[:], 0xFFFFFFFF) // -1 NULL
		} else {
			binary.BigEndian.PutUint32(l[:], uint32(len(f)))
		}
		dst = append(dst, l[:]...)
		if f != nil {
			dst = append(dst, f...)
		}
	}
	return dst
}

func bcopyEOF(dst []byte) []byte {
	var m [2]byte
	binary.BigEndian.PutUint16(m[:], 0xFFFF) // int16 -1
	return append(dst, m[:]...)
}

// genSlice builds one segment slice: a self-contained binary COPY stream of
// nRows rows (mixed NULL/empty/data), as the kernel would feed it to the
// segreceiver on stdin.
func genSlice(rnd *rand.Rand, nRows int) []byte {
	b := bcopyHeader()
	for i := 0; i < nRows; i++ {
		nf := rnd.Intn(4)
		if nf == 0 && rnd.Intn(3) == 0 {
			b = bcopyRow(b) // zero-field row
			continue
		}
		if nf == 0 {
			nf = 1
		}
		var fields [][]byte
		for j := 0; j < nf; j++ {
			switch rnd.Intn(8) {
			case 0:
				fields = append(fields, nil) // NULL
			case 1:
				fields = append(fields, []byte{})
			default:
				l := rnd.Intn(500)
				fb := make([]byte, l)
				rnd.Read(fb)
				fields = append(fields, fb)
			}
		}
		b = bcopyRow(b, fields...)
	}
	return bcopyEOF(b)
}

// countRows references the stream structurally (independent of RowCounter).
func countRows(stream []byte) int64 {
	n := int64(0)
	off := 19
	for off < len(stream) {
		fc := int16(binary.BigEndian.Uint16(stream[off:]))
		off += 2
		if fc == -1 {
			break
		}
		for j := 0; j < int(fc); j++ {
			ln := int32(binary.BigEndian.Uint32(stream[off:]))
			off += 4
			if ln >= 0 {
				off += int(ln)
			}
		}
		n++
	}
	return n
}

// fakeSegment plays the kernel + segreceiver: it dials the controller and
// sends the slice on a fresh connection. The outcome (nil on success) is
// reported on ch so the test fails fast without calling t.Fatalf from a
// goroutine.
func fakeSegment(ch chan<- error, addr string, slice []byte, jobID, unitID uint32, content int16) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		ch <- fmt.Errorf("fake segment dial: %w", err)
		return
	}
	if _, err := Send(context.Background(), conn, bytes.NewReader(slice), jobID, unitID, content, false); err != nil {
		ch <- fmt.Errorf("Send: %w", err)
		return
	}
	ch <- nil
}

// TestSendReceiveRoundTrip: one segment slice over real TCP — byte
// transparency, row count and the trailer digest must all match.
func TestSendReceiveRoundTrip(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	slice := genSlice(rnd, 300)
	wantRows := countRows(slice)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	ch := make(chan error, 1)
	go fakeSegment(ch, addr, slice, 7, 3, 5)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var got []byte
	results, err := Receive(ctx, l, 1, func(contentID int16, p []byte) error {
		if contentID != 5 {
			t.Errorf("content %d, want 5", contentID)
		}
		got = append(got, p...)
		return nil
	})
	if serr := <-ch; serr != nil {
		t.Fatalf("fake segment: %v", serr)
	}
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	r := results[0]
	if r.Err != nil {
		t.Fatalf("slice err: %v", r.Err)
	}
	if r.ContentID != 5 {
		t.Fatalf("content = %d, want 5", r.ContentID)
	}
	if r.Rows != wantRows {
		t.Fatalf("rows = %d, want %d", r.Rows, wantRows)
	}
	if !bytes.Equal(got, slice) {
		t.Fatalf("payload not byte-transparent: got %d bytes, want %d", len(got), len(slice))
	}
	sum := sha256.Sum256(slice)
	if r.SHA256 != sum {
		t.Fatalf("trailer sha256 mismatch: %x vs %x", r.SHA256, sum)
	}
}

// TestReceiveMultipleStreams: two segments stream in parallel; the
// controller must collect both slices with the right content tags and
// trailers, regardless of arrival order.
func TestReceiveMultipleStreams(t *testing.T) {
	rnd := rand.New(rand.NewSource(2))
	slices := map[int16][]byte{
		0: genSlice(rnd, 200),
		1: genSlice(rnd, 400),
	}
	wantTotal := countRows(slices[0]) + countRows(slices[1])

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	ch0 := make(chan error, 1)
	ch1 := make(chan error, 1)
	go fakeSegment(ch0, addr, slices[0], 1, 1, 0)
	go fakeSegment(ch1, addr, slices[1], 1, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	per := map[int16]*bytes.Buffer{}
	results, err := Receive(ctx, l, 2, func(contentID int16, p []byte) error {
		mu.Lock()
		defer mu.Unlock()
		b, ok := per[contentID]
		if !ok {
			b = &bytes.Buffer{}
			per[contentID] = b
		}
		_, werr := b.Write(p)
		return werr
	})
	if serr := <-ch0; serr != nil {
		t.Fatalf("fake segment 0: %v", serr)
	}
	if serr := <-ch1; serr != nil {
		t.Fatalf("fake segment 1: %v", serr)
	}
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	var totalRows int64
	seen := map[int16]bool{}
	mu.Lock()
	snap := map[int16][]byte{}
	for c, b := range per {
		snap[c] = b.Bytes()
	}
	mu.Unlock()
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("slice %d err: %v", r.ContentID, r.Err)
		}
		seen[r.ContentID] = true
		totalRows += r.Rows
		if !bytes.Equal(snap[r.ContentID], slices[r.ContentID]) {
			t.Fatalf("slice %d not byte-transparent: %d vs %d bytes", r.ContentID, len(snap[r.ContentID]), len(slices[r.ContentID]))
		}
		sum := sha256.Sum256(slices[r.ContentID])
		if r.SHA256 != sum {
			t.Fatalf("slice %d sha256 mismatch", r.ContentID)
		}
	}
	if !seen[0] || !seen[1] {
		t.Fatalf("missing slices: %v", seen)
	}
	if totalRows != wantTotal {
		t.Fatalf("total rows = %d, want %d", totalRows, wantTotal)
	}
}

// TestReceiveTruncated: a segment that dies mid-stream (no trailer) must be
// flagged, and the remaining streams still collected.
func TestReceiveTruncated(t *testing.T) {
	rnd := rand.New(rand.NewSource(3))
	slice := genSlice(rnd, 100)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	// Segment 0: clean. Segment 9: one raw frame, then the connection dies
	// (no trailer) — the controller must flag it as truncated.
	ch := make(chan error, 1)
	go fakeSegment(ch, addr, slice, 1, 1, 0)
	killed := make(chan error, 1)
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			killed <- fmt.Errorf("dial: %w", err)
			return
		}
		f := &frameBuilder{conn: conn}
		if werr := f.data(9, slice[:19]); werr != nil {
			killed <- fmt.Errorf("raw frame: %w", werr)
			return
		}
		conn.Close()
		killed <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := Receive(ctx, l, 2, nil)
	if serr := <-ch; serr != nil {
		t.Fatalf("fake segment: %v", serr)
	}
	if serr := <-killed; serr != nil {
		t.Fatalf("killed segment setup: %v", serr)
	}
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	var truncated bool
	for _, r := range results {
		if r.ContentID == 9 && r.Err != nil && errors.Is(r.Err, ErrTruncatedStream) {
			truncated = true
		}
		if r.ContentID == 0 && r.Err != nil {
			t.Fatalf("clean slice errored: %v", r.Err)
		}
	}
	if !truncated {
		t.Fatal("expected ErrTruncatedStream on the killed slice")
	}
}

// TestSendBadStream: a non-COPY stdin is rejected (no trailer sent).
func TestSendBadStream(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	addr := l.Addr().String()
	type recvRes struct {
		results []*SliceResult
		err     error
	}
	done := make(chan recvRes, 1)
	go func() {
		r, e := Receive(context.Background(), l, 1, nil)
		done <- recvRes{r, e}
	}()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Send(context.Background(), conn, strings.NewReader("not a copy stream"), 1, 1, 0, false); err == nil {
		t.Fatal("Send on non-COPY stdin: expected error")
	}
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("Receive: %v", res.err)
		}
		if len(res.results) != 1 || res.results[0].Err == nil {
			t.Fatalf("expected a failed slice result, got %+v", res.results)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Receive did not return (segment stream never finished)")
	}
}

// TestProgramCommand: the generated command must carry the <SEGID>
// placeholder and reject unsafe listen values.
func TestProgramCommand(t *testing.T) {
	cmd, err := ProgramCommand("/usr/local/bin/gpstream-segreceiver", "10.0.0.5:9100", 42, 7)
	if err != nil {
		t.Fatalf("ProgramCommand: %v", err)
	}
	want := "/usr/local/bin/gpstream-segreceiver --listen 10.0.0.5:9100 --job 42 --unit 7 --content <SEGID>"
	if cmd != want {
		t.Fatalf("cmd = %q, want %q", cmd, want)
	}
	// Unsafe listen values must be refused (they land in a shell string).
	for _, bad := range []string{"10.0.0.5:9100; rm -rf /", "10.0.0.5:9100 $(x)", "10.0.0.5:0", "10.0.0.5:99999", "10.0.0.5", ""} {
		if _, err := ProgramCommand("/usr/local/bin/gpstream-segreceiver", bad, 1, 1); err == nil {
			t.Errorf("ProgramCommand accepted bad listen %q", bad)
		}
	}
	// Unsafe binary paths too.
	if _, err := ProgramCommand("/bin/seg; x", "10.0.0.5:9100", 1, 1); err == nil {
		t.Error("ProgramCommand accepted a path with a shell metacharacter")
	}
}

// TestCopyOnSegmentSQL: identifier quoting + the program string lands
// single-quoted (single quotes inside the program would break it — the
// program must not contain them).
func TestCopyOnSegmentSQL(t *testing.T) {
	sql := CopyOnSegmentSQL("public", "orders", "/usr/local/bin/gpstream-segreceiver --listen h:1 --job 1 --unit 2 --content <SEGID>")
	want := `COPY "public"."orders" TO PROGRAM '/usr/local/bin/gpstream-segreceiver --listen h:1 --job 1 --unit 2 --content <SEGID>' ON SEGMENT BINARY`
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
	// Identifier escaping.
	sql2 := CopyOnSegmentSQL(`a"b`, `c"d`, "prog")
	if !strings.Contains(sql2, `"a""b"."c""d"`) {
		t.Fatalf("identifier escaping broken: %q", sql2)
	}
}

// frameBuilder writes raw v2 frames (for the truncation test, where Send's
// own trailer would make the stream look clean).
type frameBuilder struct {
	conn net.Conn
	seq  uint64
}

func (f *frameBuilder) data(content int16, payload []byte) error {
	err := transfer.WriteFrame(f.conn, &transfer.Frame{
		JobID: 1, UnitID: 1, Seq: f.seq, ContentID: content, Data: payload,
	})
	f.seq++
	return err
}

func TestUnitListen(t *testing.T) {
	cases := []struct {
		base, unit, want string
	}{
		{"10.0.0.11:9432", "0", "10.0.0.11:9432"},
		{"10.0.0.11:9432", "1", "10.0.0.11:9433"},
		{"10.0.0.11:9432", "2", "10.0.0.11:9434"},
		{"[::1]:9432", "3", "[::1]:9435"},
	}
	for _, c := range cases {
		var unit uint32
		fmt.Sscanf(c.unit, "%d", &unit)
		got, err := UnitListen(c.base, unit)
		if err != nil {
			t.Fatalf("UnitListen(%s, %s): %v", c.base, c.unit, err)
		}
		if got != c.want {
			t.Errorf("UnitListen(%s, %s) = %s, want %s", c.base, c.unit, got, c.want)
		}
	}
	for _, bad := range []string{"", "hostonly", "h:0", "h:99999"} {
		if _, err := UnitListen(bad, 0); err == nil {
			t.Errorf("UnitListen(%q): expected error, got nil", bad)
		}
	}
	// Port overflow: base near 65535.
	if _, err := UnitListen("h:65535", 1); err == nil {
		t.Error("UnitListen(h:65535, 1): expected out-of-range error")
	}
}
