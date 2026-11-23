package agent

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

// TestReadCappedLineNormal verifies in-bounds behavior matches
// bufio.ReadBytes('\n'): line + newline, err = nil.
func TestReadCappedLineNormal(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(`{"op":"ping"}` + "\n" + `{"op":"pong"}` + "\n"))
	b, err := readCappedLine(r)
	if err != nil {
		t.Fatalf("first line: %v", err)
	}
	if string(b) != `{"op":"ping"}`+"\n" {
		t.Fatalf("got %q", b)
	}
	// The next line must survive in the buffer (no over-consume).
	b, err = readCappedLine(r)
	if err != nil {
		t.Fatalf("second line: %v", err)
	}
	if string(b) != `{"op":"pong"}`+"\n" {
		t.Fatalf("got %q", b)
	}
}

// TestReadCappedLineAtCap is the boundary: a line of exactly maxControlLine
// bytes + newline must be accepted (the cap is inclusive).
func TestReadCappedLineAtCap(t *testing.T) {
	payload := make([]byte, maxControlLine)
	for i := range payload {
		payload[i] = 'a'
	}
	r := bufio.NewReader(bytes.NewReader(append(payload, '\n')))
	b, err := readCappedLine(r)
	if err != nil {
		t.Fatalf("at-cap line rejected: %v", err)
	}
	if len(b) != maxControlLine+1 || !bytes.Equal(b[:maxControlLine], payload) {
		t.Fatalf("unexpected payload (len %d)", len(b))
	}
}

// TestReadCappedLineOversized is the DoS regression: a peer streaming more
// than maxControlLine bytes without a newline must be rejected with an error
// (bounded memory), not absorbed.
func TestReadCappedLineOversized(t *testing.T) {
	payload := make([]byte, maxControlLine+64)
	for i := range payload {
		payload[i] = 'x'
	}
	r := bufio.NewReader(bytes.NewReader(payload))
	b, err := readCappedLine(r)
	if err == nil {
		t.Fatalf("oversized line accepted (len %d)", len(b))
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("wrong error: %v", err)
	}
}

// chunkReader serves its payload on demand, like a live socket: bytes
// arrive as the reader requests them (no pre-buffered line).
type chunkReader struct {
	chunk []byte
	off   int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.off >= len(c.chunk) {
		return 0, io.EOF
	}
	n := copy(p, c.chunk[c.off:])
	c.off += n
	return n, nil
}

// TestReadCappedLineOversizedLiveStream covers the wire case (not a
// pre-buffered slice): a peer dribbles newline-free bytes as the reader
// asks for them; the reader must reject as soon as the cap+1 window fills.
func TestReadCappedLineOversizedLiveStream(t *testing.T) {
	payload := make([]byte, maxControlLine+4*1024)
	for i := range payload {
		payload[i] = 'y'
	}
	cr := bufio.NewReaderSize(&chunkReader{chunk: payload}, 4096)
	_, err := readCappedLine(cr)
	if err == nil {
		t.Fatal("live oversized stream accepted")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("wrong error: %v", err)
	}
}

// TestReadCappedLineTruncatedEOF: the stream ends inside a newline-less
// final line within the cap; the remainder comes back with io.EOF (the
// original ReadBytes semantics recvLine relies on).
func TestReadCappedLineTruncatedEOF(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader([]byte(`{"op":"ping"`)))
	b, err := readCappedLine(r)
	if !errorsIs(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
	if string(b) != `{"op":"ping"` {
		t.Fatalf("got %q", b)
	}
}

// TestReadCappedLineCleanEOF: an empty stream yields (empty, io.EOF).
func TestReadCappedLineCleanEOF(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader(nil))
	b, err := readCappedLine(r)
	if !errorsIs(err, io.EOF) || len(b) != 0 {
		t.Fatalf("got (%q, %v)", b, err)
	}
}

func errorsIs(err error, target error) bool {
	return err == target || (err != nil && strings.Contains(err.Error(), target.Error()))
}
