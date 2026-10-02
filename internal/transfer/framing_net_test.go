package transfer

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// countingPipe is a net.Conn wrapper that tallies the bytes actually pushed
// through it. Used to assert what travels ON THE SOCKET (the P0 acceptance:
// the network carries the COMPRESSED volume, not the logical volume).
type countingPipe struct {
	net.Conn
	mu   sync.Mutex
	wire uint64
}

func (c *countingPipe) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.mu.Lock()
		c.wire += uint64(n)
		c.mu.Unlock()
	}
	return n, err
}

func (c *countingPipe) bytes() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wire
}

// TestFramedPipeOverTCP is the P0 acceptance: the framed transport runs over
// a REAL loopback TCP socket (two goroutines standing in for the two agent
// processes), and the bytes observed ON THE SOCKET equal the compressed
// (framed) volume — strictly less than the logical volume for compressible
// data — while the payload arrives byte-for-byte intact. This proves the
// two-agent data path (compression between the hosts) works end to end.
func TestFramedPipeOverTCP(t *testing.T) {
	// Highly compressible payload: 8 MiB of a repeating 64 KiB pattern.
	pattern := make([]byte, 64*1024)
	for i := range pattern {
		pattern[i] = byte(i % 251)
	}
	var logical []byte
	for i := 0; i < 128; i++ { // 128 * 64 KiB = 8 MiB
		logical = append(logical, pattern...)
	}

	// Real loopback TCP.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	var (
		srvConn   net.Conn
		acceptErr error
		accepted  = make(chan struct{})
	)
	go func() {
		srvConn, acceptErr = l.Accept()
		close(accepted)
	}()

	cliConn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	<-accepted
	if acceptErr != nil {
		t.Fatalf("accept: %v", acceptErr)
	}
	// Count the bytes on the SOURCE side of the socket (what the source agent
	// pushes). The destination side receives the same bytes.
	srcSide := &countingPipe{Conn: cliConn}
	dstSide := &countingPipe{Conn: srvConn}
	defer srcSide.Close()
	defer dstSide.Close()

	comp := compress.New(compress.ModeZstd, 3)
	dec := compress.NewDec(compress.ModeZstd)

	// Source half (write side) — stands in for the source agent.
	fw := NewFramedWriter(srcSide, 1, 1, comp)
	// Destination half (read side) — stands in for the dest agent.
	fr := NewFramedReader(dstSide, dec)

	var (
		dstErr  error
		dstDone = make(chan struct{})
	)
	go func() {
		defer close(dstDone)
		got := &bytes.Buffer{}
		if _, err := io.Copy(got, fr); err != nil {
			dstErr = err
			return
		}
		if !bytes.Equal(got.Bytes(), logical) {
			dstErr = fmt.Errorf("payload mismatch: got %d bytes, want %d", got.Len(), len(logical))
		}
	}()

	// Feed the logical payload to the source half, then close (end-of-copy).
	if _, err := fw.Write(logical); err != nil {
		t.Fatalf("source write: %v", err)
	}
	if err := fw.CloseFlush(); err != nil {
		t.Fatalf("source close: %v", err)
	}

	<-dstDone
	if dstErr != nil {
		t.Fatalf("destination: %v", dstErr)
	}

	// P0 acceptance: the bytes ON THE SOCKET are the compressed/framed
	// volume, strictly less than the logical volume for compressible data.
	onSocket := srcSide.bytes()
	if onSocket == 0 {
		t.Fatal("no bytes observed on the socket")
	}
	if onSocket >= uint64(len(logical)) {
		t.Fatalf("socket carried %d bytes >= logical %d: the network was not compressed", onSocket, len(logical))
	}
	// The writer's own wire counter (compressed, framed) should match what
	// actually hit the socket (it is the only writer on that end).
	if fw.Wire() != onSocket {
		t.Fatalf("writer wire counter %d != bytes on socket %d", fw.Wire(), onSocket)
	}
	// The reader's logical counter must equal the decompressed volume.
	if fr.Logical() != uint64(len(logical)) {
		t.Fatalf("reader logical %d != %d", fr.Logical(), len(logical))
	}
	t.Logf("logical=%d on-socket=%d (ratio %.1fx) writer.wire=%d reader.logical=%d",
		len(logical), onSocket, float64(len(logical))/float64(onSocket), fw.Wire(), fr.Logical())
}

// TestFramedWriterAbort is the P0/F-06 escape-hatch check over an in-memory
// (unbuffered) transport: a source writer blocked on a full pipe is unblocked
// (returns an error, does not hang) when the destination aborts. Over a real
// socket the same property holds but surfaces as a TCP RST after the send
// buffer fills — an OS-level timing detail, so the deterministic contract is
// asserted here on the in-memory pipe (the unit the transport owns).
func TestFramedWriterAbort(t *testing.T) {
	w, r := net.Pipe()
	comp := compress.New(compress.ModeZstd, 3)
	dec := compress.NewDec(compress.ModeZstd)
	fw := NewFramedWriter(w, 1, 1, comp)
	fr := NewFramedReader(r, dec)

	// The writer is blocked: nobody is reading, and the payload is large
	// enough (many frames) that the unbuffered pipe stays full.
	blocked := make(chan error, 1)
	go func() {
		big := make([]byte, 16*1024*1024)
		_, e := fw.Write(big)
		blocked <- e
	}()

	// Give the writer a moment to block on the full pipe, then abort the
	// destination side.
	time.Sleep(50 * time.Millisecond)
	if err := fr.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}

	select {
	case e := <-blocked:
		if e == nil {
			t.Fatal("source write returned nil after a destination abort (want an error)")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("source write hung after a destination abort (FR-06: must fail, not hang)")
	}
}
