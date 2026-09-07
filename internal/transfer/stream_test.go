package transfer

import (
	"context"
	"io"
	"testing"
	"time"
)

// TestFrameRoundTrip verifies encode/decode round-trips a frame.
func TestFrameRoundTrip(t *testing.T) {
	f := &Frame{JobID: 1, UnitID: 7, Seq: 42, Data: []byte("hello gpstream")}
	b := f.Encode()
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.JobID != 1 || got.UnitID != 7 || got.Seq != 42 {
		t.Errorf("header mismatch: %+v", got)
	}
	if string(got.Data) != "hello gpstream" {
		t.Errorf("data = %q", got.Data)
	}
}

// TestFrameCorruption verifies a corrupted payload is detected by the CRC.
func TestFrameCorruption(t *testing.T) {
	f := &Frame{JobID: 1, UnitID: 1, Seq: 1, Data: []byte("abcdef")}
	b := f.Encode()
	// Corrupt a payload byte (offset 24 is the first payload byte).
	b[24] ^= 0xFF
	if _, err := Decode(b); err != ErrCorrupt {
		t.Errorf("corrupted frame: got %v, want ErrCorrupt", err)
	}
}

// TestFrameBadMagic verifies a bad magic is rejected.
func TestFrameBadMagic(t *testing.T) {
	f := &Frame{JobID: 1, UnitID: 1, Seq: 1, Data: []byte("x")}
	b := f.Encode()
	b[0] = 0x00
	if _, err := Decode(b); err == nil {
		t.Error("bad magic should error")
	}
}

// TestSessionOverPipe verifies a frame sent over a pipe is received and
// CRC-verified on the other side (no network required, CР-2). The send and
// receive run concurrently because the transport is synchronous, mirroring a
// real (async) net.Conn where a write does not block on the read.
func TestSessionOverPipe(t *testing.T) {
	r, w := io.Pipe()
	src := NewSession(&pipeAdapter{r: r, w: w}, 80)
	dst := NewSession(&pipeAdapter{r: r, w: w}, 80)

	f := &Frame{JobID: 100, UnitID: 5, Seq: 1, Data: []byte("payload-data")}

	recv := make(chan *Frame, 1)
	errc := make(chan error, 1)
	go func() {
		got, err := dst.Receive(context.Background())
		if err != nil {
			errc <- err
			return
		}
		recv <- got
	}()

	if err := src.Send(f); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case err := <-errc:
		t.Fatalf("receive: %v", err)
	case got := <-recv:
		if got.JobID != 100 || got.UnitID != 5 || got.Seq != 1 {
			t.Errorf("header mismatch: %+v", got)
		}
		if string(got.Data) != "payload-data" {
			t.Errorf("data = %q", got.Data)
		}
	}
	if src.Bytes() != uint64(len(f.Data)) {
		t.Errorf("bytes = %d, want %d", src.Bytes(), len(f.Data))
	}
}

// TestReceiveError verifies that a closed transport (peer gone) is surfaced as
// an error on receive — the real back-pressure / shutdown path. The context
// is used to bound the test.
func TestReceiveError(t *testing.T) {
	r, w := io.Pipe()
	s := NewSession(&pipeAdapter{r: r, w: w}, 80)
	// Close the write end so the blocking read returns an error.
	close := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = w.Close()
		close <- struct{}{}
	}()
	ctx := context.Background()
	_, err := s.Receive(ctx)
	<-close
	if err == nil {
		t.Error("expected error on closed transport")
	}
}

// pipeAdapter adapts an *io.Pipe into a Pipe (io.Pipe has a single reader and
// a single writer; we share the same r/w on both sides).
type pipeAdapter struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p *pipeAdapter) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *pipeAdapter) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *pipeAdapter) Close() error {
	_ = p.w.Close()
	_ = p.r.Close()
	return nil
}
