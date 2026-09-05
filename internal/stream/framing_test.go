package stream

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gpstream/gpstream/internal/compress"
)

// framedPipeNet builds a FramedPipe over an in-memory net.Pipe with the given
// compressor/decompressor. The write side is the source agent's view; the read
// side is the destination agent's view.
func framedPipeNet(t *testing.T, comp *compress.Compressor, dec *compress.Decompressor) *FramedPipe {
	t.Helper()
	w, r := net.Pipe()
	return NewFramedPipe(w, r, 1, 5, comp, dec)
}

// drainRead reads from the FramedPipe until io.EOF, accumulating the
// decompressed bytes. It is bounded by ctx so it can never block forever.
func drainRead(t *testing.T, ctx context.Context, fp *FramedPipe) (string, error) {
	t.Helper()
	buf := make([]byte, 4096)
	var out []byte
	for {
		n, err := fp.Read(buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return string(out), nil
			}
			return string(out), err
		}
	}
}

// TestFramedPipeRoundTrip verifies that bytes written to the source side are
// read back on the destination side through the real framed+compressed
// transport (FR-01/FR-12). Both compression and framing are exercised.
func TestFramedPipeRoundTrip(t *testing.T) {
	for _, mode := range []compress.Mode{compress.ModeNone, compress.ModeZstd} {
		comp := compress.New(mode, 3)
		dec := compress.NewDec(mode)
		fp := framedPipeNet(t, comp, dec)

		const payload = "gpstream framed pipe round-trip payload " + "extra-data-block-to-pad"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		got := make(chan string, 1)
		errc := make(chan error, 1)
		go func() {
			s, err := drainRead(t, ctx, fp)
			if err != nil {
				errc <- err
				return
			}
			got <- s
		}()

		if _, err := fp.Write([]byte(payload)); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = fp.Close()

		select {
		case err := <-errc:
			t.Fatalf("mode %s read: %v", mode, err)
		case s := <-got:
			if s != payload {
				t.Errorf("mode %s: got %q, want %q", mode, s, payload)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("mode %s: timed out (reader blocked)", mode)
		}
		cancel()
	}
}

// TestFramedPipeEmptyWrite verifies a zero-length write is a no-op (does not
// emit a frame) and does not block the reader.
func TestFramedPipeEmptyWrite(t *testing.T) {
	comp := compress.New(compress.ModeZstd, 3)
	dec := compress.NewDec(compress.ModeZstd)
	fp := framedPipeNet(t, comp, dec)
	n, err := fp.Write([]byte{})
	if n != 0 || err != nil {
		t.Errorf("empty write: n=%d err=%v, want 0,nil", n, err)
	}
}

// TestFramedPipeCorruption verifies a CRC mismatch is surfaced (not swallowed)
// as ErrCorrupt — corruption is detected without re-copying the table
// (FR-01/§4.2). A single hand-corrupted frame is written straight to the pipe.
func TestFramedPipeCorruption(t *testing.T) {
	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()
	fp := &FramedPipe{
		write: &FramedWriter{conn: w, jobID: 1, unitID: 5, comp: compress.New(compress.ModeNone, 3)},
		read:  &FramedReader{conn: r, comp: compress.NewDec(compress.ModeNone)},
	}
	bad := (&Frame{JobID: 1, UnitID: 5, Seq: 0, Data: []byte("hello")}).Encode()
	bad[24] ^= 0xFF // corrupt the first payload byte -> CRC mismatch

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// net.Pipe is synchronous: the write blocks until the reader consumes the
	// bytes, so the write must run concurrently with the read.
	wc := make(chan error, 1)
	go func() {
		_, err := w.Write(bad)
		wc <- err
		_ = w.Close()
	}()
	// Read in a goroutine bounded by ctx so a stuck read cannot hang the suite.
	got := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := fp.Read(buf)
		got <- err
	}()
	select {
	case err := <-got:
		if !errors.Is(err, ErrCorrupt) {
			t.Errorf("expected ErrCorrupt, got %v", err)
		}
	case <-ctx.Done():
		t.Error("read blocked; expected ErrCorrupt, not a hang")
	}
}

// TestNewNetPipeDialer verifies NewNetPipe uses a supplied dialer and returns
// its Pipe (the custom-dialer branch, e.g. a TLS-wrapped dialer, FR-13).
func TestNewNetPipeDialer(t *testing.T) {
	called := false
	dialer := func(ctx context.Context, addr string) (Pipe, error) {
		called = true
		return dummyPipe{}, nil
	}
	p, err := NewNetPipe(context.Background(), "ignored", dialer)
	if err != nil {
		t.Fatalf("newnetpipe: %v", err)
	}
	if p == nil {
		t.Error("expected a non-nil pipe")
	}
	if !called {
		t.Error("custom dialer was not used")
	}
}

// TestNewNetPipeDefaultDialError verifies the default dialer surfaces a dial
// error (the defaultDial error branch). Port 1 on loopback is refused quickly.
func TestNewNetPipeDefaultDialError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := NewNetPipe(ctx, "127.0.0.1:1", nil)
	if err == nil {
		t.Error("expected a dial error, got nil")
	}
}

// dummyPipe is a no-op Pipe used by TestNewNetPipeDialer.
type dummyPipe struct{}

func (dummyPipe) Read(p []byte) (int, error)  { return 0, nil }
func (dummyPipe) Write(p []byte) (int, error) { return len(p), nil }
func (dummyPipe) Close() error               { return nil }
