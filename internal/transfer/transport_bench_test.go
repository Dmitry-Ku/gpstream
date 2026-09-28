package transfer

import (
	"io"
	"net"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// benchTransport pumps totalBytes of incompressible-ish data through a
// FramedPipe (256 KiB frames, no compression) and reports MB/s. It isolates
// the transport cost (net.Pipe vs ReliablePipe) from the COPY/PG path.
func benchTransport(b *testing.B, reliable bool, totalMB int) {
	comp := compress.New(compress.ModeNone, 0)
	dec := compress.NewDec(compress.ModeNone)
	var w, r Pipe
	if reliable {
		s, rr := NewReliablePipePair(DefaultReliablePipeConfig())
		w, r = s, rr
	} else {
		pw, pr := net.Pipe()
		w, r = pw, pr
	}
	fp := NewFramedPipeOver(w, r, 1, 1, comp, dec)

	const chunk = 256 * 1024
	payload := make([]byte, chunk)
	for i := range payload {
		payload[i] = byte(i * 7 + 13)
	}
	total := totalMB * 1024 * 1024

	b.ResetTimer()
	errCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, fp)
		errCh <- err
	}()
	for written := 0; written < total; written += chunk {
		if _, err := fp.Write(payload); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
	_ = fp.Close()
	if err := <-errCh; err != nil && err != io.EOF {
		b.Fatalf("read: %v", err)
	}
}

func BenchmarkTransportNetPipe(b *testing.B)     { benchTransport(b, false, 300) }
func BenchmarkTransportReliablePipe(b *testing.B) { benchTransport(b, true, 300) }
