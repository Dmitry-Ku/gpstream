package segstream

import (
	"bytes"
	"context"
	"math/rand"
	"net"
	"testing"
	"time"
)

// TestFullPathByteExact: the WHOLE segment-direct path (Send on the segment:
// RowCounter + framing; Receive on the controller: unframing; then the mux
// merge) must be a byte-transparent pass-through for a single slice. The
// merged output must equal the input slice byte-for-byte (the mux re-emits the
// slice's own 19-byte header and 2-byte EOF and strips nothing else).
//
// This isolates segstream from the downstream framedPipe/compression: if this
// is byte-exact, the E2 on_segment corruption ("invalid byte sequence for
// encoding UTF8: 0x00", "unexpected EOF") is NOT in segstream and lives in the
// transport/compression or destination; if it fails, the bug is here.
func TestFullPathByteExact(t *testing.T) {
	for _, seed := range []int64{1, 5, 99} {
		t.Run("seed", func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			slice := genSlice(rnd, 6000) // ~2 MB, many frames
			if len(slice) <= chunkSize {
				t.Fatalf("slice too small: %d", len(slice))
			}

			var merged bytes.Buffer
			mux := NewSliceMux(&merged, 1)
			if err := mux.Begin(); err != nil {
				t.Fatalf("Begin: %v", err)
			}

			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := l.Addr().String()
			ch := make(chan error, 1)
			go fakeSegment(ch, addr, slice, 1, 1, 0) // Send: RowCounter+frame

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, rerr := Receive(ctx, l, 1, func(contentID int16, p []byte) error {
				return mux.Write(contentID, p) // Receive -> mux
			})
			if serr := <-ch; serr != nil {
				t.Fatalf("Send: %v", serr)
			}
			if rerr != nil {
				t.Fatalf("Receive: %v", rerr)
			}
			if !mux.AllDone() {
				t.Fatal("mux not AllDone")
			}
			if err := mux.End(); err != nil {
				t.Fatalf("End: %v", err)
			}

			got := merged.Bytes()
			if !bytes.Equal(got, slice) {
				at := firstDiff(got, slice)
				t.Fatalf("full-path merged != slice: len got=%d want=%d; first diff at byte %d\n  got  % x\n  want % x",
					len(got), len(slice), at, peek(got, at), peek(slice, at))
			}
		})
	}
}
