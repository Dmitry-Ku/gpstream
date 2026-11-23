package transfer

import (
	"bytes"
	"context"
	"testing"
)

// fuzzPipe adapts a byte slice to Pipe so stream-level readers (ReadFrame)
// can be driven with attacker-shaped input: any truncation surfaces as
// io.EOF, exactly like a half-closed socket.
type fuzzPipe struct{ r *bytes.Reader }

func (p *fuzzPipe) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *fuzzPipe) Write(b []byte) (int, error) { return len(b), nil }
func (p *fuzzPipe) Close() error                { return nil }

// seedFrames builds a few well-formed v2 frames (data, trailer, ping, pong,
// unit-begin) so the fuzzer has valid neighbors to mutate from.
func seedFrames() [][]byte {
	tr := Trailer{}
	copy(tr.SHA256[:], bytes.Repeat([]byte{0xAB}, 32))
	copy(tr.Digest[:], bytes.Repeat([]byte{0xCD}, 32))
	tr.Rows = 42
	seed := []*Frame{
		{JobID: 1, UnitID: 2, Seq: 3, ContentID: 7, Data: []byte("hello-frame")},
		NewTrailerFrame(1, 2, 3, 7, tr),
		NewPingFrame(1, 2, 4),
		NewPongFrame(1, 2, 5),
		NewUnitBeginFrame(1, 2, 6, []byte("payload")),
	}
	out := make([][]byte, 0, len(seed))
	for _, f := range seed {
		out = append(out, f.Encode())
	}
	return out
}

// FuzzFrameDecode is the primary attacker-surface target: frame bytes arrive
// over the network from a peer agent (or a compromised one), so Decode must
// reject any malformed input with an error and never panic.
func FuzzFrameDecode(f *testing.F) {
	for _, b := range seedFrames() {
		f.Add(b)
	}
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(bytes.Repeat([]byte{0}, 200))
	f.Fuzz(func(t *testing.T, b []byte) {
		if fr, err := Decode(b); err == nil {
			if len(b) == 0 {
				t.Fatalf("Decode accepted an empty input")
			}
			// A frame that carries the Trailer flag must carry a parseable
			// Trailer payload: Decode and Trailer() must agree on validity.
			if fr.Flags&FlagTrailer != 0 {
				if _, terr := fr.Trailer(); terr != nil {
					t.Fatalf("Decode returned a Trailer frame whose payload does not parse: %v", terr)
				}
			}
		}
		// Trailer payload decoding over arbitrary lengths (the wire form is
		// 72 bytes; truncation and overflow must be errors, not panics).
		_, _ = DecodeTrailerPayload(b)
	})
}

// FuzzReadFrame drives the stream-level reader: header + payload reads from
// a half-closed "socket" of attacker-shaped bytes. Truncation between header
// and payload must error, not hang or panic.
func FuzzReadFrame(f *testing.F) {
	for _, b := range seedFrames() {
		f.Add(b)
	}
	f.Add([]byte{1, 2, 3}) // header claiming a payload that never arrives
	f.Fuzz(func(t *testing.T, b []byte) {
		p := &fuzzPipe{r: bytes.NewReader(b)}
		if _, err := ReadFrame(context.Background(), p); err == nil {
			// A clean read on truncated input is only possible if the bytes
			// formed a complete frame; no assertion beyond no-panic needed.
		}
	})
}

// FuzzFrameRoundTrip is a property test: any Frame we construct must
// survive Encode -> Decode with all identity fields and the payload intact.
// A violation here means the wire codec lost information (silent data
// corruption in transit), which is strictly worse than a parse error.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add(uint32(1), uint32(2), uint64(3), int16(4), byte(0), []byte("seed"))
	f.Add(uint32(0xFFFFFFFF), uint32(0), uint64(1<<63), int16(-1), byte(FlagTrailer), []byte{})
	f.Fuzz(func(t *testing.T, jobID, unitID uint32, seq uint64, content int16, flags byte, data []byte) {
		// Only the defined flag bits are legal on the wire; the codec must
		// not silently drop or reinterpret them.
		flags &= byte(FlagTrailer | FlagUnitBegin | FlagPing | FlagPong)
		fr := &Frame{JobID: jobID, UnitID: unitID, Seq: seq, ContentID: content, Flags: flags, Data: data}
		// Trailer-flagged frames carry a Trailer payload, not raw data; the
		// round-trip property for raw data applies to non-trailer frames.
		if fr.Flags&FlagTrailer != 0 {
			return
		}
		enc := fr.Encode()
		got, err := Decode(enc)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if got.JobID != jobID || got.UnitID != unitID || got.Seq != seq || got.ContentID != content || got.Flags != flags {
			t.Fatalf("round-trip identity mismatch: want {%d %d %d %d %d}, got %+v",
				jobID, unitID, seq, content, flags, got)
		}
		if !bytes.Equal(got.Data, data) {
			t.Fatalf("round-trip data mismatch: %d bytes in, %d out", len(data), len(got.Data))
		}
	})
}

// FuzzHelloID covers the identity handshake body: a peer (or an attacker
// impersonating one) sends arbitrary bytes where a StreamID is expected.
func FuzzHelloID(f *testing.F) {
	f.Add(encodeHelloID(1, StreamID{JobID: 1, UnitID: 2, Content: 3}))
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Add(bytes.Repeat([]byte{0x7F}, 33))
	f.Fuzz(func(t *testing.T, b []byte) {
		// decodeHelloID must report failure (ok=false) for malformed input
		// and never panic.
		_, _ = decodeHelloID(b)
		// Round-trip property for every representable StreamID.
		var first byte
		if len(b) > 0 {
			first = b[0]
		}
		id := StreamID{
			JobID:      uint32(len(b)) + 1,
			UnitID:     uint32(first) + 1,
			Content:    int16(len(b) % 5),
			DstContent: int16(len(b) % 7),
			Worker:     int32(len(b) % 9),
		}
		enc := encodeHelloID(9, id)
		got, ok := decodeHelloID(enc)
		if !ok || got != id {
			t.Fatalf("hello round-trip failed: want %v, got %v (ok=%v)", id, got, ok)
		}
	})
}
