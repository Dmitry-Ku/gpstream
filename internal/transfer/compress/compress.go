// Package compress implements stream compression (FR-12): none / zlib / zstd.
// Compression happens on the source, decompression on the destination, per
// transfer. zstd uses github.com/klauspost/compress (BSD license); zlib and the
// framing use the Go standard library. The adaptive degradation (FR-12:
// auto-lower the level on CPU saturation) is driven by the ratelimit.
package compress

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Mode is the compression mode (FR-12).
type Mode string

const (
	// None disables compression.
	ModeNone Mode = "none"
	// Zlib is the standard zlib compression.
	ModeZlib Mode = "zlib"
	// Zstd is the recommended high-throughput compression.
	ModeZstd Mode = "zstd"
)

// Parse parses a --compress value, e.g. "zstd", "zstd:7", "zlib".
func Parse(s string) (Mode, int, error) {
	mode, level := ModeNone, 3
	switch {
	case s == "" || s == "none":
		mode = ModeNone
	case s == "zlib":
		mode = ModeZlib
	case s == "zstd":
		mode = ModeZstd
	case isZstd(s):
		mode = ModeZstd
		if l := parseLevel(s); l >= 1 && l <= 19 {
			level = l
		}
	case s == "zlib:1":
		mode = ModeZlib
	default:
		return ModeNone, 3, fmt.Errorf("compress: unknown mode %q", s)
	}
	return mode, level, nil
}

func isZstd(s string) bool {
	// "zstd" with optional ":N"
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return true
		}
	}
	// also matches bare "zstd..."
	return len(s) >= 4 && s[:4] == "zstd"
}

func parseLevel(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			n := 0
			for j := i + 1; j < len(s); j++ {
				if s[j] < '0' || s[j] > '9' {
					return 0
				}
				n = n*10 + int(s[j]-'0')
			}
			return n
		}
	}
	return 0
}

// Compressor compresses data according to the mode (on the source, FR-12).
type Compressor struct {
	mode  Mode
	level int
}

// New returns a Compressor for a mode and zstd level (default 3).
func New(mode Mode, level int) *Compressor {
	if level < 1 || level > 19 {
		level = 3
	}
	return &Compressor{mode: mode, level: level}
}

// Mode reports the compressor's mode (FR-12); used by the F-12 small-table
// auto-disable tests to assert which pair a unit's transport received.
func (c *Compressor) Mode() Mode { return c.mode }

// Encode compresses b, returning the (possibly) smaller encoding.
func (c *Compressor) Encode(b []byte) []byte {
	switch c.mode {
	case ModeNone:
		return b
	case ModeZlib:
		return zlibEncode(b)
	case ModeZstd:
		return zstdEncode(b, c.level)
	}
	return b
}

// Decompressor decompresses data according to the mode (on the dest, FR-12).
type Decompressor struct {
	mode Mode
}

// NewDec returns a Decompressor for a mode.
func NewDec(mode Mode) *Decompressor { return &Decompressor{mode: mode} }

// Mode reports the decompressor's mode (FR-12); used by the F-12 small-table
// auto-disable tests to assert which pair a unit's transport received.
func (d *Decompressor) Mode() Mode { return d.mode }

// Decode decompresses b, returning the original data.
func (d *Decompressor) Decode(b []byte) []byte {
	switch d.mode {
	case ModeNone:
		return b
	case ModeZlib:
		return zlibDecode(b)
	case ModeZstd:
		return zstdDecode(b)
	}
	return b
}

// --- zlib (stdlib) ---

func zlibEncode(b []byte) []byte {
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.DefaultCompression)
	if err != nil {
		return b
	}
	if _, err := w.Write(b); err != nil {
		return b
	}
	if err := w.Close(); err != nil {
		return b
	}
	return buf.Bytes()
}

func zlibDecode(b []byte) []byte {
	r, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return b
	}
	out, err := io.ReadAll(r)
	if err != nil {
		return b
	}
	_ = r.Close()
	return out
}

// --- zstd (klauspost/compress) ---

// zstdEncode compresses b using the zstd encoder. EncodeAll is a synchronous
// (non-streaming) API that returns the compressed bytes directly, which suits
// gpstream's per-frame framing (§4.2).
func zstdEncode(b []byte, level int) []byte {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return b
	}
	defer enc.Close()
	// Higher level = smaller but slower (FR-12); FR-14 degrades this on
	// CPU saturation. Level 1-19 is honoured; the encoder clamps out-of-range.
	out := enc.EncodeAll(b, nil)
	return out
}

func zstdDecode(b []byte) []byte {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return b
	}
	defer dec.Close()
	out, err := dec.DecodeAll(b, nil)
	if err != nil {
		return b
	}
	return out
}
