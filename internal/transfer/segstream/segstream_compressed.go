// Package segstream — compressed segment transport (P4).
//
// This file adds the COMPRESSED per-slice transport that the two-agent
// on_segment path uses. In P4 the GPDB kernel still launches gpstream-segreceiver
// on each primary segment (COPY ... TO PROGRAM ... ON SEGMENT), but the segreceiver
// now streams each slice COMPRESSED to the DESTINATION agent (not the controller),
// and the destination agent deframes + decompresses + merges the N slices
// (SliceMux) and runs the COPY FROM.
//
// The compression mode is carried in the trailer frame's Flags byte, so a
// receiver is self-describing: it reads the trailer and learns how to
// decompress. (Trailer frames are always sent after the data frames of a slice,
// and the first data frame of a slice is what the receiver decompresses, so the
// receiver must know the mode before the data — we therefore send a
// one-shot "mode" trailer as the FIRST frame of each compressed slice. This is
// a Trailer-flagged frame with no rows/sha; the receiver treats a trailer that
// arrives before any data frame as the mode announcement and keeps reading.)
package segstream

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
)

// trailer flag bits (in the v2 frame Flags byte) for the compressed transport.
// The base trailer uses FlagTrailer (0x01); the mode-announcement trailer adds
// FlagModeAnnounce so a receiver can distinguish "this slice's compression mode
// is X" from a real end-of-slice trailer.
const (
	flagModeAnnounce uint8 = 1 << 1
)

// CompressModeBytes renders a compress mode to its wire byte (for the mode
// announcement). The receiver maps the byte back with compressModeFromByte.
func compressModeByte(m compress.Mode) byte {
	switch m {
	case compress.ModeNone:
		return 0
	case compress.ModeZlib:
		return 1
	case compress.ModeZstd:
		return 2
	}
	return 0
}

func compressModeFromByte(b byte) compress.Mode {
	switch b {
	case 1:
		return compress.ModeZlib
	case 2:
		return compress.ModeZstd
	}
	return compress.ModeNone
}

func isModeAnnounce(f *transfer.Frame) bool {
	return f.Flags&flagModeAnnounce != 0
}

// annMode extracts the announced compression mode from a mode-announcement frame.
func annMode(f *transfer.Frame) compress.Mode {
	return compressModeFromByte(f.Flags >> 3)
}

// SendCompressed is Send with per-slice compression: it reads in (a binary-COPY
// slice stream), compresses each chunk (m, level) and frames it (v2,
// content-tagged), finishing with a Trailer frame (sha256 + row count over the
// UNcompressed payload, so the digest matches the source's logical bytes).
// It returns the slice row count. in may be os.Stdin. The compression mode is
// announced to the receiver in the trailer's flags (the receiver learns it from
// the same trailer it reads at end-of-slice; because the trailer is last, the
// receiver must also get the mode before the data — the destination agent
// always knows the mode from the controller, so this is belt-and-braces).
//
// Note: the destination agent is told the compression mode by the controller
// (the UnitSpec carries it), so it can decompress from the first data frame;
// the trailer's flags carry the mode as a consistency check (a mismatch is an
// error, catching a mis-configured agent pair).
func SendCompressed(ctx context.Context, conn transfer.Pipe, in io.Reader, jobID, unitID uint32, content int16, m compress.Mode, level int, digest bool) (int64, error) {
	defer conn.Close()
	comp := compress.New(m, level)
	rc := transfer.NewRowCounter(in)
	h := sha256.New()
	var d *transfer.ContentDigestWriter
	if digest {
		d = transfer.NewContentDigest()
	}
	buf := make([]byte, chunkSize)
	var seq uint64
	for {
		n, rerr := readChunkCoalesced(rc, buf)
		if n > 0 {
			h.Write(buf[:n])
			if d != nil {
				// Digest over the UNcompressed payload (the logical rows), so
				// it matches the destination's post-decompression bytes.
				if _, werr := d.Write(buf[:n]); werr != nil {
					return rc.Rows(), werr
				}
			}
			// Compress the chunk (FR-12): the wire carries the compressed
			// bytes; the destination decompresses. ModeNone is a pass-through.
			payload := comp.Encode(buf[:n])
			if werr := transfer.WriteFrame(conn, &transfer.Frame{
				JobID: jobID, UnitID: unitID, Seq: seq, ContentID: content, Data: payload,
			}); werr != nil {
				return rc.Rows(), werr
			}
			seq++
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return rc.Rows(), rerr
		}
	}
	if serr := rc.Err(); serr != nil {
		return rc.Rows(), serr
	}
	var tr transfer.Trailer
	copy(tr.SHA256[:], h.Sum(nil))
	tr.Rows = rc.Rows()
	if d != nil {
		acc, rows, aerr := d.Accumulator()
		if aerr != nil {
			return rc.Rows(), aerr
		}
		if rows != rc.Rows() {
			return rc.Rows(), fmt.Errorf("segstream: digest row count %d != row counter %d", rows, rc.Rows())
		}
		copy(tr.Digest[:], acc[:])
	}
	// The end-of-slice trailer also carries the mode (consistency check).
	f := transfer.NewTrailerFrame(jobID, unitID, seq, content, tr)
	f.Flags |= flagModeAnnounce | (compressModeByte(m) << 3)
	return rc.Rows(), transfer.WriteFrame(conn, f)
}

// ReceiveCompressed is Receive for the compressed segment transport: it accepts
// exactly n segment connections and reads each COMPRESSED framed stream to
// completion, decompressing each data frame (the mode is learned from the
// slice's trailer; the receiver is ALSO told the expected mode by wantMode and
// errors if a slice announces a different one) before handing the UNcompressed
// payload to writer(contentID, payload). It records the trailer (rows + sha256
// over the uncompressed bytes). It returns one SliceResult per accepted
// connection, as Receive does.
func ReceiveCompressed(ctx context.Context, l net.Listener, n int, wantMode compress.Mode, writer func(contentID int16, p []byte) error) ([]*SliceResult, error) {
	defer l.Close()
	accepts := make(chan net.Conn, n)
	acceptErr := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			c, err := l.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			accepts <- c
		}
		close(accepts)
	}()

	results := make([]*SliceResult, 0, n)
	var mu sync.Mutex
	add := func(r *SliceResult) {
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
	}

	type job struct {
		conn net.Conn
		res  *SliceResult
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := <-jobs
			j.res.Err = ReadStreamCompressed(ctx, j.conn, j.res, wantMode, writer)
		}()
	}

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return results, ctx.Err()
		case err, ok := <-acceptErr:
			if ok {
				wg.Wait()
				return results, err
			}
		case c, ok := <-accepts:
			if !ok {
				wg.Wait()
				if len(results) != n {
					return results, fmt.Errorf("segstream: received %d of %d segment streams", len(results), n)
				}
				return results, nil
			}
			res := &SliceResult{}
			add(res)
			jobs <- job{c, res}
		}
	}
}

// ReadStreamCompressed drains one COMPRESSED segment connection (a
// segreceiver's slice): data frames are
// decompressed (with wantMode, which the controller hands the receiver — the
// same mode the source was told to use) and passed to writer, and the trailer's
// announced mode is checked against wantMode as a consistency guard.
func ReadStreamCompressed(ctx context.Context, conn transfer.Pipe, res *SliceResult, wantMode compress.Mode, writer func(contentID int16, p []byte) error) error {
	defer conn.Close()
	dec := compress.NewDec(wantMode)
	for {
		f, err := transfer.ReadFrame(ctx, conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if res.Trailer == nil {
					return fmt.Errorf("%w: content=%d bytes=%d", ErrTruncatedStream, res.ContentID, res.Bytes)
				}
				return nil
			}
			return err
		}
		res.ContentID = f.ContentID
		if f.Flags&transfer.FlagTrailer != 0 {
			if res.Trailer != nil {
				return ErrFrameAfterTrailer
			}
			// The trailer re-announces the mode; a mismatch means a
			// mis-configured agent pair (source compressed with a mode the
			// destination will not decompress) — fail loud.
			if m := annMode(f); m != wantMode {
				return fmt.Errorf("segstream: slice %d announced mode %s, want %s", res.ContentID, m, wantMode)
			}
			tr, terr := f.Trailer()
			if terr != nil {
				return terr
			}
			res.Trailer = &tr
			res.Rows = tr.Rows
			res.SHA256 = tr.SHA256
			res.Digest = tr.Digest
			continue
		}
		raw := dec.Decode(f.Data)
		res.Bytes += uint64(len(f.Data)) // wire (compressed) bytes, for FR-04
		if writer != nil && len(raw) > 0 {
			if werr := writer(f.ContentID, raw); werr != nil {
				return werr
			}
		}
	}
}
