// Package segstream implements the segment-direct transport (Fallback B,
// DESIGN §1.2, approach (a)+(b) confirmed 27.09).
//
// Flow (the controller is gpstream, a plain client of the SOURCE
// coordinator — it never connects to the segments itself):
//
//	gpstream (controller):
//	  BEGIN ISOLATION LEVEL REPEATABLE READ;
//	  COPY (SELECT ... FROM t) TO PROGRAM
//	      'gpstream-segreceiver --listen H:P --job J --unit U --content <SEGID>'
//	      ON SEGMENT;
//	  COMMIT;
//
//	The GPDB kernel launches gpstream-segreceiver on every primary segment
//	with the segment's LOCAL slice of the table on stdin, read under the
//	coordinator's snapshot (CopyDispatchOnSegment dispatches with
//	DF_WITH_SNAPSHOT — verified live on cluster A, 27.09). segreceiver
//	frames the bytes (Frame v2, tagged with the segment content id), streams
//	them over a direct TCP connection (segment host -> controller, LAN) and
//	closes with a Trailer frame (sha256 + row count, §4.3).
//
//	Receive (controller side) accepts one connection per primary segment,
//	passes the raw binary-COPY payload bytes of each slice to a per-content
//	writer and records the trailers. Each slice is a self-contained binary
//	COPY stream (its own PGCOPY header and int16 -1 EOF marker); merging the
//	slices into one stream for the destination COPY FROM STDIN is the job
//	manager's concern (it also feeds the T4b hash router).
//
// Why this shape (ANSWER-T01-APPROACH-AB §2.2): a direct utility-mode
// connection to a primary cannot use the coordinator snapshot (T0-1), so
// gpstream stays a coordinator client and lets the kernel do the
// dispatch; the only new artifact is the unprivileged segreceiver binary.
package segstream

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/gpstream/gpstream/internal/transfer"
)

// chunkSize is the payload size of one data frame (256 KiB keeps frame
// overhead ~0.01% while bounding per-frame latency).
const chunkSize = 256 * 1024

// ErrTruncatedStream is returned by the receiver when a segment stream ends
// without its trailer (the segment died mid-COPY).
var ErrTruncatedStream = errors.New("segstream: stream ended without trailer")

// ErrFrameAfterTrailer is returned when data frames follow the trailer.
var ErrFrameAfterTrailer = errors.New("segstream: frame after trailer")

// Send is the segment side (runs inside gpstream-segreceiver): it tees in
// (a binary COPY slice stream) through a RowCounter + sha256, frames the
// bytes (v2, content-tagged) and writes them to conn, finishing with a
// Trailer frame. It returns the slice row count. in may be os.Stdin.
func Send(ctx context.Context, conn net.Conn, in io.Reader, jobID, unitID uint32, content int16) (int64, error) {
	defer conn.Close()
	rc := transfer.NewRowCounter(in)
	h := sha256.New()
	buf := make([]byte, chunkSize)
	var seq uint64
	for {
		n, rerr := rc.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			if werr := transfer.WriteFrame(conn, &transfer.Frame{
				JobID: jobID, UnitID: unitID, Seq: seq, ContentID: content, Data: buf[:n],
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
	return rc.Rows(), transfer.WriteFrame(conn, transfer.NewTrailerFrame(jobID, unitID, seq, content, tr))
}

// SliceResult is what the controller collected from one segment stream.
type SliceResult struct {
	ContentID int16
	Rows      int64
	SHA256    [32]byte
	Trailer   *transfer.Trailer // the received trailer (nil if the stream truncated)
	Bytes     uint64            // payload bytes received (excludes framing)
	Err       error             // first error on this stream (nil = clean EOF after trailer)
}

// Receive runs the controller-side listener: it accepts exactly n segment
// connections (one per primary segment) and reads each framed stream to
// completion. For every data frame it calls writer(contentID, payload)
// (nil writer = counting only) and records the trailer. It returns one
// SliceResult per accepted connection (order = accept order; use
// Result.ContentID to identify the slice). The listener is closed on
// return. A segment that never connects is bounded by ctx (the caller sets
// a deadline covering the whole COPY).
func Receive(ctx context.Context, l net.Listener, n int, writer func(contentID int16, p []byte) error) ([]*SliceResult, error) {
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
			j.res.Err = readStream(ctx, j.conn, j.res, writer)
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

// readStream drains one segment connection: data frames go to writer, the
// trailer is recorded, and a clean EOF after the trailer is the success
// case.
func readStream(ctx context.Context, conn net.Conn, res *SliceResult, writer func(contentID int16, p []byte) error) error {
	defer conn.Close()
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
		if res.Trailer != nil {
			return ErrFrameAfterTrailer
		}
		res.ContentID = f.ContentID
		if f.Flags&transfer.FlagTrailer != 0 {
			tr, terr := f.Trailer()
			if terr != nil {
				return terr
			}
			res.Trailer = &tr
			res.Rows = tr.Rows
			res.SHA256 = tr.SHA256
			continue
		}
		res.Bytes += uint64(len(f.Data))
		if writer != nil && len(f.Data) > 0 {
			if werr := writer(f.ContentID, f.Data); werr != nil {
				return werr
			}
		}
	}
}

// validListen accepts "host:port" with a 0<port<65536 (the only external
// value embedded in the program string — see ProgramCommand).
func validListen(listen string) bool {
	host, port, err := net.SplitHostPort(listen)
	if err != nil || host == "" {
		return false
	}
	p, err := strconv.Atoi(port)
	return err == nil && p > 0 && p < 65536
}

// ProgramCommand builds the shell command the kernel executes on each
// segment (the COPY TO PROGRAM argument). <SEGID> is the mandatory
// placeholder (copy.c MangleCopyFileName replaces every occurrence with the
// segment's content id, GpIdentity.segindex). Only internal values are
// embedded (listen address, job/unit ids) — no user input — so there is no
// injection surface (ANSWER-T01-APPROACH-AB §2.4.5).
func ProgramCommand(binPath, listen string, jobID, unitID uint32) (string, error) {
	if binPath == "" || listenBinChars(binPath) {
		return "", fmt.Errorf("segstream: bad binary path %q", binPath)
	}
	if !validListen(listen) {
		return "", fmt.Errorf("segstream: bad listen address %q (want host:port)", listen)
	}
	return fmt.Sprintf("%s --listen %s --job %d --unit %d --content <SEGID>", binPath, listen, jobID, unitID), nil
}

// listenBinChars rejects anything with shell/quote metacharacters in the
// binary path (defense in depth: the path lands in a shell one-liner).
func listenBinChars(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '/' || c == '.' || c == '_' || c == '-' || c == ':':
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		default:
			return true
		}
	}
	return false
}

// CopyOnSegmentSQL builds the coordinator-side statement that triggers the
// segment-direct read (DESIGN §1.2). It must run inside the caller's
// REPEATABLE READ transaction: the kernel dispatches it with
// DF_WITH_SNAPSHOT, so every segment reads the coordinator's snapshot.
// FORMAT binary is required: RowCounter and the destination COPY speak the
// binary COPY wire format.
//
// Syntax note (open-gpdb 6.30.1 gram.y): ON SEGMENT and BINARY are both
// copy_opt_item, so they go in the same option list — `ON SEGMENT BINARY`.
// The parenthesised form `ON SEGMENT (FORMAT binary)` does NOT parse
// (ON SEGMENT is the last alternative of the parenthesised copyopt rule).
func CopyOnSegmentSQL(schema, table string, program string) string {
	return fmt.Sprintf(`COPY %s.%s TO PROGRAM '%s' ON SEGMENT BINARY`,
		quoteIdent(schema), quoteIdent(table), program)
}

// quoteIdent double-quotes a SQL identifier, escaping embedded quotes.
func quoteIdent(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '"')
	return string(out)
}

// GUCName is the Yandex-fork gate for COPY TO PROGRAM (PGC_POSTMASTER; must
// be set in postgresql.conf of every data-dir — master and all segments —
// then gpstop -ar).
const GUCName = "ycmdb.yc_allow_copy_to_program"

// CheckOnSegment is a doctor-side probe (capability on_segment). It runs
// three checks against the source coordinator:
//
//  1. the GUC is on (current_setting),
//  2. ON SEGMENT dispatch works at all (a trivial COPY TO PROGRAM 'true'),
//  3. the segreceiver binary is on the segment hosts (command -v).
//
// Each returns (ok, detail). The caller (doctor) maps them to PASS/WARN/FAIL.
// On non-Greenplum clusters all three fail with "not Greenplum" — the doctor
// skips the capability then.
type OnSegmentProbe struct {
	GUC        bool   // ycmdb.yc_allow_copy_to_program = on
	GUCValue   string // the raw current_setting value ("" if the query failed)
	DispatchOK bool   // COPY ... TO PROGRAM 'true <SEGID>' ON SEGMENT succeeded
	Dispatch   string // detail (ok or the error)
	BinaryOK   bool   // gpstream-segreceiver found on the segment hosts
	Binary     string // detail (ok or the error)
}
