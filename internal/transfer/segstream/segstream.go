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
	"strings"
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
		n, rerr := readChunkCoalesced(rc, buf)
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

// readChunkCoalesced reads up to len(buf) bytes from rc, coalescing the
// partial reads RowCounter returns (it yields as soon as one complete row is
// available, so an unbuffered io.Pipe yields one row per Read). Without this
// the send loop frames one row at a time; when compressed, each frame re-
// initialises a zstd encoder (~13MB allocation), a churn that livelocks the
// source agent at >=300k rows (P5: source agent pegged at 2 CPU, 0 rows on
// the destination). It keeps framing at chunkSize granularity, blocks on
// backpressure (no busy loop), and returns the first underlying error
// (possibly together with the partial bytes already read).
func readChunkCoalesced(rc *transfer.RowCounter, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := rc.Read(buf[total:])
		if n > 0 {
			total += n
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
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

// HandshakeFilter wraps a listener whose Accept can fail on a rejected
// handshake (a *tls.Listener) and keeps accepting: a refused/noisy
// connection (a probe, a port scan, a monitor, a client that fails
// certificate verification) is closed and simply does not count toward the
// n expected segment streams. Without it, a single failed handshake on the
// port would abort the whole in-flight copy (tls.Listener.Accept surfaces
// handshake errors as Accept errors). A genuine listener-closed error
// (net.ErrClosed) is still returned so Receive's ctx/done logic is intact.
// The n-way wait is bounded by the caller's ctx either way.
func HandshakeFilter(l net.Listener) net.Listener { return &handshakeFilter{l: l} }

type handshakeFilter struct{ l net.Listener }

func (h *handshakeFilter) Accept() (net.Conn, error) {
	for {
		c, err := h.l.Accept()
		if err == nil {
			return c, nil
		}
		if errors.Is(err, net.ErrClosed) {
			return nil, err
		}
		// Handshake/auth failure on a stray connection: drop it and keep
		// waiting for the legitimate segment streams.
		if c != nil {
			c.Close()
		}
	}
}

func (h *handshakeFilter) Addr() net.Addr { return h.l.Addr() }
func (h *handshakeFilter) Close() error   { return h.l.Close() }

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

// UnitListen derives the listen address for one unit from the operator's
// base address: the same host with port + unitID. Units copy in parallel
// (WorkersPerPair > 1) and each runs its own controller listener, so they
// must not share one port — a second net.Listen on the same address fails
// immediately (E2 e2e, 27.09: units 2/3 failed their segment-direct
// listen while unit 1 held the port).
func UnitListen(base string, unitID uint32) (string, error) {
	host, port, err := net.SplitHostPort(base)
	if err != nil || host == "" {
		return "", fmt.Errorf("segstream: bad listen address %q (want host:port)", base)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p+int(unitID) > 65535 {
		return "", fmt.Errorf("segstream: listen port %d + unit %d out of range", p, unitID)
	}
	return net.JoinHostPort(host, strconv.Itoa(p+int(unitID))), nil
}

// ProgramCommand builds the shell command the kernel executes on each
// segment (the COPY TO PROGRAM argument). <SEGID> is the mandatory
// placeholder (copy.c MangleCopyFileName replaces every occurrence with the
// segment's content id, GpIdentity.segindex). Only internal values are
// embedded (listen address, job/unit ids) — no user input — so there is no
// injection surface (ANSWER-T01-APPROACH-AB §2.4.5).
func ProgramCommand(binPath, listen string, jobID, unitID uint32) (string, error) {
	return ProgramCommandTLS(binPath, listen, jobID, unitID, "", "", "")
}

// ProgramCommandTLS is ProgramCommand with the optional data-plane TLS
// options (F-13). When cert and key are both set, the segreceiver is told to
// speak TLS to the controller (it validates the controller certificate via
// ca and presents cert/key as its client certificate — mTLS when the
// controller requires one). The cert/key/ca PATHS land in the shell one-liner
// just like the binary path, so they are rejected on any shell metacharacter
// (defense in depth against command injection).
func ProgramCommandTLS(binPath, listen string, jobID, unitID uint32, cert, key, ca string) (string, error) {
	return ProgramCommandFull(binPath, listen, jobID, unitID, cert, key, ca, "")
}

// ProgramCommandFull is ProgramCommandTLS plus the optional per-slice
// compression mode (P4, on_segment through agents). When compress is non-empty
// ("none"/"zlib"/"zstd"[:level]), the segreceiver compresses each slice's
// chunks before framing (FR-12), and the destination agent decompresses. The
// mode is a fixed token from the controller (not user input), so it is
// validated against the allowed set and rejected on any other character.
func ProgramCommandFull(binPath, listen string, jobID, unitID uint32, cert, key, ca, compressMode string) (string, error) {
	if binPath == "" || listenBinChars(binPath) {
		return "", fmt.Errorf("segstream: bad binary path %q", binPath)
	}
	if !validListen(listen) {
		return "", fmt.Errorf("segstream: bad listen address %q (want host:port)", listen)
	}
	cmd := fmt.Sprintf("%s --listen %s --job %d --unit %d --content <SEGID>", binPath, listen, jobID, unitID)
	// Enable the data-plane TLS flags on the SAME rule the controller uses
	// to serve TLS (a serving keypair = both cert and key). A lone cert or
	// key is a misconfiguration — fail loud rather than half-enable.
	switch {
	case cert != "" && key != "":
		if listenBinChars(cert) || listenBinChars(key) || (ca != "" && listenBinChars(ca)) {
			return "", fmt.Errorf("segstream: unsafe TLS path set (cert=%q key=%q ca=%q)", cert, key, ca)
		}
		cmd += fmt.Sprintf(" --ssl-cert %s --ssl-key %s", cert, key)
		if ca != "" {
			cmd += fmt.Sprintf(" --ssl-ca %s", ca)
		}
	case cert != "" || key != "":
		return "", fmt.Errorf("segstream: TLS enabled with only one of --ssl-cert/--ssl-key (cert=%q key=%q); provide both or neither", cert, key)
	}
	// P4: per-slice compression (the destination agent decompresses). The
	// mode is a controller-chosen token; validate it strictly.
	if compressMode != "" {
		if !validCompressMode(compressMode) {
			return "", fmt.Errorf("segstream: bad compress mode %q", compressMode)
		}
		cmd += " --compress " + compressMode
	}
	return cmd, nil
}

// ProgramCommandRouted (P9) builds the shell command the kernel executes on
// each source segment in the routed (S≠D) data path: the segreceiver routes
// its slice to D destination segments (cdbhash + jump-consistent-hash over
// the distribution key) and fans each (source-seg,dest-seg) pair over
// workers sub-streams, dialing destAddr D×W times. NEITHER cluster
// coordinator is in the data path. Only internal values are embedded
// (listen address, ids, key OIDs/positions, segment count) — no user input —
// so there is no injection surface (ANSWER-T01-APPROACH-AB §2.4.5).
// routeKey is "oid:pos,oid:pos" (digits and ':' only — validated).
func ProgramCommandRouted(binPath, destAddr string, jobID, unitID uint32, cert, key, ca, compressMode, routeKey string, routeD, workers int) (string, error) {
	if binPath == "" || listenBinChars(binPath) {
		return "", fmt.Errorf("segstream: bad binary path %q", binPath)
	}
	if !validListen(destAddr) {
		return "", fmt.Errorf("segstream: bad dest address %q (want host:port)", destAddr)
	}
	if routeD < 1 || workers < 1 {
		return "", fmt.Errorf("segstream: bad routed topology d=%d workers=%d (want >= 1)", routeD, workers)
	}
	if routeKey == "" {
		return "", fmt.Errorf("segstream: routed mode requires a route key")
	}
	for i := 0; i < len(routeKey); i++ {
		c := routeKey[i]
		if !((c >= '0' && c <= '9') || c == ':' || c == ',') {
			return "", fmt.Errorf("segstream: bad route key %q (want digits, ':', ',')", routeKey)
		}
	}
	cmd := fmt.Sprintf("%s --dest-addr %s --job %d --unit %d --content <SEGID> --route-key %s --route-d %d --route-workers %d",
		binPath, destAddr, jobID, unitID, routeKey, routeD, workers)
	switch {
	case cert != "" && key != "":
		if listenBinChars(cert) || listenBinChars(key) || (ca != "" && listenBinChars(ca)) {
			return "", fmt.Errorf("segstream: unsafe TLS path set (cert=%q key=%q ca=%q)", cert, key, ca)
		}
		cmd += fmt.Sprintf(" --ssl-cert %s --ssl-key %s", cert, key)
		if ca != "" {
			cmd += fmt.Sprintf(" --ssl-ca %s", ca)
		}
	case cert != "" || key != "":
		return "", fmt.Errorf("segstream: TLS enabled with only one of --ssl-cert/--ssl-key (cert=%q key=%q); provide both or neither", cert, key)
	}
	if compressMode != "" {
		if !validCompressMode(compressMode) {
			return "", fmt.Errorf("segstream: bad compress mode %q", compressMode)
		}
		cmd += " --compress " + compressMode
	}
	return cmd, nil
}

// validCompressMode accepts the compression tokens the controller may embed in
// the segreceiver program string (a fixed set, not arbitrary user input).
func validCompressMode(s string) bool {
	switch {
	case s == "none" || s == "zlib" || s == "zstd":
		return true
	case strings.HasPrefix(s, "zstd:"):
		lvl := s[len("zstd:"):]
		if lvl == "" {
			return false
		}
		for _, c := range lvl {
			if c < '0' || c > '9' {
				return false
			}
		}
		n, err := strconv.Atoi(lvl)
		return err == nil && n >= 1 && n <= 19
	}
	return false
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
