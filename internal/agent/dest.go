package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/compress"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// DestOptions configures one destination-agent serve of a single unit.
type DestOptions struct {
	// Listen is the address the destination agent listens on for the source
	// agent's connection (host:port). The source dials it (ADR-001).
	Listen string
	// DestConn is the destination postgres conn string.
	DestConn string
	// PG-plane TLS (the agent is a client of the destination DB).
	SSLCert, SSLKey, SSLCA, SSLMinTLS string
	// Data-plane TLS for the agent<->agent socket (the dest agent is the
	// SERVER). Cert/Key required when enabled; CA present = mTLS (require +
	// verify the source agent's client cert, F-13/ADR-001).
	DataTLSCert, DataTLSKey, DataTLSCA, DataTLSMinTLS string
	// Swap makes the agent perform the atomic swap (ALTER ... RENAME) after a
	// successful load. The swap is always a controller decision (P3 passes
	// this per unit); the agent never swaps on its own.
	Swap bool
	// AcceptTimeout bounds the wait for the source agent's connection (a
	// source that never dials cannot hang the dest forever).
	AcceptTimeout time.Duration
	// StreamTimeout bounds the whole stream.
	StreamTimeout time.Duration
	// ListenNotify, when non-nil, is called with the bound listen address
	// immediately after the socket is bound (before the accept). A controller
	// that launches the dest agent with a wildcard port (127.0.0.1:0) uses
	// this to learn the real endpoint the source agent must dial.
	ListenNotify func(addr string)
	// FrameAck enables the frame-level ACK + RST-resume transport (DESIGN
	// §4.6) on the data socket: the framed stream rides a ReliablePipe
	// (retransmit ring + re-dial/resume) instead of a plain conn. The
	// UnitSpec header is still read from the INITIAL connection (the
	// pre-stream header, outside the frame channel).
	FrameAck bool
	// ReliablePipe carries the §4.6 transport parameters (ack timeout,
	// reconnect budget, retransmit window, resume timeout).
	ReliablePipe transfer.ReliablePipeConfig
	// SharedListen, when non-nil, is the agent long-lived data listener
	// (design 04.10: one data port per agent; units multiplex by unit_id).
	// The serve accepts its connections from it and does NOT close it on
	// exit (the long-lived server owns it).
	SharedListen net.Listener
}

// DestResult is what the destination agent reports after a serve (FR-04
// volume + the row count the destination actually wrote — the authoritative
// count, exactly as the single-controller path treats the dest's count).
type DestResult struct {
	Spec UnitSpec
	// ListenAddr is the address the destination agent actually bound (the
	// source agent dials it). Carried so a controller that launched the dest
	// agent with a wildcard port (127.0.0.1:0) learns the real endpoint.
	ListenAddr string
	// Rows is the row count the destination wrote (COPY command tag).
	Rows int64
	// Logical is the decompressed byte count the destination served into the
	// COPY (FR-04).
	Logical uint64
	// Swapped reports whether the atomic rename was performed.
	Swapped bool
	// Digest is the row-order-independent content digest of the payload the
	// destination loaded (FR-02, P9-E inline verify), computed on the data
	// path as the rows flow into the COPY FROM (O(1) memory). Empty when the
	// source did not set UnitSpec.VerifyDigest (--verify off): the verify
	// path then falls back to re-deriving the checksum on the staging table.
	Digest string
}

// Serve listens on o.Listen for ONE source agent connection, reads the
// UnitSpec header, builds the staging table, loads it from the framed +
// compressed stream (decompressing on the wire — FR-12, the ADR-002 debt
// closed), and optionally performs the atomic swap. It is the destination
// half of the two-agent data path that the single-controller build does as
// an in-process goroutine.
func Serve(ctx context.Context, o DestOptions) (DestResult, error) {
	var out DestResult
	if o.AcceptTimeout <= 0 {
		o.AcceptTimeout = 5 * time.Minute
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 6 * time.Hour
	}

	// 1. Destination pg connection.
	dq, err := pg.NewConn(ctx, o.DestConn, o.SSLCert, o.SSLKey, o.SSLCA, o.SSLMinTLS)
	if err != nil {
		return out, fmt.Errorf("dest agent: connect dest: %w", err)
	}
	defer dq.Close()
	da := dest.New(dq)

	// 2. Listen for the source agent (optionally a TLS server).
	var rawL net.Listener
	if o.SharedListen != nil {
		rawL = o.SharedListen // long-lived: the server closes it, not the serve
	} else {
		rawL, err = net.Listen("tcp", o.Listen)
		if err != nil {
			return out, fmt.Errorf("dest agent: listen %s: %w", o.Listen, err)
		}
		defer rawL.Close()
	}
	l := net.Listener(rawL)
	if transfer.TLSEnabled(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA) {
		scfg, serr := transfer.ServerTLSConfig(o.DataTLSCert, o.DataTLSKey, o.DataTLSCA, o.DataTLSMinTLS)
		if serr != nil {
			return out, fmt.Errorf("dest agent: tls config: %w", serr)
		}
		l = tls.NewListener(rawL, scfg)
	}

	// 3. Accept the source agent's connection (bounded: a source that never
	// dials cannot hang the dest forever — accept in a goroutine + timer).
	// The conn is held as a DupConn (the frame-ack path may wrap it in a
	// prefix-conn after peeking the first bytes).
	var conn transfer.DupConn
	acceptOne := func() (transfer.DupConn, error) {
		type r struct {
			conn net.Conn
			err  error
		}
		ch := make(chan r, 1)
		go func() {
			c, aerr := l.Accept()
			ch <- r{conn: c, err: aerr}
		}()
		select {
		case ar := <-ch:
			return ar.conn, ar.err
		case <-time.After(o.AcceptTimeout):
			return nil, fmt.Errorf("accept from source: timeout after %v", o.AcceptTimeout)
		}
	}
	conn, aerr := acceptOne()
	if aerr != nil {
		return out, fmt.Errorf("dest agent: %w", aerr)
	}
	// With frame-ack on, a STALE resume connection from a previous,
	// already-ended unit can still arrive on the shared listener (that
	// unit's sender re-dials a few times after its serve closed). A resume
	// opens with a ctrl HELLO; the fresh connection for THIS unit opens with
	// the UnitSpec header. Skip the stale resumes until the fresh UnitSpec
	// connection arrives. (A conn that dies before yielding 4 bytes is kept:
	// ReadUnitSpec below reports the error, matching the plain path.)
	for o.FrameAck {
		c, isResume := transfer.PeekStreamPrefix(conn)
		if c == nil {
			break
		}
		if !isResume {
			conn = c // fresh UnitSpec conn (the prefix replays the header bytes)
			break
		}
		_ = c.Close() // a stale resume from a previous unit
		conn, aerr = acceptOne()
		if aerr != nil {
			return out, fmt.Errorf("dest agent: %w", aerr)
		}
	}
	defer conn.Close()

	// 4. UnitSpec header: the destination schema + the source table shape.
	spec, serr := ReadUnitSpec(conn)
	if serr != nil {
		return out, serr
	}
	out.Spec = spec

	sctx, scancel := context.WithTimeout(ctx, o.StreamTimeout)
	defer scancel()

	// 5. Build the staging table exactly as the single-controller manager
	// does (EnsureSchema + CreateStaging; a surviving staging from a crashed
	// run is emptied first — a killed COPY FROM leaves partial rows).
	if err := da.EnsureSchema(sctx, spec.Schema); err != nil {
		return out, fmt.Errorf("dest agent: ensure schema %s: %w", spec.Schema, err)
	}
	if da.Exists(sctx, spec.Schema, spec.Staging) {
		if terr := da.Truncate(sctx, spec.Schema, spec.Staging); terr != nil {
			return out, fmt.Errorf("dest agent: truncate surviving staging %s.%s: %w", spec.Schema, spec.Staging, terr)
		}
	}
	if err := da.CreateStaging(sctx, spec.Schema, spec.Cols, spec.Staging, spec.Dist, spec.Part); err != nil {
		return out, fmt.Errorf("dest agent: create staging %s.%s: %w", spec.Schema, spec.Staging, err)
	}

	// 6. Load: framed + compressed stream -> decompress -> COPY FROM.
	mode, _, perr := compress.Parse(spec.Compress)
	if perr != nil {
		return out, fmt.Errorf("dest agent: compress %q: %w", spec.Compress, perr)
	}
	var rp *transfer.ReliablePipe
	frameConn := transfer.Pipe(conn)
	if o.FrameAck {
		// The framed stream rides a ReliablePipe: the source re-dials this
		// same listener on a break and replays its un-ACKed frames; the
		// receiver dedups by seq, so the open COPY FROM below keeps reading
		// the in-order stream across the break (no re-COPY, no re-UnitSpec —
		// the spec was read from the initial connection, before the frame
		// channel). rawL is the (possibly TLS-wrapped) shared listener the
		// re-accepts come from.
		var rerr error
		rp, rerr = transfer.NewReliablePipeReceiverOnConn(o.ReliablePipe, conn, l)
		if rerr != nil {
			return out, fmt.Errorf("dest agent: frame-ack transport: %w", rerr)
		}
		frameConn = rp
	}
	fr := transfer.NewFramedReader(frameConn, compress.NewDec(mode))
	// The row-order-independent content digest is ALWAYS computed on the data
	// path (O(1) memory): the destination verifies on the data path without a
	// re-read, so it is unconditional (no --verify gate).
	dsum := transfer.NewContentDigest()
	in := &digestReader{next: dsum, src: fr}
	rows, cerr := da.CopyIntoStream(sctx, spec.Schema, spec.Staging, in)
	if cerr != nil {
		if rp != nil {
			// Destination failure: tell the sender to stop (its blocked Write
			// fails fast instead of re-dialing a destination that is done).
			rp.Abort()
		}
		return out, fmt.Errorf("dest agent: copy into %s.%s: %w", spec.Schema, spec.Staging, cerr)
	}
	out.Rows = rows
	out.Logical = fr.Logical()
	if rp != nil {
		_ = rp.Close() // clean end-of-stream (FIN) to the sender
	}
	d, derr := dsum.Sum()
	if derr != nil {
		return out, fmt.Errorf("dest agent: inline digest %s.%s: %w", spec.Schema, spec.Staging, derr)
	}
	out.Digest = d

	// 7. The swap is a controller decision (P3 passes o.Swap per unit).
	if o.Swap {
		if werr := da.Swap(sctx, spec.Schema, spec.Table, spec.Staging, spec.Overwrite); werr != nil {
			return out, fmt.Errorf("dest agent: swap %s.%s: %w", spec.Schema, spec.Table, werr)
		}
		out.Swapped = true
	}
	return out, nil
}

// digestReader (io.Reader) tees every chunk read from src into a
// ContentDigestWriter (FR-02, P9-E) and serves it to the next reader, so the
// destination computes the row-order-independent content digest ON THE DATA
// PATH (as the rows flow into the COPY FROM, O(1) memory) instead of
// re-reading the staging table after the copy. It is only in the chain when
// UnitSpec.VerifyDigest is set (--verify on), so the default (verify-off)
// path pays no digest cost.
type digestReader struct {
	next io.Writer
	src  io.Reader
}

func (dr *digestReader) Read(p []byte) (int, error) {
	n, err := dr.src.Read(p)
	if n > 0 {
		if _, werr := dr.next.Write(p[:n]); werr != nil {
			return n, werr
		}
	}
	return n, err
}
