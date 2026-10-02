package agent

import (
	"context"
	"crypto/tls"
	"fmt"
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
	rawL, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return out, fmt.Errorf("dest agent: listen %s: %w", o.Listen, err)
	}
	defer rawL.Close()
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
	var conn net.Conn
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		c, aerr := l.Accept()
		acceptCh <- acceptResult{conn: c, err: aerr}
	}()
	select {
	case ar := <-acceptCh:
		if ar.err != nil {
			return out, fmt.Errorf("dest agent: accept from source: %w", ar.err)
		}
		conn = ar.conn
	case <-time.After(o.AcceptTimeout):
		return out, fmt.Errorf("dest agent: accept from source: timeout after %v", o.AcceptTimeout)
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
	fr := transfer.NewFramedReader(conn, compress.NewDec(mode))
	rows, cerr := da.CopyIntoStream(sctx, spec.Schema, spec.Staging, fr)
	if cerr != nil {
		return out, fmt.Errorf("dest agent: copy into %s.%s: %w", spec.Schema, spec.Staging, cerr)
	}
	out.Rows = rows
	out.Logical = fr.Logical()

	// 7. The swap is a controller decision (P3 passes o.Swap per unit).
	if o.Swap {
		if werr := da.Swap(sctx, spec.Schema, spec.Table, spec.Staging, spec.Overwrite); werr != nil {
			return out, fmt.Errorf("dest agent: swap %s.%s: %w", spec.Schema, spec.Table, werr)
		}
		out.Swapped = true
	}
	return out, nil
}
