package agent

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/gpstream/gpstream/internal/transfer"
	"github.com/gpstream/gpstream/internal/transfer/dest"
	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// ServePersistentUnit serves ONE unit off a persistent coordinator channel
// (P1c): it receives the unit (spec + the decompressed rows, ending at the
// Trailer frame), builds the staging table, loads the rows with COPY FROM
// (teeing the inline digest when VerifyDigest is set, P9-E), and optionally
// performs the atomic swap. The connection is NOT closed — the caller keeps
// the PersistentReceiver for the next unit.
//
// This is the destination half of the persistent channel: the same
// staging→COPY→swap sequence Serve() performs on a per-unit connection, but
// the unit arrives over the long-lived connection (delimited by UnitBegin/
// Trailer frames) instead of a fresh TCP+TLS+UnitSpec handshake.
func ServePersistentUnit(ctx context.Context, dq *pg.Conn, da *dest.Agent, rcv *PersistentReceiver, swap bool, streamTimeout time.Duration) (DestResult, error) {
	var out DestResult
	if streamTimeout <= 0 {
		streamTimeout = 6 * time.Hour
	}
	// Receive the unit (blocks until its UnitBegin, skipping idle Pings).
	spec, data, err := rcv.ReceiveUnit(ctx)
	if err != nil {
		return out, fmt.Errorf("persistent serve: receive unit: %w", err)
	}
	out.Spec = spec

	sctx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()

	// Build the staging table (a surviving staging from a crashed unit is
	// emptied first — a killed COPY FROM leaves partial rows, FR-07).
	if err := da.EnsureSchema(sctx, spec.Schema); err != nil {
		return out, fmt.Errorf("persistent serve: ensure schema %s: %w", spec.Schema, err)
	}
	if da.Exists(sctx, spec.Schema, spec.Staging) {
		if terr := da.Truncate(sctx, spec.Schema, spec.Staging); terr != nil {
			return out, fmt.Errorf("persistent serve: truncate surviving staging %s.%s: %w", spec.Schema, spec.Staging, terr)
		}
	}
	if err := da.CreateStaging(sctx, spec.Schema, spec.Cols, spec.Staging, spec.Dist, spec.Part); err != nil {
		return out, fmt.Errorf("persistent serve: create staging %s.%s: %w", spec.Schema, spec.Staging, err)
	}

	// Load: the unit's decompressed rows (data) -> [digest tee] -> COPY FROM.
	// The reader ends at the unit's Trailer frame (io.EOF), so the COPY sees
	// exactly this unit's rows — the next unit's UnitBegin is left for the
	// next ReceiveUnit.
	var dsum *transfer.ContentDigestWriter
	var in io.Reader = data
	if spec.VerifyDigest {
		dsum = transfer.NewContentDigest()
		in = &digestReader{next: dsum, src: data}
	}
	rows, cerr := da.CopyIntoStream(sctx, spec.Schema, spec.Staging, in)
	if cerr != nil {
		return out, fmt.Errorf("persistent serve: copy into %s.%s: %w", spec.Schema, spec.Staging, cerr)
	}
	out.Rows = rows
	out.Logical = data.Logical()

	// Inline content digest (P9-E): the destination's digest of the loaded
	// rows, for the controller to compare against the source's.
	if dsum != nil {
		d, derr := dsum.Sum()
		if derr != nil {
			return out, fmt.Errorf("persistent serve: inline digest %s.%s: %w", spec.Schema, spec.Staging, derr)
		}
		out.Digest = d
	}
	// Cross-check the source's trailer row count against the COPY row count.
	if tr := data.Trailer(); tr.Rows != 0 && tr.Rows != rows {
		return out, fmt.Errorf("persistent serve: trailer row count %d != copy row count %d", tr.Rows, rows)
	}

	// The swap is a controller decision (per unit).
	if swap {
		if werr := da.Swap(sctx, spec.Schema, spec.Table, spec.Staging, spec.Overwrite); werr != nil {
			return out, fmt.Errorf("persistent serve: swap %s.%s: %w", spec.Schema, spec.Table, werr)
		}
		out.Swapped = true
	}
	return out, nil
}
