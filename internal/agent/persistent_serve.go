package agent

import (
	"context"
	"fmt"
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
// ServePersistentUnit serves ONE unit off a persistent coordinator channel
// (P1c): it receives the unit and serves it (ServeUnitData). Kept for the
// one-shot / test callers that drive a single unit on their own connection.
func ServePersistentUnit(ctx context.Context, dq pg.StreamingQuerier, da *dest.Agent, rcv *PersistentReceiver, swap bool, streamTimeout time.Duration) (DestResult, error) {
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
	res, err := ServeUnitData(ctx, dq, da, spec, data, swap, streamTimeout)
	res.Spec = spec
	return res, err
}

// ServeUnitData is the data half of a persistent-channel unit serve (P1d):
// staging DDL + COPY (+ inline digest) + the swap, on the destination
// connection q. The persistent-channel serve loop (DestServer) calls it for
// every unit off a channel; the staging table is created per unit, so
// concurrent units (different tables) never collide.
func ServeUnitData(ctx context.Context, dq pg.StreamingQuerier, da *dest.Agent, spec UnitSpec, data *unitDataReader, swap bool, streamTimeout time.Duration) (DestResult, error) {
	var out DestResult
	if streamTimeout <= 0 {
		streamTimeout = 6 * time.Hour
	}

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
	// The row-order-independent content digest is ALWAYS computed on the data
	// path (O(1) memory): the destination verifies on the data path without a
	// re-read, so it is unconditional (no --verify gate).
	dsum := transfer.NewContentDigest()
	in := &digestReader{next: dsum, src: data}
	rows, cerr := da.CopyIntoStream(sctx, spec.Schema, spec.Staging, in)
	if cerr != nil {
		return out, fmt.Errorf("persistent serve: copy into %s.%s: %w", spec.Schema, spec.Staging, cerr)
	}
	out.Rows = rows
	out.Logical = data.Logical()

	// The destination's digest of the loaded rows (ALWAYS computed), for the
	// destination to compare against the source's.
	d, derr := dsum.Sum()
	if derr != nil {
		return out, fmt.Errorf("persistent serve: inline digest %s.%s: %w", spec.Schema, spec.Staging, derr)
	}
	out.Digest = d
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
