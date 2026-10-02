package agent

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// UnitSpec is the per-unit schema header the source agent sends to the
// destination agent before the framed data stream (the two-agent data path).
// It carries everything the destination needs to create the staging table and
// load it: the destination schema/table names, the staging name, the source
// column definitions, the distribution policy and the partition spec. The
// destination agent builds the staging DDL from this (mirroring the
// single-controller manager's CreateStaging), so the load target is shaped
// exactly as the single-controller path shapes it.
type UnitSpec struct {
	JobID     uint32
	UnitID    uint32
	Schema    string // destination schema
	Table     string // destination final table name (post-rename)
	Staging   string // staging table name (dest.StagingName)
	Overwrite bool   // destination drops the existing table on swap
	// SrcSchema/SrcTable are the source object (for the source's data COPY).
	// The server (P3) sets these; the one-shot Push sets them equal to the
	// source table it streams.
	SrcSchema, SrcTable string
	// Compress is the FR-12 mode string the source used to compress the
	// stream ("none"/"zlib"/"zstd[:level]"). Carried here so the destination
	// decompresses with exactly the matching decoder (no flag-mismatch).
	Compress string
	// Cols/Dist/Part are the source table's shape, reproduced on the dest.
	Cols []pg.Column
	Dist pg.Distribution
	Part pg.PartitionSpec
}

const unitSpecVersion = 1

// WriteUnitSpec serialises s as a 5-byte header (1 version byte + 4-byte
// little-endian JSON length) followed by the JSON payload, and writes it to w.
// It is the first thing on the agent<->agent socket (before the framed
// stream), so the destination knows how to build the staging table.
func WriteUnitSpec(w io.Writer, s UnitSpec) error {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("agent: marshal unit spec: %w", err)
	}
	hdr := make([]byte, 5)
	hdr[0] = unitSpecVersion
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(b)))
	if _, err := w.Write(hdr); err != nil {
		return fmt.Errorf("agent: write unit spec header: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("agent: write unit spec payload: %w", err)
	}
	return nil
}

// ReadUnitSpec reads a UnitSpec written by WriteUnitSpec from r (the
// destination agent, before reading the framed stream).
func ReadUnitSpec(r io.Reader) (UnitSpec, error) {
	var s UnitSpec
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return s, fmt.Errorf("agent: read unit spec header: %w", err)
	}
	if hdr[0] != unitSpecVersion {
		return s, fmt.Errorf("agent: unit spec version %d (want %d)", hdr[0], unitSpecVersion)
	}
	n := binary.LittleEndian.Uint32(hdr[1:5])
	if n > 1<<20 { // a unit spec is small; bound it against a corrupt length
		return s, fmt.Errorf("agent: unit spec payload too large: %d bytes", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return s, fmt.Errorf("agent: read unit spec payload: %w", err)
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("agent: unmarshal unit spec: %w", err)
	}
	return s, nil
}
