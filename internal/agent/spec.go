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
	// VerifyDigest (FR-02, P9-E) is the controller's request, carried on the
	// data connection with the spec, for the source to compute the
	// row-order-independent content digest on the data path (reported in
	// SourceResult/StreamResp) and for the destination to do the same (it
	// reads it here, reported in DestResult/DestUnitDone). The controller
	// compares the two digests in the verify step instead of re-reading the
	// tables. Set when --verify is on; empty/false on the default path.
	VerifyDigest bool
}

// DirectLaunch is the P5 direct-mode launch payload the controller hands the
// destination agent at start (--direct-spec JSON, mirroring P4's --spec). The
// destination agent builds the staging table from the source shape (Cols /
// Dist / Part) and performs D utility-mode per-segment COPYs over DstDSNs.
// The per-segment compressed slice streams themselves arrive over the data
// socket (the source agent routes each row to a segment and streams D
// content-tagged compressed slices); this payload carries everything the
// destination needs to receive + load them.
type DirectLaunch struct {
	// UnitID is the unit ordinal (for the agent's report line).
	UnitID uint32
	// Destination staging shape (the source table's layout, so the loaded
	// data is byte-faithful).
	Schema    string
	Table     string
	Staging   string
	Overwrite bool
	Cols      []pg.Column
	Dist      pg.Distribution
	Part      pg.PartitionSpec
	// Compress is the FR-12 mode the source used (the dest decompresses).
	Compress string
	// D is the number of destination primary segments (== len(DstDSNs)); the
	// destination accepts D slice connections and opens D utility COPYs.
	D int32
	// DstDSNs is one utility-mode destination DSN per segment (content
	// 0..D-1); the destination COPYs each segment's slice into its local
	// staging slice over that DSN (gp_session_role=utility).
	DstDSNs []string
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
