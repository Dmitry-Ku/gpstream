// Version-exchange protocol (1F, architecture 08.10): before the first unit,
// the SOURCE agent reports its own build identity plus the segreceiver
// versions it gathered by SSH on the S source segments, DIRECTLY to the
// destination agent (a source->dest control channel — the controller takes
// no part). The destination agent cross-checks its own version, the mini
// versions (from the MiniHello's), the source version and the segreceiver
// versions; a mismatch is a HARD error with the list of discrepancies,
// before any data flows (the "all versions must be equal" rule, confirmed
// 08.10).

package agent

import "fmt"

// SourceVersion is the source agent's first message on the destination's
// peer channel.
type SourceVersion struct {
	Op string `json:"op"` // "source-version"
	// Version is the source agent's build identity ("ver (commit)").
	Version string `json:"version"`
	// S is the number of source segments (0 = plain PostgreSQL, no
	// segment-direct dispatch — SegReceivers is empty).
	S int `json:"s"`
	// SegReceivers is the per-source-segment-host segreceiver build identity
	// (the source agent SSH-gathered "<binary> --version" on every segment
	// host, the dry-run A+ mechanism).
	SegReceivers []SegVersion `json:"segreceivers,omitempty"`
}

// SegVersion is one source segment host's segreceiver identity.
type SegVersion struct {
	Host    string `json:"host"`
	Version string `json:"version"`
}

// PeerAck is the destination agent's reply to a SourceVersion: the verdict
// of the pre-unit cross-check. The source agent fails its in-flight unit
// with Error when Ok is false (no data flows).
type PeerAck struct {
	Op    string `json:"op"` // "peer-ack"
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// CheckVersions is the destination's pre-unit cross-check (1F-4): its own
// build identity, the mini versions (supervisor mode), the source agent's
// version and the segreceiver versions — all must be equal. Returns a hard
// error listing every discrepancy. sv is the source's report; miniVersions
// is the per-seg mini identity (empty outside supervisor mode).
func CheckVersions(own string, sv SourceVersion, miniVersions map[int]string) error {
	want := own
	var mismatches []string
	if sv.Version != "" && sv.Version != want {
		mismatches = append(mismatches, fmt.Sprintf("source-agent=%s (want %s)", sv.Version, want))
	}
	for _, g := range sv.SegReceivers {
		if g.Version != want {
			mismatches = append(mismatches, fmt.Sprintf("segreceiver@%s=%s (want %s)", g.Host, g.Version, want))
		}
	}
	for seg, v := range miniVersions {
		if v != want {
			mismatches = append(mismatches, fmt.Sprintf("mini[seg%d]=%s (want %s)", seg, v, want))
		}
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("version mismatch before the first unit (all components must run the same build): %s", joinComma(mismatches))
	}
	return nil
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "; "
		}
		out += p
	}
	return out
}
