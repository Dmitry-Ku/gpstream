package segstream

import (
	"fmt"
	"strings"
)

// ParseDestAddrs parses the segreceiver's --dest-addr value into the D×W
// dial grid's address book. Two forms are accepted:
//
//  1. A single host:port — BROADCAST mode (P9, the destination AGENT accepts
//     all S×D×W sub-streams on one data listener): every (d, w) sub-stream
//     dials the same address. The grid is the address repeated D times.
//
//  2. A comma-separated list of exactly D host:port entries — MESH Routed
//     mode (P4): entry d is destination segment d's MINI-AGENT data
//     endpoint; each (d, w) sub-stream dials entry d (so the cluster carries
//     S×D×W segment-to-mini-agent streams and neither cluster coordinator is
//     in the data path).
//
// Any other shape (an empty entry, a malformed host:port, or a list whose
// length is neither 1 nor D) is a hard error — the segreceiver fails the
// kernel's COPY rather than dialing a wrong endpoint.
func ParseDestAddrs(destAddr string, D int) ([]string, error) {
	if D < 1 {
		return nil, fmt.Errorf("segstream: D %d (want >= 1)", D)
	}
	parts := strings.Split(destAddr, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		if parts[i] == "" {
			return nil, fmt.Errorf("segstream: bad dest address %q (empty entry)", destAddr)
		}
	}
	for _, p := range parts {
		if !validListen(p) {
			return nil, fmt.Errorf("segstream: bad dest address %q (want host:port)", p)
		}
	}
	switch len(parts) {
	case 1:
		grid := make([]string, D)
		for i := range grid {
			grid[i] = parts[0]
		}
		return grid, nil
	case D:
		return parts, nil
	default:
		return nil, fmt.Errorf("segstream: %d dest addresses, want 1 (broadcast) or %d (one per destination segment)", len(parts), D)
	}
}
