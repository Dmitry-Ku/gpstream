// Package topology models Greenplum cluster topology (the
// gp_segment_configuration catalog) and provides probes that the rest of
// gpstream (doctor, planner) use to reason about segment counts, roles, and
// whether a connected cluster is a Greenplum at all.
//
// The catalog is the single source of truth for "how many segments, where,
// in what role/mode"; this package keeps the query in one place so callers
// never hand-roll gp_segment_configuration SQL (TASK-SPEC-v4 §9).
package topology

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// Segment is one row of gp_segment_configuration.
type Segment struct {
	// Content is the content id: -1 for the coordinator, >=0 for segments,
	// duplicated across primary/mirror rows.
	Content int `json:"content"`
	// Role is "p" (primary) or "m" (mirror).
	Role string `json:"role"`
	// PreferredRole is "p" or "m" — the role the segment was created with.
	PreferredRole string `json:"preferred_role"`
	// Mode is "s" (synced) or "n" (not in sync).
	Mode string `json:"mode"`
	// Status is "u" (up) or "d" (down).
	Status string `json:"status"`
	// Address is the host the segment listens on.
	Address string `json:"address"`
	// Port is the segment's port.
	Port int `json:"port"`
}

// Description returns a stable one-line description, e.g. "content=1 primary synced 10.0.0.3:6000".
func (s Segment) Description() string {
	role := "primary"
	if s.Role == "m" {
		role = "mirror"
	}
	mode := "synced"
	if s.Mode != "s" {
		mode = "not-in-sync"
	}
	return fmt.Sprintf("content=%d %s %s %s:%d", s.Content, role, mode, s.Address, s.Port)
}

// Topology is the parsed view of a cluster's gp_segment_configuration.
type Topology struct {
	Coordinator *Segment  `json:"coordinator,omitempty"`
	Segments    []Segment `json:"segments"` // primary+mirror rows, sorted by (content, role)
}

// SegmentCount returns the number of distinct segment contents (i.e. the
// logical segment count, mirrors excluded). A coordinator-only catalog (no
// segment rows) yields 0.
func (t *Topology) SegmentCount() int {
	seen := make(map[int]bool)
	for _, s := range t.Segments {
		if s.Content >= 0 {
			seen[s.Content] = true
		}
	}
	return len(seen)
}

// UpCount returns how many distinct contents have an up primary
// (role 'p', status 'u'). Mirrors are never counted: a content is usable
// when its primary is up, regardless of mirror sync state. Zero means the
// cluster has no usable segment — the doctor fails it.
func (t *Topology) UpCount() int {
	up := make(map[int]bool)
	for _, s := range t.Segments {
		if s.Content >= 0 && s.Role == "p" && s.Status == "u" {
			up[s.Content] = true
		}
	}
	return len(up)
}

// IsGreenplum reports whether the catalog was found and parsed.
func (t *Topology) IsGreenplum() bool {
	return t != nil
}

// ProbeSQL resolves gp_segment_configuration by name. Returning a NULL OID
// means the catalog does not exist (plain PostgreSQL, not Greenplum). This
// Greenplum build exposes the catalog in pg_catalog (always in the implicit
// search_path); some builds also expose it in gp_toolkit, so try that
// qualifier first and fall back to the unqualified name (lab e2e,
// 27.09: to_regclass('gp_toolkit.gp_segment_configuration') = NULL while the
// unqualified form resolves).
const ProbeSQL = `SELECT COALESCE(to_regclass('gp_toolkit.gp_segment_configuration'), to_regclass('gp_segment_configuration'))`

// catalogSQL reads the rows we need for planning and doctor.
//
// NOTE 1: this open-gpdb 6.x build's gp_segment_configuration has NO
// `datname` column (verified live: address, content, datadir, dbid, hostname,
// mode, port, preferred_role, role, status). Selecting it makes the whole
// query error, which doctor reports as "could not enumerate segments".
//
// NOTE 2: role/preferred_role/mode/status are "char" (pg_char, OID 18); pgx
// decodes them as int32 ASCII codes (112='p'), so cast ::text in SQL to keep
// the comparison driver-type-agnostic (same convention as source.go).
const catalogSQL = `SELECT content, role::text, preferred_role::text, mode::text, status::text, hostname, port
FROM gp_segment_configuration
ORDER BY content, role`

// IsGreenplum probes whether the connection is to a Greenplum cluster.
//
// to_regclass returns NULL when the catalog is absent (plain PostgreSQL) and a
// non-NULL regclass otherwise. pgx decodes a bare regclass as its TEXT form
// (the type name, e.g. "gp_segment_configuration") in the text protocol — NOT
// a numeric OID — so this must NOT numeric-coerce the value (toInt("gp_segment_
// configuration") == 0 would mis-detect a real cluster as plain-PG and make
// Read return (nil, nil), which doctor reports as "could not enumerate
// segments"). The correct test is "non-nil / non-empty", consistent with the
// other to_regclass call sites (source.go, plan/route.go, planner.go).
func IsGreenplum(ctx context.Context, q pg.Querier) bool {
	oid, err := pg.Row(ctx, q, ProbeSQL)
	if err != nil {
		return false
	}
	return !isEmptyOID(oid)
}

// isEmptyOID reports whether a to_regclass result means "catalog absent"
// (NULL). It tolerates the driver returning either a nil interface (NULL) or,
// on some paths, an empty/whitespace string, without numeric coercion.
func isEmptyOID(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(x) == ""
	default:
		return false
	}
}

// Read loads the full topology from q. It returns an error when the catalog
// exists but cannot be read; for a non-Greenplum cluster it returns
// (nil, nil) — callers distinguish "not Greenplum" from a read fault.
func Read(ctx context.Context, q pg.Querier) (*Topology, error) {
	if !IsGreenplum(ctx, q) {
		return nil, nil
	}
	rows, err := q.Query(ctx, catalogSQL)
	if err != nil {
		return nil, fmt.Errorf("read gp_segment_configuration: %w", err)
	}
	t := &Topology{}
	for _, r := range rows {
		if len(r) < 7 {
			continue
		}
		seg := Segment{
			Content:       toInt(r[0]),
			Role:          toStr(r[1]),
			PreferredRole: toStr(r[2]),
			Mode:          toStr(r[3]),
			Status:        toStr(r[4]),
			Address:       toStr(r[5]),
			Port:          toInt(r[6]),
		}
		if seg.Content < 0 {
			t.Coordinator = &seg
		} else {
			t.Segments = append(t.Segments, seg)
		}
	}
	sort.Slice(t.Segments, func(i, j int) bool {
		if t.Segments[i].Content != t.Segments[j].Content {
			return t.Segments[i].Content < t.Segments[j].Content
		}
		// Primary rows first within a content, regardless of role spelling.
		return t.Segments[i].Role == "p" && t.Segments[j].Role != "p"
	})
	return t, nil
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int8:
		return int(x)
	case int16: // gp_segment_configuration.content is smallint (pgx -> int16)
		return int(x)
	case int32:
		return int(x)
	case int64:
		return int(x)
	case uint16:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)
	case float64:
		return int(x)
	case bool:
		if x {
			return 1
		}
		return 0
	case string:
		n := 0
		for _, c := range x {
			if c < '0' || c > '9' {
				return 0
			}
			n = n*10 + int(c-'0')
		}
		return n
	}
	return 0
}

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case nil:
		return ""
	}
	return fmt.Sprintf("%v", v)
}
