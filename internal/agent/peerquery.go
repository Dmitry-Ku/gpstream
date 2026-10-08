// Package agent — source->dest QUERY-PROXY channel (Phase 2, architecture
// 08.10): the SOURCE agent runs plan.Build with its LOCAL source pool and a
// remote destination querier; the remote querier proxies each read-only
// destination-catalog query to the DESTINATION agent (over this channel),
// which runs it on its own LOCAL coordinator pool. The controller therefore
// opens NO direct destination connection (2A plan, 2E resume, 2C dry-run all
// ride this channel).
//
// Type fidelity: plan.Build consumes the destination rows' Go types (int64 for
// bigint, string for text, []byte for bytea, nil for NULL). The cells are
// round-tripped over newline-JSON with an explicit type tag (queryCell) so the
// source reconstructs the same types the local pool would have produced.
package agent

import "encoding/json"

// QueryReq is one proxied destination query (read-only).
type QueryReq struct {
	Op    string `json:"op"` // "dst-query"
	ReqID uint64 `json:"req_id"`
	SQL   string `json:"sql"`
	// Args are the query parameters (plan.Build's conflict check passes the
	// destination schema + table names; the others pass none).
	Args []json.RawMessage `json:"args,omitempty"`
}

// QueryResp is the destination's reply: the result rows (type-tagged cells)
// or an error.
type QueryResp struct {
	Op    string        `json:"op"` // "dst-query-resp"
	ReqID uint64        `json:"req_id"`
	OK    bool          `json:"ok"`
	Rows  [][]queryCell `json:"rows,omitempty"`
	Error string        `json:"error,omitempty"`
}

// queryCell is a type-tagged table cell so the JSON round-trip preserves the
// Go type plan.Build expects.
type queryCell struct {
	T     string `json:"t"` // "n" nil | "i64" | "s" | "b" | "bytes" | "num" | "raw"
	I64   int64  `json:"i,omitempty"`
	S     string `json:"s,omitempty"`
	B     bool   `json:"b,omitempty"`
	Bytes []byte `json:"by,omitempty"`
	// Num holds a number that is not int64 (float64 / a small int from a
	// JSON round-trip). Raw holds a value that did not match the common
	// catalog types (rare; serialized verbatim).
	Num json.RawMessage `json:"num,omitempty"`
	Raw json.RawMessage `json:"raw,omitempty"`
}

// encodeQueryCell tags one any cell for the wire.
func encodeQueryCell(v any) queryCell {
	switch x := v.(type) {
	case nil:
		return queryCell{T: "n"}
	case int64:
		return queryCell{T: "i64", I64: x}
	case int:
		return queryCell{T: "i64", I64: int64(x)}
	case int32:
		return queryCell{T: "i64", I64: int64(x)}
	case string:
		return queryCell{T: "s", S: x}
	case bool:
		return queryCell{T: "b", B: x}
	case []byte:
		return queryCell{T: "bytes", Bytes: x}
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return queryCell{T: "n"}
		}
		// A plain number that is not an int64 (float64, json.Number, a small
		// int) rides Num; anything else rides Raw.
		if isJSONNumber(b) {
			return queryCell{T: "num", Num: b}
		}
		return queryCell{T: "raw", Raw: b}
	}
}

// decodeQueryCell rebuilds the Go type from the wire tag.
func (c queryCell) decode() any {
	switch c.T {
	case "n", "":
		return nil
	case "i64":
		return c.I64
	case "s":
		return c.S
	case "b":
		return c.B
	case "bytes":
		return c.Bytes
	case "num":
		var n any
		if err := json.Unmarshal(c.Num, &n); err != nil {
			return nil
		}
		// plan's toInt64 wants int64 for bigint columns; a JSON number for a
		// small integral value decodes to float64 — coerce integral floats.
		if f, ok := n.(float64); ok && f == float64(int64(f)) {
			return int64(f)
		}
		return n
	case "raw":
		var n any
		if err := json.Unmarshal(c.Raw, &n); err != nil {
			return nil
		}
		return n
	}
	return nil
}

func isJSONNumber(b []byte) bool {
	switch {
	case len(b) == 0:
		return false
	case b[0] == '-' || (b[0] >= '0' && b[0] <= '9'):
		return true
	}
	return false
}
