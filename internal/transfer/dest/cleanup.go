package dest

import (
	"context"
	"fmt"
)

// DropTarget is one destination-side object owned by gpstream (FR-02
// cleanup): a staging table, a final table, or a created schema. It is the
// wire shape the controller hands the destination agent for a
// `cleanup --mode drop` (the controller derives the targets from the
// operation journal mirror — the journal IS the ownership registration —
// and never touches the destination DB itself).
type DropTarget struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Kind   string `json:"kind"` // "staging" | "table" | "schema"
}

// DropDetail reports the outcome of dropping ONE target (the agent echoes
// these in its JSON result so the controller can audit each object).
type DropDetail struct {
	Kind   string `json:"kind"`
	Schema string `json:"schema"`
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}

// DropOutcome is the aggregate result of a DropObjects pass.
type DropOutcome struct {
	Dropped int          `json:"dropped"`
	Errors  []string     `json:"errors,omitempty"`
	Details []DropDetail `json:"details,omitempty"`
}

// DropObjects removes the gpstream-owned destination objects in ownership
// order: the OBJECTS (staging tables + final tables) first, then the
// created SCHEMAS — and a schema only when it is empty (public is never
// dropped; it pre-exists on every cluster). This is the FR-02 cleanup
// core ("all objects created by gpstream on the destination are removable
// to restore the pre-operation state").
//
// It is CONSERVATIVE and IDEMPOTENT: every drop is DROP IF EXISTS (a table
// already gone — a successful swap, or an in-place --truncate/--append unit
// that never created staging — is a no-op, not an error), and a non-empty
// or absent schema is a no-op. The caller (the destination agent, which
// runs on the destination coordinator and connects to its OWN coordinator
// locally — architecture 08.10) reports the outcome; a per-object failure
// does not abort the pass (the remaining objects are still dropped and the
// error is recorded), so one bad object never leaves the rest behind.
func (a *Agent) DropObjects(ctx context.Context, targets []DropTarget) (DropOutcome, error) {
	var out DropOutcome
	// Objects first (staging + tables), then the created schemas.
	for _, t := range targets {
		if t.Kind == "schema" {
			continue
		}
		d := DropDetail{Kind: t.Kind, Schema: t.Schema, Name: t.Name}
		var derr error
		switch t.Kind {
		case "staging":
			derr = a.DropStaging(ctx, t.Schema, t.Name)
		case "table":
			derr = a.DropTable(ctx, t.Schema, t.Name)
		default:
			derr = fmt.Errorf("unknown drop kind %q", t.Kind)
		}
		if derr != nil {
			d.Error = derr.Error()
			out.Errors = append(out.Errors, fmt.Sprintf("%s %s.%s: %v", t.Kind, t.Schema, t.Name, derr))
		} else {
			d.OK = true
			out.Dropped++
		}
		out.Details = append(out.Details, d)
	}
	// Created schemas last, only when empty.
	for _, t := range targets {
		if t.Kind != "schema" {
			continue
		}
		d := DropDetail{Kind: "schema", Schema: t.Schema, Name: t.Schema}
		if derr := a.DropSchemaIfEmpty(ctx, t.Schema); derr != nil {
			d.Error = derr.Error()
			out.Errors = append(out.Errors, fmt.Sprintf("schema %s: %v", t.Schema, derr))
		} else {
			d.OK = true
			out.Dropped++
		}
		out.Details = append(out.Details, d)
	}
	if len(out.Errors) > 0 {
		return out, fmt.Errorf("%d object(s) not dropped", len(out.Errors))
	}
	return out, nil
}
