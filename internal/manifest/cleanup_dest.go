package manifest

import (
	"sort"

	"github.com/gpstream/gpstream/internal/transfer/dest"
)

// DropTarget is one destination-side object that `gpstream cleanup --mode
// drop` removes, with the reason it is owned by gpstream (FR-02: "all
// objects created by gpstream on the destination are registered in the
// manifest with a status" — the journal IS that registration).
type DropTarget struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Kind   string `json:"kind"`    // "staging" | "table" | "schema"
	UnitID int64  `json:"unit_id"` // 0 for schema-level targets
	Reason string `json:"reason"`  // human-readable ownership proof
}

// CleanupDestObjects derives, from the operation journal, the destination
// objects gpstream created for this operation. The derivation is
// CONSERVATIVE by design — a pre-existing destination object is never
// reported for removal:
//
//   - staging table `_gs_<job>_<unit>` (default/--overwrite path): reported
//     for every unit that started copying. A DROP IF EXISTS is a no-op when
//     the table is already gone (successful swap, or the in-place
//     --truncate/--append path that never created staging), so the report is
//     safe either way.
//   - final table: reported when gpstream created it, which the journal can
//     prove two ways — a rename (DstName/DstSchema differ from the source:
//     the destination name did not exist at plan time) or an unconflicted
//     same-name unit (the planner's conflict check saw the destination
//     table ABSENT, so gpstream created it — staging+swap or in-place).
//     A conflicted same-name unit PRE-EXISTED as the copy target and is
//     never reported.
//   - schema: reported when it is a rename (gpstream created it). The caller
//     drops it only after all of its tables are gone AND only when empty
//     (dest.DropSchemaIfEmpty). `public` is never reported.
//
// Units in terminal states (completed/swapped/failed) are all considered:
// after a kill -9 the journal holds the last state, and every listed object
// is gpstream-owned regardless of how far the unit got.
func CleanupDestObjects(j *Journal) []DropTarget {
	var out []DropTarget
	seen := map[string]bool{}
	add := func(t DropTarget) {
		key := t.Kind + "|" + t.Schema + "|" + t.Name
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, t)
	}

	for _, u := range j.Units() {
		ds, dn := u.DstSchema, u.DstName
		if ds == "" {
			ds = u.Schema
		}
		if dn == "" {
			dn = u.Table
		}
		// Staging: only meaningful once the unit reached the copy phase.
		// pending units never created a staging table (planner-only state).
		switch u.State {
		case StateCopying, StateVerifying, StateCompleted, StateSwapped, StateFailed:
		default:
			continue
		}
		// Staging table (no-op drop when absent).
		add(DropTarget{
			Schema: ds, Name: dest.StagingName(u.JobID, u.UnitID),
			Kind: "staging", UnitID: u.UnitID,
			Reason: "unit staging table (scratch for the in-flight copy)",
		})
		// Final table. Two conservative cases where gpstream OWNED the
		// destination name:
		//  - renamed-in (DstSchema/DstName differ from the source): the
		//    destination name was created by this operation;
		//  - same name, no conflict: the planner's conflict check saw the
		//    destination table ABSENT at plan time, so gpstream created it
		//    (staging+swap or in-place create-then-copy).
		// A conflicted target PRE-EXISTED (operator data, possibly
		// overwritten on purpose) and is NEVER reported.
		renamed := (u.DstSchema != "" && u.DstSchema != u.Schema) || dn != u.Table
		switch {
		case renamed && !u.Conflict:
			add(DropTarget{
				Schema: ds, Name: dn,
				Kind: "table", UnitID: u.UnitID,
				Reason: "renamed-in table (destination name created by gpstream)",
			})
		case !renamed && !u.Conflict:
			add(DropTarget{
				Schema: ds, Name: dn,
				Kind: "table", UnitID: u.UnitID,
				Reason: "table created by gpstream (absent at plan time, no conflict)",
			})
		}
	}
	// Schemas: only renamed-in (gpstream-created) schemas, never public.
	for _, u := range j.Units() {
		if u.DstSchema == "" || u.DstSchema == u.Schema || u.DstSchema == "public" {
			continue
		}
		add(DropTarget{
			Schema: u.DstSchema, Name: u.DstSchema,
			Kind:   "schema",
			Reason: "schema created by gpstream (renamed-in)",
		})
	}
	sort.Slice(out, func(i, k int) bool {
		if out[i].Kind != out[k].Kind {
			rank := map[string]int{"staging": 0, "table": 1, "schema": 2}
			return rank[out[i].Kind] < rank[out[k].Kind]
		}
		if out[i].Schema != out[k].Schema {
			return out[i].Schema < out[k].Schema
		}
		return out[i].Name < out[k].Name
	})
	return out
}
