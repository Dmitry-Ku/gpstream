package config

import (
	"strings"
	"testing"
)

// TestAdvisoryNoIndexes (F-17) verifies that --no-indexes + explicit tuning
// flags produce an advisory (they are ignored, not an error), while a bare
// --no-indexes (the default tuning) produces none.
func TestAdvisoryNoIndexes(t *testing.T) {
	// bare --no-indexes: no advisory (IndexWorkers=0, IndexPriority=size).
	c := Default()
	c.NoIndexes = true
	if w := c.AdvisoryWarnings(); len(w) != 0 {
		t.Fatalf("bare --no-indexes: got advisories %v, want none", w)
	}
	// --no-indexes + --index-workers 4: advisory names the ignored flag.
	c = Default()
	c.NoIndexes = true
	c.IndexWorkers = 4
	w := c.AdvisoryWarnings()
	if len(w) != 1 || !strings.Contains(w[0], "--index-workers") || !strings.Contains(w[0], "is ignored") || !strings.Contains(w[0], "remove it") {
		t.Fatalf("no-indexes+workers: got %v", w)
	}
	// --no-indexes + both tuned flags: advisory names both, plural verb.
	c = Default()
	c.NoIndexes = true
	c.IndexWorkers = 4
	c.IndexPriority = "fifo"
	w = c.AdvisoryWarnings()
	if len(w) != 1 || !strings.Contains(w[0], "--index-workers") || !strings.Contains(w[0], "--index-priority") || !strings.Contains(w[0], "are ignored") || !strings.Contains(w[0], "remove them") {
		t.Fatalf("no-indexes+both: got %v", w)
	}
	// no --no-indexes: never an advisory, even with tuning.
	c = Default()
	c.IndexWorkers = 4
	c.IndexPriority = "fifo"
	if w := c.AdvisoryWarnings(); len(w) != 0 {
		t.Fatalf("no --no-indexes: got %v, want none", w)
	}
}
