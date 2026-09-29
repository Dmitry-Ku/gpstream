package pg

import (
	"strings"
	"testing"
)

func TestStorageOptionsSQLAO(t *testing.T) {
	// A typical AO/CO table: the destination must recreate it as AO, not heap.
	got := StorageOptionsSQL([]string{"appendonly=on", "orientation=column", "compress=2"})
	want := "WITH (appendonly = on, orientation = column, compress = 2)"
	if got != want {
		t.Errorf("StorageOptionsSQL = %q, want %q", got, want)
	}
}

func TestStorageOptionsSQLBareAppendonly(t *testing.T) {
	// "appendonly" without an explicit value (the catalog renders AO tables
	// this way) must still produce a valid clause.
	got := StorageOptionsSQL([]string{"appendonly"})
	if got != "WITH (appendonly = true)" {
		t.Errorf("bare appendonly = %q, want %q", got, "WITH (appendonly = true)")
	}
}

func TestStorageOptionsSQLEmpty(t *testing.T) {
	if got := StorageOptionsSQL(nil); got != "" {
		t.Errorf("nil reopts = %q, want empty", got)
	}
	if got := StorageOptionsSQL([]string{}); got != "" {
		t.Errorf("empty reopts = %q, want empty", got)
	}
}

func TestStorageOptionsSQLWhitelist(t *testing.T) {
	// Non-whitelisted keys (autovacuum tuning, user-defined) must be dropped:
	// an unknown storage option would fail the CREATE and sink the unit.
	got := StorageOptionsSQL([]string{"autovacuum_enabled=off", "appendonly=on", "user_defined=weird"})
	if got != "WITH (appendonly = on)" {
		t.Errorf("whitelist = %q, want only appendonly", got)
	}
}

func TestStorageOptionsSQLInjectionRejected(t *testing.T) {
	// A value that could inject SQL (spaces / quotes / parens) is dropped.
	for _, bad := range []string{
		"appendonly=on) DROP TABLE x; --",
		"compress=zstd' OR '1'='1",
		"blocksize=8192 x",
		"fillfactor=100); DROP",
	} {
		if got := StorageOptionsSQL([]string{bad}); got != "" {
			t.Errorf("injection %q produced %q, want empty", bad, got)
		}
	}
	// Numeric options with non-numeric values are dropped.
	if got := StorageOptionsSQL([]string{"fillfactor=abc"}); got != "" {
		t.Errorf("fillfactor=abc produced %q, want empty", got)
	}
}

func TestStorageOptionsSQLFillfactorNumeric(t *testing.T) {
	got := StorageOptionsSQL([]string{"appendonly=on", "fillfactor=70"})
	if !strings.Contains(got, "fillfactor = 70") {
		t.Errorf("fillfactor missing/invalid: %q", got)
	}
}
