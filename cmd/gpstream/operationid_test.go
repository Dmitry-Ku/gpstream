package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

var tsIDRe = regexp.MustCompile(`^\d{8}T\d{6}Z(-\d+)?$`)

func TestDefaultOperationIDFormat(t *testing.T) {
	id := defaultOperationID()
	if !tsIDRe.MatchString(id) {
		t.Fatalf("id %q does not match YYYYMMDDTHHMMSSZ[-n]", id)
	}
	// Must parse and be (very close to) now in UTC.
	ts, err := time.Parse("20060102T150405Z", id[:16])
	if err != nil {
		t.Fatalf("id %q does not parse: %v", id, err)
	}
	if d := time.Since(ts); d < -time.Minute || d > time.Minute {
		t.Errorf("id time %v is %s from now (want ~now)", ts, d)
	}
}

func TestResolveOperationIDRunGeneratesTimestamp(t *testing.T) {
	dir := t.TempDir()
	id, note, err := resolveOperationID("", dir, true)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !tsIDRe.MatchString(id) {
		t.Errorf("generated id %q not a timestamp", id)
	}
	if note == "" {
		t.Error("expected a 'generated' note for the operator")
	}
}

func TestResolveOperationIDRunCollisionSuffix(t *testing.T) {
	dir := t.TempDir()
	// Pre-create a folder with the current-second name to force a collision.
	first, _, err := resolveOperationID("", dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, first), 0o755); err != nil {
		t.Fatal(err)
	}
	second, _, err := resolveOperationID("", dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("collision: second id == first id (%q)", first)
	}
	if !tsIDRe.MatchString(second) {
		t.Errorf("collision id %q not a timestamp", second)
	}
	if second[:15] != first[:15] {
		t.Errorf("collision id %q should keep the same base second as %q", second, first)
	}
}

func TestResolveOperationIDExplicitPassthrough(t *testing.T) {
	id, note, err := resolveOperationID("my-named-run", t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	if id != "my-named-run" {
		t.Errorf("explicit id changed to %q", id)
	}
	if note != "" {
		t.Errorf("explicit id should carry no note, got %q", note)
	}
}

func TestResolveOperationIDReadRequiresID(t *testing.T) {
	if _, _, err := resolveOperationID("", t.TempDir(), false); err == nil {
		t.Fatal("read command with empty id: want error")
	}
}

func TestLatestOperationIDNewestTimestamp(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"20260101T000000Z", "20260102T000000Z", "20260101T000000Z-2"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	got, err := latestOperationID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != "20260102T000000Z" {
		t.Errorf("latest = %q, want 20260102T000000Z", got)
	}
}

func TestLatestOperationIDFallbackMtime(t *testing.T) {
	dir := t.TempDir()
	old, recent := filepath.Join(dir, "alpha"), filepath.Join(dir, "beta")
	for _, p := range []string{old, recent} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	got, err := latestOperationID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != "beta" {
		t.Errorf("latest = %q, want beta (newer mtime)", got)
	}
}

func TestLatestOperationIDEmpty(t *testing.T) {
	if _, err := latestOperationID(t.TempDir()); err == nil {
		t.Fatal("empty state dir: want error")
	}
}

func TestParseOperationArgsLatest(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "20260101T000000Z"), 0o755); err != nil {
		t.Fatal(err)
	}
	opID, sd, err := parseOperationArgs([]string{"--operation-id", "latest", "--state-dir", dir})
	if err != nil {
		t.Fatal(err)
	}
	if opID != "20260101T000000Z" || sd != dir {
		t.Errorf("got (%q, %q), want (20260101T000000Z, %q)", opID, sd, dir)
	}
}

func TestFolderTimestamp(t *testing.T) {
	if ts, ok := folderTimestamp("20261001T153000Z"); !ok || ts != "20261001T153000Z" {
		t.Errorf("plain: (%q, %v)", ts, ok)
	}
	if ts, ok := folderTimestamp("20261001T153000Z-3"); !ok || ts != "20261001T153000Z" {
		t.Errorf("suffixed: (%q, %v)", ts, ok)
	}
	if _, ok := folderTimestamp("t30-main"); ok {
		t.Error("operator name should not be a timestamp")
	}
}
