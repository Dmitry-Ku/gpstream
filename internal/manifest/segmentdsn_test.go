package manifest

import (
	"strings"
	"testing"
)

// TestSegmentDSN_URLForm verifies the segment-to-segment DSN builder swaps the
// operator's URL-form --dest host:port for the segment's and appends the
// utility session role, preserving the user/database and other query params.
func TestSegmentDSN_URLForm(t *testing.T) {
	seg := destSegment{Content: 1, Hostname: "10.0.0.7", Port: 20003}
	got, err := segmentDSN("postgres://gpadmin@10.0.0.5:5432/salesdb?sslmode=disable", seg)
	if err != nil {
		t.Fatalf("URL-form DSN should not error, got %v", err)
	}
	// host:port swapped to the segment's, utility role appended, user/db kept.
	if !strings.Contains(got, "10.0.0.7:20003") {
		t.Errorf("expected segment host:port 10.0.0.7:20003 in %q", got)
	}
	if strings.Contains(got, "10.0.0.5:5432") {
		t.Errorf("original master host:port should be replaced in %q", got)
	}
	if !strings.Contains(got, "gpadmin@") {
		t.Errorf("expected user preserved in %q", got)
	}
	if !strings.Contains(got, "gp_session_role=utility") {
		t.Errorf("expected gp_session_role=utility in %q", got)
	}
	if !strings.Contains(got, "/salesdb") {
		t.Errorf("expected database preserved in %q", got)
	}
	if !strings.Contains(got, "sslmode=disable") {
		t.Errorf("expected sslmode=disable preserved in %q", got)
	}
}

// TestSegmentDSN_URLForm_PostgresqlScheme verifies the postgresql:// alias is
// accepted like postgres://.
func TestSegmentDSN_URLForm_PostgresqlScheme(t *testing.T) {
	seg := destSegment{Content: 0, Hostname: "10.0.0.7", Port: 20000}
	got, err := segmentDSN("postgresql://u@h:5432/db", seg)
	if err != nil {
		t.Fatalf("postgresql:// scheme should not error, got %v", err)
	}
	if !strings.Contains(got, "10.0.0.7:20000") {
		t.Errorf("expected segment host:port in %q", got)
	}
}

// TestSegmentDSN_KeywordValue verifies a keyword/value --dest (host=... port=...)
// is REJECTED with a clear URL-form message — the routed / direct_1to1 callers
// catch this and degrade the run to the merge path rather than fail.
func TestSegmentDSN_KeywordValue(t *testing.T) {
	seg := destSegment{Content: 0, Hostname: "10.0.0.7", Port: 20000}
	_, err := segmentDSN("host=10.0.0.5 port=5432 user=gpadmin dbname=salesdb", seg)
	if err == nil {
		t.Fatal("keyword/value DSN should error, got nil")
	}
	if !strings.Contains(err.Error(), "URL-form") {
		t.Errorf("expected the error to name the URL-form requirement, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "degrades") {
		t.Errorf("expected the error to explain the degrade-to-merge fallback, got %q", err.Error())
	}
}

// TestSegmentDSN_NoScheme verifies a bare host (no scheme) is rejected too.
func TestSegmentDSN_NoScheme(t *testing.T) {
	seg := destSegment{Content: 0, Hostname: "10.0.0.7", Port: 20000}
	if _, err := segmentDSN("10.0.0.5:5432", seg); err == nil {
		t.Fatal("bare host:port (no scheme) should error, got nil")
	}
}
