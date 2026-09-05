package pg

import (
	"context"
	"crypto/tls"
	"testing"
)

func TestFakeConnQuery(t *testing.T) {
	f := NewFakeConn()
	f.On("hello", [][]any{{int64(1)}})
	rows, err := f.Query(context.Background(), "select hello")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Fatalf("bad rows: %v", rows)
	}
}

func TestFakeConnUnknownErrors(t *testing.T) {
	f := NewFakeConn()
	if _, err := f.Query(context.Background(), "select unknown"); err == nil {
		t.Fatal("expected error for unregistered query")
	}
}

func TestFakeConnOff(t *testing.T) {
	f := NewFakeConn()
	f.On("version", [][]any{{"v"}})
	f.Off("version")
	if _, err := f.Query(context.Background(), "select version"); err == nil {
		t.Fatal("Off should remove the registration")
	}
}

func TestFakeConnPing(t *testing.T) {
	f := NewFakeConn()
	if err := f.Ping(context.Background()); err != nil {
		t.Fatalf("default ping should be ok")
	}
	f.SetPingError(context.Canceled)
	if err := f.Ping(context.Background()); err == nil {
		t.Fatal("expected ping error")
	}
}

func TestRowStr(t *testing.T) {
	f := NewFakeConn()
	f.On("version", [][]any{{"GP6"}})
	if got := RowStr(context.Background(), f, "select version"); got != "GP6" {
		t.Fatalf("got %q", got)
	}
	// unknown -> ""
	if got := RowStr(context.Background(), f, "select nope"); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestBuildTLS(t *testing.T) {
	c := buildTLS("", "", "", "1.3")
	if c.MinVersion != tls.VersionTLS13 {
		t.Fatalf("min version = %v", c.MinVersion)
	}
	if _, err := minVersion("1.3"); err != nil {
		t.Fatalf("minVersion 1.3: %v", err)
	}
	if _, err := minVersion("bogus"); err == nil {
		t.Fatal("expected error for bogus version")
	}
}

func TestFormatConn(t *testing.T) {
	if FormatConn("abc") == "" {
		t.Fatal("expected non-empty")
	}
}
