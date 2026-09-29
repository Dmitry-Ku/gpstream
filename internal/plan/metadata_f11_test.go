package plan

import (
	"context"
	"testing"

	"github.com/gpstream/gpstream/internal/transfer/pg"
)

// TestDetectOtherObjects verifies (F-11 metadata phase) that views, matviews
// and functions in the selected schemas are detected and reported.
func TestDetectOtherObjects(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("relkind = 'v'", [][]any{{"public.orders_summary"}})
	src.On("relkind = 'm'", [][]any{{"public.daily_agg"}})
	src.On("count(*)", [][]any{{int64(42)}})

	tabs := []Table{{Schema: "public", Name: "orders"}}
	oo := detectOtherObjects(context.Background(), src, tabs)

	if len(oo.Views) != 1 || oo.Views[0] != "public.orders_summary" {
		t.Errorf("views = %v, want [public.orders_summary]", oo.Views)
	}
	if len(oo.MatViews) != 1 || oo.MatViews[0] != "public.daily_agg" {
		t.Errorf("matviews = %v, want [public.daily_agg]", oo.MatViews)
	}
	if oo.Functions != 42 {
		t.Errorf("functions = %d, want 42", oo.Functions)
	}
}

// TestDetectOtherObjectsEmpty verifies the detection is a no-op (and does not
// fail the plan) when a catalog query is unreadable.
func TestDetectOtherObjectsEmpty(t *testing.T) {
	src := pg.NewFakeConn() // no canned results -> every query errors
	tabs := []Table{{Schema: "public", Name: "orders"}}
	oo := detectOtherObjects(context.Background(), src, tabs)
	if len(oo.Views) != 0 || len(oo.MatViews) != 0 || oo.Functions != 0 {
		t.Errorf("expected empty detection on catalog fault, got %+v", oo)
	}
}

// TestDetectMissingExtensions verifies (F-11 metadata phase) that an extension
// type used by a copied column but absent on the destination is reported.
func TestDetectMissingExtensions(t *testing.T) {
	src := pg.NewFakeConn()
	// The source has a column of a PostGIS type.
	src.On("pg_depend", [][]any{{"postgis"}})
	dst := pg.NewFakeConn()
	// The destination has plpgsql but NOT postgis.
	dst.On("SELECT extname FROM pg_extension", [][]any{{"plpgsql"}})

	tabs := []Table{{Schema: "public", Name: "geo"}}
	missing := detectMissingExtensions(context.Background(), src, dst, tabs)
	if len(missing) != 1 || missing[0] != "postgis" {
		t.Errorf("missing = %v, want [postgis]", missing)
	}
}

// TestDetectMissingExtensionsInstalled verifies no finding when the extension
// is present on the destination.
func TestDetectMissingExtensionsInstalled(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_depend", [][]any{{"postgis"}})
	dst := pg.NewFakeConn()
	dst.On("SELECT extname FROM pg_extension", [][]any{{"plpgsql"}, {"postgis"}})

	tabs := []Table{{Schema: "public", Name: "geo"}}
	missing := detectMissingExtensions(context.Background(), src, dst, tabs)
	if len(missing) != 0 {
		t.Errorf("missing = %v, want empty", missing)
	}
}

// TestDetectMissingExtensionsNoExtensions verifies the common case (no
// extension-typed columns) yields no finding.
func TestDetectMissingExtensionsNoExtensions(t *testing.T) {
	src := pg.NewFakeConn()
	src.On("pg_depend", [][]any{}) // no extension types used
	dst := pg.NewFakeConn()

	tabs := []Table{{Schema: "public", Name: "orders"}}
	missing := detectMissingExtensions(context.Background(), src, dst, tabs)
	if len(missing) != 0 {
		t.Errorf("missing = %v, want empty", missing)
	}
}
