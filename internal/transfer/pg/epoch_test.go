package pg

import (
	"testing"
	"time"
)

// TestSpreadOf verifies the ε-window width (max(t_fix) − min(t_fix)) for the
// snapshot-epoch pin-spread computation (P1d).
func TestSpreadOf(t *testing.T) {
	t0 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		fix  []time.Time
		want time.Duration
	}{
		{"empty", nil, 0},
		{"single", []time.Time{t0}, 0},
		{"two equal", []time.Time{t0, t0}, 0},
		{"two apart", []time.Time{t0, t0.Add(5 * time.Millisecond)}, 5 * time.Millisecond},
		// Unordered: min/max must be independent of slice order.
		{"unordered", []time.Time{t0.Add(9 * time.Millisecond), t0, t0.Add(3 * time.Millisecond)}, 9 * time.Millisecond},
		{"three", []time.Time{t0, t0.Add(2 * time.Millisecond), t0.Add(4 * time.Millisecond)}, 4 * time.Millisecond},
	}
	for _, c := range cases {
		if got := spreadOf(c.fix); got != c.want {
			t.Errorf("%s: spreadOf = %s, want %s", c.name, got, c.want)
		}
	}
}

// TestMinFix verifies the earliest-pin (baseMin) computation a heal re-pin is
// validated against.
func TestMinFix(t *testing.T) {
	t0 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	if got := minFix(nil); !got.IsZero() {
		t.Errorf("minFix(nil) = %v, want zero", got)
	}
	got := minFix([]time.Time{t0.Add(5 * time.Millisecond), t0, t0.Add(9 * time.Millisecond)})
	if !got.Equal(t0) {
		t.Errorf("minFix = %v, want %v", got, t0)
	}
}

// TestValidateSpread verifies the --max-snapshot-delta gate: a pin spread wider
// than the limit rejects the run, within (or exactly at) the limit accepts, and
// limit <= 0 disables the check.
func TestValidateSpread(t *testing.T) {
	limit := 1 * time.Second
	if err := validateSpread(0, limit); err != nil {
		t.Errorf("spread 0 within limit: unexpected err %v", err)
	}
	if err := validateSpread(500*time.Millisecond, limit); err != nil {
		t.Errorf("spread 500ms within 1s limit: unexpected err %v", err)
	}
	// Exactly at the limit is allowed (the bound is "exceeds", not ">=").
	if err := validateSpread(1*time.Second, limit); err != nil {
		t.Errorf("spread == limit: unexpected err %v", err)
	}
	if err := validateSpread(1500*time.Millisecond, limit); err == nil {
		t.Error("spread 1.5s > 1s limit: want error, got nil")
	}
	// limit <= 0 disables the check regardless of spread.
	if err := validateSpread(10*time.Second, 0); err != nil {
		t.Errorf("disabled check (limit 0): unexpected err %v", err)
	}
	if err := validateSpread(10*time.Second, -1); err != nil {
		t.Errorf("disabled check (limit <0): unexpected err %v", err)
	}
}

// TestHealLagOKTimes verifies the heal re-pin validation against the epoch's
// earliest pin: a re-pin lagging baseMin by more than the limit is rejected
// (the unit must fail → resume rather than copy a newer snapshot).
func TestHealLagOKTimes(t *testing.T) {
	base := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	limit := 1 * time.Second
	if !healLagOKTimes(base, base, limit) {
		t.Error("re-pin == base: want OK")
	}
	if !healLagOKTimes(base, base.Add(500*time.Millisecond), limit) {
		t.Error("re-pin 500ms after base within 1s: want OK")
	}
	if healLagOKTimes(base, base.Add(1500*time.Millisecond), limit) {
		t.Error("re-pin 1.5s after base > 1s: want NOT OK")
	}
	// limit <= 0 disables the check.
	if !healLagOKTimes(base, base.Add(10*time.Second), 0) {
		t.Error("disabled check (limit 0): want OK")
	}
	// A re-pin BEFORE the base (clock skew) is trivially within the limit.
	if !healLagOKTimes(base, base.Add(-50*time.Millisecond), limit) {
		t.Error("re-pin before base: want OK")
	}
}
