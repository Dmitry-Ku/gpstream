package main

import "testing"

// TestCatInt64 guards the segment-catalog column coercion (minilaunch.go).
// gp_segment_configuration: content is int2 (pgx scans to int16), port is int4
// (pgx scans to int32). The original raw row[n].(int64) silently returned 0 for
// both — the live A→A mesh run died with "contents not dense (got 0 at
// position 1)" because every segment parsed as content=0. These are the actual
// scan types, so they must round-trip.
func TestCatInt64(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int64
	}{
		{"int16-content", int16(2), 2},      // content column (int2)
		{"int32-port", int32(20002), 20002}, // port column (int4)
		{"int64", int64(7), 7},
		{"int", int(9), 9},
		{"uint32", uint32(5), 5},
		{"uint64", uint64(6), 6},
		{"float64", float64(8), 8},
		{"string", "123", 123},
		{"nil", nil, 0},
		{"other", true, 0},
	}
	for _, c := range cases {
		if got := catInt64(c.in); got != c.want {
			t.Errorf("%s: catInt64(%v) = %d, want %d", c.name, c.in, got, c.want)
		}
	}
}

// TestSegmentUtilityDSN guards the mini's segment-local connection
// (architecture 08.10: mini DB connections are local to the segment). The
// first live A->A run exposed that only the URL form was rewritten: a
// keyword/value DSN (host=... port=..., what operators pass on the CLI) kept
// the COORDINATOR's host:port, so all D minis utility-connected to the
// coordinator and the unit's rows landed in the master slice while the
// segments stayed empty. Both forms must land on the segment's host:port.
func TestSegmentUtilityDSN(t *testing.T) {
	const segHost, segPort = "192.168.122.13", 20001
	cases := []struct {
		name    string
		dsn     string
		want    string
		wantErr bool
	}{
		{
			name: "keyword form rewrites host and port, keeps the rest",
			dsn:  "host=127.0.0.1 port=5432 dbname=t_live user=gpadmin sslmode=disable",
			want: "host=192.168.122.13 port=20001 dbname=t_live user=gpadmin sslmode=disable gp_session_role=utility",
		},
		{
			name: "keyword form without port gains the segment port",
			dsn:  "host=127.0.0.1 dbname=t_live",
			want: "host=192.168.122.13 dbname=t_live port=20001 gp_session_role=utility",
		},
		{
			name: "url form rewrites the host and adds utility",
			dsn:  "postgres://gpadmin@127.0.0.1:5432/t_live?sslmode=disable",
			want: "postgres://gpadmin@192.168.122.13:20001/t_live?gp_session_role=utility&sslmode=disable",
		},
		{
			name:    "no host= field is an error",
			dsn:     "port=5432 dbname=t_live",
			wantErr: true,
		},
	}
	for _, c := range cases {
		got, err := segmentUtilityDSN(c.dsn, segHost, segPort)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected error, got %q", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s:\n got  %q\n want %q", c.name, got, c.want)
		}
	}
}
