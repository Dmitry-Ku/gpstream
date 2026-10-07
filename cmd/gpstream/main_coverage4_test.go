package main

// Coverage for the flags that had no parse test: --port-range,
// --no-routed-write, and the --control-tls-cert/key/ca trio (control-channel
// TLS, design 04.10). Also the invalid-port-range validation error.

import (
	"strings"
	"testing"
)

// TestParseConfigTransportFlagsC: --port-range / --no-routed-write /
// --control-tls-cert / --control-tls-key / --control-tls-ca bind into the
// config fields (parse layer only; the runtime effect of each is tested in
// the behavior packages).
func TestParseConfigTransportFlagsC(t *testing.T) {
	c, err := parseConfig([]string{
		"--port-range", "9000-9007",
		"--no-routed-write",
		"--control-tls-cert", "/tmp/ctl-cert.pem",
		"--control-tls-key", "/tmp/ctl-key.pem",
		"--control-tls-ca", "/tmp/ctl-ca.pem",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if c.PortRange != "9000-9007" {
		t.Errorf("PortRange = %q, want 9000-9007", c.PortRange)
	}
	if !c.NoRoutedWrite {
		t.Error("NoRoutedWrite = false, want true")
	}
	if c.ControlTLSCert != "/tmp/ctl-cert.pem" ||
		c.ControlTLSKey != "/tmp/ctl-key.pem" ||
		c.ControlTLSCA != "/tmp/ctl-ca.pem" {
		t.Errorf("control-tls = %q/%q/%q, want the three paths",
			c.ControlTLSCert, c.ControlTLSKey, c.ControlTLSCA)
	}
}

// TestParseConfigPortRangeSingleBase: a single base port ("LO", no dash)
// parses and is treated as a one-port range by the size check.
func TestParseConfigPortRangeSingleBase(t *testing.T) {
	c, err := parseConfig([]string{"--port-range", "9500"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if c.PortRange != "9500" {
		t.Fatalf("PortRange = %q, want 9500", c.PortRange)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate (single base port is a valid range): %v", err)
	}
	if got := c.PortRangeSize(); got != 1 {
		t.Errorf("PortRangeSize() = %d, want 1", got)
	}
}

// TestParseConfigPortRangeInvalid: a non-numeric range fails validation
// (parseConfig runs Validate at the end, so the error surfaces here) with a
// clear message naming the flag.
func TestParseConfigPortRangeInvalid(t *testing.T) {
	if _, err := parseConfig([]string{"--port-range", "not-a-port"}); err == nil ||
		!strings.Contains(err.Error(), "--port-range") {
		t.Errorf("parseConfig(invalid range) = %v, want an error naming --port-range", err)
	}
}

// TestParseConfigNoRoutedWriteDefault: the flag is a boolean — its absence
// leaves NoRoutedWrite false (the routed path stays enabled when the other
// preconditions hold).
func TestParseConfigNoRoutedWriteDefault(t *testing.T) {
	c, err := parseConfig([]string{})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if c.NoRoutedWrite {
		t.Error("NoRoutedWrite defaulted to true, want false")
	}
}
