package pg

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"strings"
	"testing"
	"time"
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
	c, err := buildTLS("", "", "", "", "1.3")
	if err != nil {
		t.Fatalf("buildTLS: %v", err)
	}
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

// TestBuildTLSVerifyFullHostname (F-13): the enforced sslmode=verify-full
// must actually verify the server's hostname. buildTLS (which REPLACES
// pgx's TLS config) must therefore set ServerName to the DSN host — and
// must not, for an IP-literal host (no verifiable name).
func TestBuildTLSVerifyFullHostname(t *testing.T) {
	c, err := buildTLS("db.example.com", "", "", "", "1.2")
	if err != nil {
		t.Fatalf("buildTLS: %v", err)
	}
	if c.ServerName != "db.example.com" {
		t.Errorf("ServerName = %q, want %q (verify-full hostname check)", c.ServerName, "db.example.com")
	}
	c, err = buildTLS("127.0.0.1", "", "", "", "1.2")
	if err != nil {
		t.Fatalf("buildTLS: %v", err)
	}
	if c.ServerName != "" {
		t.Errorf("ServerName = %q for IP-literal host, want empty (no verifiable name)", c.ServerName)
	}
}

// TestBuildTLSLoadsCA (F-13): a supplied CA bundle must land in RootCAs;
// a missing or CA-less file is a hard error (no silent downgrade).
func TestBuildTLSLoadsCA(t *testing.T) {
	ca := t.TempDir() + "/ca.pem"
	der, key, err := genCA(t)
	if err != nil {
		t.Fatal(err)
	}
	_ = key
	pem := pemCert(der)
	if err := os.WriteFile(ca, pem, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := buildTLS("h", "", "", ca, "1.2")
	if err != nil {
		t.Fatalf("buildTLS with CA: %v", err)
	}
	if c.RootCAs == nil {
		t.Fatal("RootCAs not set from the CA bundle")
	}
	if _, err := buildTLS("h", "", "", t.TempDir()+"/missing.pem", "1.2"); err == nil {
		t.Error("missing CA file must fail, not be ignored")
	}
	bad := t.TempDir() + "/bad.pem"
	if err := os.WriteFile(bad, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildTLS("h", "", "", bad, "1.2"); err == nil {
		t.Error("CA file without certificates must fail, not be ignored")
	}
}

// TestHostOf extracts the DSN host for the TLS ServerName (both DSN forms).
func TestHostOf(t *testing.T) {
	cases := []struct{ in, want string }{
		{"postgresql://u:***@h:5432/db", "h"},
		{"postgres://db.example.com/mydb?sslmode=verify-full", "db.example.com"},
		{"host=db.example.com port=5432 user=u password=*** dbname=db", "db.example.com"},
		{"host=10.0.0.5 port=5432", "10.0.0.5"},
	}
	for _, tc := range cases {
		got, err := hostOf(tc.in)
		if err != nil {
			t.Fatalf("hostOf(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("hostOf(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatConn(t *testing.T) {
	if FormatConn("abc") == "" {
		t.Fatal("expected non-empty")
	}
}

// FR-13: an unspecified sslmode must default to verify-full; an explicit
// sslmode is respected as-is.
func TestEnsureVerifyFull(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"url no query", "postgresql://u:***@h:5432/db", "postgresql://u:***@h:5432/db?sslmode=verify-full"},
		{"url with query", "postgresql://u:***@h:5432/db?sslrootcert=/x", "postgresql://u:***@h:5432/db?sslrootcert=/x&sslmode=verify-full"},
		{"url explicit respected", "postgresql://u:***@h:5432/db?sslmode=require", "postgresql://u:***@h:5432/db?sslmode=require"},
		{"url explicit disable respected", "postgresql://u:***@h:5432/db?sslmode=disable", "postgresql://u:***@h:5432/db?sslmode=disable"},
		{"kv form", "host=h port=5432 user=u password=*** dbname=db", "host=h port=5432 user=u password=*** dbname=db sslmode=verify-full"},
		{"kv explicit respected", "host=h sslmode=require", "host=h sslmode=require"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ensureVerifyFull(tc.in); got != tc.want {
				t.Errorf("ensureVerifyFull(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// genCA generates a self-signed CA (certificate DER + key) for the TLS
// tests (F-13). The certificate is a CA (IsCA) so a buildTLS RootCAs check
// can be exercised without shipping fixture files.
func genCA(t *testing.T) (certDER []byte, key *ecdsa.PrivateKey, err error) {
	t.Helper()
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gpstream-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	return der, key, err
}

func pemCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// genLeaf generates a server certificate (SAN DNS = host) signed by the
// CA from genCA — the shape of a real PG server certificate.
func genLeaf(t *testing.T, caDER []byte, caKey *ecdsa.PrivateKey, host string) (leafDER []byte, key *ecdsa.PrivateKey, err error) {
	t.Helper()
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err = x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	return leafDER, key, err
}

func pemKey(key *ecdsa.PrivateKey) []byte {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// TestBuildTLSRejectsWrongHostname (F-13 acceptance: «при неверном
// сертификате — отказ с диагностикой») exercises the REAL handshake: a TLS
// server presents a certificate valid for wrong.example.com (signed by a
// test CA); a client built by buildTLS with a non-matching DSN host must
// fail the handshake with a hostname diagnostic, and a matching host must
// complete it (chain verified against the CA bundle).
func TestBuildTLSRejectsWrongHostname(t *testing.T) {
	caDER, caKey, err := genCA(t)
	if err != nil {
		t.Fatal(err)
	}
	leafDER, leafKey, err := genLeaf(t, caDER, caKey, "wrong.example.com")
	if err != nil {
		t.Fatal(err)
	}
	srvCert, err := tls.X509KeyPair(pemCert(leafDER), pemKey(leafKey))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsLn := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{srvCert}})
	go func() {
		for {
			c, err := tlsLn.Accept()
			if err != nil {
				return
			}
			// Complete the server-side handshake so the client gets the
			// real verification result (not a mid-handshake EOF): the
			// certificate check happens in this handshake.
			if tc, ok := c.(*tls.Conn); ok {
				_ = tc.Handshake()
			}
			_ = c.Close()
		}
	}()
	defer tlsLn.Close()
	addr := ln.Addr().String()

	caPEM := t.TempDir() + "/ca.pem"
	if err := os.WriteFile(caPEM, pemCert(caDER), 0o600); err != nil {
		t.Fatal(err)
	}

	// Wrong DSN host: the certificate does not cover the name the
	// connection was made to -> the handshake MUST fail, with a
	// diagnostic naming the certificate's real name.
	bad, err := buildTLS("right.example.com", "", "", caPEM, "1.2")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", addr, bad)
	if err == nil {
		conn.Close()
		t.Fatal("handshake must fail: the certificate is not valid for the DSN host")
	}
	if !strings.Contains(err.Error(), "wrong.example.com") {
		t.Errorf("handshake error lacks a diagnostic: %v", err)
	}
	var he x509.HostnameError
	if !errors.As(err, &he) {
		t.Logf("note: wrapped error type %T (diagnostic text checked above)", err)
	}

	// Matching DSN host: chain + name both verify -> handshake succeeds.
	good, err := buildTLS("wrong.example.com", "", "", caPEM, "1.2")
	if err != nil {
		t.Fatal(err)
	}
	conn, err = tls.Dial("tcp", addr, good)
	if err != nil {
		t.Fatalf("handshake with a matching host must succeed: %v", err)
	}
	_ = conn.Close()

	// Unknown CA (no bundle): the self-signed chain must be rejected too
	// (chain validation is part of verify-full, not only the hostname).
	noCA, err := buildTLS("wrong.example.com", "", "", "", "1.2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tls.Dial("tcp", addr, noCA); err == nil {
		t.Error("handshake without the CA bundle must fail (unknown authority)")
	}
}
