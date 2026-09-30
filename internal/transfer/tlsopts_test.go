package transfer

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- F-13 data-plane TLS: config builders + a real handshake round-trip. ---

// genTestCA makes a self-signed CA; genTestLeaf makes a leaf signed by it
// (DNS SAN = host when host != ""). They mirror the pg package's F-13 test
// helpers so the data-plane TLS is tested with the same shape of PKI.
func genTestCA(t *testing.T) (certDER []byte, key *ecdsa.PrivateKey) {
	t.Helper()
	var err error
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
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
	certDER, err = x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return certDER, key
}

func genTestLeaf(t *testing.T, caDER []byte, caKey *ecdsa.PrivateKey, serial int64, host string, ip string, clientAuth bool) (leafDER []byte, key *ecdsa.PrivateKey) {
	t.Helper()
	var err error
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "gpstream-test-" + itoa(serial)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if host != "" {
		tmpl.DNSNames = []string{host}
	}
	if ip != "" {
		if p := net.ParseIP(ip); p != nil {
			tmpl.IPAddresses = []net.IP{p}
		}
	}
	if clientAuth {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	leafDER, err = x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return leafDER, key
}

func itoa(n int64) string {
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}

func writePEMCert(t *testing.T, dir, name string, der []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func writePEMKey(t *testing.T, dir, name string, key *ecdsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestTLSEnabledRule (F-13): the data plane is opt-in on the SERVING
// keypair. A CA alone must NOT enable it (a --ssl-ca without a serving
// keypair is a legitimate PG-only verify-full config and must keep working
// as plain TCP).
func TestTLSEnabledRule(t *testing.T) {
	cases := []struct {
		name          string
		cert, key, ca string
		want          bool
	}{
		{"none", "", "", "", false},
		{"ca only (PG-only verify-full config)", "", "", "ca.pem", false},
		{"cert only (misconfig)", "c.pem", "", "", false},
		{"key only (misconfig)", "", "k.pem", "", false},
		{"serving pair", "c.pem", "k.pem", "", true},
		{"serving pair + ca (mTLS)", "c.pem", "k.pem", "ca.pem", true},
	}
	for _, c := range cases {
		if got := TLSEnabled(c.cert, c.key, c.ca); got != c.want {
			t.Errorf("TLSEnabled(%q,%q,%q) = %v, want %v (%s)", c.cert, c.key, c.ca, got, c.want, c.name)
		}
	}
}

// TestServerTLSConfigErrors: a serving side without a complete keypair is a
// hard, diagnosed error (F-13: 'on a bad certificate — refusal with
// diagnostics'), and a missing CA file is one too.
func TestServerTLSConfigErrors(t *testing.T) {
	if _, err := ServerTLSConfig("", "", "", ""); err == nil {
		t.Fatal("ServerTLSConfig(no keypair) must error")
	}
	if _, err := ServerTLSConfig("c.pem", "", "", ""); err == nil {
		t.Fatal("ServerTLSConfig(cert only) must error")
	}
	if _, err := ServerTLSConfig("c.pem", "k.pem", t.TempDir()+"/missing.pem", ""); err == nil {
		t.Fatal("ServerTLSConfig(missing CA file) must error")
	}
}

// TestClientTLSConfigHostname: the listen host always becomes ServerName
// (named hosts validate against DNS SANs, IP literals against IP SANs —
// both are checked by Go), so the controller certificate must cover the
// listen address; an empty host leaves it unset (a misconfiguration the
// caller avoids by always passing the listen host).
func TestClientTLSConfigHostname(t *testing.T) {
	cfg, err := ClientTLSConfig("gp6.example.com", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "gp6.example.com" {
		t.Fatalf("ServerName = %q, want gp6.example.com", cfg.ServerName)
	}
	cfg2, err := ClientTLSConfig("10.0.0.11", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.ServerName != "10.0.0.11" {
		t.Fatalf("ServerName = %q for an IP literal, want the IP (validated against IP SANs)", cfg2.ServerName)
	}
	cfg3, err := ClientTLSConfig("", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg3.ServerName != "" {
		t.Fatalf("ServerName = %q for an empty host, want empty", cfg3.ServerName)
	}
}

// TestDataPlaneTLSEndToEnd (F-13 acceptance: 'traffic between clusters
// is only encrypted') drives a REAL handshake in both directions: the controller
// serves with ServerTLSConfig, the (fake) segreceiver dials with
// ClientTLSConfig, and bytes round-trip. Variants:
//   - one-way TLS (no CA): client verifies the server chain+name, succeeds;
//   - mTLS (CA on the server): a client WITHOUT a certificate is rejected
//     during the handshake (RequireAndVerifyClientCert); a client WITH a
//     CA-signed certificate completes and round-trips data.
//   - wrong CA on the client: the handshake fails (diagnosed).
func TestDataPlaneTLSEndToEnd(t *testing.T) {
	caDER, caKey := genTestCA(t)
	srvDER, srvKey := genTestLeaf(t, caDER, caKey, 2, "localhost", "127.0.0.1", false)
	cliDER, cliKey := genTestLeaf(t, caDER, caKey, 3, "", "", true)
	// A server certificate that does NOT cover the listen address (no
	// 127.0.0.1 IP SAN): the client must reject it (diagnosed).
	wrongDER, wrongKey := genTestLeaf(t, caDER, caKey, 4, "wrong.example.com", "", false)

	dir := t.TempDir()
	caPEM := writePEMCert(t, dir, "ca.pem", caDER)
	srvCert := writePEMCert(t, dir, "srv.pem", srvDER)
	srvKeyPEM := writePEMKey(t, dir, "srv.key", srvKey)
	wrongCert := writePEMCert(t, dir, "wrong.pem", wrongDER)
	wrongKeyPEM := writePEMKey(t, dir, "wrong.key", wrongKey)
	cliCert := writePEMCert(t, dir, "cli.pem", cliDER)
	cliKeyPEM := writePEMKey(t, dir, "cli.key", cliKey)
	// A second, unrelated CA to test the "wrong CA" rejection.
	foreignDER, _ := genTestCA(t)
	foreignPEM := writePEMCert(t, dir, "foreign.pem", foreignDER)

	payload := bytes.Repeat([]byte("gpstream-data-plane-tls-f13-"), 4096) // ~132 KiB

	runRoundTrip := func(t *testing.T, srvCertPEM, srvKeyPEM, srvCA, clientCA, clientCert, clientKey string, wantOK bool) {
		t.Helper()
		plain, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		scfg, err := ServerTLSConfig(srvCertPEM, srvKeyPEM, srvCA, "")
		if err != nil {
			t.Fatalf("ServerTLSConfig: %v", err)
		}
		l := tls.NewListener(plain, scfg)
		defer l.Close()

		ccfg, err := ClientTLSConfig("127.0.0.1", clientCert, clientKey, clientCA, "")
		if err != nil {
			t.Fatalf("ClientTLSConfig: %v", err)
		}
		// The server always accepts + reads the payload: in one-way TLS the
		// read is a plain drain; in mTLS the first read triggers the
		// handshake that rejects a certificate-less client.
		srvErr := make(chan error, 1)
		recv := make(chan []byte, 1)
		srvDone := make(chan struct{})
		go func() {
			defer close(srvDone)
			conn, aerr := l.Accept()
			if aerr != nil {
				srvErr <- aerr
				return
			}
			defer conn.Close()
			buf := make([]byte, len(payload))
			n, rerr := ioReadFull(conn, buf)
			srvErr <- rerr
			if rerr == nil {
				recv <- buf[:n]
			}
		}()

		dconn, derr := net.DialTimeout("tcp", plain.Addr().String(), 5*time.Second)
		if derr != nil {
			t.Fatalf("dial: %v", derr)
		}
		tconn := tls.Client(dconn, ccfg)
		herr := tconn.Handshake()
		if wantOK {
			if herr != nil {
				t.Fatalf("handshake: %v", herr)
			}
			if _, werr := tconn.Write(payload); werr != nil {
				t.Fatalf("write: %v", werr)
			}
			tconn.Close()
			select {
			case e := <-srvErr:
				if e != nil {
					t.Fatalf("server read: %v", e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("server did not finish reading")
			}
			select {
			case got := <-recv:
				if !bytes.Equal(got, payload) {
					t.Fatal("payload mismatch after TLS round-trip")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("server received no payload")
			}
		} else {
			// In TLS 1.3 the client may complete ITS side of the handshake
			// even when the server will reject it (a missing client cert is
			// reported as a post-handshake alert, not before the client
			// finishes). So the version-robust security invariant is not
			// "the client handshake errors" but "the server never accepts
			// the full payload". Try the write (it may block or fail); then
			// assert the server got no complete payload.
			if herr == nil {
				// Best-effort write; a rejection surfaces here or at read.
				_ = tconn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				_, _ = tconn.Write(payload)
			}
			tconn.Close()
			dconn.Close()
			select {
			case got := <-recv:
				t.Fatalf("rejection case: server received a full payload (%d bytes) — TLS was not enforced", len(got))
			case <-time.After(2 * time.Second):
				// Expected: no full payload. (The client handshake errored,
				// or the server dropped the connection before the payload.)
			}
			<-srvDone
		}
	}

	// One-way TLS: client trusts the CA and verifies the server.
	runRoundTrip(t, srvCert, srvKeyPEM, "", caPEM, "", "", true)
	// mTLS: server requires a client certificate.
	runRoundTrip(t, srvCert, srvKeyPEM, caPEM, caPEM, cliCert, cliKeyPEM, true)
	// mTLS: a client without a certificate is rejected.
	runRoundTrip(t, srvCert, srvKeyPEM, caPEM, caPEM, "", "", false)
	// Client trusting the WRONG CA: chain validation fails (diagnosed).
	runRoundTrip(t, srvCert, srvKeyPEM, "", foreignPEM, "", "", false)
	// Server cert that does not cover the listen address: hostname check
	// fails (diagnosed) — F-13 'on a bad certificate — refusal'.
	runRoundTrip(t, wrongCert, wrongKeyPEM, "", caPEM, "", "", false)
}

// ioReadFull reads exactly len(buf) bytes (the test server knows the payload
// size).
func ioReadFull(r interface{ Read([]byte) (int, error) }, buf []byte) (int, error) {
	var off int
	for off < len(buf) {
		n, err := r.Read(buf[off:])
		off += n
		if err != nil {
			return off, err
		}
	}
	return off, nil
}
