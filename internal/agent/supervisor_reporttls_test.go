package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

// genReportTLSCerts generates a CA + a leaf cert (serverAuth+clientAuth,
// SAN 127.0.0.1/localhost) as PEM files, returning cert/key/CA paths. The
// leaf serves both roles: the supervisor (server, mTLS) and the mini (client).
func genReportTLSCerts(t *testing.T) (cert, key, caPEM string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gpstream-report-tls-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "gpstream-report-tls-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, "leaf.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, "leaf.key")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath, caPath
}

// TestServeMiniReportOverDataTLS (BUG #11 regression): with the data-plane
// TLS enabled, the supervisor wraps the report listener in TLS (mTLS) and the
// mini must dial it as the TLS client. Before the fix the mini sent a
// plaintext hello against the TLS listener: the handshake failed,
// AcceptMinis errored and closed the report listener (nil reportL), and the
// DestServer's Hello() then panicked in Supervisor.Addr(). Here the hello
// must complete over TLS: AcceptMinis returns the mini's data address and
// the mini exits cleanly on mini-done.
func TestServeMiniReportOverDataTLS(t *testing.T) {
	cert, key, caPEM := genReportTLSCerts(t)

	sup, err := NewSupervisor("127.0.0.1:0", nil, cert, key, caPEM, "")
	if err != nil {
		t.Fatalf("NewSupervisor (tls): %v", err)
	}
	reportAddr := sup.Addr()
	if reportAddr == "" {
		t.Fatal("supervisor report address is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type acceptOut struct {
		addr string
		err  error
	}
	acceptCh := make(chan acceptOut, 1)
	go func() {
		mm, aerr := sup.AcceptMinis(ctx, 1)
		addr := ""
		if aerr == nil {
			addr = mm[0]
		}
		acceptCh <- acceptOut{addr: addr, err: aerr}
	}()

	miniOut := make(chan error, 1)
	go func() {
		_, merr := ServeMini(ctx, MiniOptions{
			Seg:           0,
			DataListen:    "127.0.0.1:0",
			PrimaryDSN:    "host=127.0.0.1 port=1 dbname=unused user=x sslmode=disable",
			ReportAddr:    reportAddr,
			DataTLSCert:   cert,
			DataTLSKey:    key,
			DataTLSCA:     caPEM,
			AcceptTimeout: 5 * time.Second,
			StreamTimeout: 10 * time.Second,
			Version:       "test",
		})
		miniOut <- merr
	}()

	select {
	case a := <-acceptCh:
		if a.err != nil {
			t.Fatalf("AcceptMinis over data-TLS failed (BUG #11: the mini did not present its TLS client cert / handshake): %v", a.err)
		}
		if a.addr == "" {
			t.Fatal("AcceptMinis: empty mini data address")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("AcceptMinis did not complete: the mini's report hello never arrived (report TLS mismatch)")
	}

	// Clean shutdown: mini-done over the (TLS) report channel.
	sup.Stop()
	select {
	case merr := <-miniOut:
		if merr != nil {
			t.Fatalf("ServeMini: %v", merr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ServeMini did not exit on mini-done")
	}
	sup.Close()
}
