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

// genCA / genServerCert generate a self-signed CA and a server certificate
// for 127.0.0.1 (the control-channel TLS test): the controller verifies the
// agent's certificate against the CA (ServerName = the dial host).
func genCA(t *testing.T) (certPEM string, caDER []byte, key *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gpstream-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caDER = der
	certPEM = writeCertPEM(t, "ca.pem", der)
	return
}

func genServerCert(t *testing.T, caDER []byte, caKey *ecdsa.PrivateKey) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "gpstream-test-agent"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath := writeCertPEM(t, filepath.Join(dir, "agent.pem"), der)
	keyPath := filepath.Join(dir, "agent.key")
	kb := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, kb, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath + "|" + keyPath
}

func writeCertPEM(t *testing.T, path string, der []byte) string {
	t.Helper()
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestControlTLSHandshake verifies the control-channel TLS end to end:
// (1) the controller with the CA verifies the agent's certificate and the
// hello arrives; (2) without the CA (plain dial) the handshake is refused.
func TestControlTLSHandshake(t *testing.T) {
	caPEM, caDER, caKey := genCA(t)
	caPath := caPEM
	certAndKey := genServerCert(t, caDER, caKey)
	certPath, keyPath := splitPipe(t, certAndKey)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctrlAddr := l.Addr().String()
	l.Close()
	l2, _ := net.Listen("tcp", "127.0.0.1:0")
	dataAddr := l2.Addr().String()
	l2.Close()

	srv, err := NewDestServer(DestServerOptions{
		DataListen:     dataAddr,
		ControlListen:  ctrlAddr,
		DestConn:       "postgresql://nouser@127.0.0.1:1/nodb?sslmode=disable",
		ControlTLSCert: certPath,
		ControlTLSKey:  keyPath,
	})
	if err != nil {
		t.Fatalf("NewDestServer (control tls): %v", err)
	}
	defer srv.Close()

	// (1) With the CA: the handshake verifies and the hello arrives.
	client, err := DialDestControlTLS(context.Background(), ctrlAddr, ControlTLS{CA: caPath})
	if err != nil {
		t.Fatalf("DialDestControlTLS (with CA): %v", err)
	}
	if client.Hello.Op != "hello" {
		t.Fatalf("hello = %+v", client.Hello)
	}
	_ = client.Close()

	// (2) Without the CA (plain dial against a TLS listener): no hello ever
	// arrives (the server waits for a TLS ClientHello the plain client never
	// sends) — a bounded read must time out.
	{
		pc, perr := net.Dial("tcp", ctrlAddr)
		if perr != nil {
			t.Fatalf("plain dial: %v", perr)
		}
		_ = pc.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		if _, rerr := pc.Read(buf); rerr == nil {
			t.Fatalf("plain dial against a TLS control listener: got data, want none")
		}
		_ = pc.Close()
	}

	// (3) A controller CA that does NOT sign the agent certificate: the
	// handshake must fail verification.
	foreignCA, _, _ := genCA(t)
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	if _, err := DialDestControlTLS(ctx3, ctrlAddr, ControlTLS{CA: foreignCA}); err == nil {
		t.Fatalf("dial with a foreign CA: want a verification error, got hello")
	}
}

func splitPipe(t *testing.T, s string) (string, string) {
	t.Helper()
	for i := 0; i < len(s); i++ {
		if s[i] == '|' {
			return s[:i], s[i+1:]
		}
	}
	t.Fatal("no pipe in certAndKey")
	return "", ""
}
