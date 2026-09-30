package transfer

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSEnabled reports whether the data-plane TLS option set is active. It is
// opt-in and keyed on the SERVING keypair: the coordinator path (source COPY
// TO STDOUT / destination COPY FROM STDIN) is already encrypted by the two
// sslmode=verify-full PG connections, so only the segment-direct on_segment
// hop (segreceiver -> controller, F-13 «TLS between sender/receiver») needs
// an explicit switch, and serving it requires a controller certificate.
// A CA alone does NOT enable the data plane — a --ssl-ca without a serving
// keypair is a legitimate PG-only configuration (verify-full against a
// self-signed cluster CA) and must keep working unchanged. When enabled, the
// CA (if set) additionally makes the controller require a client
// certificate (mTLS).
func TLSEnabled(cert, key, ca string) bool {
	return cert != "" && key != ""
}

// ServerTLSConfig builds the controller-side (server) TLS configuration for
// the segment-direct data plane (F-13). cert/key are the controller's
// serving keypair; ca (when set) makes the server REQUIRE a client
// certificate signed by that CA — mTLS: a segreceiver without a valid
// certificate is rejected during the handshake. minTLS ("1.2"/"1.3") pins
// the protocol floor (default 1.2). The hostname check is the client's job
// (ClientTLSConfig sets ServerName); the server never validates the peer's
// name. A wrong/missing certificate or key is a hard, diagnosed error — a
// silently-downgraded TLS config would defeat the guarantee (F-13: 'on a bad
// certificate — refusal with diagnostics').
func ServerTLSConfig(cert, key, ca, minTLS string) (*tls.Config, error) {
	if cert == "" || key == "" {
		return nil, fmt.Errorf("data-plane TLS: --ssl-cert and --ssl-key are both required for the serving side (got cert=%q key=%q)", cert, key)
	}
	peer, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("data-plane TLS: load serving keypair: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{peer},
	}
	if v, verr := minTLSVersion(minTLS); verr == nil {
		cfg.MinVersion = v
	}
	if ca != "" {
		pool, perr := loadCAPool(ca)
		if perr != nil {
			return nil, perr
		}
		// mTLS: require and verify a client certificate (F-13).
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// ClientTLSConfig builds the segreceiver-side (client) TLS configuration for
// the segment-direct data plane (F-13). host is the controller's listen
// host: it becomes ServerName so the client validates the controller
// certificate — Go checks a named host against the DNS SANs and an IP
// literal against the IP SANs, so the controller certificate must cover the
// listen address (for an IP listen, an IP SAN). A certificate that does not
// cover it is a hard, diagnosed handshake failure. ca (when set) is the
// trusted CA bundle for chain validation; cert/key (when set) present the
// segreceiver's client certificate to an mTLS controller; minTLS pins the
// protocol floor (default 1.2).
func ClientTLSConfig(host, cert, key, ca, minTLS string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if host != "" {
		cfg.ServerName = host
	}
	if v, verr := minTLSVersion(minTLS); verr == nil {
		cfg.MinVersion = v
	}
	if ca != "" {
		pool, err := loadCAPool(ca)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if cert != "" && key != "" {
		peer, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("data-plane TLS: load client keypair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{peer}
	}
	return cfg, nil
}

// minTLSVersion maps a version string ("1.2"/"1.3") to a tls.Version.
func minTLSVersion(s string) (uint16, error) {
	switch s {
	case "1.0":
		return tls.VersionTLS10, nil
	case "1.1":
		return tls.VersionTLS11, nil
	case "1.2", "":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("data-plane TLS: unknown min TLS version %q", s)
	}
}

// loadCAPool reads a PEM CA bundle into a certificate pool.
func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("data-plane TLS: --ssl-ca %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("data-plane TLS: --ssl-ca %s: no valid certificates found", path)
	}
	return pool, nil
}
