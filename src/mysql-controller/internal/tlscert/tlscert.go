// Package tlscert provides fast self-signed TLS certificate generation
// using Ed25519 keys. Optimized for speed, not security — suitable for
// internal/dev use with Go net/http clients that accept self-signed certs.
package tlscert

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

// GeneratePEM returns a self-signed PEM-encoded certificate and private key
// backed by a freshly generated Ed25519 key pair. Useful when you need raw
// PEM bytes, e.g. to embed in a config file.
//
// Pass no hosts to get a cert valid for localhost + 127.0.0.1 only.
func GeneratePEM(hosts ...string) (certPEM, keyPEM []byte, err error) {
	// --- 1. Generate Ed25519 key pair -----------------------------------
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	// --- 2. Build a minimal certificate template -----------------------
	serial, err := rand.Int(rand.Reader, big.NewInt(128)) // tiny serial, fast
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Minute), // small back-date for clock skew
		NotAfter:     now.Add(24 * time.Hour),

		// KeyUsage / ExtKeyUsage: bare minimum for TLS server auth.
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// --- 3. Populate SANs ----------------------------------------------
	if len(hosts) == 0 {
		hosts = []string{"localhost", "127.0.0.1"}
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}

	// --- 4. Self-sign --------------------------------------------------
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return nil, nil, err
	}

	// --- 5. PEM-encode -------------------------------------------------
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})

	return certPEM, keyPEM, nil
}

// Generate returns a tls.Certificate backed by a freshly generated Ed25519
// key pair. The certificate is self-signed and valid for the given hosts
// (DNS names or IP addresses). Pass no hosts to get a cert valid for
// localhost + 127.0.0.1 only.
//
// Speed notes:
//   - Ed25519 key generation is ~10–50× faster than RSA-2048.
//   - Serial number is a single random byte (minimal entropy draw).
//   - Validity window is 24 h — short enough to not matter, long enough
//     to survive clock skew in tests.
//   - No CRL/OCSP, no SANs beyond what you pass, no extensions beyond
//     the bare minimum x509 requires.
func Generate(hosts ...string) (tls.Certificate, error) {
	certPEM, keyPEM, err := GeneratePEM(hosts...)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// ServerTLSConfig returns a *tls.Config ready to pass to http.Server.TLSConfig.
// Call srv.ListenAndServeTLS("", "") (empty strings) afterwards — the cert is
// already embedded in the config.
func ServerTLSConfig(hosts ...string) (*tls.Config, error) {
	cert, err := Generate(hosts...)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		// TLS 1.3 only — Ed25519 + TLS 1.3 is the fastest handshake Go supports.
		MinVersion: tls.VersionTLS13,
	}, nil
}

// ClientTLSConfig returns a *tls.Config that trusts only the supplied
// self-signed certificate. Use this on the client side when you obtained
// the server cert via Generate or ServerTLSConfig.
//
// Example:
//
//	serverCert, _ := tlscert.Generate("localhost")
//	clientCfg, _ := tlscert.ClientTLSConfig(serverCert)
//	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientCfg}}
func ClientTLSConfig(serverCert tls.Certificate) (*tls.Config, error) {
	leaf, err := x509.ParseCertificate(serverCert.Certificate[0])
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}, nil
}
