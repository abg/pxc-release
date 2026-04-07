package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
)

func ServerTLSConfig(cert, key string) (*tls.Config, error) {
	certPair, err := tls.X509KeyPair([]byte(cert), []byte(key))
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{certPair},
	}, nil
}

func ClientTLSConfig(ca, serverName string) (*tls.Config, error) {
	rootCAs := x509.NewCertPool()

	if ok := rootCAs.AppendCertsFromPEM([]byte(ca)); !ok {
		return nil, errors.New("failed to parse ca certificates")
	}

	return &tls.Config{
		RootCAs:    rootCAs,
		ServerName: serverName,
	}, nil
}
