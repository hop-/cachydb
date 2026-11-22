package mcpserver

import (
	"errors"
	"net"
	"strconv"
)

// DefaultHost is the listen host of a ListenConfig without one: this
// machine only.
const DefaultHost = "127.0.0.1"

// ListenConfig is where and how the HTTP transport listens. Client
// certificates, certificate reload, or ACME would be further fields.
type ListenConfig struct {
	// Host is the listen host; empty means DefaultHost. 0.0.0.0 or ::
	// listens on every interface.
	Host string
	// Port is the TCP port; 0 picks a free one.
	Port int
	// TLSCert and TLSKey are PEM files. With both set the server serves
	// HTTPS; with neither, plain HTTP. One without the other is invalid.
	TLSCert string
	TLSKey  string
}

// HostOrDefault returns Host without one pair of surrounding brackets (so
// "[::1]" and "::1" are the same), or DefaultHost if it is empty.
func (c ListenConfig) HostOrDefault() string {
	if c.Host == "" {
		return DefaultHost
	}
	if len(c.Host) > 2 && c.Host[0] == '[' && c.Host[len(c.Host)-1] == ']' {
		return c.Host[1 : len(c.Host)-1]
	}
	return c.Host
}

// Addr returns the host:port address to listen on.
func (c ListenConfig) Addr() string {
	return net.JoinHostPort(c.HostOrDefault(), strconv.Itoa(c.Port))
}

// TLS reports whether the listener serves HTTPS.
func (c ListenConfig) TLS() bool { return c.TLSCert != "" && c.TLSKey != "" }

// Validate rejects a TLS certificate without a key, or a key without a
// certificate.
func (c ListenConfig) Validate() error {
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("TLS needs both a certificate and a key (--tls-cert and --tls-key, or TLS_CERT and TLS_KEY); only one is set")
	}
	return nil
}
