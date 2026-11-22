package mcpserver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestListenConfigHostAndAddr(t *testing.T) {
	for _, tt := range []struct {
		host, wantHost, wantAddr string
	}{
		{"", "127.0.0.1", "127.0.0.1:7601"},
		{"127.0.0.1", "127.0.0.1", "127.0.0.1:7601"},
		{"::1", "::1", "[::1]:7601"},
		{"[::1]", "::1", "[::1]:7601"},
		{"[]", "[]", "[]:7601"}, // too short to strip; listening on it fails
		{"0.0.0.0", "0.0.0.0", "0.0.0.0:7601"},
		{"::", "::", "[::]:7601"},
		{"localhost", "localhost", "localhost:7601"},
	} {
		c := ListenConfig{Host: tt.host, Port: 7601}
		if got := c.HostOrDefault(); got != tt.wantHost {
			t.Errorf("HostOrDefault(%q) = %q, want %q", tt.host, got, tt.wantHost)
		}
		if got := c.Addr(); got != tt.wantAddr {
			t.Errorf("Addr(%q) = %q, want %q", tt.host, got, tt.wantAddr)
		}
	}
	if DefaultHost != "127.0.0.1" {
		t.Fatalf("DefaultHost = %q", DefaultHost)
	}
}

func TestListenConfigTLSValidate(t *testing.T) {
	for _, tt := range []struct {
		cert, key string
		tls, ok   bool
	}{
		{"", "", false, true},
		{"c.pem", "k.pem", true, true},
		{"c.pem", "", false, false},
		{"", "k.pem", false, false},
	} {
		c := ListenConfig{TLSCert: tt.cert, TLSKey: tt.key}
		if c.TLS() != tt.tls {
			t.Errorf("TLS(%q, %q) = %v", tt.cert, tt.key, c.TLS())
		}
		err := c.Validate()
		if (err == nil) != tt.ok {
			t.Errorf("Validate(%q, %q) = %v", tt.cert, tt.key, err)
		}
		if err != nil && (!strings.Contains(err.Error(), "--tls-cert") || !strings.Contains(err.Error(), "TLS_KEY")) {
			t.Errorf("error %q does not name the settings", err)
		}
	}
}

// TestTLSOneFileIsAStartupError: New refuses a certificate without a key
// and a key without a certificate.
func TestTLSOneFileIsAStartupError(t *testing.T) {
	s := newTestServer(t)
	for _, l := range []ListenConfig{{TLSCert: "c.pem"}, {TLSKey: "k.pem"}} {
		if _, err := New(context.Background(), s.engine, Config{DefaultDatabase: testDefaultDB, Transport: "http", Listen: l}); err == nil {
			t.Errorf("New with %+v succeeded", l)
		}
	}
	// Tokens required without a store is refused too.
	if _, err := New(context.Background(), s.engine, Config{DefaultDatabase: testDefaultDB, Transport: "http", TokensRequired: true}); err == nil {
		t.Error("New with TokensRequired and no store succeeded")
	}
	if _, err := NewServer(context.Background(), s.engine, testDefaultDB, "http", "nohostport", testVersion); err == nil {
		t.Error("NewServer with a bad address succeeded")
	}
	if _, err := NewServer(context.Background(), s.engine, testDefaultDB, "http", "127.0.0.1:x", testVersion); err == nil {
		t.Error("NewServer with a bad port succeeded")
	}
}

// writeSelfSigned writes a self-signed certificate for 127.0.0.1 and ::1
// and its key to dir and returns their paths.
func writeSelfSigned(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "cachydb-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// lockedBuffer is a goroutine-safe stderr sink.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *lockedBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *lockedBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

var listenLine = regexp.MustCompile(`listening on (https?)://([^/]+)/mcp`)

// startOnLoopback runs s.Start on 127.0.0.1:0 and returns the scheme and
// address it reports; cleanup stops it.
func startOnLoopback(t *testing.T, s *Server) (scheme, addr string) {
	t.Helper()
	s.transport = "http"
	s.httpAddr = "127.0.0.1:0"
	out := &lockedBuffer{}
	s.stderr = out
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("Start: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Start did not return")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m := listenLine.FindStringSubmatch(out.String()); m != nil {
			return m[1], m[2]
		}
		select {
		case err := <-errc:
			t.Fatalf("Start returned early: %v (%s)", err, out.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("no listen line in %q", out.String())
	return "", ""
}

// TestTLSServesHTTPS: with both TLS files the server serves HTTPS (TLS
// 1.2 or later) on loopback with a certificate generated here, /healthz
// answers over it, a plain-HTTP request is not served, and a client
// capped at TLS 1.1 is rejected.
func TestTLSServesHTTPS(t *testing.T) {
	s := newTestServer(t)
	s.tlsCert, s.tlsKey = writeSelfSigned(t, t.TempDir())
	scheme, addr := startOnLoopback(t, s)
	if scheme != "https" {
		t.Fatalf("scheme = %s, want https", scheme)
	}

	pemBytes, err := os.ReadFile(s.tlsCert)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pemBytes)
	client := func(cfg *tls.Config) *http.Client {
		return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg}}
	}

	resp, err := client(&tls.Config{RootCAs: pool}).Get("https://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("GET https /healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.TLS == nil || resp.TLS.Version < tls.VersionTLS12 {
		t.Fatalf("HTTPS /healthz: %d, TLS %+v", resp.StatusCode, resp.TLS)
	}

	if resp, err := client(nil).Get("http://" + addr + "/healthz"); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("plain HTTP served on the TLS listener")
		}
	}

	old := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS11} //nolint:gosec // the test checks that the server refuses it
	if resp, err := client(old).Get("https://" + addr + "/healthz"); err == nil {
		resp.Body.Close()
		t.Fatal("a TLS 1.1 client was served")
	}
}

// TestTLSBadFilesFailStart: unreadable or mismatched TLS files fail Start
// before it listens.
func TestTLSBadFilesFailStart(t *testing.T) {
	dir := t.TempDir()
	cert, _ := writeSelfSigned(t, dir)
	_, otherKey := writeSelfSigned(t, t.TempDir())
	for name, files := range map[string][2]string{
		"missing":    {filepath.Join(dir, "nope.pem"), filepath.Join(dir, "nope.key")},
		"mismatched": {cert, otherKey},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t)
			s.transport = "http"
			s.httpAddr = "127.0.0.1:0"
			s.tlsCert, s.tlsKey = files[0], files[1]
			err := s.Start(context.Background())
			if err == nil || !strings.Contains(err.Error(), "TLS certificate") {
				t.Fatalf("Start = %v, want a TLS certificate error", err)
			}
		})
	}
}

// TestListenPlainHTTPReportsTokens: the start-up line says whether tokens
// are required.
func TestListenPlainHTTPReportsTokens(t *testing.T) {
	env := newAuthEnv(t)
	scheme, _ := startOnLoopback(t, env.s)
	if scheme != "http" {
		t.Fatalf("scheme = %s", scheme)
	}
	if out := env.s.stderr.(*lockedBuffer).String(); !strings.Contains(out, "tokens required") {
		t.Fatalf("start-up line %q does not say tokens are required", out)
	}
}
