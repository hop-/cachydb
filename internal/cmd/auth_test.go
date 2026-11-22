package cmd

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/hop-/cachydb/internal/config"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

// authLiveServer serves a tokens-required MCP server on dir over loopback
// HTTP with users root (server admin) and alice (read on "kb"), and
// returns its /mcp URL and their tokens.
func authLiveServer(t *testing.T, dir string) (string, map[string]string) {
	t.Helper()
	ctx := context.Background()
	engine, err := db.Open(ctx, db.Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	users, err := auth.Open(ctx, engine)
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{}
	for _, u := range []struct {
		name  string
		admin bool
	}{{"root", true}, {"alice", false}} {
		tok, err := users.CreateUser(ctx, u.name, auth.UserOptions{ServerAdmin: u.admin})
		if err != nil {
			t.Fatal(err)
		}
		tokens[u.name] = tok
	}
	if err := users.Grant(ctx, "alice", "kb", auth.LevelRead); err != nil {
		t.Fatal(err)
	}
	s, err := mcpserver.New(ctx, engine, mcpserver.Config{
		DefaultDatabase: "default", Transport: "http", Version: "v-test",
		TokensRequired: true, Users: users, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.HTTPHandler())
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := s.Close(closeCtx); err != nil {
			t.Errorf("server Close: %v", err)
		}
		ts.Close()
		if err := engine.Close(closeCtx); err != nil {
			t.Errorf("engine Close: %v", err)
		}
	})
	return ts.URL + "/mcp", tokens
}

// TestClientTokenEverySubcommand: with --token or CACHYDB_TOKEN every
// client subcommand authenticates against a server that requires tokens;
// without a token, or with a wrong one, it fails with a hint naming
// --token and CACHYDB_TOKEN. The flag wins over the environment.
func TestClientTokenEverySubcommand(t *testing.T) {
	ctx := context.Background()
	url, tokens := authLiveServer(t, t.TempDir())
	files := t.TempDir()
	dump := filepath.Join(files, "dump.jsonl")
	in := filepath.Join(files, "in.jsonl")
	kb := t.TempDir()
	seedKB(t, kb)
	if code, _, errOut := runCLI(t, ctx, "utils", "export", "-R", kb, "-d", "", "-c", "", "-o", in); code != 0 {
		t.Fatalf("utils export: %s", errOut)
	}
	wrong := auth.TokenPrefix + strings.Repeat("Z", 43)
	t.Setenv("CACHYDB_URL", "")

	type cliCase struct {
		name     string
		env      string // CACHYDB_TOKEN
		args     []string
		wantCode int
		wantOut  string
		wantErr  string
	}
	cases := []cliCase{
		{name: "tools with --token", args: []string{"client", "tools", "--url", url, "--token", tokens["root"], "--format", "text"}, wantOut: "list_users"},
		{name: "tools with CACHYDB_TOKEN", env: tokens["alice"], args: []string{"client", "tools", "--url", url, "--token", "", "--format", "text"}, wantOut: "whoami"},
		{name: "call whoami with --token", args: []string{"client", "call", "--url", url, "--token", tokens["alice"], "whoami"}, wantOut: `"user": "alice"`},
		{name: "call whoami with CACHYDB_TOKEN", env: tokens["root"], args: []string{"client", "call", "--url", url, "--token", "", "whoami"}, wantOut: `"user": "root"`},
		{name: "flag wins over env", env: tokens["root"], args: []string{"client", "call", "--url", url, "--token", tokens["alice"], "whoami"}, wantOut: `"user": "alice"`},
		{name: "call denied", args: []string{"client", "call", "--url", url, "--token", tokens["alice"], "create_database", `{"name":"x"}`}, wantCode: 1, wantErr: "permission denied"},
		{name: "import with --token", args: []string{"client", "import", "--url", url, "--token", tokens["root"], "-i", in, "--on-conflict", "fail"}, wantErr: "inserted"},
		{name: "export with CACHYDB_TOKEN", env: tokens["root"], args: []string{"client", "export", "--url", url, "--token", "", "-d", "", "-c", "", "-o", dump}},
		{name: "tools without token", args: []string{"client", "tools", "--url", url, "--token", "", "--format", "text"}, wantCode: 1, wantErr: "pass --token or set CACHYDB_TOKEN"},
		{name: "call without token", args: []string{"client", "call", "--url", url, "--token", "", "list_databases"}, wantCode: 1, wantErr: "pass --token or set CACHYDB_TOKEN"},
		{name: "export without token", args: []string{"client", "export", "--url", url, "--token", "", "-d", "", "-c", "", "-o", ""}, wantCode: 1, wantErr: "pass --token or set CACHYDB_TOKEN"},
		{name: "import without token", args: []string{"client", "import", "--url", url, "--token", "", "-i", in, "--on-conflict", "skip"}, wantCode: 1, wantErr: "pass --token or set CACHYDB_TOKEN"},
		{name: "wrong token", args: []string{"client", "call", "--url", url, "--token", wrong, "whoami"}, wantCode: 1, wantErr: "rejected the token"},
		{name: "wrong token from env", env: wrong, args: []string{"client", "tools", "--url", url, "--token", "", "--format", "text"}, wantCode: 1, wantErr: "check --token / CACHYDB_TOKEN"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CACHYDB_TOKEN", tt.env)
			code, out, errOut := runCLI(t, ctx, tt.args...)
			if code != tt.wantCode {
				t.Fatalf("exit %d, want %d; stderr %s", code, tt.wantCode, errOut)
			}
			if !strings.Contains(out, tt.wantOut) {
				t.Errorf("stdout %q lacks %q", out, tt.wantOut)
			}
			if !strings.Contains(errOut, tt.wantErr) {
				t.Errorf("stderr %q lacks %q", errOut, tt.wantErr)
			}
			for _, tok := range []string{tokens["root"], tokens["alice"], wrong} {
				if strings.Contains(errOut, tok) {
					t.Fatal("stderr contains a token")
				}
			}
		})
	}
	// The export through the server holds the imported data and no
	// system database.
	data, err := os.ReadFile(dump)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"kb"`) || strings.Contains(string(data), "_system") || strings.Contains(string(data), "tokens") {
		t.Fatalf("export:\n%s", data)
	}
}

// recordingTransport records the requests it sees and answers 200.
type recordingTransport struct {
	mu   sync.Mutex
	seen []*http.Request
}

func (rt *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	rt.seen = append(rt.seen, r)
	rt.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}, Request: r}, nil
}

// TestClientTokenBearerTransportScope: the token goes only to the
// endpoint's scheme and host (host compared without case), never to
// another host, port, or scheme, and nowhere when the endpoint does not
// parse or has no host. The caller's request is not modified.
func TestClientTokenBearerTransportScope(t *testing.T) {
	const tok = "cdb_secret"
	for _, tt := range []struct {
		name, endpoint, target string
		want                   bool
	}{
		{"same endpoint", "https://db.example:7601/mcp", "https://db.example:7601/mcp", true},
		{"other path", "https://db.example:7601/mcp", "https://db.example:7601/other", true},
		{"host case differs", "https://DB.example:7601/mcp", "https://db.EXAMPLE:7601/mcp", true},
		{"ipv6 loopback", "http://[::1]:7601/mcp", "http://[::1]:7601/mcp", true},
		{"other host", "https://db.example:7601/mcp", "https://evil.example:7601/mcp", false},
		{"other port", "https://db.example:7601/mcp", "https://db.example:7602/mcp", false},
		{"https to http downgrade", "https://db.example:7601/mcp", "http://db.example:7601/mcp", false},
		{"no scheme", "db.example:7601/mcp", "http://db.example:7601/mcp", false},
		{"unparsable endpoint", "http://[::1/mcp", "http://[::1]/mcp", false},
		{"empty endpoint", "", "http://127.0.0.1/mcp", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bt := newBearerTransport(tt.endpoint, tok)
			rec := &recordingTransport{}
			bt.base = rec
			req, err := http.NewRequest(http.MethodPost, tt.target, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := bt.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			got := rec.seen[0].Header.Get("Authorization")
			if (got == "Bearer "+tok) != tt.want || (!tt.want && got != "") {
				t.Fatalf("Authorization = %q, want sent = %v", got, tt.want)
			}
			if req.Header.Get("Authorization") != "" {
				t.Fatal("RoundTrip modified the caller's request")
			}
		})
	}
	// No token: nothing is sent, and a 401 hint names the flag.
	bt := newBearerTransport("http://127.0.0.1/mcp", "")
	if bt.authHint() != "" {
		t.Fatal("hint before any 401")
	}
	bt.base = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "" {
			t.Error("Authorization sent without a token")
		}
		return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})
	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1/mcp", nil)
	resp, err := bt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if h := bt.authHint(); !strings.Contains(h, "--token") || !strings.Contains(h, "CACHYDB_TOKEN") {
		t.Fatalf("hint = %q", h)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestClientTokenRedirectRefused: a server answering /mcp with a 307 or
// 302 to another server is not followed: withClient fails naming the
// target, and the target gets no request (so never the token).
func TestClientTokenRedirectRefused(t *testing.T) {
	var reached atomic.Int32
	var sawAuth atomic.Bool
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		if r.Header.Get("Authorization") != "" {
			sawAuth.Store(true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(b.Close)
	saveGlobals(t)
	clientTokenFlag = "cdb_redirect-test-token"

	for _, code := range []int{http.StatusTemporaryRedirect, http.StatusFound, http.StatusPermanentRedirect} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, b.URL+"/mcp", code)
			}))
			t.Cleanup(a.Close)
			err := withClient(context.Background(), a.URL+"/mcp", func(*mcp.ClientSession) error { return nil })
			if err == nil {
				t.Fatal("withClient followed a redirect")
			}
			if !strings.Contains(err.Error(), b.URL) || !strings.Contains(err.Error(), "does not follow redirects") {
				t.Fatalf("error %q does not name the redirect target", err)
			}
			if strings.Contains(err.Error(), clientTokenFlag) {
				t.Fatal("error contains the token")
			}
		})
	}
	if n := reached.Load(); n != 0 || sawAuth.Load() {
		t.Fatalf("redirect target reached %d times (Authorization seen: %v)", n, sawAuth.Load())
	}
}

// writeTestCert writes a self-signed certificate for 127.0.0.1 and its
// key to dir.
func writeTestCert(t *testing.T, dir string) (string, string) {
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
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}), 0o600); err != nil {
		t.Fatal(err)
	}
	return cert, keyFile
}

// TestHealthcheckHTTPS: an HTTPS URL on loopback is probed without
// verifying the certificate; with --tls-cert and --tls-key the default
// URL is https; a redirect answer is unhealthy and its target is not
// reached; an HTTPS URL on another host still verifies.
func TestHealthcheckHTTPS(t *testing.T) {
	ctx := context.Background()
	ok := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ok.Close)
	if err := healthcheck(ctx, io.Discard, ok.URL+"/healthz", "", 0); err != nil {
		t.Fatalf("https loopback: %v", err)
	}
	localhost := strings.Replace(ok.URL, "127.0.0.1", "localhost", 1)
	if err := healthcheck(ctx, io.Discard, localhost+"/healthz", "", 0); err != nil {
		t.Fatalf("https localhost: %v", err)
	}

	saveGlobals(t)
	port, err := strconv.Atoi(ok.URL[strings.LastIndex(ok.URL, ":")+1:])
	if err != nil {
		t.Fatal(err)
	}
	healthcheckTLSCert, healthcheckTLSKey = "cert.pem", "key.pem"
	if err := healthcheck(ctx, io.Discard, "", "http", port); err != nil {
		t.Fatalf("default URL with TLS flags: %v", err)
	}
	healthcheckTLSCert, healthcheckTLSKey = "cert.pem", ""
	if err := healthcheck(ctx, io.Discard, "", "http", port); err == nil {
		t.Fatal("one TLS flag still probed https")
	}
	healthcheckTLSCert = ""

	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/healthz", http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	err = healthcheck(ctx, io.Discard, redirect.URL+"/healthz", "", 0)
	if err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("redirect: %v, want unhealthy 302", err)
	}
	if reached.Load() != 0 {
		t.Fatal("the redirect target was reached")
	}

	// A non-loopback https URL uses the verifying default client.
	if c := healthClient(mustParseURL(t, "https://db.example/healthz")); c != http.DefaultClient {
		t.Fatal("non-loopback https skips verification")
	}
	if c := healthClient(mustParseURL(t, "http://127.0.0.1/healthz")); c != http.DefaultClient {
		t.Fatal("plain http uses the insecure client")
	}
}

// TestHealthcheckTLSFlags: --tls-cert and --tls-key are healthcheck flags.
func TestHealthcheckTLSFlags(t *testing.T) {
	for _, name := range []string{"tls-cert", "tls-key"} {
		if healthcheckCmd.Flags().Lookup(name) == nil {
			t.Errorf("healthcheck has no --%s", name)
		}
	}
}

// startApp runs "cachydb app" with args in-process and returns a function
// that stops it and returns its exit status and stderr.
func startApp(t *testing.T, args ...string) (stop func() (int, string), done <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	saveGlobals(t)
	resetContexts(rootCmd)
	var stdout bytes.Buffer
	stderr := &lockedWriter{}
	rootCmd.SetArgs(append([]string{"app"}, args...))
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(stderr)
	finished := make(chan struct{})
	var code int
	go func() {
		code = execute(ctx, stderr)
		close(finished)
	}()
	stopped := false
	stop = func() (int, string) {
		if !stopped {
			stopped = true
			cancel()
			select {
			case <-finished:
			case <-time.After(15 * time.Second):
				t.Fatal("app did not stop within 15s")
			}
		}
		return code, stderr.String()
	}
	t.Cleanup(func() { stop() })
	return stop, finished
}

type lockedWriter struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// waitHealthy probes until the server answers 200 or the app exits.
func waitHealthy(t *testing.T, done <-chan struct{}, probe func(ctx context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := probe(ctx)
		cancel()
		if err == nil {
			return
		}
		select {
		case <-done:
			t.Fatalf("app exited early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not healthy within 10s: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// setConfigEnv sets env for config.Init and reloads the configuration,
// restoring both at cleanup.
func setConfigEnv(t *testing.T, env map[string]string) {
	t.Helper()
	t.Cleanup(func() {
		if err := config.Init(); err != nil {
			t.Errorf("restoring config: %v", err)
		}
	})
	for k, v := range env {
		t.Setenv(k, v)
	}
	if err := config.Init(); err != nil {
		t.Fatal(err)
	}
}

// TestAuthAppTLSStart: "cachydb app" with --tls-cert and --tls-key and a
// certificate generated here serves HTTPS on loopback; healthcheck with
// the same TLS flags probes it over https and finds it healthy.
func TestAuthAppTLSStart(t *testing.T) {
	cert, key := writeTestCert(t, t.TempDir())
	port := closedPort(t)
	stop, done := startApp(t, "-t", "http", "--host", "127.0.0.1", "--auth", "auto", "-p", strconv.Itoa(port),
		"--tls-cert", cert, "--tls-key", key, "-R", t.TempDir(), "-d", "main")
	// startApp saved the globals; only healthcheck's are set here, which
	// the running app does not read.
	waitHealthy(t, done, func(ctx context.Context) error {
		healthcheckTLSCert, healthcheckTLSKey = cert, key
		return healthcheck(ctx, io.Discard, "", "http", port)
	})
	// Plain http on the TLS port is not healthy.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := healthcheck(ctx, io.Discard, fmt.Sprintf("http://127.0.0.1:%d/healthz", port), "", 0); err == nil {
		t.Error("plain HTTP answered 200 on the TLS listener")
	}
	if code, errOut := stop(); code != 0 {
		t.Fatalf("app exit %d: %s", code, errOut)
	}
}

// TestAuthAppOneTLSFileFails: one TLS file alone is a start-up error.
func TestAuthAppOneTLSFileFails(t *testing.T) {
	ctx := context.Background()
	code, _, errOut := runCLI(t, ctx, "app", "-t", "http", "--host", "127.0.0.1", "--auth", "auto", "-p", "0",
		"--tls-cert", "cert.pem", "--tls-key", "", "-R", t.TempDir(), "-d", "main")
	if code != 1 || !strings.Contains(errOut, "TLS needs both") {
		t.Fatalf("exit %d, stderr %s", code, errOut)
	}
	code, _, errOut = runCLI(t, ctx, "app", "-t", "http", "--host", "127.0.0.1", "--auth", "sometimes", "-p", "0",
		"--tls-cert", "", "--tls-key", "", "-R", t.TempDir(), "-d", "main")
	if code != 1 || !strings.Contains(errOut, "AUTH") {
		t.Fatalf("invalid --auth: exit %d, stderr %s", code, errOut)
	}
}

// TestAuthAppRefusesWithoutUsers: --auth required on loopback with no
// user refuses to start (without listening) and says how to fix it.
func TestAuthAppRefusesWithoutUsers(t *testing.T) {
	code, _, errOut := runCLI(t, context.Background(), "app", "-t", "http", "--host", "127.0.0.1", "--auth", "required",
		"-p", strconv.Itoa(closedPort(t)), "--tls-cert", "", "--tls-key", "", "-R", t.TempDir(), "-d", "main")
	if code != 1 {
		t.Fatalf("exit %d, want 1: %s", code, errOut)
	}
	for _, part := range []string{"refusing to start", "utils user add", "BOOTSTRAP_ADMIN_TOKEN", "AUTH=off"} {
		if !strings.Contains(errOut, part) {
			t.Errorf("refusal lacks %q: %s", part, errOut)
		}
	}
}

// TestAuthAppBootstrapAndClient: with AUTH=required and
// BOOTSTRAP_ADMIN_TOKEN_FILE on loopback the server starts, the bootstrap
// token authenticates the client as admin through CACHYDB_TOKEN, and a
// client without a token gets the hint.
func TestAuthAppBootstrapAndClient(t *testing.T) {
	const tok = "bootstrap-0123456789abcdef0123456789abcdef"
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(tok+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setConfigEnv(t, map[string]string{"BOOTSTRAP_ADMIN_TOKEN": "", "BOOTSTRAP_ADMIN_TOKEN_FILE": file})
	port := closedPort(t)
	stop, done := startApp(t, "-t", "http", "--host", "127.0.0.1", "--auth", "required", "-p", strconv.Itoa(port),
		"--tls-cert", "", "--tls-key", "", "-R", t.TempDir(), "-d", "main")
	waitHealthy(t, done, func(ctx context.Context) error {
		return healthcheck(ctx, io.Discard, fmt.Sprintf("http://127.0.0.1:%d/healthz", port), "", 0)
	})
	url := fmt.Sprintf("http://127.0.0.1:%d/mcp", port)
	// The client runs through withClient, not the CLI: the running app
	// owns the cobra command tree. clientTokenFlag is empty (startApp
	// saved and cleared nothing else), so the token comes from
	// CACHYDB_TOKEN.
	clientTokenFlag = ""
	t.Setenv("CACHYDB_TOKEN", tok)
	var out bytes.Buffer
	err := withClient(context.Background(), url, func(cs *mcp.ClientSession) error {
		return callClientTool(context.Background(), &out, cs, "whoami", nil)
	})
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	var who map[string]any
	if err := json.Unmarshal(out.Bytes(), &who); err != nil || who["user"] != auth.BootstrapAdmin || who["server_admin"] != true {
		t.Fatalf("whoami = %s (%v)", out.String(), err)
	}

	t.Setenv("CACHYDB_TOKEN", "")
	err = withClient(context.Background(), url, func(*mcp.ClientSession) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "pass --token or set CACHYDB_TOKEN") {
		t.Fatalf("no token: %v", err)
	}

	code, errOut := stop()
	if code != 0 {
		t.Fatalf("app exit %d: %s", code, errOut)
	}
	if strings.Contains(errOut, tok) {
		t.Fatal("app output contains the bootstrap token")
	}
}

// TestAuthFlagEnvPrecedence: --host, --auth, --tls-cert, and --tls-key
// take precedence over HOST, AUTH, TLS_CERT, and TLS_KEY, which take
// precedence over the defaults (127.0.0.1, auto, none).
func TestAuthFlagEnvPrecedence(t *testing.T) {
	keys := []string{"HOST", "AUTH", "TLS_CERT", "TLS_KEY"}
	saveGlobals(t)
	type want struct{ host, auth, cert, key string }
	for _, tt := range []struct {
		name string
		env  map[string]string
		args []string
		want want
	}{
		{name: "defaults", want: want{"127.0.0.1", "auto", "", ""}},
		{name: "env", env: map[string]string{"HOST": "::1", "AUTH": "off", "TLS_CERT": "/e/c", "TLS_KEY": "/e/k"}, want: want{"::1", "off", "/e/c", "/e/k"}},
		{name: "flags", args: []string{"--host", "0.0.0.0", "--auth", "required", "--tls-cert", "/f/c", "--tls-key", "/f/k"}, want: want{"0.0.0.0", "required", "/f/c", "/f/k"}},
		{
			name: "flags over env",
			env:  map[string]string{"HOST": "::1", "AUTH": "off", "TLS_CERT": "/e/c", "TLS_KEY": "/e/k"},
			args: []string{"--host", "localhost", "--auth", "auto", "--tls-cert", "/f/c", "--tls-key", "/f/k"},
			want: want{"localhost", "auto", "/f/c", "/f/k"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Cleanup(func() {
				if err := config.Init(); err != nil {
					t.Errorf("restoring config: %v", err)
				}
			})
			for _, k := range keys {
				t.Setenv(k, tt.env[k])
				if _, ok := tt.env[k]; !ok {
					if err := os.Unsetenv(k); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := config.Init(); err != nil {
				t.Fatal(err)
			}
			cmd := &cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { return nil }}
			setAllFlagsToCmd(cmd)
			cmd.SetArgs(tt.args)
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			if got := (want{generalHost, generalAuth, generalTLSCert, generalTLSKey}); got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestUserCommands runs every "cachydb utils user" command: add and
// token add print the token alone on stdout (the note goes to stderr);
// list never shows a token or its digest; the changes persist; errors
// exit 1.
func TestUserCommands(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	run := func(t *testing.T, wantCode int, args ...string) (string, string) {
		t.Helper()
		code, out, errOut := runCLI(t, ctx, append(append([]string{"utils", "user"}, args...), "-R", dir)...)
		if code != wantCode {
			t.Fatalf("utils user %v: exit %d, want %d; stderr %s", args, code, wantCode, errOut)
		}
		return out, errOut
	}
	tokenLine := func(t *testing.T, out string) string {
		t.Helper()
		if strings.Count(out, "\n") != 1 || !strings.HasPrefix(out, auth.TokenPrefix) {
			t.Fatalf("stdout %q is not the token alone", out)
		}
		return strings.TrimSpace(out)
	}

	out, _ := run(t, 0, "list", "--format", "text")
	if !strings.Contains(out, "No users") {
		t.Fatalf("empty list: %q", out)
	}
	out, errOut := run(t, 0, "add", "root", "--admin", "--default-database", "")
	rootTok := tokenLine(t, out)
	if !strings.Contains(errOut, "Server admin") || strings.Contains(errOut, rootTok) {
		t.Fatalf("add note: %q", errOut)
	}
	out, errOut = run(t, 0, "add", "alice", "--admin=false", "--default-database", "kb")
	aliceTok := tokenLine(t, out)
	if !strings.Contains(errOut, `User "alice" created`) {
		t.Fatalf("add note: %q", errOut)
	}
	_, errOut = run(t, 0, "grant", "alice", "kb", "write")
	if !strings.Contains(errOut, `write access to database "kb"`) {
		t.Fatalf("grant: %q", errOut)
	}
	_, errOut = run(t, 0, "grant", "alice", "*", "read")
	if !strings.Contains(errOut, "every database (*)") {
		t.Fatalf("grant *: %q", errOut)
	}
	out, errOut = run(t, 0, "token", "add", "alice", "--label", "laptop")
	laptop := tokenLine(t, out)
	if !strings.Contains(errOut, `"laptop"`) {
		t.Fatalf("token add note: %q", errOut)
	}
	out, _ = run(t, 0, "token", "add", "alice", "--label", "")
	spare := tokenLine(t, out)

	secrets := []string{rootTok, aliceTok, laptop, spare}
	out, _ = run(t, 0, "list", "--format", "text")
	for _, want := range []string{"NAME", "root", "yes", "alice", "kb", "*=read,kb=write", "default,default-2,laptop"} {
		if !strings.Contains(out, want) {
			t.Errorf("list lacks %q:\n%s", want, out)
		}
	}
	jsonOut, _ := run(t, 0, "list", "--format", "json")
	var users []mcpserver.UserInfo
	if err := json.Unmarshal([]byte(jsonOut), &users); err != nil || len(users) != 2 || users[0].Name != "alice" || users[0].Grants["kb"] != "write" || len(users[0].Tokens) != 3 {
		t.Fatalf("list json = %s (%v)", jsonOut, err)
	}
	for _, s := range secrets {
		if strings.Contains(out+jsonOut, s) || strings.Contains(out+jsonOut, digestHex(s)) {
			t.Fatal("list shows a token or its digest")
		}
	}

	_, _ = run(t, 0, "revoke", "alice", "*")
	_, _ = run(t, 0, "token", "revoke", "alice", "laptop")

	// The stored state: tokens authenticate, the revoked one does not.
	e := openEngine(t, dir)
	s, err := auth.Open(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	for tok, want := range map[string]bool{rootTok: true, aliceTok: true, spare: true, laptop: false} {
		if _, err := s.Authenticate(tok); (err == nil) != want {
			t.Errorf("Authenticate = %v, want ok %v", err, want)
		}
	}
	u, err := s.User("alice")
	if err != nil || u.DefaultDatabase != "kb" || len(u.Grants) != 1 || u.Grants["kb"] != auth.LevelWrite {
		t.Fatalf("alice = %+v, %v", u, err)
	}
	closeEngine(t, e)

	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"add", "alice", "--admin=false", "--default-database", ""}, "exists"},
		{[]string{"add", "_bad", "--admin=false", "--default-database", ""}, "invalid"},
		{[]string{"grant", "alice", "kb", "owner"}, "invalid access level"},
		{[]string{"grant", "ghost", "kb", "read"}, "not found"},
		{[]string{"revoke", "alice", "nope"}, "not found"},
		{[]string{"token", "add", "alice", "--label", "default"}, "exists"},
		{[]string{"token", "revoke", "alice", "laptop"}, "not found"},
		{[]string{"remove", "root"}, "only server admin"},
		{[]string{"list", "--format", "yaml"}, "invalid --format"},
	} {
		_, errOut := run(t, 1, tt.args...)
		if !strings.Contains(errOut, tt.want) {
			t.Errorf("utils user %v: stderr %q lacks %q", tt.args, errOut, tt.want)
		}
	}

	_, errOut = run(t, 0, "remove", "alice")
	if !strings.Contains(errOut, `"alice"`) {
		t.Fatalf("remove: %q", errOut)
	}
	out, _ = run(t, 0, "list", "--format", "text")
	if strings.Contains(out, "alice") {
		t.Fatalf("alice still listed:\n%s", out)
	}
	// utils export leaves the system database out.
	code, exported, errOut := runCLI(t, ctx, "utils", "export", "-R", dir, "-d", "", "-c", "", "-o", "")
	if code != 0 || strings.Contains(exported, "_system") || strings.Contains(exported, rootTok) || strings.Contains(exported, digestHex(rootTok)) {
		t.Fatalf("utils export: exit %d, stderr %s\n%s", code, errOut, exported)
	}
	// The system database stays out of the ordinary listing.
	var listing bytes.Buffer
	if err := listDatabases(ctx, &listing, db.Options{Dir: dir}, true); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(listing.String(), "_system") {
		t.Fatalf("utils list shows _system:\n%s", listing.String())
	}
}

// TestUserCommandsWhileServerRuns: utils user takes the data-directory
// lock, so it fails while a server holds it.
func TestUserCommandsWhileServerRuns(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	defer closeEngine(t, e)
	code, out, errOut := runCLI(t, context.Background(), "utils", "user", "add", "x", "--admin=false", "--default-database", "", "-R", dir)
	if code != 1 || out != "" {
		t.Fatalf("exit %d, stdout %q, stderr %s", code, out, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "lock") && !strings.Contains(errOut, "in use") && !strings.Contains(errOut, "running") {
		t.Errorf("stderr %q does not explain the lock", errOut)
	}
}

func digestHex(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func mustParseURL(t *testing.T, s string) *neturl.URL {
	t.Helper()
	u, err := neturl.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
