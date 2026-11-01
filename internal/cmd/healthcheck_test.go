package cmd

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// healthServer serves /healthz with status code and returns its URL and
// port.
func healthServer(t *testing.T, code int) (string, int) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(code)
	}))
	t.Cleanup(ts.Close)
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return ts.URL + "/healthz", port
}

// closedPort returns a local port with nothing listening on it.
func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func TestHealthcheck(t *testing.T) {
	okURL, okPort := healthServer(t, http.StatusOK)
	downURL, _ := healthServer(t, http.StatusServiceUnavailable)
	refused := closedPort(t)

	tests := []struct {
		name      string
		url       string
		transport string
		port      int
		wantErr   string // "" = healthy
		wantNote  bool   // the stdio note on stderr
	}{
		{name: "healthy url", url: okURL, transport: "stdio"},
		{name: "healthy default url from port", transport: "http", port: okPort},
		{name: "unavailable", url: downURL, transport: "http", wantErr: "503"},
		{name: "connection refused", transport: "http", port: refused, wantErr: "connection refused"},
		{name: "stdio has nothing to probe", transport: "stdio", port: refused, wantNote: true},
		{name: "invalid transport", transport: "sse", port: okPort, wantErr: "invalid transport"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
			defer cancel()
			err := healthcheck(ctx, &stderr, tt.url, tt.transport, tt.port)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("healthcheck: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("healthcheck = %v, want an error containing %q", err, tt.wantErr)
			}
			note := "healthcheck: transport is stdio; nothing to probe\n"
			if got := stderr.String(); tt.wantNote && got != note || !tt.wantNote && got != "" {
				t.Fatalf("stderr = %q, want note %v", got, tt.wantNote)
			}
		})
	}
}

func TestHealthcheckTimeout(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var stderr bytes.Buffer
	if err := healthcheck(ctx, &stderr, ts.URL+"/healthz", "http", 0); err == nil {
		t.Fatal("healthcheck of a hung server succeeded")
	}
}

func TestHealthcheckFlags(t *testing.T) {
	for _, tt := range []struct{ name, shorthand, env string }{
		{"url", "", ""},
		{"port", "p", "PORT"},
		{"transport", "t", "TRANSPORT"},
	} {
		f := healthcheckCmd.Flags().Lookup(tt.name)
		if f == nil || f.Shorthand != tt.shorthand || !strings.Contains(f.Usage, tt.env) {
			t.Errorf("--%s flag = %+v", tt.name, f)
		}
	}
}
