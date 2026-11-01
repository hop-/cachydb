package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// nopWriteCloser adapts an io.Writer to io.WriteCloser.
type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// runStdio runs s's stdio transport over input (stdin) and returns
// everything written to stdout and Start's result.
func runStdio(t *testing.T, s *Server, input string) (string, error) {
	t.Helper()
	var out strings.Builder
	s.stdio = &mcp.IOTransport{
		Reader: io.NopCloser(strings.NewReader(input)), // EOF right after input
		Writer: nopWriteCloser{&out},
	}
	done := make(chan error, 1)
	go func() { done <- s.Start(context.Background()) }()
	select {
	case err := <-done:
		return out.String(), err
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after stdin closed")
		return "", nil
	}
}

// TestStdioEarlyEOF: a client that closes stdin, with or without requests
// in flight, is a clean shutdown (Start returns nil, so the process exits
// 0). Invalid input is still an error.
func TestStdioEarlyEOF(t *testing.T) {
	initialize := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}` + "\n" +
		`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n"
	call := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_databases","arguments":{}}}` + "\n"
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "closed immediately", input: ""},
		{name: "after initialize", input: initialize},
		{name: "with a call in flight", input: initialize + call},
		// Not an end of input but a broken client: still an error.
		{name: "invalid JSON", input: "not json\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t)
			out, err := runStdio(t, s, tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Start = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Start = %v, want nil (clean shutdown)", err)
			}
			// Responses to requests in flight at EOF may be dropped by the
			// SDK; whatever was written must be JSON-RPC only.
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				if line != "" && !strings.HasPrefix(line, `{"jsonrpc":"2.0"`) {
					t.Errorf("non-JSON-RPC output on stdout: %q", line)
				}
			}
		})
	}
}

func TestIsEndOfInput(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "EOF", err: io.EOF, want: true},
		{name: "wrapped EOF", err: fmt.Errorf("reading: %w", io.EOF), want: true},
		{name: "closed pipe", err: io.ErrClosedPipe, want: true},
		{name: "connection closed", err: fmt.Errorf("x: %w", mcp.ErrConnectionClosed), want: true},
		{name: "server is closing", err: fmt.Errorf("x: %w", errServerClosing), want: true},
		{name: "other", err: errors.New("disk on fire"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isEndOfInput(tt.err); got != tt.want {
				t.Fatalf("isEndOfInput(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
