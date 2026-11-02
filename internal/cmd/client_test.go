package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/exportfmt"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// liveServer serves the real MCP server on dir over HTTP and returns its
// /mcp URL and a function that stops it (closing the engine).
func liveServer(t *testing.T, dir string) (string, func()) {
	t.Helper()
	ctx := context.Background()
	engine, err := db.Open(ctx, db.Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	s, err := mcpserver.NewServer(ctx, engine, "default", "http", "", "v-test")
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.HTTPHandler())
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := s.Close(closeCtx); err != nil {
			t.Errorf("server Close: %v", err)
		}
		ts.Close()
		if err := engine.Close(closeCtx); err != nil {
			t.Errorf("engine Close: %v", err)
		}
	}
	t.Cleanup(stop)
	return ts.URL + "/mcp", stop
}

func clientDo(t *testing.T, url string, fn func(cs *mcp.ClientSession) error) error {
	t.Helper()
	return withClient(context.Background(), url, fn)
}

func TestClientToolsAndCall(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	seedKB(t, dir)
	url, _ := liveServer(t, dir)

	var out bytes.Buffer
	if err := clientDo(t, url, func(cs *mcp.ClientSession) error { return listClientTools(ctx, &out, cs, "text") }); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(out.String(), "\n    "); n != 21 || !strings.Contains(out.String(), "count_documents\n") {
		t.Errorf("tools text lists %d tools:\n%s", n, out.String())
	}
	out.Reset()
	if err := clientDo(t, url, func(cs *mcp.ClientSession) error { return listClientTools(ctx, &out, cs, "json") }); err != nil {
		t.Fatal(err)
	}
	var tools []map[string]any
	if err := json.Unmarshal(out.Bytes(), &tools); err != nil || len(tools) != 21 || tools[0]["inputSchema"] == nil {
		t.Errorf("tools json: %d tools, %v", len(tools), err)
	}

	out.Reset()
	args := map[string]any{"database": "kb", "collection": "notes", "query": map[string]any{
		"filters": []any{map[string]any{"field": "tags", "operator": "has", "value": "storage"}}, "fields": []any{"title"}}}
	if err := clientDo(t, url, func(cs *mcp.ClientSession) error { return callClientTool(ctx, &out, cs, "find_documents", args) }); err != nil {
		t.Fatal(err)
	}
	var res mcpserver.FindDocumentsOutput
	if err := json.Unmarshal(out.Bytes(), &res); err != nil || res.Count != 2 || res.Documents[0]["_id"] != "n2" || len(res.Documents[0]) != 2 {
		t.Errorf("call find_documents printed %s (%v)", out.String(), err)
	}

	err := clientDo(t, url, func(cs *mcp.ClientSession) error {
		return callClientTool(ctx, io.Discard, cs, "find_documents", map[string]any{"database": "kb", "collection": "nope"})
	})
	if !errors.Is(err, errToolFailed) || !strings.Contains(err.Error(), "not found") {
		t.Errorf("tool error = %v", err)
	}
	if err := withClient(ctx, "http://127.0.0.1:1/mcp", func(*mcp.ClientSession) error { return nil }); err == nil || !strings.Contains(err.Error(), "cachydb -t http") {
		t.Errorf("unreachable server: %v", err)
	}
}

func TestClientCallCommand(t *testing.T) {
	dir := t.TempDir()
	seedKB(t, dir)
	url, _ := liveServer(t, dir)
	for _, tt := range []struct {
		name    string
		args    []string
		stdin   string
		want    string
		wantErr string
	}{
		{"args", []string{"client", "call", "count_documents", `{"database":"kb","collection":"notes"}`, "--url", url}, "", `"count": 3`, ""},
		{"stdin", []string{"client", "call", "describe_collection", "-", "--url", url}, `{"database":"kb","collection":"notes"}`, `"name": "by_tags"`, ""},
		{"no args", []string{"client", "call", "list_databases", "--url", url}, "", `"kb"`, ""},
		{"not an object", []string{"client", "call", "list_databases", "[1]", "--url", url}, "", "", "JSON object"},
		{"tool error", []string{"client", "call", "drop_index", `{"database":"kb","collection":"notes","index_name":"x"}`, "--url", url}, "", "", "tool error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			rootCmd.SetOut(&out)
			rootCmd.SetErr(io.Discard)
			rootCmd.SetIn(strings.NewReader(tt.stdin))
			rootCmd.SetArgs(tt.args)
			t.Cleanup(func() {
				rootCmd.SetOut(nil)
				rootCmd.SetErr(nil)
				rootCmd.SetIn(nil)
				rootCmd.SetArgs(nil)
				clientURLFlag = ""
			})
			err := rootCmd.ExecuteContext(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tt.want) {
				t.Errorf("output %s does not contain %s", out.String(), tt.want)
			}
		})
	}
}

// TestClientExportImport: client export equals utils export of the same
// data, and client import into a server round-trips.
func TestClientExportImport(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	seedKB(t, dir)
	// Enough documents for several pages.
	e := openEngine(t, dir)
	d, _ := e.Database("kb")
	if err := d.CreateCollection(ctx, "bulk", nil); err != nil {
		t.Fatal(err)
	}
	c, _ := d.Collection("bulk")
	for i := range 2*clientPageSize + 7 {
		if _, err := c.Insert(ctx, &db.Document{Data: map[string]any{"i": i, "big": 12345678901.0}}); err != nil {
			t.Fatal(err)
		}
	}
	closeEngine(t, e)

	url, stop := liveServer(t, dir)
	var viaClient bytes.Buffer
	if err := clientDo(t, url, func(cs *mcp.ClientSession) error {
		return exportfmt.Export(ctx, &viaClient, clientStore{cs: cs}, exportfmt.Scope{}, "v", time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	stop()
	var offline bytes.Buffer
	if err := exportOffline(ctx, &offline, db.Options{Dir: dir}, exportfmt.Scope{}); err != nil {
		t.Fatal(err)
	}
	strip := func(s string) string { return strings.SplitN(s, "\n", 2)[1] } // header differs (version, created)
	if a, b := strip(viaClient.String()), strip(offline.String()); a != b {
		t.Fatalf("client export differs from utils export (%d vs %d bytes)", len(a), len(b))
	}

	dst := t.TempDir()
	url2, stop2 := liveServer(t, dst)
	var st exportfmt.Stats
	err := clientDo(t, url2, func(cs *mcp.ClientSession) error {
		var err error
		st, err = exportfmt.Import(ctx, strings.NewReader(offline.String()), clientStore{cs: cs}, exportfmt.Fail)
		return err
	})
	if err != nil || st.Inserted != 3+2*clientPageSize+7 {
		t.Fatalf("client import: %+v, %v", st, err)
	}
	err = clientDo(t, url2, func(cs *mcp.ClientSession) error {
		_, err := exportfmt.Import(ctx, strings.NewReader(offline.String()), clientStore{cs: cs}, exportfmt.Fail)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("second client import with fail: %v", err)
	}
	err = clientDo(t, url2, func(cs *mcp.ClientSession) error {
		var err error
		st, err = exportfmt.Import(ctx, strings.NewReader(offline.String()), clientStore{cs: cs}, exportfmt.Skip)
		return err
	})
	if err != nil || st.Skipped != st.Lines-5 {
		t.Errorf("client import with skip: %+v, %v", st, err)
	}
	stop2()
	var again bytes.Buffer
	if err := exportOffline(ctx, &again, db.Options{Dir: dst}, exportfmt.Scope{}); err != nil {
		t.Fatal(err)
	}
	if strip(again.String()) != strip(offline.String()) {
		t.Error("client import did not reproduce the data")
	}
}
