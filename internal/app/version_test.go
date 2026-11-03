package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWithVersionReportedByHealthz checks that the version set with
// WithVersion is the one the server reports at /healthz.
func TestWithVersionReportedByHealthz(t *testing.T) {
	ctx := context.Background()
	a, err := NewBuilder().WithRootDir(t.TempDir()).WithDBName("main").WithTransport("http").WithPort(0).
		WithVersion("v9.8.7-test").Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() {
		if err := a.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	ts := httptest.NewServer(a.mcpServer.HTTPHandler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || body.Status != "ok" || body.Version != "v9.8.7-test" {
		t.Fatalf("GET /healthz = %d %+v, want ok and v9.8.7-test", resp.StatusCode, body)
	}
}
