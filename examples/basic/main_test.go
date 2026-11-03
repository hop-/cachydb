package main

import (
	"context"
	"testing"
)

// TestRun runs the whole example against a temporary data directory, so
// `go test ./...` catches an example that no longer works with the
// current pkg/db API. run returns an error when a result isn't what the
// example expects.
func TestRun(t *testing.T) {
	if err := run(context.Background(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
