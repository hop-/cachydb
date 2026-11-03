package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestRun builds the cachydb binary from this module into a temporary
// directory and runs the whole example against it over stdio, so
// `go test ./...` catches an example that no longer matches the server's
// tools. run returns an error when a step fails, or when a step that is
// expected to fail succeeds.
func TestRun(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the cachydb binary; skipped with -short")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not on PATH:", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	bin := filepath.Join(t.TempDir(), "cachydb")
	build := exec.CommandContext(ctx, goBin, "build", "-o", bin, "github.com/hop-/cachydb")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("building cachydb: %v", err)
	}

	if err := run(ctx, bin, t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
