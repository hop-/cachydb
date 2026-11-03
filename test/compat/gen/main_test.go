package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/exportfmt"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/hop-/cachydb/test/compat"
)

// TestGenerateAtHead runs the generator against the current engine (the
// next release generates its fixture with it) and checks that the data
// directory holds exactly the content of the expected.jsonl it writes.
func TestGenerateAtHead(t *testing.T) {
	ctx := context.Background()
	out := t.TempDir()
	dir := filepath.Join(out, "data")
	g, err := generate(ctx, dir, "head")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if err := g.engine.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	expectedPath := filepath.Join(out, "expected.jsonl")
	if err := g.writeExpected(expectedPath); err != nil {
		t.Fatalf("writeExpected: %v", err)
	}
	want, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatal(err)
	}

	e, err := db.Open(ctx, db.Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := e.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	var got bytes.Buffer
	if err := exportfmt.Export(ctx, &got, exportfmt.Engine{E: e}, exportfmt.Scope{}, "head", time.Now()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := compat.CompareExport(want, got.Bytes()); err != nil {
		t.Fatal(err)
	}
}
