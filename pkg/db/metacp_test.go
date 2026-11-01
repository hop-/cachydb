package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func replayRead(e *Engine) int64 {
	return e.recov.replayApplied.Load() + e.recov.replaySkipped.Load()
}

func readMetacpFile(t *testing.T, dir string) uint64 {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, metaCheckpointFileName))
	if err != nil {
		t.Fatal(err)
	}
	var f metaCheckpointFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f.Offset
}

// buildMetacpDir creates an old database first (it pins the fallback
// replay start at offset 2), then a collection with 50 flushed documents,
// and closes the engine cleanly. It returns the expected state.
func buildMetacpDir(t *testing.T, opts Options) model {
	t.Helper()
	ctx := context.Background()
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	mustCreateDB(t, e, "old")
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "c")
	want := model{"old": {}, "app": {"c": {indexes: map[string]string{}, docs: map[string]map[string]any{}}}}
	for i := range 50 {
		id := fmt.Sprintf("d%02d", i)
		mustInsert(t, c, id, map[string]any{"i": float64(i)})
		want["app"]["c"].docs[id] = map[string]any{"i": float64(i)}
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return want
}

func TestMetaCheckpointBoundsReplay(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(t *testing.T, dir string)
		wantRead func(n int64) bool
		wantWarn bool
	}{
		{name: "checkpoint present", wantRead: func(n int64) bool { return n == 0 }},
		{
			name:     "checkpoint missing",
			mutate:   func(t *testing.T, dir string) { mustRemove(t, filepath.Join(dir, metaCheckpointFileName)) },
			wantRead: func(n int64) bool { return n >= 50 },
		},
		{
			name:     "checkpoint corrupt",
			mutate:   func(t *testing.T, dir string) { mustWrite(t, filepath.Join(dir, metaCheckpointFileName), "{not json") },
			wantRead: func(n int64) bool { return n >= 50 },
			wantWarn: true,
		},
		{
			name: "checkpoint past the end of the WAL",
			mutate: func(t *testing.T, dir string) {
				mustWrite(t, filepath.Join(dir, metaCheckpointFileName), `{"offset": 1000000}`)
			},
			wantRead: func(n int64) bool { return n >= 50 },
			wantWarn: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := crashOpts(dir)
			want := buildMetacpDir(t, opts)
			if tt.mutate != nil {
				tt.mutate(t, dir)
			}
			logs := &syncBuffer{}
			opts.Logger = slog.New(slog.NewTextHandler(logs, nil))
			e := openEngine(t, opts)
			if n := replayRead(e); !tt.wantRead(n) {
				t.Errorf("replay read %d records", n)
			}
			if got := strings.Contains(logs.String(), "ignoring the metadata checkpoint"); got != tt.wantWarn {
				t.Errorf("warning logged = %v, want %v:\n%s", got, tt.wantWarn, logs.String())
			}
			checkModel(t, e, want)
			// Open rewrote a valid checkpoint: the next open reads nothing.
			e = reopen(t, e)
			if n := replayRead(e); n != 0 {
				t.Errorf("replay after reopen read %d records, want 0", n)
			}
			checkModel(t, e, want)
		})
	}
}

// TestMetaCheckpointNeverCoversUnappliedRecord: a metadata operation that
// crashed between its WAL append and its meta-file change is not covered
// by the checkpoint, so replay still applies it.
func TestMetaCheckpointNeverCoversUnappliedRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := crashOpts(dir)
	e := openEngine(t, opts)
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "a", nil); err != nil {
		t.Fatal(err)
	}
	e.abandon()
	before := readMetacpFile(t, dir)

	armed := opts
	crashAt(t, &armed, hookMetaWrite)
	e = openEngine(t, armed)
	next := e.wal.NextOffset() // the offset create_coll "b" gets
	if err := mustDB(t, e, "app").CreateCollection(ctx, "b", nil); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("CreateCollection = %v, want a simulated crash", err)
	}
	e.abandon()
	if got := readMetacpFile(t, dir); got >= next {
		t.Fatalf("checkpoint %d covers the unapplied create_coll at %d (was %d)", got, next, before)
	}

	e = openEngine(t, opts)
	if _, err := mustDB(t, e, "app").Collection("b"); err != nil {
		t.Fatalf("collection b not recovered: %v", err)
	}
	if got := readMetacpFile(t, dir); got < next {
		t.Errorf("checkpoint after recovery = %d, want >= %d", got, next)
	}
}

func TestMetaCheckpointIgnoredByFormatChecks(t *testing.T) {
	dir := t.TempDir()
	buildMetacpDir(t, crashOpts(dir))
	if _, err := os.Stat(filepath.Join(dir, metaCheckpointFileName)); err != nil {
		t.Fatalf("no %s after Close: %v", metaCheckpointFileName, err)
	}
	if old, err := hasOldLayout(dir); err != nil || old {
		t.Fatalf("hasOldLayout = %v, %v; want false", old, err)
	}
	if err := checkDataDir(dir); err != nil {
		t.Fatalf("checkDataDir: %v", err)
	}
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}
