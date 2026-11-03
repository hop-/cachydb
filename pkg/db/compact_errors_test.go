package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// compactableColl returns a collection with several sealed segments, most
// of whose documents are deleted, so a compaction is due, and the
// documents that remain.
func compactableColl(t *testing.T) (*Engine, *Collection, map[string]map[string]any) {
	t.Helper()
	ctx := context.Background()
	e, c, _ := newFlushColl(t, func(o *Options) {
		o.DataSegmentSize = 8 << 10
		o.CompactionRatio = 0.3
	})
	if err := c.CreateIndex(ctx, "by_k", "k"); err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]any{}
	for i := range 120 {
		id := fmt.Sprintf("doc-%03d", i)
		data := map[string]any{"k": float64(i % 5), "pad": fmt.Sprintf("%0100d", i)}
		mustInsert(t, c, id, data)
		want[id] = data
		if i%20 == 19 {
			mustFlush(t, c)
		}
	}
	for i := range 120 {
		if i%4 != 0 {
			id := fmt.Sprintf("doc-%03d", i)
			if err := c.Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
			delete(want, id)
		}
	}
	mustFlushSeal(t, c)
	// One more flushed segment moves the newest checkpoint marker out of
	// the segments with the garbage, so they are compaction candidates.
	want["tail"] = map[string]any{"k": 9.0}
	mustInsert(t, c, "tail", want["tail"])
	mustFlushSeal(t, c)
	return e, c, want
}

// recordSegs lists the data segments of dir that hold records. A
// compaction removes an empty active segment even when it fails later.
func recordSegs(t *testing.T, dir string) []uint32 {
	t.Helper()
	var out []uint32
	for _, seg := range listSegs(t, dir) {
		fi, err := os.Stat(segment.DataPath(dir, seg))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() > 16 { // more than the segment header
			out = append(out, seg)
		}
	}
	return out
}

// nextSegOf returns the id the next compaction output will get.
func nextSegOf(c *Collection) uint32 {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	return c.nextSeg
}

// firstLivePut returns the location of a live document in the oldest
// segment, which the compaction reads.
func firstLivePut(t *testing.T, c *Collection) segment.Location {
	t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()
	var best segment.Location
	found := false
	for _, ke := range c.keydir {
		if !found || ke.loc.Seg < best.Seg || (ke.loc.Seg == best.Seg && ke.loc.Pos < best.Pos) {
			best, found = ke.loc, true
		}
	}
	if !found {
		t.Fatal("no live document")
	}
	return best
}

// TestCompactionErrorPaths makes a compaction fail at different steps and
// checks that a failed compaction changes nothing (same segments, same
// data, no leftover output), that compactAll reports the error, that the
// next pass succeeds once the cause is gone, and that the result
// survives a reopen.
func TestCompactionErrorPaths(t *testing.T) {
	tests := []struct {
		name string
		// inject makes the next compaction fail and returns a function
		// that removes the cause.
		inject func(t *testing.T, c *Collection) (undo func())
		ctx    func() context.Context
		wantIs error
	}{
		{
			name: "output segment cannot be created",
			inject: func(t *testing.T, c *Collection) func() {
				path := segment.DataPath(c.dir, nextSegOf(c))
				if err := os.WriteFile(path, []byte("in the way"), 0o644); err != nil {
					t.Fatal(err)
				}
				return func() {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
		{
			name: "hint file cannot be written",
			inject: func(t *testing.T, c *Collection) func() {
				path := segment.HintPath(c.dir, nextSegOf(c))
				if err := os.Mkdir(path, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "keep"), nil, 0o644); err != nil {
					t.Fatal(err)
				}
				return func() {
					if err := os.RemoveAll(path); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
		{
			name: "input record is corrupt",
			inject: func(t *testing.T, c *Collection) func() {
				loc := firstLivePut(t, c)
				path := segment.DataPath(c.dir, loc.Seg)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				orig := slices.Clone(data)
				data[loc.Pos+uint64(loc.Size)-1] ^= 0xff
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
				return func() {
					if err := os.WriteFile(path, orig, 0o644); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantIs: segment.ErrCorrupt,
		},
		{
			name:   "context cancelled",
			inject: func(*testing.T, *Collection) func() { return func() {} },
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantIs: context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, c, want := compactableColl(t)
			idx := map[string]string{"by_k": "k"}
			before := recordSegs(t, c.dir)

			undo := tt.inject(t, c)
			ctx := context.Background()
			if tt.ctx != nil {
				ctx = tt.ctx()
			}
			err := e.compactAll(ctx)
			if err == nil {
				t.Fatal("compactAll succeeded despite the injected failure")
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Fatalf("compactAll error = %v, want %v", err, tt.wantIs)
			}
			undo()

			if got := recordSegs(t, c.dir); !slices.Equal(got, before) {
				t.Fatalf("segments after a failed compaction = %v, want %v", got, before)
			}
			checkModel(t, e, docsModel(idx, want))
			checkKeydirLive(t, c)

			if err := e.compactAll(context.Background()); err != nil {
				t.Fatalf("compactAll retry: %v", err)
			}
			if got := recordSegs(t, c.dir); slices.Equal(got, before) {
				t.Fatalf("the retried compaction changed nothing: segments %v", got)
			}
			checkModel(t, e, docsModel(idx, want))
			checkKeydirLive(t, c)

			e, _ = reopenColl(t, e)
			checkModel(t, e, docsModel(idx, want))
		})
	}
}

// TestMapWALErr checks that WAL errors are wrapped so that both the
// engine's and the wal package's sentinels match, and that other errors
// pass through unchanged.
func TestMapWALErr(t *testing.T) {
	other := errors.New("disk on fire")
	tests := []struct {
		name    string
		in      error
		want    []error // must all match
		notWant []error // must not match
	}{
		{name: "nil", in: nil},
		{name: "corrupt", in: wal.ErrCorrupt, want: []error{ErrWALCorrupt, wal.ErrCorrupt}, notWant: []error{ErrWALFailed, ErrClosed}},
		{name: "failed", in: wal.ErrFailed, want: []error{ErrWALFailed, wal.ErrFailed}, notWant: []error{ErrWALCorrupt, ErrClosed}},
		{name: "closed", in: wal.ErrClosed, want: []error{ErrClosed, wal.ErrClosed}, notWant: []error{ErrWALCorrupt, ErrWALFailed}},
		{
			name:    "wrapped failed",
			in:      fmt.Errorf("%w: syncing wal-1.log: %w", wal.ErrFailed, other),
			want:    []error{ErrWALFailed, wal.ErrFailed, other},
			notWant: []error{ErrWALCorrupt, ErrClosed},
		},
		{
			name:    "wrapped corrupt",
			in:      fmt.Errorf("opening: %w", fmt.Errorf("%w: bad crc", wal.ErrCorrupt)),
			want:    []error{ErrWALCorrupt, wal.ErrCorrupt},
			notWant: []error{ErrWALFailed, ErrClosed},
		},
		{name: "other", in: other, want: []error{other}, notWant: []error{ErrWALCorrupt, ErrWALFailed, ErrClosed}},
		{name: "context", in: context.Canceled, want: []error{context.Canceled}, notWant: []error{ErrClosed}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapWALErr(tt.in)
			if tt.in == nil {
				if got != nil {
					t.Fatalf("mapWALErr(nil) = %v", got)
				}
				return
			}
			for _, w := range tt.want {
				if !errors.Is(got, w) {
					t.Errorf("mapWALErr(%v) = %v, does not match %v", tt.in, got, w)
				}
			}
			for _, w := range tt.notWant {
				if errors.Is(got, w) {
					t.Errorf("mapWALErr(%v) = %v, matches %v", tt.in, got, w)
				}
			}
			if len(tt.want) == 1 && got != tt.in {
				t.Errorf("mapWALErr(%v) = %v, want the error unchanged", tt.in, got)
			}
		})
	}
}
