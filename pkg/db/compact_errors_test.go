package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// compactableColl returns a collection with several sealed segments, most
// of whose documents are deleted, so a compaction is due, and the
// documents that remain.
// mod, if not nil, changes the options passed to Open.
func compactableColl(t *testing.T, mod func(*Options)) (*Engine, *Collection, map[string]map[string]any) {
	t.Helper()
	ctx := context.Background()
	e, c, _ := newFlushColl(t, func(o *Options) {
		o.DataSegmentSize = 8 << 10
		o.CompactionRatio = 0.3
		if mod != nil {
			mod(o)
		}
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
	// One more flushed segment: compaction never takes the active
	// segment, sealed or not, so the segments with the garbage must not
	// be the newest one to be compaction candidates.
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
	var best segment.Location
	found := false
	for _, ke := range keyTable(t, c) {
		if loc := entryLocation(ke); !found || loc.Seg < best.Seg || (loc.Seg == best.Seg && loc.Pos < best.Pos) {
			best, found = loc, true
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
//
// The index transaction of the swap can fail too (task 02 Amendment 2):
// before its commit it rolls back, and the compaction removes its output
// like any other failure. A commit that fails late, after bbolt wrote the
// transaction, may have left the key table pointing into the output, so
// the compaction keeps the inputs and the output, and the next open
// removes the side the index file does not register (the inputs).
func TestCompactionErrorPaths(t *testing.T) {
	tests := []struct {
		name string
		// inject makes the next compaction fail and returns a function
		// that removes the cause.
		inject func(t *testing.T, c *Collection, f *commitFault) (undo func())
		ctx    func() context.Context
		wantIs error
		// late: the swap transaction committed before the failure; the
		// files are kept until the next open instead of being unchanged.
		late bool
	}{
		{
			name: "output segment cannot be created",
			inject: func(t *testing.T, c *Collection, _ *commitFault) func() {
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
			name: "input record is corrupt",
			inject: func(t *testing.T, c *Collection, _ *commitFault) func() {
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
			inject: func(*testing.T, *Collection, *commitFault) func() { return func() {} },
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantIs: context.Canceled,
		},
		{
			name: "index commit fails before the commit",
			inject: func(_ *testing.T, _ *Collection, f *commitFault) func() {
				f.arm(indexCommitCompact, false)
				return func() {}
			},
			wantIs: errCommitInjected,
		},
		{
			name: "index commit fails late",
			inject: func(_ *testing.T, _ *Collection, f *commitFault) func() {
				f.arm(indexCommitCompact, true)
				return func() {}
			},
			wantIs: errCommitInjected,
			late:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fault := &commitFault{}
			e, c, want := compactableColl(t, func(o *Options) { o.testIndexCommit = fault.hook })
			idx := map[string]string{"by_k": "k"}
			before := recordSegs(t, c.dir)
			out := nextSegOf(c) // the output id of the next compaction

			undo := tt.inject(t, c, fault)
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
			if tt.wantIs == errCommitInjected && fault.firedCount() != 1 {
				t.Fatalf("the index commit fault fired %d times, want 1", fault.firedCount())
			}
			undo()

			var inputs []uint32 // late: the inputs the committed swap unregistered
			if tt.late {
				if !strings.Contains(err.Error(), "kept until the next open") {
					t.Errorf("compactAll error = %v, want it to say the files are kept until the next open", err)
				}
				// Nothing was removed: the inputs and the output are on disk,
				// and the index file registers the output, not the inputs.
				if got, wantSegs := recordSegs(t, c.dir), append(slices.Clone(before), out); !slices.Equal(got, wantSegs) {
					t.Fatalf("segments after a late commit failure = %v, want %v (inputs and output kept)", got, wantSegs)
				}
				totals, _ := segAccounting(t, c)
				if _, ok := totals[out]; !ok {
					t.Fatalf("the index file does not register the output segment %d after the committed swap: %v", out, totals)
				}
				for _, s := range before {
					if _, ok := totals[s]; !ok {
						inputs = append(inputs, s)
					}
				}
				if len(inputs) == 0 {
					t.Fatalf("the committed swap unregistered no input segment: registered %v, on disk %v", totals, before)
				}
			} else {
				if got := recordSegs(t, c.dir); !slices.Equal(got, before) {
					t.Fatalf("segments after a failed compaction = %v, want %v", got, before)
				}
				if _, err := os.Stat(segment.DataPath(c.dir, out)); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("the output segment %d of the failed compaction is still on disk (%v)", out, err)
				}
			}
			checkModel(t, e, docsModel(idx, want))
			checkKeydirLive(t, c)

			if err := e.compactAll(context.Background()); err != nil {
				t.Fatalf("compactAll retry: %v", err)
			}
			if got := recordSegs(t, c.dir); !tt.late && slices.Equal(got, before) {
				t.Fatalf("the retried compaction changed nothing: segments %v", got)
			}
			checkModel(t, e, docsModel(idx, want))
			checkKeydirLive(t, c)

			e, c = reopenColl(t, e)
			checkModel(t, e, docsModel(idx, want))
			checkKeydirLive(t, c)
			if tt.late {
				// The open removed the unregistered inputs and kept the
				// output the key table points into.
				onDisk := listSegs(t, c.dir)
				for _, s := range inputs {
					if slices.Contains(onDisk, s) {
						t.Errorf("input segment %d, which the index file does not register, is still on disk after reopen: %v", s, onDisk)
					}
				}
				if !slices.Contains(onDisk, out) {
					t.Errorf("the registered output segment %d is gone after reopen: %v", out, onDisk)
				}
			}
			totals, _ := segAccounting(t, c)
			if got := listSegs(t, c.dir); !slices.Equal(got, slices.Sorted(maps.Keys(totals))) {
				t.Errorf("segments on disk after reopen = %v, the index file registers %v", got, slices.Sorted(maps.Keys(totals)))
			}
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
