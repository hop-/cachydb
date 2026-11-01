package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

var errInjected = errors.New("injected I/O error")

// segHooks records and steers the I/O of the active segment writers the
// flusher opens (via Options.testSegWriter).
type segHooks struct {
	mu          sync.Mutex
	appends     [][]segment.Record // records of every successful Append call
	syncs       int                // Sync calls
	failAppend  int                // upcoming Append calls that fail
	failSync    int                // upcoming Sync calls that fail (after the data was written)
	blockSync   bool               // the next Sync blocks until release is closed
	syncEntered chan struct{}      // receives when a blocked Sync is entered
	release     chan struct{}
}

func (h *segHooks) blockNextSync() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.blockSync = true
	h.syncEntered = make(chan struct{}, 1)
	h.release = make(chan struct{})
}

func (h *segHooks) counts() (appends, syncs int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.appends), h.syncs
}

type hookedSegWriter struct {
	segmentWriter
	h *segHooks
}

func (w *hookedSegWriter) Append(recs []segment.Record) ([]segment.Location, error) {
	w.h.mu.Lock()
	if w.h.failAppend > 0 {
		w.h.failAppend--
		w.h.mu.Unlock()
		return nil, errInjected
	}
	w.h.appends = append(w.h.appends, slices.Clone(recs))
	w.h.mu.Unlock()
	return w.segmentWriter.Append(recs)
}

func (w *hookedSegWriter) Sync() error {
	w.h.mu.Lock()
	w.h.syncs++
	fail := w.h.failSync > 0
	if fail {
		w.h.failSync--
	}
	var entered, release chan struct{}
	if w.h.blockSync {
		w.h.blockSync = false
		entered, release = w.h.syncEntered, w.h.release
	}
	w.h.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
		<-release
	}
	if fail {
		return errInjected
	}
	return w.segmentWriter.Sync()
}

// newFlushColl is newTestColl with the flusher's timer effectively off
// (flushes are triggered by the test) and the segment writer hooked.
func newFlushColl(t *testing.T, mod func(*Options)) (*Engine, *Collection, *segHooks) {
	t.Helper()
	h := &segHooks{}
	e, c := newTestColl(t, func(o *Options) {
		o.FlushInterval = time.Hour
		o.testSegWriter = func(w segmentWriter) segmentWriter { return &hookedSegWriter{segmentWriter: w, h: h} }
		if mod != nil {
			mod(o)
		}
	}, nil)
	return e, c, h
}

func mustFlush(t *testing.T, c *Collection) {
	t.Helper()
	if err := c.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func reopenColl(t *testing.T, e *Engine) (*Engine, *Collection) {
	t.Helper()
	e2 := reopen(t, e)
	return e2, mustColl(t, mustDB(t, e2, "app"), "c")
}

// memOffset returns the WAL offset of id's memtable entry.
func memOffset(t *testing.T, c *Collection, id string) uint64 {
	t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()
	me, ok := c.memtable[id]
	if !ok {
		t.Fatalf("%q not in memtable", id)
	}
	return me.walOffset
}

func inKeydir(c *Collection, id string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.keydir[id]
	return ok
}

func checkpointOf(c *Collection) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.checkpoint
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// segRecords returns every record of every data segment in dir, in order.
func segRecords(t *testing.T, dir string) []segment.Record {
	t.Helper()
	segs, err := segment.ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []segment.Record
	for _, seg := range segs {
		if _, err := segment.Scan(dir, seg, func(r segment.Record, _ segment.Location) error {
			out = append(out, r)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func markers(recs []segment.Record) []uint64 {
	var out []uint64
	for _, r := range recs {
		if r.Type == segment.TypeCheckpoint {
			out = append(out, r.WALOffset)
		}
	}
	return out
}

func TestCheckpointMath(t *testing.T) {
	tests := []struct {
		name    string
		next    uint64
		pending []uint64
		wantLW  uint64
		wantCP  uint64
	}{
		{"nothing pending", 10, nil, 10, 9},
		{"empty log", 1, nil, 1, 0},
		{"one pending", 10, []uint64{4}, 4, 3},
		{"smallest of several", 10, []uint64{7, 3, 5}, 3, 2},
		{"pending at next", 10, []uint64{10}, 10, 9},
		{"pending bound above next is ignored", 10, []uint64{12}, 10, 9},
		{"first record pending", 5, []uint64{1}, 1, 0},
		{"zero", 0, nil, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lw := lowWaterMark(tt.next, tt.pending...)
			if lw != tt.wantLW {
				t.Errorf("lowWaterMark(%d, %v) = %d, want %d", tt.next, tt.pending, lw, tt.wantLW)
			}
			if cp := checkpointBelow(lw); cp != tt.wantCP {
				t.Errorf("checkpointBelow(%d) = %d, want %d", lw, cp, tt.wantCP)
			}
		})
	}
}

func TestPendingMinAndLowWater(t *testing.T) {
	e, c, _ := newFlushColl(t, nil)
	next := e.wal.NextOffset()
	if lw := e.lowWater(); lw != next {
		t.Fatalf("lowWater with nothing pending = %d, want next %d", lw, next)
	}

	mustInsert(t, c, "a", map[string]any{"v": 1})
	a := memOffset(t, c, "a")
	mustInsert(t, c, "b", map[string]any{"v": 2})
	if _, err := c.Update(context.Background(), "a", map[string]any{"v": 3}); err != nil {
		t.Fatal(err)
	}
	// a's first version was overwritten, so the memtable's true minimum
	// is b; the tracked bound may stay at a (conservative).
	if lw := e.lowWater(); lw != a {
		t.Errorf("lowWater with memtable = %d, want %d", lw, a)
	}

	c.mu.Lock()
	c.inflight = []uint64{a - 1} // as registered by a write in progress
	m, ok := c.pendingMinLocked()
	c.mu.Unlock()
	if !ok || m != a-1 {
		t.Errorf("pendingMin with inflight = %d, %v; want %d", m, ok, a-1)
	}
	c.mu.Lock()
	c.inflight = nil
	c.mu.Unlock()

	mustFlush(t, c)
	if lw, next := e.lowWater(), e.wal.NextOffset(); lw != next {
		t.Errorf("lowWater after flush = %d, want next %d", lw, next)
	}

	t.Run("metadata in flight", func(t *testing.T) {
		var armed atomic.Bool
		var atHook, metaOff uint64
		var e2 *Engine
		o := testOptions(t.TempDir())
		o.FlushInterval = time.Hour
		o.testHook = func(p hookPoint) error {
			if p != hookMetaWrite || !armed.Load() {
				return nil
			}
			metaOff = e2.wal.NextOffset() - 1 // the record just committed
			atHook = e2.lowWater()
			return nil
		}
		e2 = openEngine(t, o)
		// CreateDatabase holds no Database.mu during its meta write, so
		// the hook may compute the low-water mark synchronously.
		armed.Store(true)
		if _, err := e2.CreateDatabase(context.Background(), "app"); err != nil {
			t.Fatal(err)
		}
		armed.Store(false)
		if atHook > metaOff {
			t.Errorf("lowWater during the meta write = %d, want <= %d (the record is not processed yet)", atHook, metaOff)
		}
		if lw, next := e2.lowWater(), e2.wal.NextOffset(); lw != next {
			t.Errorf("lowWater after the meta write = %d, want %d", lw, next)
		}
	})
}

func TestFlushTriggers(t *testing.T) {
	tests := []struct {
		name    string
		mod     func(*Options)
		trigger func(t *testing.T, e *Engine, c *Collection) *Collection
	}{
		{
			name: "interval",
			mod:  func(o *Options) { o.FlushInterval = 5 * time.Millisecond },
			trigger: func(t *testing.T, e *Engine, c *Collection) *Collection {
				waitFor(t, "interval flush", func() bool { return inKeydir(c, "a") })
				return c
			},
		},
		{
			name: "memtable size",
			mod:  func(o *Options) { o.FlushInterval = time.Hour; o.MemtableSize = 1 },
			trigger: func(t *testing.T, e *Engine, c *Collection) *Collection {
				waitFor(t, "size-triggered flush", func() bool { return inKeydir(c, "a") })
				return c
			},
		},
		{
			name: "close",
			mod:  func(o *Options) { o.FlushInterval = time.Hour },
			trigger: func(t *testing.T, e *Engine, c *Collection) *Collection {
				if inKeydir(c, "a") {
					t.Fatal("flushed before Close")
				}
				_, c2 := reopenColl(t, e)
				return c2
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, c := newTestColl(t, tt.mod, nil)
			mustInsert(t, c, "a", map[string]any{"v": 1})
			c = tt.trigger(t, e, c)
			recs := segRecords(t, c.dir)
			if len(recs) != 2 || recs[0].Type != segment.TypePut || recs[0].ID != "a" || recs[1].Type != segment.TypeCheckpoint {
				t.Fatalf("segment records = %+v, want put a + checkpoint", recs)
			}
			if got := mustGet(t, c, "a"); got.Data["v"] != 1.0 {
				t.Errorf("Get(a) = %+v", got)
			}
		})
	}
}

func TestFlushWritesOnlyChanges(t *testing.T) {
	_, c, h := newFlushColl(t, nil)
	for _, id := range []string{"a", "b", "c"} {
		mustInsert(t, c, id, map[string]any{"id": id})
	}
	mustFlush(t, c)
	if n, s := h.counts(); n != 1 || s != 1 {
		t.Fatalf("after first flush: %d appends, %d syncs; want 1, 1", n, s)
	}
	first := h.appends[0]
	if len(first) != 4 {
		t.Fatalf("first flush wrote %d records, want 3 puts + marker", len(first))
	}
	if !slices.IsSortedFunc(first[:3], func(a, b segment.Record) int { return int(a.WALOffset) - int(b.WALOffset) }) {
		t.Errorf("records not sorted by WAL offset: %+v", first)
	}

	if _, err := c.Update(context.Background(), "b", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, c)
	if n, s := h.counts(); n != 2 || s != 2 {
		t.Fatalf("after second flush: %d appends, %d syncs; want 2, 2", n, s)
	}
	second := h.appends[1]
	if len(second) != 2 || second[0].ID != "b" || second[0].Type != segment.TypePut || second[1].Type != segment.TypeCheckpoint {
		t.Errorf("second flush wrote %+v, want put b + marker", second)
	}
	if got, want := second[1].WALOffset, checkpointOf(c); got != want {
		t.Errorf("marker = %d, checkpoint = %d", got, want)
	}

	mustFlush(t, c) // nothing changed
	if n, s := h.counts(); n != 2 || s != 2 {
		t.Errorf("empty flush did I/O: %d appends, %d syncs", n, s)
	}
}

// TestFlushDoesNotBlockWrites checks that no collection lock is held
// during segment I/O: reads and writes complete while a flush is stuck in
// fsync.
func TestFlushDoesNotBlockWrites(t *testing.T) {
	ctx := context.Background()
	_, c, h := newFlushColl(t, nil)
	mustInsert(t, c, "a", map[string]any{"v": 1})
	h.blockNextSync()
	flushErr := make(chan error, 1)
	go func() { flushErr <- c.flush() }()
	<-h.syncEntered

	done := make(chan error, 1)
	go func() {
		done <- func() error {
			if _, err := c.Insert(ctx, &Document{ID: "b", Data: map[string]any{"v": 2}}); err != nil {
				return err
			}
			if doc, err := c.Get(ctx, "a"); err != nil || doc.Data["v"] != 1.0 {
				return fmt.Errorf("Get(a) from flushing = %+v, %v", doc, err)
			}
			if _, err := c.Update(ctx, "a", map[string]any{"v": 3}); err != nil {
				return err
			}
			docs, err := c.Find(ctx, nil)
			if err != nil {
				return err
			}
			if got := docIDs(docs); !slices.Equal(got, []string{"a", "b"}) {
				return fmt.Errorf("Find = %v", got)
			}
			if c.Count() != 2 {
				return fmt.Errorf("Count = %d", c.Count())
			}
			return nil
		}()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("document operations blocked by a flush in fsync")
	}
	close(h.release)
	if err := <-flushErr; err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, c, "a"); got.Data["v"] != 3.0 {
		t.Errorf("Get(a) after flush = %+v, want the newer memtable version", got)
	}
}

// TestFlushCheckpointExcludesConcurrentWrites (F10): the checkpoint never
// covers a write that is durable but not yet applied when the flush
// starts, nor one made while the flush runs.
func TestFlushCheckpointExcludesConcurrentWrites(t *testing.T) {
	var block atomic.Bool
	hookEntered, hookRelease := make(chan struct{}), make(chan struct{})
	e, c, h := newFlushColl(t, func(o *Options) {
		o.testHook = func(p hookPoint) error {
			if p == hookAfterWALSync && block.CompareAndSwap(true, false) {
				close(hookEntered)
				<-hookRelease
			}
			return nil
		}
	})
	mustInsert(t, c, "a", map[string]any{"v": 1})
	a := memOffset(t, c, "a")

	// x is durable in the WAL but not yet applied when the flush starts.
	block.Store(true)
	insErr := make(chan error, 1)
	go func() {
		_, err := c.Insert(context.Background(), &Document{ID: "x", Data: map[string]any{"v": 2}})
		insErr <- err
	}()
	<-hookEntered
	x := e.wal.NextOffset() - 1

	h.blockNextSync()
	flushErr := make(chan error, 1)
	go func() { flushErr <- c.flush() }()
	<-h.syncEntered
	mustInsert(t, c, "b", map[string]any{"v": 3}) // during the flush
	b := memOffset(t, c, "b")
	close(hookRelease)
	if err := <-insErr; err != nil {
		t.Fatal(err)
	}
	close(h.release)
	if err := <-flushErr; err != nil {
		t.Fatal(err)
	}

	cp := checkpointOf(c)
	if cp < a || cp >= x || cp >= b {
		t.Fatalf("checkpoint = %d; want a=%d <= cp < x=%d and < b=%d", cp, a, x, b)
	}
	if m := markers(segRecords(t, c.dir)); len(m) != 1 || m[0] != cp {
		t.Errorf("markers on disk = %v, want [%d]", m, cp)
	}
	if !inKeydir(c, "a") || inKeydir(c, "x") || inKeydir(c, "b") {
		t.Error("keydir after the flush should hold a only")
	}
	if lw := e.lowWater(); lw > x {
		t.Errorf("lowWater = %d, want <= %d", lw, x)
	}

	mustFlush(t, c)
	if cp := checkpointOf(c); cp < x || cp < b {
		t.Errorf("second checkpoint = %d, want >= %d and %d", cp, x, b)
	}
	_, c = reopenColl(t, e)
	for _, id := range []string{"a", "x", "b"} {
		mustGet(t, c, id)
	}
}

// TestFlushFailureKeepsCheckpoint (F8): a failed flush keeps flushing
// readable and the old checkpoint, the retry writes everything, and
// nothing is lost after reopen.
func TestFlushFailureKeepsCheckpoint(t *testing.T) {
	for _, mode := range []string{"append", "sync"} {
		t.Run(mode, func(t *testing.T) {
			e, c, h := newFlushColl(t, nil)
			mustInsert(t, c, "a", map[string]any{"v": 1})
			mustFlush(t, c)
			cp0 := checkpointOf(c)

			mustInsert(t, c, "b", map[string]any{"v": 2})
			b := memOffset(t, c, "b")
			if err := c.Delete(context.Background(), "a"); err != nil {
				t.Fatal(err)
			}
			h.mu.Lock()
			if mode == "append" {
				h.failAppend = 1
			} else {
				h.failSync = 1
			}
			h.mu.Unlock()
			if err := c.flush(); !errors.Is(err, errInjected) {
				t.Fatalf("flush = %v, want the injected error", err)
			}

			c.mu.RLock()
			nFlushing, cp := len(c.flushing), c.checkpoint
			c.mu.RUnlock()
			if nFlushing != 2 {
				t.Errorf("flushing holds %d entries after failure, want 2", nFlushing)
			}
			if cp != cp0 {
				t.Errorf("checkpoint moved from %d to %d on a failed flush", cp0, cp)
			}
			if got := mustGet(t, c, "b"); got.Data["v"] != 2.0 {
				t.Errorf("Get(b) from flushing = %+v", got)
			}
			if _, err := c.Get(context.Background(), "a"); !errors.Is(err, ErrNotFound) {
				t.Errorf("Get(a) after delete = %v, want ErrNotFound", err)
			}
			if lw := e.lowWater(); lw > b {
				t.Errorf("lowWater = %d after failed flush, want <= %d", lw, b)
			}
			if m := markers(segRecords(t, c.dir)); len(m) != 1 {
				t.Errorf("markers on disk after failure = %v, want only the first flush's", m)
			}

			mustInsert(t, c, "c", map[string]any{"v": 3})
			cOff := memOffset(t, c, "c")
			mustFlush(t, c) // retry: flushing ∪ memtable
			if cp := checkpointOf(c); cp < cOff {
				t.Errorf("checkpoint after retry = %d, want >= %d", cp, cOff)
			}
			c.mu.RLock()
			idle := c.flushing == nil && len(c.memtable) == 0
			c.mu.RUnlock()
			if !idle {
				t.Error("flushing or memtable not empty after a successful retry")
			}
			if m := markers(segRecords(t, c.dir)); len(m) != 2 {
				t.Errorf("markers on disk = %v, want 2", m)
			}

			_, c = reopenColl(t, e)
			if _, err := c.Get(context.Background(), "a"); !errors.Is(err, ErrNotFound) {
				t.Errorf("after reopen Get(a) = %v, want ErrNotFound", err)
			}
			mustGet(t, c, "b")
			mustGet(t, c, "c")
			if c.Count() != 2 {
				t.Errorf("Count after reopen = %d, want 2", c.Count())
			}
		})
	}
}

func TestFlushFailureRetriedOnTick(t *testing.T) {
	h := &segHooks{failAppend: 2}
	_, c := newTestColl(t, func(o *Options) {
		o.FlushInterval = 2 * time.Millisecond
		o.testSegWriter = func(w segmentWriter) segmentWriter { return &hookedSegWriter{segmentWriter: w, h: h} }
	}, nil)
	mustInsert(t, c, "a", map[string]any{"v": 1})
	waitFor(t, "flush after injected failures", func() bool { return inKeydir(c, "a") })
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failAppend != 0 {
		t.Errorf("%d injected failures left, want 0", h.failAppend)
	}
}

func TestSegmentRoll(t *testing.T) {
	ctx := context.Background()
	var (
		mu     sync.Mutex
		points []hookPoint
		c      *Collection
	)
	e, c, _ := newFlushColl(t, func(o *Options) {
		o.DataSegmentSize = 1 // every flush seals its segment
		o.testHook = func(p hookPoint) error {
			if p != hookAfterSegWrite && p != hookAfterSegSync {
				return nil
			}
			mu.Lock()
			points = append(points, p)
			mu.Unlock()
			// The flushed entries are still in flushing and the keydir
			// does not point at them yet.
			c.mu.RLock()
			defer c.mu.RUnlock()
			if len(c.flushing) == 0 {
				t.Errorf("at %s: flushing is empty", p)
			}
			for id, me := range c.flushing {
				if ke, ok := c.keydir[id]; ok && ke.walOffset >= me.walOffset {
					t.Errorf("at %s: keydir already repointed for %q", p, id)
				}
			}
			return nil
		}
	})
	if err := c.CreateIndex(ctx, "byv", "v"); err != nil {
		t.Fatal(err)
	}

	mustInsert(t, c, "d", map[string]any{"v": "one"})
	mustFlush(t, c)
	mu.Lock()
	gotPoints := slices.Clone(points)
	mu.Unlock()
	if want := []hookPoint{hookAfterSegWrite, hookAfterSegSync}; !slices.Equal(gotPoints, want) {
		t.Fatalf("hook points = %v, want %v", gotPoints, want)
	}

	// Segment 1 is sealed: hint and snapshot written, segment 2 open.
	hint, err := segment.ReadHint(c.dir, 1)
	if err != nil {
		t.Fatalf("ReadHint(1): %v", err)
	}
	if len(hint) != 2 || hint[0].ID != "d" || hint[0].Type != segment.TypePut || hint[1].Type != segment.TypeCheckpoint {
		t.Errorf("hint of segment 1 = %+v", hint)
	}
	fi, err := os.Stat(segment.DataPath(c.dir, 1))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := segment.ReadIndexSnapshot(c.dir, "byv")
	if err != nil {
		t.Fatalf("ReadIndexSnapshot: %v", err)
	}
	if snap.Field != "v" || snap.CoversSeg != 1 || snap.CoversPos != uint64(fi.Size()) || snap.CoversWAL != checkpointOf(c) {
		t.Errorf("snapshot header = %+v (segment size %d, checkpoint %d)", snap, fi.Size(), checkpointOf(c))
	}
	if ids := snap.Keys["s:one"]; !slices.Equal(ids, []string{"d"}) {
		t.Errorf("snapshot keys = %v", snap.Keys)
	}
	if _, err := os.Stat(segment.DataPath(c.dir, 2)); err != nil {
		t.Errorf("next segment not created: %v", err)
	}

	if _, err := c.Update(ctx, "d", map[string]any{"v": "two"}); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, c)
	if _, err := segment.ReadHint(c.dir, 2); err != nil {
		t.Errorf("ReadHint(2): %v", err)
	}
	if snap, err := segment.ReadIndexSnapshot(c.dir, "byv"); err != nil || snap.CoversSeg != 2 || len(snap.Keys["s:two"]) != 1 || len(snap.Keys["s:one"]) != 0 {
		t.Errorf("snapshot after second roll = %+v, %v", snap, err)
	}
	c.mu.RLock()
	dead1 := c.deadBytes[1]
	c.mu.RUnlock()
	if dead1 == 0 {
		t.Error("superseded version in segment 1 not counted as dead bytes")
	}

	e, c = reopenColl(t, e)
	if got := mustGet(t, c, "d"); got.Data["v"] != "two" {
		t.Errorf("Get(d) after reopen = %+v", got)
	}
	docs, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "v", Operator: "eq", Value: "two"}}})
	if err != nil || !slices.Equal(docIDs(docs), []string{"d"}) {
		t.Errorf("indexed Find after reopen = %v, %v", docIDs(docs), err)
	}
	mustInsert(t, c, "e", map[string]any{"v": "three"})
	mustFlush(t, c)
	_, c = reopenColl(t, e)
	if c.Count() != 2 {
		t.Errorf("Count after second reopen = %d, want 2", c.Count())
	}
}

// TestRegressionF9DeleteFlushReopen: a deleted document stays deleted
// after flush and reopen.
func TestRegressionF9DeleteFlushReopen(t *testing.T) {
	for _, flushBetween := range []bool{true, false} {
		t.Run(fmt.Sprintf("flush-between=%v", flushBetween), func(t *testing.T) {
			ctx := context.Background()
			e, c, _ := newFlushColl(t, nil)
			mustInsert(t, c, "x", map[string]any{"v": 1})
			mustInsert(t, c, "keep", map[string]any{"v": 2})
			if flushBetween {
				mustFlush(t, c)
			}
			if err := c.Delete(ctx, "x"); err != nil {
				t.Fatal(err)
			}
			mustFlush(t, c)
			_, c = reopenColl(t, e)
			if _, err := c.Get(ctx, "x"); !errors.Is(err, ErrNotFound) {
				t.Errorf("Get(x) after reopen = %v, want ErrNotFound", err)
			}
			docs, err := c.Find(ctx, nil)
			if err != nil || !slices.Equal(docIDs(docs), []string{"keep"}) {
				t.Errorf("Find after reopen = %v, %v", docIDs(docs), err)
			}
			if c.Count() != 1 {
				t.Errorf("Count = %d, want 1", c.Count())
			}
		})
	}
}

// TestRegressionF1TwoFlushesReopen: several flushes followed by reopens
// keep every acknowledged change.
func TestRegressionF1TwoFlushesReopen(t *testing.T) {
	ctx := context.Background()
	e, c, _ := newFlushColl(t, nil)
	for i := range 3 {
		mustInsert(t, c, fmt.Sprintf("d%d", i), map[string]any{"v": i})
	}
	mustFlush(t, c)
	if _, err := c.Update(ctx, "d1", map[string]any{"v": 10}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "d2"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "d3", map[string]any{"v": 3})
	mustFlush(t, c)

	want := map[string]float64{"d0": 0, "d1": 10, "d3": 3}
	check := func(c *Collection) {
		t.Helper()
		docs, err := c.Find(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for _, d := range docs {
			got[d.ID] = d.Data["v"].(float64)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("documents = %v, want %v", got, want)
		}
		if c.Count() != len(want) {
			t.Errorf("Count = %d, want %d", c.Count(), len(want))
		}
	}
	e, c = reopenColl(t, e)
	check(c)

	// More writes, flushed only by Close.
	if _, err := c.Update(ctx, "d0", map[string]any{"v": 100}); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "d4", map[string]any{"v": 4})
	want["d0"], want["d4"] = 100, 4
	_, c = reopenColl(t, e)
	check(c)
}

func TestDeadBytes(t *testing.T) {
	ctx := context.Background()
	e, c, _ := newFlushColl(t, nil)
	mustInsert(t, c, "a", map[string]any{"v": 1})
	mustInsert(t, c, "b", map[string]any{"v": 1})
	mustFlush(t, c)
	if _, err := c.Update(ctx, "a", map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	mustFlush(t, c)

	c.flushMu.Lock()
	hint := slices.Clone(c.activeHint)
	c.flushMu.Unlock()
	// Records: put a, put b, marker | put a', tombstone b, marker. Only a'
	// is live.
	if len(hint) != 6 {
		t.Fatalf("active segment holds %d records, want 6", len(hint))
	}
	var total, live int64
	for i, h := range hint {
		total += int64(h.Loc.Size)
		if i == 3 {
			live = int64(h.Loc.Size)
		}
	}
	c.mu.RLock()
	got := c.deadBytes[1]
	c.mu.RUnlock()
	if got != total-live {
		t.Errorf("deadBytes[1] = %d, want %d", got, total-live)
	}

	_, c = reopenColl(t, e)
	c.mu.RLock()
	reloaded := c.deadBytes[1]
	c.mu.RUnlock()
	if reloaded != got {
		t.Errorf("deadBytes after reopen = %d, want %d", reloaded, got)
	}
}

func walStarts(t *testing.T, dir string) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for _, ent := range entries {
		name := ent.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"), 10, 64)
		if err != nil {
			t.Fatalf("WAL file %s: %v", name, err)
		}
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

func TestRetentionUsesLowWater(t *testing.T) {
	e, c, _ := newFlushColl(t, func(o *Options) {
		o.WALSegmentSize = 128 // every document write rotates
		o.WALRetentionSize = 1 // delete every processed segment
		o.WALRetentionAge = -1
	})
	pad := strings.Repeat("x", 200)
	var first uint64
	for i := range 5 {
		id := fmt.Sprintf("d%d", i)
		mustInsert(t, c, id, map[string]any{"pad": pad})
		if i == 0 {
			first = memOffset(t, c, id)
		}
	}
	if lw := e.lowWater(); lw != first {
		t.Fatalf("lowWater = %d, want the first unflushed offset %d", lw, first)
	}
	if err := e.wal.EnforceRetention(); err != nil {
		t.Fatal(err)
	}
	if starts := walStarts(t, e.root); len(starts) == 0 || starts[0] > first {
		t.Fatalf("retention deleted unflushed records: WAL segments start at %v, first unflushed %d", starts, first)
	}
	if len(walStarts(t, e.root)) < 5 {
		t.Fatalf("expected the unflushed WAL segments to be kept, have %v", walStarts(t, e.root))
	}

	mustFlush(t, c)
	if lw, next := e.lowWater(), e.wal.NextOffset(); lw != next {
		t.Fatalf("lowWater after flush = %d, want %d", lw, next)
	}
	if err := e.wal.EnforceRetention(); err != nil {
		t.Fatal(err)
	}
	if starts := walStarts(t, e.root); len(starts) != 1 {
		t.Errorf("WAL segments after flush and retention = %v, want only the active one", starts)
	}
	_, c = reopenColl(t, e)
	if c.Count() != 5 {
		t.Errorf("Count after reopen = %d, want 5", c.Count())
	}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestTornDataTailTruncatedAndLogged(t *testing.T) {
	e, c, _ := newFlushColl(t, nil)
	mustInsert(t, c, "a", map[string]any{"v": 1})
	dir := c.dir
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := segment.DataPath(dir, 1)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil { // a torn record header
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	logs := &syncBuffer{}
	opts := e.opts
	opts.Logger = slog.New(slog.NewTextHandler(logs, nil))
	e2 := openEngine(t, opts)
	c2 := mustColl(t, mustDB(t, e2, "app"), "c")
	mustGet(t, c2, "a")
	if !strings.Contains(logs.String(), "torn tail") || !strings.Contains(logs.String(), filepath.Base(path)) {
		t.Errorf("torn tail not logged: %q", logs.String())
	}
	if fi2, err := os.Stat(path); err != nil || fi2.Size() != fi.Size() {
		t.Errorf("segment size after open = %v, %v; want %d", fi2.Size(), err, fi.Size())
	}
}

func TestDropWaitsForFlush(t *testing.T) {
	ctx := context.Background()
	e, c, h := newFlushColl(t, nil)
	mustInsert(t, c, "a", map[string]any{"v": 1})
	h.blockNextSync()
	flushErr := make(chan error, 1)
	go func() { flushErr <- c.flush() }()
	<-h.syncEntered

	dropErr := make(chan error, 1)
	go func() { dropErr <- mustDB(t, e, "app").DropCollection(ctx, "c") }()
	select {
	case err := <-dropErr:
		t.Fatalf("drop finished during a flush: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(h.release)
	if err := <-flushErr; err != nil {
		t.Fatal(err)
	}
	if err := <-dropErr; err != nil {
		t.Fatal(err)
	}
	if err := c.flush(); err != nil {
		t.Errorf("flush of a dropped collection = %v, want nil", err)
	}
}
