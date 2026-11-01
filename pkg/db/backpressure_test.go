package db

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

const bpLimit = 2 << 10

// newBackpressureColl returns a collection whose writes wait once 2 KiB
// are unflushed. Only backpressure kicks the flusher (MemtableSize is
// large, the timer is off), and the segment writer is hooked so a test
// can make the flush's fsync slow.
func newBackpressureColl(t *testing.T) (*Engine, *Collection, *segHooks) {
	t.Helper()
	return newFlushColl(t, func(o *Options) {
		o.MemtableSize = 64 << 20
		o.MaxPendingBytes = bpLimit
		o.FlushWorkers = 1
	})
}

// fillPending inserts documents until the unflushed bytes reach the limit
// and returns how many it inserted.
func fillPending(t *testing.T, e *Engine, c *Collection) int {
	t.Helper()
	n := 0
	for !e.overLimit() {
		mustInsert(t, c, fmt.Sprintf("fill%03d", n), map[string]any{"pad": strings.Repeat("x", 200)})
		n++
		if n > 100 {
			t.Fatal("pending bytes never reached the limit")
		}
	}
	return n
}

func TestBackpressureBlocksUntilFlush(t *testing.T) {
	ctx := context.Background()
	e, c, h := newBackpressureColl(t)
	h.blockNextSync()
	n := fillPending(t, e, c)
	atLimit := e.pending.load()

	done := make(chan error, 1)
	go func() {
		_, err := c.Insert(ctx, &Document{ID: "waiter", Data: map[string]any{"v": 1}})
		done <- err
	}()
	// The waiting writer kicks the flusher, whose fsync now blocks.
	select {
	case <-h.syncEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting writer did not start a flush")
	}
	select {
	case err := <-done:
		t.Fatalf("insert returned (%v) while over MaxPendingBytes", err)
	case <-time.After(50 * time.Millisecond):
	}

	// A write whose ctx ends while waiting fails with ctx's error and
	// writes nothing.
	cctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := c.Insert(cctx, &Document{ID: "cancelled", Data: map[string]any{"v": 1}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("insert with an expiring ctx = %v, want DeadlineExceeded", err)
	}
	if _, err := c.Get(ctx, "cancelled"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(cancelled) = %v, want ErrNotFound", err)
	}
	if got := e.pending.load(); got != atLimit {
		t.Fatalf("pending bytes grew while writers waited: %d -> %d", atLimit, got)
	}

	close(h.release) // the flush completes and releases the bytes
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("insert after the flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting writer was not woken by the flush")
	}
	if got := c.Count(); got != n+1 {
		t.Fatalf("Count = %d, want %d", got, n+1)
	}
	mustFlush(t, c)
	if got := e.pending.load(); got != 0 {
		t.Fatalf("pending bytes after flushing everything = %d, want 0", got)
	}
}

// TestBackpressureConcurrentWriters: many writers against a slow disk stay
// bounded (each can overshoot by at most its own document) and all finish.
func TestBackpressureConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	e, c, _ := newBackpressureColl(t)
	const writers, perWriter = 8, 40
	errs := make(chan error, writers)
	stopSample := make(chan struct{})
	maxSeen := make(chan int64, 1)
	go func() {
		var m int64
		for {
			select {
			case <-stopSample:
				maxSeen <- m
				return
			default:
				m = max(m, e.pending.load())
				runtime.Gosched()
			}
		}
	}()
	for w := range writers {
		go func() {
			for i := range perWriter {
				_, err := c.Insert(ctx, &Document{ID: fmt.Sprintf("w%d-%d", w, i), Data: map[string]any{"pad": strings.Repeat("y", 150)}})
				if err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range writers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	close(stopSample)
	m := <-maxSeen
	docSize := int64(len("w0-00")+len(`{"_id":"w0-00","pad":""}`)+150) + memEntryOverhead
	if bound := int64(bpLimit) + writers*docSize; m > bound {
		t.Fatalf("pending bytes peaked at %d, want <= %d", m, bound)
	}
	if got := c.Count(); got != writers*perWriter {
		t.Fatalf("Count = %d, want %d", got, writers*perWriter)
	}
}

func TestBackpressureReleasedByDropAndClose(t *testing.T) {
	ctx := context.Background()
	t.Run("drop", func(t *testing.T) {
		e, c, _ := newBackpressureColl(t)
		fillPending(t, e, c)
		if err := mustDB(t, e, "app").DropCollection(ctx, "c"); err != nil {
			t.Fatal(err)
		}
		if got := e.pending.load(); got != 0 {
			t.Fatalf("pending bytes after drop = %d, want 0", got)
		}
	})
	t.Run("close", func(t *testing.T) {
		e, c, h := newBackpressureColl(t)
		h.blockNextSync()
		fillPending(t, e, c)
		done := make(chan error, 1)
		go func() {
			_, err := c.Insert(ctx, &Document{ID: "waiter", Data: map[string]any{"v": 1}})
			done <- err
		}()
		<-h.syncEntered
		closeErr := make(chan error, 1)
		go func() { closeErr <- e.Close(ctx) }()
		select {
		case err := <-done:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("waiting insert during Close = %v, want ErrClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not wake the waiting writer")
		}
		close(h.release)
		if err := <-closeErr; err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

// TestFlushAllParallel: one pass flushes every collection, several at a
// time.
func TestFlushAllParallel(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, func() Options {
		o := testOptions(t.TempDir())
		o.FlushInterval = time.Hour
		o.FlushWorkers = 4
		return o
	}())
	d := mustCreateDB(t, e, "app")
	for i := range 10 {
		name := fmt.Sprintf("c%d", i)
		if err := d.CreateCollection(ctx, name, nil); err != nil {
			t.Fatal(err)
		}
		mustInsert(t, mustColl(t, d, name), "a", map[string]any{"i": float64(i)})
	}
	if err := e.flushAll(ctx, false); err != nil {
		t.Fatal(err)
	}
	if got := e.pending.load(); got != 0 {
		t.Fatalf("pending bytes after flushAll = %d, want 0", got)
	}
	for i := range 10 {
		c := mustColl(t, d, fmt.Sprintf("c%d", i))
		c.mu.RLock()
		mem, fl := len(c.memtable), c.flushing
		c.mu.RUnlock()
		if mem != 0 || fl != nil {
			t.Errorf("c%d not flushed: memtable %d, flushing %v", i, mem, fl != nil)
		}
	}
	e = reopen(t, e)
	d = mustDB(t, e, "app")
	for i := range 10 {
		if got := mustGet(t, mustColl(t, d, fmt.Sprintf("c%d", i)), "a"); got.Data["i"] != float64(i) {
			t.Errorf("c%d/a = %v", i, got.Data)
		}
	}
}
