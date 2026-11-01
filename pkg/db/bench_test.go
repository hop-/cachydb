package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Performance benchmarks (plan 001, P8). Run them on a real disk:
//
//	go test -run '^$' -bench . -benchmem ./pkg/db/...
//
// Data lives in b.TempDir(), i.e. under $TMPDIR. If /tmp is a tmpfs, fsync
// is free and the numbers are meaningless for durability costs; point
// TMPDIR at a directory on the disk under test, e.g.
//
//	TMPDIR=$HOME/.cache/cachydb-bench go test -run '^$' -bench . ./pkg/db/...

// benchOptions returns quiet engine options for dir.
func benchOptions(dir string) Options {
	return Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// openBench opens an engine on a fresh directory, with mod applied to the
// options, creates database "bench" with schemaless collection "c", and
// closes the engine at cleanup.
func openBench(b *testing.B, mod func(*Options)) (*Engine, *Collection) {
	b.Helper()
	opts := benchOptions(b.TempDir())
	if mod != nil {
		mod(&opts)
	}
	ctx := context.Background()
	e, err := Open(ctx, opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() {
		if err := e.Close(context.Background()); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	d, err := e.CreateDatabase(ctx, "bench")
	if err != nil {
		b.Fatalf("CreateDatabase: %v", err)
	}
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		b.Fatalf("CreateCollection: %v", err)
	}
	c, err := d.Collection("c")
	if err != nil {
		b.Fatalf("Collection: %v", err)
	}
	return e, c
}

// benchID is the id of the i-th loaded document.
func benchID(i int) string { return fmt.Sprintf("doc-%07d", i) }

// benchDoc returns a small, realistic JSON document (~150 bytes encoded).
// "group" takes 100 distinct values, so an eq filter on it matches 1% of
// the documents; "tag" carries the same value as an unindexed field.
func benchDoc(id string, i int) *Document {
	return &Document{ID: id, Data: map[string]any{
		"name":   "user " + strconv.Itoa(i),
		"email":  "user" + strconv.Itoa(i) + "@example.com",
		"age":    20 + i%50,
		"group":  i % 100,
		"tag":    "t" + strconv.Itoa(i%100),
		"active": i%2 == 0,
	}}
}

// loadDocs inserts n documents with ids benchID(0..n-1) using concurrent
// writers (so group commit keeps the load fast) and flushes them into the
// collection's data segments.
func loadDocs(b *testing.B, c *Collection, n int) {
	b.Helper()
	const writers = 64
	ctx := context.Background()
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for range writers {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				if _, err := c.Insert(ctx, benchDoc(benchID(i), i)); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		b.Fatalf("loading documents: %v", err)
	}
	if err := c.flush(); err != nil {
		b.Fatalf("flushing loaded documents: %v", err)
	}
}

// BenchmarkInsertParallel measures durable insert throughput (each Insert
// returns after its WAL record is fsynced) with 1..64 concurrent writers.
// Group commit batches concurrent records into one fsync, so throughput
// should grow with the writer count. Compare the inserts/s metric across
// sub-benchmarks.
//
// It runs exactly N goroutines rather than using b.RunParallel with
// b.SetParallelism, because RunParallel starts p×GOMAXPROCS goroutines and
// so cannot express "1 writer".
func BenchmarkInsertParallel(b *testing.B) {
	for _, writers := range []int{1, 4, 16, 64} {
		b.Run("goroutines="+strconv.Itoa(writers), func(b *testing.B) {
			_, c := openBench(b, nil)
			ctx := context.Background()
			var next atomic.Int64
			var failed atomic.Value
			var wg sync.WaitGroup
			b.ReportAllocs()
			b.ResetTimer()
			start := time.Now()
			for range writers {
				wg.Go(func() {
					for {
						i := int(next.Add(1) - 1)
						if i >= b.N {
							return
						}
						if _, err := c.Insert(ctx, benchDoc(benchID(i), i)); err != nil {
							failed.CompareAndSwap(nil, err)
							return
						}
					}
				})
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()
			if err, _ := failed.Load().(error); err != nil {
				b.Fatalf("Insert: %v", err)
			}
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "inserts/s")
		})
	}
}

// BenchmarkFlushOneChange measures one background flush that carries a
// single changed document, in collections of 1k and 100k documents. The
// flush appends only the changed records and a checkpoint marker to the
// active segment and fsyncs once, so its cost must not grow with the
// collection size (the legacy engine rewrote the whole collection).
func BenchmarkFlushOneChange(b *testing.B) {
	for _, size := range []struct {
		name string
		n    int
	}{{"1k", 1_000}, {"100k", 100_000}} {
		// Load once per size: the sub-benchmark body runs several times
		// while the framework sizes b.N. The flusher ticks rarely and the
		// memtable never fills, so only the explicit flush writes segments.
		_, c := openBench(b, func(o *Options) {
			o.FlushInterval = time.Hour
			o.MemtableSize = 1 << 40
		})
		loadDocs(b, c, size.n)
		ctx := context.Background()
		b.Run("size="+size.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				b.StopTimer()
				id := benchID(i % size.n)
				if _, err := c.Update(ctx, id, map[string]any{"age": i}); err != nil {
					b.Fatalf("Update: %v", err)
				}
				b.StartTimer()
				if err := c.flush(); err != nil {
					b.Fatalf("flush: %v", err)
				}
			}
		})
	}
}

// BenchmarkGet measures Get by id on a flushed collection of 10k
// documents: "cached" with the document cache warm, "uncached" with the
// cache disabled, so every Get reads and decodes the record from its data
// segment (through the OS page cache).
func BenchmarkGet(b *testing.B) {
	const n = 10_000
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		cacheSize int64
	}{{"cached", 0 /* default */}, {"uncached", -1 /* disabled */}} {
		_, c := openBench(b, func(o *Options) { o.CacheSize = tc.cacheSize })
		loadDocs(b, c, n)
		for i := range n { // warm the cache (a no-op when disabled)
			if _, err := c.Get(ctx, benchID(i)); err != nil {
				b.Fatalf("Get: %v", err)
			}
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := c.Get(ctx, benchID(i%n)); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		})
	}
}

// BenchmarkFind measures an eq query that matches 100 of 10k flushed
// documents (cache warm): "indexed" filters on a field with a secondary
// index and examines only the matches; "scan" filters on an unindexed
// field with the same selectivity and examines every document.
func BenchmarkFind(b *testing.B) {
	const n = 10_000
	_, c := openBench(b, nil)
	loadDocs(b, c, n)
	ctx := context.Background()
	if err := c.CreateIndex(ctx, "group_idx", "group"); err != nil {
		b.Fatalf("CreateIndex: %v", err)
	}
	for _, tc := range []struct {
		name   string
		filter QueryFilter
	}{
		{"indexed", QueryFilter{Field: "group", Operator: "eq", Value: 42}},
		{"scan", QueryFilter{Field: "tag", Operator: "eq", Value: "t42"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			q := &Query{Filters: []QueryFilter{tc.filter}}
			docs, err := c.Find(ctx, q) // also warms the cache
			if err != nil {
				b.Fatalf("Find: %v", err)
			}
			if len(docs) != n/100 {
				b.Fatalf("Find returned %d documents, want %d", len(docs), n/100)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := c.Find(ctx, q); err != nil {
					b.Fatalf("Find: %v", err)
				}
			}
		})
	}
}
