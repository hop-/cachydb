package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scale benchmarks (plan 013, approach-index-and-query.md §11). They are
// opt-in and never run in the default `go test` path or in CI's test
// workflow (the nightly workflow runs BenchmarkScale at 1M documents and
// only reports it). Recorded runs: dev-docs/DEVELOPMENT.md, "Scale
// benchmark".
//
//	CACHYDB_SCALE_DOCS=1000000 TMPDIR=$HOME/.cache/cachydb-bench \
//	  go test -run '^$' -bench 'Scale' -benchtime 1x -timeout 30m ./pkg/db/
//
// Knobs:
//
//   - CACHYDB_SCALE_DOCS=N (unset = skip): BenchmarkScale loads N documents
//     into one collection with three indexes and reports the queries of
//     investigation 003 table E2, the Go heap after reopen, the reopen
//     time, the on-disk sizes, cold-cache times, and flushes under
//     sustained random updates.
//   - CACHYDB_SCALE_UUID_IDS=1: ids are 36-character random (version-4
//     shaped) UUIDs instead of "doc-0000000". Random ids are the worst case
//     for the page fill of the key table and the index entries.
//   - CACHYDB_SCALE_UPDATE_SECONDS=S (default 20): length of the sustained
//     update phase; 0 skips it.
//   - CACHYDB_SCALE_COLLECTIONS=K (unset = skip): BenchmarkScaleCollections
//     opens K collections of 1,000 documents with three indexes each.
//
// Run them on a real disk (TMPDIR as above; a tmpfs hides fsync costs).
// The data lives in b.TempDir() and is removed afterwards. A 10M run needs
// up to 9 GB of free space there and about half an hour. The targets of §11 are reported, not
// enforced: a missed target prints "MISSED" in the log, and the benchmark
// still passes (performance gating is not part of CI).

// Targets of approach §11 at N = 1M.
const (
	scaleTargetQuery   = time.Millisecond
	scaleTargetHeapMiB = 150
	scaleTargetReopen  = time.Second
)

// scaleEnvInt returns the integer value of env var name, def when unset.
func scaleEnvInt(b *testing.B, name string, def int) int {
	b.Helper()
	s, ok := os.LookupEnv(name)
	if !ok || s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		b.Fatalf("%s=%q: want a non-negative integer", name, s)
	}
	return n
}

// mix64 is the splitmix64 finalizer: a fixed bijection that spreads
// consecutive integers over the whole uint64 range.
func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// scaleIDs derives document ids from their ordinal without keeping a list
// (a list of 10M ids would distort the heap measurement).
type scaleIDs struct{ uuid bool }

func (s scaleIDs) id(i int) string {
	if !s.uuid {
		return benchID(i)
	}
	var u [16]byte
	binary.BigEndian.PutUint64(u[:8], mix64(uint64(i)))
	binary.BigEndian.PutUint64(u[8:], mix64(uint64(i)^0x5bd1e9955bd1e995))
	u[6] = u[6]&0x0f | 0x40 // version 4
	u[8] = u[8]&0x3f | 0x80 // RFC 9562 variant
	const hex = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for k, c := range u {
		if k == 4 || k == 6 || k == 8 || k == 10 {
			out = append(out, '-')
		}
		out = append(out, hex[c>>4], hex[c&0x0f])
	}
	return string(out)
}

// scaleN is the number field of document i: uniform in [0, 1e9), in random
// order relative to the ids.
func scaleN(i int) int { return int(mix64(uint64(i)^0xc3a5c85c97cb3127) % 1_000_000_000) }

const scaleFiller = "The quick brown fox jumps over the lazy dog while the agent writes notes " +
	"about storage engines, indexes, flushes, and recovery; each line differs a little. " +
	"Segments are append-only and compressed, the write-ahead log is fsynced per group, " +
	"and the index file follows the flusher. "

// scaleDoc returns document i, about 330 bytes of JSON: "cat" is a string
// with 100 values (1% each), "n" a number, "tags" a two-element array
// ("t0".."t49", 2% each, and "even"/"odd", 50% each). "name" and "text"
// are not indexed.
func scaleDoc(id string, i int) *Document {
	parity := "odd"
	if i%2 == 0 {
		parity = "even"
	}
	return &Document{ID: id, Data: map[string]any{
		"name": "user " + strconv.Itoa(i),
		"cat":  "c" + strconv.Itoa(i%100),
		"n":    scaleN(i),
		"tags": []any{"t" + strconv.Itoa(i%50), parity},
		"text": scaleFiller[:150+i%100] + strconv.Itoa(i),
	}}
}

// scaleIndexes are the three indexes of the scale collection.
var scaleIndexes = [][2]string{{"by_cat", "cat"}, {"by_n", "n"}, {"by_tags", "tags"}}

// scaleOpen opens an engine on dir with mod applied to quiet options.
func scaleOpen(b *testing.B, dir string, mod func(*Options)) (*Engine, *Collection, time.Duration) {
	b.Helper()
	opts := benchOptions(dir)
	if mod != nil {
		mod(&opts)
	}
	ctx := context.Background()
	start := time.Now()
	e, err := Open(ctx, opts)
	took := time.Since(start)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	d, err := e.Database("bench")
	if err != nil {
		b.Fatalf("Database: %v", err)
	}
	c, err := d.Collection("c")
	if err != nil {
		b.Fatalf("Collection: %v", err)
	}
	return e, c, took
}

// scaleClose closes e and returns how long Close took.
func scaleClose(b *testing.B, e *Engine) time.Duration {
	b.Helper()
	start := time.Now()
	if err := e.Close(context.Background()); err != nil {
		b.Fatalf("Close: %v", err)
	}
	return time.Since(start)
}

// liveHeapMiB returns the Go heap in use after two forced collections.
func liveHeapMiB() float64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.HeapAlloc) / (1 << 20)
}

// scaleSizes holds the bytes of a data directory by kind of file.
type scaleSizes struct{ index, segments, wal, other int64 }

func dirSizes(b *testing.B, root string) scaleSizes {
	b.Helper()
	var s scaleSizes
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch name := d.Name(); {
		case name == "index.db":
			s.index += info.Size()
		case strings.HasPrefix(name, "seg-"):
			s.segments += info.Size()
		case filepath.Dir(path) == root && strings.HasPrefix(name, "wal"):
			s.wal += info.Size()
		default:
			s.other += info.Size()
		}
		return nil
	})
	if err != nil {
		b.Fatalf("sizing %s: %v", root, err)
	}
	return s
}

func mib(n int64) float64 { return float64(n) / (1 << 20) }

// scaleReport collects metrics and a readable log of one run.
type scaleReport struct {
	b     *testing.B
	lines []string
}

func (r *scaleReport) metric(v float64, unit, format string, args ...any) {
	r.b.ReportMetric(v, unit)
	r.lines = append(r.lines, fmt.Sprintf(format, args...))
}

// print writes the readable report to stdout: b.Log output of a benchmark
// is cut to a few lines without -v.
func (r *scaleReport) print() {
	fmt.Println(strings.Join(r.lines, "\n"))
}

func (r *scaleReport) logf(format string, args ...any) {
	r.lines = append(r.lines, fmt.Sprintf(format, args...))
}

// target logs whether got met the target (smaller is better).
func (r *scaleReport) target(what string, got, want float64, unit string) {
	verdict := "met"
	if got >= want {
		verdict = "MISSED"
	}
	r.lines = append(r.lines, fmt.Sprintf("target %-34s %10.3f %s (target < %g %s): %s", what, got, unit, want, unit, verdict))
}

// timing is the result of running one query repeatedly.
type timing struct {
	first time.Duration // first run
	mean  time.Duration // mean of the runs after the first
	runs  int
}

// timeRuns runs fn once, then repeatedly until 1,000 runs or about 300 ms
// have passed (at least once more unless the first run took over a second).
// fn gets the run number, so it can vary its arguments.
func timeRuns(b *testing.B, fn func(k int) error) timing {
	b.Helper()
	start := time.Now()
	if err := fn(0); err != nil {
		b.Fatal(err)
	}
	t := timing{first: time.Since(start)}
	if t.first > time.Second {
		t.mean, t.runs = t.first, 1
		return t
	}
	var total time.Duration
	for k := 1; k <= 1000 && (k < 2 || total < 300*time.Millisecond); k++ {
		s := time.Now()
		if err := fn(k); err != nil {
			b.Fatal(err)
		}
		total += time.Since(s)
		t.runs = k
	}
	t.mean = total / time.Duration(t.runs)
	return t
}

func us(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }

// scaleQuery is one query of the report.
type scaleQuery struct {
	name   string // metric name
	desc   string
	target bool // one of the §11 sub-millisecond targets
	run    func(ctx context.Context, c *Collection, k int) (int, error)
}

// find runs q and checks it returned want documents (-1: any number).
func find(ctx context.Context, c *Collection, q *Query, want int) (int, error) {
	docs, err := c.Find(ctx, q)
	if err != nil {
		return 0, err
	}
	if want >= 0 && len(docs) != want {
		return 0, fmt.Errorf("query %+v returned %d documents, want %d", *q, len(docs), want)
	}
	return len(docs), nil
}

func scaleQueries(ids scaleIDs, n int) []scaleQuery {
	eq := func(f string, v any) QueryFilter { return QueryFilter{Field: f, Operator: "eq", Value: v} }
	has := func(v string) QueryFilter { return QueryFilter{Field: "tags", Operator: "has", Value: v} }
	lim := min(10, n)
	return []scaleQuery{
		{"id_eq", "find _id eq (point lookup)", true, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Filters: []QueryFilter{eq("_id", ids.id((k*7919+n/2)%n))}}, 1)
		}},
		{"all_limit10", "find, no filter, limit 10 (_id order)", true, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Limit: 10}, lim)
		}},
		{"eq_1pct_limit10", "find cat eq (1%), limit 10", true, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Filters: []QueryFilter{eq("cat", "c"+strconv.Itoa(k%100))}, Limit: 10}, -1)
		}},
		{"has_2pct_limit10", "find tags has t* (2%), limit 10", false, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Filters: []QueryFilter{has("t" + strconv.Itoa(k%50))}, Limit: 10}, -1)
		}},
		{"has_50pct_limit10", "find tags has even (50%), limit 10", false, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Filters: []QueryFilter{has("even")}, Limit: 10}, -1)
		}},
		{"range_sorted_limit10", "find n gte x sort n asc, limit 10", true, func(ctx context.Context, c *Collection, k int) (int, error) {
			lo := (k % 99) * 10_000_000
			return find(ctx, c, &Query{
				Filters: []QueryFilter{{Field: "n", Operator: "gte", Value: lo}},
				Sort:    []SortKey{{Field: "n"}}, Limit: 10}, -1)
		}},
		{"sort_desc_limit10", "find sort n desc, limit 10", true, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Sort: []SortKey{{Field: "n", Order: "desc"}}, Limit: 10}, lim)
		}},
		{"range_1pct_limit10", "find n gte 990M (1%), limit 10 (_id order)", false, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Filters: []QueryFilter{{Field: "n", Operator: "gte", Value: 990_000_000}}, Limit: 10}, -1)
		}},
		{"count_eq", "count cat eq (1%)", false, func(ctx context.Context, c *Collection, k int) (int, error) {
			return c.CountDocuments(ctx, &Query{Filters: []QueryFilter{eq("cat", "c"+strconv.Itoa(k%100))}})
		}},
		{"count_two_indexed", "count cat eq (1%) and tags has even (50%)", false, func(ctx context.Context, c *Collection, k int) (int, error) {
			return c.CountDocuments(ctx, &Query{Filters: []QueryFilter{eq("cat", "c"+strconv.Itoa(2*(k%50))), has("even")}})
		}},
		{"contains_scan_limit10", "find name contains (no match), limit 10: full scan", false, func(ctx context.Context, c *Collection, k int) (int, error) {
			return find(ctx, c, &Query{Filters: []QueryFilter{{Field: "name", Operator: "contains", Value: "zzz"}}, Limit: 10}, 0)
		}},
	}
}

// loadScale inserts documents [from, to) with 256 concurrent writers (group
// commit) through the normal write path and flusher.
func loadScale(b *testing.B, c *Collection, ids scaleIDs, from, to int) {
	b.Helper()
	const writers = 256
	ctx := context.Background()
	var next atomic.Int64
	next.Store(int64(from))
	var failed atomic.Value
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= to || failed.Load() != nil {
					return
				}
				if _, err := c.Insert(ctx, scaleDoc(ids.id(i), i)); err != nil {
					failed.CompareAndSwap(nil, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err, _ := failed.Load().(error); err != nil {
		b.Fatalf("loading documents: %v", err)
	}
}

// BenchmarkScale is the scale benchmark of approach §11; see the comment at
// the top of this file.
func BenchmarkScale(b *testing.B) {
	n := scaleEnvInt(b, "CACHYDB_SCALE_DOCS", 0)
	if n == 0 {
		b.Skip("set CACHYDB_SCALE_DOCS=N to run the scale benchmark (opt-in)")
	}
	ids := scaleIDs{uuid: os.Getenv("CACHYDB_SCALE_UUID_IDS") == "1"}
	updateFor := time.Duration(scaleEnvInt(b, "CACHYDB_SCALE_UPDATE_SECONDS", 20)) * time.Second
	ctx := context.Background()
	r := &scaleReport{b: b}
	defer r.print()

	idKind := "sequential (doc-0000000)"
	if ids.uuid {
		idKind = "random UUID (36 bytes)"
	}
	r.logf("scale benchmark: %d documents, ids %s, GOMAXPROCS %d, %s", n, idKind, runtime.GOMAXPROCS(0), runtime.Version())

	// the metrics below are the result; ns/op is meaningless
	for b.Loop() {
		root := b.TempDir()

		// 1. Load with the indexes in place, through the flusher. The
		// loading engine is closed and unreachable afterwards (scaleLoad
		// keeps every reference), so the heap below is the reopened one's.
		load, closeTime := scaleLoad(b, root, ids, n)
		r.metric(load.Seconds(), "load_s", "load: %.1f s (%.0f docs/s, 256 writers, three indexes)", load.Seconds(), float64(n)/load.Seconds())
		r.metric(closeTime.Seconds(), "close_s", "close after load: %.3f s", closeTime.Seconds())

		sz := dirSizes(b, root)
		r.metric(mib(sz.index), "index_MiB", "index.db: %.1f MiB (%.0f bytes/doc)", mib(sz.index), float64(sz.index)/float64(n))
		r.b.ReportMetric(float64(sz.index)/float64(n), "index_B/doc")
		r.metric(mib(sz.segments), "segments_MiB", "data segments: %.1f MiB (%.0f bytes/doc)", mib(sz.segments), float64(sz.segments)/float64(n))
		r.metric(mib(sz.wal), "wal_MiB", "WAL: %.1f MiB; other files %.1f KiB", mib(sz.wal), float64(sz.other)/1024)

		// 2. Reopen after a clean close: time and heap.
		heapBefore := liveHeapMiB()
		e, c, reopen := scaleOpen(b, root, nil)
		heapOpen := liveHeapMiB()
		r.metric(reopen.Seconds(), "reopen_s", "reopen after clean close: %.3f s", reopen.Seconds())
		r.metric(heapOpen, "heap_open_MiB", "Go heap after open: %.1f MiB (%.1f MiB before the open)", heapOpen, heapBefore)
		if got := c.Count(); got != n {
			b.Fatalf("Count after reopen = %d, want %d", got, n)
		}

		// 3. Queries, warm (the index file in the page cache).
		queries := scaleQueries(ids, n)
		warm := map[string]timing{}
		for _, q := range queries {
			t := timeRuns(b, func(k int) error { _, err := q.run(ctx, c, k); return err })
			warm[q.name] = t
			r.metric(us(t.mean), q.name+"_us", "query %-22s mean %10.1f µs over %4d runs (first %.1f µs): %s",
				q.name, us(t.mean), t.runs, us(t.first), q.desc)
		}
		heapQueries := liveHeapMiB()
		r.metric(heapQueries, "heap_queries_MiB", "Go heap after the queries (document cache filled): %.1f MiB", heapQueries)

		r.logf("targets (approach §11, defined at 1M documents):")
		for _, q := range queries {
			if q.target {
				r.target(q.name, us(warm[q.name].mean), us(scaleTargetQuery), "µs")
			}
		}
		r.target("heap after open", heapOpen, scaleTargetHeapMiB, "MiB")
		r.target("reopen after clean close", reopen.Seconds(), scaleTargetReopen.Seconds(), "s")
		scaleClose(b, e)

		// 4. Sustained random updates, warm: writers run without pause, and
		// a flush runs every interval (the background flusher is held off
		// so each flush can be timed; 1 s is the default FlushInterval).
		if updateFor > 0 {
			e, c, _ = scaleOpen(b, root, func(o *Options) {
				o.FlushInterval = time.Hour
				o.MemtableSize = 1 << 40
			})
			for _, interval := range []time.Duration{time.Second, 5 * time.Second} {
				scaleUpdates(b, r, c, ids, n, updateFor/2, interval)
			}
			scaleClose(b, e)
			sz = dirSizes(b, root)
			r.metric(mib(sz.index), "index_after_updates_MiB", "index.db after the updates: %.1f MiB (%.0f bytes/doc)", mib(sz.index), float64(sz.index)/float64(n))
		}

		// 5. Cold page cache: evict every file of the directory (no root
		// needed), reopen, and run each query once.
		if err := evictDir(root); err != nil {
			r.logf("cold cache: not measured (%v)", err)
		} else {
			e, c, reopen = scaleOpen(b, root, nil)
			r.metric(reopen.Seconds(), "cold_reopen_s", "cold cache: reopen %.3f s", reopen.Seconds())
			for _, q := range queries {
				if q.name == "contains_scan_limit10" {
					continue // a full scan; its cold time is mostly segment reads
				}
				s := time.Now()
				if _, err := q.run(ctx, c, 37); err != nil {
					b.Fatal(err)
				}
				took := time.Since(s)
				r.metric(us(took), "cold_"+q.name+"_us", "cold cache: query %-22s %10.1f µs (first run after the eviction)", q.name, us(took))
			}
			scaleClose(b, e)
		}
	}
}

// scaleLoad creates collection bench/c in root with the three indexes,
// loads n documents, flushes, and closes the engine. It returns the load
// and close times.
func scaleLoad(b *testing.B, root string, ids scaleIDs, n int) (load, closeTime time.Duration) {
	b.Helper()
	ctx := context.Background()
	e, err := Open(ctx, benchOptions(root))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
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
	for _, ix := range scaleIndexes {
		if err := c.CreateIndex(ctx, ix[0], ix[1]); err != nil {
			b.Fatalf("CreateIndex: %v", err)
		}
	}
	start := time.Now()
	loadScale(b, c, ids, 0, n)
	if err := c.flush(); err != nil {
		b.Fatalf("flush: %v", err)
	}
	load = time.Since(start)
	return load, scaleClose(b, e)
}

// scaleUpdates runs 64 writers that update the indexed fields "n" and "cat"
// of random documents for d, flushing every interval, and reports the flush
// times and sizes.
func scaleUpdates(b *testing.B, r *scaleReport, c *Collection, ids scaleIDs, n int, d, interval time.Duration) {
	b.Helper()
	const writers = 64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var updates atomic.Int64
	var failed atomic.Value
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for k := uint64(w); ctx.Err() == nil; k += writers {
				x := mix64(k ^ uint64(time.Now().UnixNano()))
				i := int(x % uint64(n))
				set := map[string]any{"n": int(x>>20) % 1_000_000_000, "cat": "c" + strconv.Itoa(int(x>>8)%100)}
				if _, err := c.Update(ctx, ids.id(i), set); err != nil {
					if ctx.Err() == nil {
						failed.CompareAndSwap(nil, err)
					}
					return
				}
				updates.Add(1)
			}
		})
	}
	var flushes []time.Duration
	var sizes []int
	start := time.Now()
	for time.Since(start) < d {
		time.Sleep(interval)
		c.mu.RLock()
		size := len(c.memtable)
		c.mu.RUnlock()
		s := time.Now()
		if err := c.flush(); err != nil {
			b.Fatalf("flush: %v", err)
		}
		flushes = append(flushes, time.Since(s))
		sizes = append(sizes, size)
	}
	cancel()
	wg.Wait()
	elapsed := time.Since(start)
	if err, _ := failed.Load().(error); err != nil {
		b.Fatalf("update: %v", err)
	}
	if err := c.flush(); err != nil { // the tail, not timed
		b.Fatalf("flush: %v", err)
	}
	sorted := slices.Clone(flushes)
	slices.Sort(sorted)
	var docs int
	for _, s := range sizes {
		docs += s
	}
	var total time.Duration
	for _, f := range flushes {
		total += f
	}
	p50 := sorted[len(sorted)/2]
	worst := sorted[len(sorted)-1]
	perDoc := 0.0
	if docs > 0 {
		perDoc = us(total) / float64(docs)
	}
	var indexMiB float64
	if info, err := os.Stat(filepath.Join(c.dir, "index.db")); err == nil {
		indexMiB = mib(info.Size())
	}
	tag := "flush" + strconv.Itoa(int(interval/time.Second)) + "s"
	r.metric(float64(p50)/float64(time.Millisecond), tag+"_p50_ms",
		"sustained updates, flush every %v: %d flushes, %.0f docs/flush, p50 %.0f ms, max %.0f ms, %.1f µs/doc; %.0f updates/s (64 writers); index.db now %.1f MiB",
		interval, len(flushes), float64(docs)/float64(len(flushes)), float64(p50)/float64(time.Millisecond),
		float64(worst)/float64(time.Millisecond), perDoc, float64(updates.Load())/elapsed.Seconds(), indexMiB)
	r.b.ReportMetric(float64(worst)/float64(time.Millisecond), tag+"_max_ms")
	r.b.ReportMetric(perDoc, tag+"_us/doc")
}

// BenchmarkScaleCollections opens K collections (CACHYDB_SCALE_COLLECTIONS)
// of 1,000 documents with three indexes each and reports the open time,
// heap, open files, and one flush pass over all of them.
func BenchmarkScaleCollections(b *testing.B) {
	k := scaleEnvInt(b, "CACHYDB_SCALE_COLLECTIONS", 0)
	if k == 0 {
		b.Skip("set CACHYDB_SCALE_COLLECTIONS=K to run the many-collections benchmark (opt-in)")
	}
	const perColl = 1_000
	ctx := context.Background()
	r := &scaleReport{b: b}
	defer r.print()
	r.logf("many collections: %d collections x %d documents, three indexes each", k, perColl)
	name := func(j int) string { return fmt.Sprintf("c%04d", j) }

	for b.Loop() {
		root := b.TempDir()
		e, err := Open(ctx, benchOptions(root))
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		d, err := e.CreateDatabase(ctx, "bench")
		if err != nil {
			b.Fatalf("CreateDatabase: %v", err)
		}
		start := time.Now()
		colls := make([]*Collection, k)
		for j := range k {
			if err := d.CreateCollection(ctx, name(j), nil); err != nil {
				b.Fatalf("CreateCollection: %v", err)
			}
			if colls[j], err = d.Collection(name(j)); err != nil {
				b.Fatalf("Collection: %v", err)
			}
			for _, ix := range scaleIndexes {
				if err := colls[j].CreateIndex(ctx, ix[0], ix[1]); err != nil {
					b.Fatalf("CreateIndex: %v", err)
				}
			}
		}
		r.metric(time.Since(start).Seconds(), "create_s", "create %d collections with 3 indexes: %.2f s", k, time.Since(start).Seconds())
		start = time.Now()
		var wg sync.WaitGroup
		var failed atomic.Value
		for j := range k {
			wg.Go(func() {
				for i := range perColl {
					if _, err := colls[j].Insert(ctx, scaleDoc(benchID(i), i)); err != nil {
						failed.CompareAndSwap(nil, err)
						return
					}
				}
			})
		}
		wg.Wait()
		if err, _ := failed.Load().(error); err != nil {
			b.Fatalf("Insert: %v", err)
		}
		if err := e.flushAll(ctx, false); err != nil {
			b.Fatalf("flushAll: %v", err)
		}
		r.metric(time.Since(start).Seconds(), "load_s", "load %d documents: %.2f s", k*perColl, time.Since(start).Seconds())
		closeTime := scaleClose(b, e)
		r.metric(closeTime.Seconds(), "close_s", "close: %.3f s", closeTime.Seconds())
		sz := dirSizes(b, root)
		r.metric(mib(sz.index), "index_MiB", "index.db files: %.1f MiB total, %.0f KiB per collection", mib(sz.index), float64(sz.index)/1024/float64(k))

		fdsBefore := openFiles()
		heapBefore := liveHeapMiB()
		start = time.Now()
		e, err = Open(ctx, benchOptions(root))
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		reopen := time.Since(start)
		heap := liveHeapMiB()
		r.metric(reopen.Seconds(), "reopen_s", "reopen: %.3f s", reopen.Seconds())
		r.metric(heap, "heap_open_MiB", "Go heap after open: %.1f MiB (%.1f MiB before; %.1f KiB per collection)", heap, heapBefore, (heap-heapBefore)*1024/float64(k))
		if fds := openFiles(); fds >= 0 {
			r.metric(float64(fds-fdsBefore), "open_files", "open files after open: %d more than before (%.2f per collection)", fds-fdsBefore, float64(fds-fdsBefore)/float64(k))
		}
		if vm := vmSizeMiB(); vm > 0 {
			r.logf("process virtual size after open: %.0f MiB (bbolt maps each index.db)", vm)
		}

		d, err = e.Database("bench")
		if err != nil {
			b.Fatalf("Database: %v", err)
		}
		for j := range k {
			if colls[j], err = d.Collection(name(j)); err != nil {
				b.Fatalf("Collection: %v", err)
			}
		}
		start = time.Now()
		for j := range k {
			if _, err := colls[j].Find(ctx, &Query{Filters: []QueryFilter{{Field: "cat", Operator: "eq", Value: "c7"}}, Limit: 10}); err != nil {
				b.Fatalf("Find: %v", err)
			}
		}
		took := time.Since(start)
		r.metric(us(took)/float64(k), "find_per_coll_us", "first indexed query in each collection: %.0f µs per collection", us(took)/float64(k))

		for j := range k {
			if _, err := colls[j].Update(ctx, benchID(j%perColl), map[string]any{"n": j}); err != nil {
				b.Fatalf("Update: %v", err)
			}
		}
		start = time.Now()
		if err := e.flushAll(ctx, false); err != nil {
			b.Fatalf("flushAll: %v", err)
		}
		took = time.Since(start)
		r.metric(took.Seconds()*1000, "flush_pass_ms", "one flush pass, one changed document in every collection: %.0f ms (%d flush workers)", float64(took)/float64(time.Millisecond), e.opts.FlushWorkers)
		scaleClose(b, e)
	}
}

// openFiles returns the number of open file descriptors of the process, or
// -1 where /proc is not available.
func openFiles() int {
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(ents)
}

// vmSizeMiB returns the process's virtual memory size from /proc, or 0.
func vmSizeMiB() float64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for line := range strings.Lines(string(data)) {
		if rest, ok := strings.CutPrefix(line, "VmSize:"); ok {
			kb, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 64)
			if err != nil || math.IsNaN(kb) {
				return 0
			}
			return kb / 1024
		}
	}
	return 0
}
