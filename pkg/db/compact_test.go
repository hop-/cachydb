package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/segment"
)

// mustSeal seals c's active segment the way a full segment is sealed on
// flush (hint, snapshots, next segment opened).
func mustSeal(t *testing.T, c *Collection) {
	t.Helper()
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	if c.active == nil {
		t.Fatal("no active segment to seal")
	}
	c.sealActive()
}

func mustFlushSeal(t *testing.T, c *Collection) {
	t.Helper()
	mustFlush(t, c)
	mustSeal(t, c)
}

func mustCompact(t *testing.T, c *Collection) bool {
	t.Helper()
	ok, err := c.compact(context.Background())
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	return ok
}

func listSegs(t *testing.T, dir string) []uint32 {
	t.Helper()
	segs, err := segment.ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	return segs
}

// segmentRecords returns the records of one data segment.
func segmentRecords(t *testing.T, dir string, seg uint32) []segment.Record {
	t.Helper()
	var out []segment.Record
	if _, err := segment.Scan(dir, seg, func(r segment.Record, _ segment.Location) error {
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatalf("Scan(%d): %v", seg, err)
	}
	return out
}

// recordKinds renders records as "put:id", "tomb:id", "cp".
func recordKinds(recs []segment.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		switch r.Type {
		case segment.TypePut:
			out[i] = "put:" + r.ID
		case segment.TypeTombstone:
			out[i] = "tomb:" + r.ID
		default:
			out[i] = "cp"
		}
	}
	return out
}

func fileExistsT(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

// dataBytes returns the total size of the data segment files in dir.
func dataBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var n int64
	for _, seg := range listSegs(t, dir) {
		fi, err := os.Stat(segment.DataPath(dir, seg))
		if err != nil {
			t.Fatal(err)
		}
		n += fi.Size()
	}
	return n
}

// docsModel returns the model of database app, collection c with docs.
func docsModel(indexes map[string]string, docs map[string]map[string]any) model {
	if indexes == nil {
		indexes = map[string]string{}
	}
	return model{"app": {"c": {indexes: indexes, docs: docs}}}
}

// checkKeydirLive checks that every keydir entry points into a live,
// non-retired segment.
func checkKeydirLive(t *testing.T, c *Collection) {
	t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()
	for id, ke := range c.keydir {
		if _, ok := c.retired[ke.loc.Seg]; ok {
			t.Errorf("keydir[%q] points into retired segment %d", id, ke.loc.Seg)
		}
		if _, ok := c.segBytes[ke.loc.Seg]; !ok {
			t.Errorf("keydir[%q] points into unknown segment %d", id, ke.loc.Seg)
		}
	}
	for seg := range c.readers {
		if _, ok := c.retired[seg]; ok {
			t.Errorf("reader open for retired segment %d", seg)
		}
	}
}

func TestPickCompactionRun(t *testing.T) {
	const big = 1 << 20
	tests := []struct {
		name    string
		segs    []uint32
		skip    []uint32
		total   map[uint32]int64
		dead    map[uint32]int64
		ratio   float64
		maxLive int64
		want    []uint32
	}{
		{
			name:  "below ratio",
			segs:  []uint32{1, 2, 3},
			skip:  []uint32{3},
			total: map[uint32]int64{1: 100, 2: 100},
			dead:  map[uint32]int64{1: 40, 2: 50},
			ratio: 0.5, maxLive: big,
		},
		{
			name:  "fewer than two sealed segments",
			segs:  []uint32{1, 2},
			skip:  []uint32{2},
			total: map[uint32]int64{1: 100},
			dead:  map[uint32]int64{1: 100},
			ratio: 0.5, maxLive: big,
		},
		{
			name:  "oldest contiguous run when everything fits",
			segs:  []uint32{1, 2, 3, 4},
			skip:  []uint32{4},
			total: map[uint32]int64{1: 100, 2: 100, 3: 100},
			dead:  map[uint32]int64{1: 60, 2: 60, 3: 60},
			ratio: 0.5, maxLive: big,
			want: []uint32{1, 2, 3},
		},
		{
			name:  "skipped segments split runs",
			segs:  []uint32{1, 2, 3, 4, 5},
			skip:  []uint32{3, 5},
			total: map[uint32]int64{1: 100, 2: 100, 4: 100},
			dead:  map[uint32]int64{1: 60, 2: 60, 4: 90},
			ratio: 0.5, maxLive: big,
			want: []uint32{1, 2},
		},
		{
			name:  "live bytes cap the run",
			segs:  []uint32{1, 2, 3, 4},
			skip:  []uint32{4},
			total: map[uint32]int64{1: 100, 2: 100, 3: 100},
			dead:  map[uint32]int64{1: 50, 2: 50, 3: 90},
			ratio: 0.5, maxLive: 70,
			want: []uint32{2, 3}, // live 50+10; dead 140 beats {1,2}: 100 and {1}: 50
		},
		{
			name:  "single segment when nothing else fits",
			segs:  []uint32{1, 2, 3},
			skip:  []uint32{3},
			total: map[uint32]int64{1: 100, 2: 100},
			dead:  map[uint32]int64{1: 10, 2: 95},
			ratio: 0.5, maxLive: 1,
			want: []uint32{2},
		},
		{
			name:  "oldest on ties",
			segs:  []uint32{1, 2, 3},
			skip:  []uint32{3},
			total: map[uint32]int64{1: 100, 2: 100},
			dead:  map[uint32]int64{1: 80, 2: 80},
			ratio: 0.5, maxLive: 1,
			want: []uint32{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skip := map[uint32]bool{}
			for _, s := range tt.skip {
				skip[s] = true
			}
			got := pickCompactionRun(tt.segs, skip, tt.total, tt.dead, tt.ratio, tt.maxLive)
			if !slices.Equal(got, tt.want) {
				t.Errorf("run = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCoversOlderVersions(t *testing.T) {
	tests := []struct {
		name   string
		run    []uint32
		onDisk []uint32
		want   bool
	}{
		{"starts at oldest", []uint32{1, 2}, []uint32{1, 2, 3, 7}, true},
		{"older segment outside", []uint32{2, 3}, []uint32{1, 2, 3, 4}, false},
		{"orphan inside the range", []uint32{2, 4}, []uint32{2, 3, 4, 5}, false},
		{"newer segments do not matter", []uint32{5}, []uint32{5, 6, 9}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coversOlderVersions(tt.run, tt.onDisk); got != tt.want {
				t.Errorf("coversOlderVersions(%v, %v) = %v, want %v", tt.run, tt.onDisk, got, tt.want)
			}
		})
	}
}

// TestCompactionRecords checks which records a compaction copies: only
// puts the keydir still points at, tombstones only when the run does not
// start at the oldest segment, never checkpoint markers. It also checks
// file retirement, segment ids, and the state after reopening.
func TestCompactionRecords(t *testing.T) {
	ctx := context.Background()
	bigDoc := strings.Repeat("x", 2048)
	tests := []struct {
		name    string
		mod     func(*Options)
		setup   func(t *testing.T, c *Collection)
		wantRun []uint32
		wantOut []string // output records
		want    map[string]map[string]any
	}{
		{
			name: "run from the oldest segment drops tombstones",
			setup: func(t *testing.T, c *Collection) {
				mustInsert(t, c, "a", map[string]any{"v": 1})
				mustInsert(t, c, "b", map[string]any{"v": 1})
				mustInsert(t, c, "c", map[string]any{"v": 1})
				mustFlushSeal(t, c) // 1: a b c cp
				if _, err := c.Update(ctx, "a", map[string]any{"v": 2}); err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(ctx, "b"); err != nil {
					t.Fatal(err)
				}
				mustFlushSeal(t, c) // 2: a' tomb(b) cp
				mustInsert(t, c, "d", map[string]any{"v": 1})
				mustFlushSeal(t, c) // 3: d cp (newest marker); 4: empty active
			},
			wantRun: []uint32{1, 2},
			wantOut: []string{"put:c", "put:a"},
			want: map[string]map[string]any{
				"a": {"v": float64(2)}, "c": {"v": float64(1)}, "d": {"v": float64(1)},
			},
		},
		{
			name: "run after the oldest segment keeps tombstones",
			mod:  func(o *Options) { o.DataSegmentSize = 1 }, // every flush seals; runs hold one live segment
			setup: func(t *testing.T, c *Collection) {
				mustInsert(t, c, "y", map[string]any{"v": 1})
				mustInsert(t, c, "x", map[string]any{"v": bigDoc})
				mustFlush(t, c) // 1: y x cp
				if err := c.Delete(ctx, "y"); err != nil {
					t.Fatal(err)
				}
				mustInsert(t, c, "v", map[string]any{"v": bigDoc + bigDoc})
				mustFlush(t, c) // 2: tomb(y) v cp
				if _, err := c.Update(ctx, "v", map[string]any{"v": 1}); err != nil {
					t.Fatal(err)
				}
				mustFlush(t, c) // 3: v' cp (newest marker); 4: empty active
			},
			wantRun: []uint32{2},
			wantOut: []string{"tomb:y"},
			want: map[string]map[string]any{
				"x": {"v": bigDoc}, "v": {"v": float64(1)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, c, _ := newFlushColl(t, func(o *Options) {
				o.CompactionRatio = 0.01
				if tt.mod != nil {
					tt.mod(o)
				}
			})
			tt.setup(t, c)
			cpBefore := checkpointOf(c)
			before := listSegs(t, c.dir)

			if !mustCompact(t, c) {
				t.Fatal("compact did nothing")
			}
			after := listSegs(t, c.dir)
			var out []uint32
			for _, s := range after {
				if !slices.Contains(before, s) {
					out = append(out, s)
				}
			}
			if len(out) != 1 || out[0] <= slices.Max(before) {
				t.Fatalf("segments %v -> %v: want one new output segment with a higher id", before, after)
			}
			for _, s := range tt.wantRun {
				if slices.Contains(after, s) || fileExistsT(t, segment.HintPath(c.dir, s)) {
					t.Errorf("files of compacted segment %d still exist", s)
				}
			}
			outRecs := segmentRecords(t, c.dir, out[0])
			if got := recordKinds(outRecs); !slices.Equal(got, tt.wantOut) {
				t.Errorf("output records = %v, want %v", got, tt.wantOut)
			}
			hint, err := segment.ReadHint(c.dir, out[0])
			if err != nil || len(hint) != len(outRecs) {
				t.Errorf("output hint = %d entries, %v; want %d", len(hint), err, len(outRecs))
			}
			c.mu.RLock()
			outDead := c.deadBytes[out[0]]
			c.mu.RUnlock()
			if outDead != 0 {
				t.Errorf("deadBytes[output] = %d, want 0 (copied tombstones are needed)", outDead)
			}
			checkKeydirLive(t, c)
			checkModel(t, e, docsModel(nil, tt.want))

			// New writes go to a segment newer than the output.
			mustInsert(t, c, "new", map[string]any{"v": 1})
			mustFlush(t, c)
			c.flushMu.Lock()
			active := c.activeSeg
			c.flushMu.Unlock()
			if active <= out[0] {
				t.Errorf("active segment %d is not newer than output %d", active, out[0])
			}
			tt.want["new"] = map[string]any{"v": float64(1)}

			e, c = reopenColl(t, e)
			checkModel(t, e, docsModel(nil, tt.want))
			if cp := checkpointOf(c); cp < cpBefore {
				t.Errorf("checkpoint after reopen = %d, below %d before compaction", cp, cpBefore)
			}
			mustInsert(t, c, "after", map[string]any{"v": 1})
			mustFlush(t, c)
			tt.want["after"] = map[string]any{"v": float64(1)}
			e, _ = reopenColl(t, e)
			checkModel(t, e, docsModel(nil, tt.want))
		})
	}
}

// TestCompactionSwapIsCompareAndSwap: documents changed while the output
// is written keep their new version; their copies count as dead bytes.
func TestCompactionSwapIsCompareAndSwap(t *testing.T) {
	ctx := context.Background()
	var (
		c     *Collection
		fired bool
	)
	e, c, _ := newFlushColl(t, func(o *Options) {
		o.CompactionRatio = 0.01
		o.testHook = func(p hookPoint) error {
			if p != hookCompactBeforeSwap || fired {
				return nil
			}
			fired = true
			if _, err := c.Update(ctx, "b", map[string]any{"v": "new"}); err != nil {
				t.Errorf("Update during compaction: %v", err)
			}
			if err := c.Delete(ctx, "c"); err != nil {
				t.Errorf("Delete during compaction: %v", err)
			}
			if err := c.flush(); err != nil {
				t.Errorf("flush during compaction: %v", err)
			}
			return nil
		}
	})
	mustInsert(t, c, "a", map[string]any{"v": "old"})
	mustInsert(t, c, "b", map[string]any{"v": "old"})
	mustInsert(t, c, "c", map[string]any{"v": "old"})
	mustFlushSeal(t, c) // 1
	if _, err := c.Update(ctx, "a", map[string]any{"v": "a2"}); err != nil {
		t.Fatal(err)
	}
	mustFlushSeal(t, c) // 2
	mustInsert(t, c, "z", map[string]any{"v": "z"})
	mustFlushSeal(t, c) // 3 (newest marker)

	if !mustCompact(t, c) || !fired {
		t.Fatalf("compaction did not run through the swap (fired %v)", fired)
	}
	segs := listSegs(t, c.dir)
	out := segs[len(segs)-2] // the output, then the active segment of the flush in the hook
	outRecs := segmentRecords(t, c.dir, out)
	if got, want := recordKinds(outRecs), []string{"put:b", "put:c", "put:a"}; !slices.Equal(got, want) {
		t.Fatalf("output %d records = %v, want %v", out, got, want)
	}
	c.mu.RLock()
	ka, kb := c.keydir["a"], c.keydir["b"]
	_, hasC := c.keydir["c"]
	dead := c.deadBytes[out]
	c.mu.RUnlock()
	if ka.loc.Seg != out {
		t.Errorf("a not repointed to the output: %+v", ka)
	}
	if kb.loc.Seg == out {
		t.Errorf("b updated during compaction was overwritten by its old copy: %+v", kb)
	}
	if hasC {
		t.Error("c deleted during compaction is back in the keydir")
	}
	hint, err := segment.ReadHint(c.dir, out)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(hint[0].Loc.Size + hint[1].Loc.Size); dead != want {
		t.Errorf("deadBytes[output] = %d, want %d (stale copies of b and c)", dead, want)
	}
	want := docsModel(nil, map[string]map[string]any{"a": {"v": "a2"}, "b": {"v": "new"}, "z": {"v": "z"}})
	checkModel(t, e, want)
	e, _ = reopenColl(t, e)
	checkModel(t, e, want)
}

// TestCompactionRetiredSegmentReads: an in-flight read keeps a retired
// file until it releases it; later reads never open a retired segment and
// find the document at its new location.
func TestCompactionRetiredSegmentReads(t *testing.T) {
	_, c, _ := newFlushColl(t, func(o *Options) {
		o.CompactionRatio = 0.01
		o.CacheSize = -1 // every read goes to the segments
	})
	mustInsert(t, c, "a", map[string]any{"v": 1})
	mustInsert(t, c, "gone", map[string]any{"v": 1})
	mustFlushSeal(t, c) // 1
	mustInsert(t, c, "b", map[string]any{"v": 2})
	if err := c.Delete(context.Background(), "gone"); err != nil {
		t.Fatal(err)
	}
	mustFlushSeal(t, c) // 2
	mustInsert(t, c, "z", map[string]any{"v": 3})
	mustFlushSeal(t, c) // 3

	c.mu.RLock()
	staleB := c.keydir["b"] // in segment 2, which no read has opened
	c.mu.RUnlock()
	held, err := c.acquireReader(1)
	if err != nil {
		t.Fatal(err)
	}
	if !mustCompact(t, c) {
		t.Fatal("compact did nothing")
	}
	if !fileExistsT(t, segment.DataPath(c.dir, 1)) {
		t.Error("segment 1 removed while a read holds it")
	}
	if fileExistsT(t, segment.HintPath(c.dir, 1)) || fileExistsT(t, segment.DataPath(c.dir, 2)) {
		t.Error("retired files not removed")
	}
	if _, err := c.acquireReader(1); !errors.Is(err, errSegmentRetired) {
		t.Errorf("acquireReader(retired 1) = %v, want errSegmentRetired", err)
	}
	if _, err := c.readStored("b", staleB); !errors.Is(err, errSegmentRetired) {
		t.Errorf("read through stale location = %v, want errSegmentRetired", err)
	}
	for id, v := range map[string]any{"a": float64(1), "b": float64(2), "z": float64(3)} {
		if got := mustGet(t, c, id); got.Data["v"] != v {
			t.Errorf("Get(%s) = %v", id, got.Data)
		}
	}
	held.Release()
	if fileExistsT(t, segment.DataPath(c.dir, 1)) {
		t.Error("segment 1 not removed after the last read released it")
	}
	checkKeydirLive(t, c)
}

// TestCompactionReclaimsDiskSpace: deleting most documents and compacting
// shrinks the data segments accordingly.
func TestCompactionReclaimsDiskSpace(t *testing.T) {
	ctx := context.Background()
	e, c, _ := newFlushColl(t, func(o *Options) {
		o.DataSegmentSize = 16 << 10
		o.CompactionRatio = 0.3
	})
	if err := c.CreateIndex(ctx, "by_k", "k"); err != nil {
		t.Fatal(err)
	}
	const n = 400
	rng := rand.New(rand.NewPCG(7, 7))
	want := map[string]map[string]any{}
	for i := range n {
		id := fmt.Sprintf("doc-%03d", i)
		pad := make([]byte, 120)
		for j := range pad {
			pad[j] = byte(rng.Uint32())
		}
		data := map[string]any{"k": float64(i % 7), "pad": fmt.Sprintf("%x", pad)} // ~incompressible
		mustInsert(t, c, id, data)
		if i%10 == 0 {
			want[id] = data
		}
		if i%25 == 24 {
			mustFlush(t, c)
		}
	}
	mustFlush(t, c)
	full := dataBytes(t, c.dir)
	for i := range n {
		if i%10 != 0 {
			if err := c.Delete(ctx, fmt.Sprintf("doc-%03d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	mustFlush(t, c)
	mustSeal(t, c)
	withGarbage := dataBytes(t, c.dir)

	runs := 0
	for mustCompact(t, c) {
		if runs++; runs > 50 {
			t.Fatal("compaction does not converge")
		}
	}
	compacted := dataBytes(t, c.dir)
	t.Logf("data bytes: full %d, after deletes %d, after %d compactions %d", full, withGarbage, runs, compacted)
	if runs == 0 {
		t.Fatal("no compaction ran")
	}
	// 10% of the documents are left; allow for tombstones, markers and
	// the segments compaction must not touch.
	if compacted > withGarbage/4 {
		t.Errorf("data bytes after compaction = %d, want <= %d (a quarter of %d)", compacted, withGarbage/4, withGarbage)
	}
	idx := map[string]string{"by_k": "k"}
	checkModel(t, e, docsModel(idx, want))
	checkKeydirLive(t, c)
	e, _ = reopenColl(t, e)
	checkModel(t, e, docsModel(idx, want))
}

// TestCrashCompactBeforeSwap: a crash after the output segment is durable
// but before the swap leaves both copies on disk; reopening yields the
// acknowledged state without duplicates, and later compactions clean up.
func TestCrashCompactBeforeSwap(t *testing.T) {
	tests := []struct {
		name      string
		mod       func(*Options)
		wantTombs bool // the crashed compaction's output holds tombstones
	}{
		{"run drops tombstones", nil, false},
		// Every flush seals and runs hold one live segment: the run is
		// segment 2 alone (most garbage), so its tombstones are copied.
		{"run keeps tombstones", func(o *Options) { o.DataSegmentSize = 1 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			opts := crashOpts(t.TempDir())
			opts.CompactionRatio = 0.01
			if tt.mod != nil {
				tt.mod(&opts)
			}
			armed := opts
			crashAt(t, &armed, hookCompactBeforeSwap)
			e := openEngine(t, armed)
			d := mustCreateDB(t, e, "app")
			if err := d.CreateCollection(ctx, "c", nil); err != nil {
				t.Fatal(err)
			}
			c := mustColl(t, d, "c")
			if err := c.CreateIndex(ctx, "by_g", "g"); err != nil {
				t.Fatal(err)
			}
			want := map[string]map[string]any{}
			sealed := opts.DataSegmentSize == 1
			step := func() {
				mustFlush(t, c)
				if !sealed {
					mustSeal(t, c)
				}
			}
			for i := range 6 {
				id := fmt.Sprintf("d%d", i)
				data := map[string]any{"g": float64(i % 2), "big": strings.Repeat("b", 100*i)}
				mustInsert(t, c, id, data)
				want[id] = data
			}
			step()
			for _, id := range []string{"d1", "d4"} {
				if err := c.Delete(ctx, id); err != nil {
					t.Fatal(err)
				}
				delete(want, id)
			}
			if _, err := c.Update(ctx, "d5", map[string]any{"g": float64(7)}); err != nil {
				t.Fatal(err)
			}
			want["d5"]["g"] = float64(7)
			noise := make([]byte, 2500)
			for i := range noise {
				noise[i] = byte(i*7919 + i*i)
			}
			mustInsert(t, c, "tmp", map[string]any{"big": fmt.Sprintf("%x", noise)}) // compresses poorly
			step()
			if _, err := c.Update(ctx, "tmp", map[string]any{"big": "small"}); err != nil {
				t.Fatal(err)
			}
			want["tmp"] = map[string]any{"big": "small"}
			mustInsert(t, c, "last", map[string]any{"g": float64(0)})
			want["last"] = map[string]any{"g": float64(0)}
			step()
			mustInsert(t, c, "unflushed", map[string]any{"g": float64(1)})
			want["unflushed"] = map[string]any{"g": float64(1)}

			before := listSegs(t, c.dir)
			if _, err := c.compact(ctx); !errors.Is(err, errSimulatedCrash) {
				t.Fatalf("compact = %v, want simulated crash", err)
			}
			e.abandon()
			after := listSegs(t, c.dir)
			for _, s := range before {
				if !slices.Contains(after, s) && s != before[len(before)-1] { // the empty active may go
					t.Errorf("segment %d removed before the swap", s)
				}
			}
			out := after[len(after)-1]
			if out <= before[len(before)-1] {
				t.Fatalf("no output segment on disk: %v -> %v", before, after)
			}
			hasTombs := slices.ContainsFunc(segmentRecords(t, c.dir, out), func(r segment.Record) bool {
				return r.Type == segment.TypeTombstone
			})
			if hasTombs != tt.wantTombs {
				t.Errorf("output holds tombstones = %v, want %v", hasTombs, tt.wantTombs)
			}

			idx := map[string]string{"by_g": "g"}
			e = openEngine(t, opts)
			checkModel(t, e, docsModel(idx, want))

			// Compaction works on the recovered state (with the duplicate
			// output) and the result survives restarts.
			c = mustColl(t, mustDB(t, e, "app"), "c")
			mustInsert(t, c, "post", map[string]any{"g": float64(0)})
			want["post"] = map[string]any{"g": float64(0)}
			mustFlush(t, c)
			for i := 0; mustCompact(t, c); i++ {
				if i > 20 {
					t.Fatal("compaction does not converge")
				}
			}
			checkModel(t, e, docsModel(idx, want))
			checkKeydirLive(t, c)
			e = reopen(t, e)
			checkModel(t, e, docsModel(idx, want))
		})
	}
}

// TestConcurrentCompactionStress runs writers, readers, the background
// flusher, and compactions concurrently (run with -race). Nothing is lost,
// no read fails, and no keydir entry or reader is left on a retired
// segment.
func TestConcurrentCompactionStress(t *testing.T) {
	ctx := context.Background()
	e, c := newTestColl(t, func(o *Options) {
		o.FlushInterval = 2 * time.Millisecond
		o.DataSegmentSize = 4 << 10
		o.MemtableSize = 2 << 10
		o.CompactionRatio = 0.2
		o.CacheSize = -1 // reads go to the segments
	}, nil)
	if err := c.CreateIndex(ctx, "by_owner", "owner"); err != nil {
		t.Fatal(err)
	}

	const (
		writers = 4
		keys    = 24
		ops     = 300
		readers = 4
	)
	var (
		wg      sync.WaitGroup
		stop    = make(chan struct{})
		results = make([]map[string]map[string]any, writers)
	)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			mine := map[string]map[string]any{}
			for i := range ops {
				id := fmt.Sprintf("w%d-k%02d", w, rng.IntN(keys))
				data := map[string]any{"owner": fmt.Sprintf("w%d", w), "i": float64(i), "pad": strings.Repeat("z", rng.IntN(300))}
				var err error
				switch _, exists := mine[id]; {
				case !exists:
					_, err = c.Insert(ctx, &Document{ID: id, Data: data})
					mine[id] = data
				case rng.IntN(3) == 0:
					err = c.Delete(ctx, id)
					delete(mine, id)
				default:
					_, err = c.Update(ctx, id, data)
					mine[id] = data
				}
				if err != nil {
					t.Errorf("writer %d op %d on %s: %v", w, i, id, err)
					return
				}
			}
			results[w] = mine
		}()
	}
	var rwg sync.WaitGroup
	for r := range readers {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 2))
			for {
				select {
				case <-stop:
					return
				default:
				}
				w := rng.IntN(writers)
				id := fmt.Sprintf("w%d-k%02d", w, rng.IntN(keys))
				doc, err := c.Get(ctx, id)
				switch {
				case errors.Is(err, ErrNotFound):
				case err != nil:
					t.Errorf("Get(%s): %v", id, err)
					return
				case doc.Data["owner"] != fmt.Sprintf("w%d", w):
					t.Errorf("Get(%s) = %v", id, doc.Data)
				}
				if rng.IntN(20) == 0 {
					owner := fmt.Sprintf("w%d", w)
					docs, err := c.Find(ctx, &Query{Filters: []QueryFilter{{Field: "owner", Operator: "eq", Value: owner}}})
					if err != nil {
						t.Errorf("Find: %v", err)
						return
					}
					for _, d := range docs {
						if d.Data["owner"] != owner {
							t.Errorf("Find(owner=%s) returned %v", owner, d.Data)
						}
					}
				}
			}
		}()
	}
	// Explicit compactions on top of the background compactor.
	rwg.Add(1)
	go func() {
		defer rwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := c.compact(ctx); err != nil {
				t.Errorf("compact: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(stop)
	rwg.Wait()
	if t.Failed() {
		return
	}

	want := map[string]map[string]any{}
	for _, mine := range results {
		maps.Copy(want, mine)
	}
	idx := map[string]string{"by_owner": "owner"}
	checkModel(t, e, docsModel(idx, want))
	checkKeydirLive(t, c)
	c.mu.RLock()
	retired := len(c.retired)
	c.mu.RUnlock()
	if retired == 0 {
		t.Error("no compaction ran")
	}
	t.Logf("%d data segments retired by compaction", retired)
	e = reopen(t, e)
	checkModel(t, e, docsModel(idx, want))
}

// TestCompactorRunsInBackground: the background compactor compacts on
// its own (and Close joins it, at cleanup).
func TestCompactorRunsInBackground(t *testing.T) {
	ctx := context.Background()
	e, c, _ := newFlushColl(t, func(o *Options) { o.CompactionRatio = 0.01 })
	mustInsert(t, c, "a", map[string]any{"v": 1})
	mustFlushSeal(t, c)
	if err := c.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	mustInsert(t, c, "b", map[string]any{"v": 1})
	mustFlushSeal(t, c)
	mustInsert(t, c, "c", map[string]any{"v": 1})
	mustFlushSeal(t, c)

	// Restart with a short interval so the compactor ticks.
	opts := e.opts
	opts.FlushInterval = 5 * time.Millisecond
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	e = openEngine(t, opts)
	c = mustColl(t, mustDB(t, e, "app"), "c")
	waitFor(t, "background compaction", func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return len(c.retired) > 0
	})
	checkModel(t, e, docsModel(nil, map[string]map[string]any{"b": {"v": float64(1)}, "c": {"v": float64(1)}}))
}

// TestDropWaitsForCompaction: dropping a collection (or its database)
// waits for a running compaction, which never writes into a dropped
// directory.
func TestDropWaitsForCompaction(t *testing.T) {
	for _, dropDB := range []bool{false, true} {
		t.Run(fmt.Sprintf("drop-database=%v", dropDB), func(t *testing.T) {
			ctx := context.Background()
			entered := make(chan struct{})
			release := make(chan struct{})
			e, c, _ := newFlushColl(t, func(o *Options) {
				o.CompactionRatio = 0.01
				o.testHook = func(p hookPoint) error {
					if p == hookCompactBeforeSwap {
						close(entered)
						<-release
					}
					return nil
				}
			})
			mustInsert(t, c, "a", map[string]any{"v": 1})
			mustFlushSeal(t, c)
			if _, err := c.Update(ctx, "a", map[string]any{"v": 2}); err != nil {
				t.Fatal(err)
			}
			mustFlushSeal(t, c)
			mustInsert(t, c, "b", map[string]any{"v": 1})
			mustFlushSeal(t, c)

			compactErr := make(chan error, 1)
			go func() {
				_, err := c.compact(ctx)
				compactErr <- err
			}()
			<-entered
			dropErr := make(chan error, 1)
			go func() {
				if dropDB {
					dropErr <- e.DropDatabase(ctx, "app")
				} else {
					dropErr <- mustDB(t, e, "app").DropCollection(ctx, "c")
				}
			}()
			select {
			case err := <-dropErr:
				t.Fatalf("drop finished during a compaction: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			if err := <-compactErr; err != nil {
				t.Fatal(err)
			}
			if err := <-dropErr; err != nil {
				t.Fatal(err)
			}
			if ok, err := c.compact(ctx); ok || err != nil {
				t.Errorf("compact of a dropped collection = %v, %v; want false, nil", ok, err)
			}
		})
	}
}
