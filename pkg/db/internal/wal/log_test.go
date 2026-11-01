package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testSyncer counts fsyncs once armed and lets a test intercept the n-th
// one (1-based, counted from arm). Fsyncs before arm (e.g. during Open)
// are neither counted nor intercepted.
type testSyncer struct {
	armed atomic.Bool
	calls atomic.Int64
	hook  func(n int64) error // nil = always succeed
}

func (s *testSyncer) arm() { s.armed.Store(true) }

func (s *testSyncer) sync(f *os.File) error {
	if !s.armed.Load() {
		return f.Sync()
	}
	n := s.calls.Add(1)
	if s.hook != nil {
		if err := s.hook(n); err != nil {
			return err
		}
	}
	return f.Sync()
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

type testEnv struct {
	dir     string
	segSize int64
	logs    *syncBuffer

	// Optional; zero values keep the defaults of newEnv / Open.
	retentionAge  time.Duration
	retentionSize int64
	logger        *slog.Logger
	now           func() time.Time
}

func newEnv(t *testing.T, segSize int64) *testEnv {
	t.Helper()
	return &testEnv{dir: t.TempDir(), segSize: segSize, logs: &syncBuffer{}}
}

func (e *testEnv) open(t *testing.T, s *testSyncer) (*Log, error) {
	t.Helper()
	if s == nil {
		s = &testSyncer{}
	}
	logger := e.logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(e.logs, nil))
	}
	now := e.now
	if now == nil {
		now = time.Now
	}
	l, err := open(Options{
		Dir:           e.dir,
		SegmentSize:   e.segSize,
		RetentionAge:  e.retentionAge,
		RetentionSize: e.retentionSize,
		Logger:        logger,
	}, s.sync, now)
	if err == nil {
		t.Cleanup(func() { _ = l.Close() }) // idempotent; tests check Close explicitly where it matters
	}
	return l, err
}

func (e *testEnv) mustOpen(t *testing.T, s *testSyncer) *Log {
	t.Helper()
	l, err := e.open(t, s)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l
}

// testRec returns a record whose encoded size does not depend on i (< 10000).
func testRec(i int) Record {
	id := fmt.Sprintf("doc-%04d", i)
	return Record{Op: OpPutDoc, DB: "db", Coll: "coll", ID: id, Payload: []byte(`{"_id":"` + id + `"}`)}
}

func mustAppend(t *testing.T, l *Log, r Record) uint64 {
	t.Helper()
	off, err := l.Append(context.Background(), r)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return off
}

func mustClose(t *testing.T, l *Log) {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func readAll(t *testing.T, l *Log, from uint64) []Record {
	t.Helper()
	var out []Record
	if err := l.ReadFrom(from, func(r Record) error {
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatalf("ReadFrom(%d): %v", from, err)
	}
	return out
}

func walFiles(t *testing.T, dir string) []string {
	t.Helper()
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	var names []string
	for _, s := range segs {
		names = append(names, filepath.Base(s.path))
	}
	return names
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return fi.Size()
}

// waitQueued waits until n requests sit in the writer's queue.
func waitQueued(t *testing.T, l *Log, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for len(l.reqs) < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d queued appends (have %d)", n, len(l.reqs))
		}
		time.Sleep(time.Millisecond)
	}
}

// writeSegment crafts a segment file holding testRec records with the
// given offsets (which need not be consistent with start).
func writeSegment(t *testing.T, dir string, start uint64, offsets ...uint64) string {
	t.Helper()
	buf := segmentHeader()
	for _, off := range offsets {
		r := testRec(int(off))
		r.Offset = off
		r.Time = time.Unix(0, int64(off))
		buf = appendRecord(buf, &r)
	}
	path := filepath.Join(dir, segmentFileName(start))
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("writing segment: %v", err)
	}
	return path
}

func TestOpenEmptyDir(t *testing.T) {
	env := newEnv(t, 0)
	l := env.mustOpen(t, nil)
	if got := l.NextOffset(); got != 1 {
		t.Errorf("NextOffset = %d, want 1", got)
	}
	if got := walFiles(t, env.dir); !slices.Equal(got, []string{"wal-00000000000000000001.log"}) {
		t.Errorf("wal files = %v", got)
	}
	if got := readAll(t, l, 0); len(got) != 0 {
		t.Errorf("ReadFrom on empty log returned %d records", len(got))
	}
	if _, err := Open(Options{}); err == nil {
		t.Error("Open without Dir succeeded")
	}
}

func TestAppendRoundTripAllOps(t *testing.T) {
	env := newEnv(t, 0)
	l := env.mustOpen(t, nil)
	recs := allOpsRecords()
	before := time.Now()
	for i, r := range recs {
		r.Offset, r.Time = 999, time.Unix(1, 0) // must be overwritten
		if off := mustAppend(t, l, r); off != uint64(i)+1 {
			t.Fatalf("Append #%d offset = %d, want %d", i, off, i+1)
		}
	}
	after := time.Now()
	mustClose(t, l)

	l = env.mustOpen(t, nil)
	got := readAll(t, l, 1)
	if len(got) != len(recs) {
		t.Fatalf("read %d records, want %d", len(got), len(recs))
	}
	for i, want := range recs {
		t.Run(want.Op.String(), func(t *testing.T) {
			g := got[i]
			if g.Time.Before(before.Truncate(0)) || g.Time.After(after) {
				t.Errorf("Time %v not within [%v, %v]", g.Time, before, after)
			}
			want.Offset, want.Time = uint64(i)+1, g.Time
			assertRecordEqual(t, g, want)
		})
	}
}

func TestAppendRejectsInvalidRecord(t *testing.T) {
	env := newEnv(t, 0)
	l := env.mustOpen(t, nil)
	tests := []struct {
		name string
		rec  Record
	}{
		{name: "bad op", rec: Record{Op: 0, DB: "d"}},
		{name: "name too long", rec: Record{Op: OpCreateDB, DB: strings.Repeat("x", maxNameLen+1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := l.Append(context.Background(), tt.rec)
			if err == nil || errors.Is(err, ErrFailed) {
				t.Errorf("Append error = %v, want a validation error", err)
			}
		})
	}
	if off := mustAppend(t, l, testRec(1)); off != 1 {
		t.Errorf("offset after rejected appends = %d, want 1", off)
	}
}

func TestGroupCommit(t *testing.T) {
	const n = 50
	entered := make(chan int64, 4)
	release := make(chan struct{})
	s := &testSyncer{hook: func(k int64) error {
		if k <= 2 { // gate the two batch fsyncs
			entered <- k
			<-release
		}
		return nil
	}}
	env := newEnv(t, 0)
	l := env.mustOpen(t, s)
	s.arm()

	type res struct {
		i   int
		off uint64
		err error
	}
	results := make(chan res, n)
	appendAsync := func(i int) {
		go func() {
			off, err := l.Append(context.Background(), testRec(i))
			results <- res{i, off, err}
		}()
	}

	// First append: its batch is now syncing and must not have returned.
	appendAsync(0)
	if k := <-entered; k != 1 {
		t.Fatalf("first fsync call = %d", k)
	}
	// Queue the rest while the first fsync is in flight.
	for i := 1; i < n; i++ {
		appendAsync(i)
	}
	waitQueued(t, l, n-1)
	select {
	case r := <-results:
		t.Fatalf("append %d returned before its batch was synced", r.i)
	default:
	}

	release <- struct{}{}
	first := <-results
	if first.err != nil || first.i != 0 || first.off != 1 {
		t.Fatalf("first append = %+v, want i=0 off=1", first)
	}
	if k := <-entered; k != 2 {
		t.Fatalf("second fsync call = %d", k)
	}
	select {
	case r := <-results:
		t.Fatalf("append %d returned before its batch was synced", r.i)
	default:
	}
	release <- struct{}{}

	offs := []uint64{first.off}
	for i := 1; i < n; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("append %d: %v", r.i, r.err)
		}
		offs = append(offs, r.off)
	}
	slices.Sort(offs)
	for i, off := range offs {
		if off != uint64(i)+1 {
			t.Fatalf("offsets = %v, want 1..%d", offs, n)
		}
	}
	if got := s.calls.Load(); got != 2 {
		t.Errorf("fsyncs = %d for %d records, want 2", got, n)
	}
	if got := l.NextOffset(); got != n+1 {
		t.Errorf("NextOffset = %d, want %d", got, n+1)
	}
	if got := readAll(t, l, 1); len(got) != n {
		t.Errorf("read %d records, want %d", len(got), n)
	}
}

func TestConcurrentAppend(t *testing.T) {
	const workers, perWorker = 16, 40
	s := &testSyncer{}
	env := newEnv(t, 4<<10) // small segments: exercise rotation under load
	l := env.mustOpen(t, s)
	s.arm()

	var mu sync.Mutex
	byOffset := map[uint64]string{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				r := testRec(w*perWorker + j)
				off, err := l.Append(context.Background(), r)
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				mu.Lock()
				if prev, dup := byOffset[off]; dup {
					t.Errorf("offset %d assigned to %s and %s", off, prev, r.ID)
				}
				byOffset[off] = r.ID
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	const total = workers * perWorker
	if len(byOffset) != total {
		t.Fatalf("got %d distinct offsets, want %d", len(byOffset), total)
	}
	got := readAll(t, l, 1)
	if len(got) != total {
		t.Fatalf("read %d records, want %d", len(got), total)
	}
	for i, r := range got {
		if r.Offset != uint64(i)+1 || r.ID != byOffset[r.Offset] {
			t.Fatalf("record %d = offset %d id %q, want offset %d id %q", i, r.Offset, r.ID, i+1, byOffset[uint64(i)+1])
		}
	}
	if len(walFiles(t, env.dir)) < 2 {
		t.Error("expected rotation to create several segments")
	}
	t.Logf("%d records, %d fsyncs", total, s.calls.Load())
}

func TestReopenContinuesInNewestSegment(t *testing.T) {
	env := newEnv(t, 0)
	l := env.mustOpen(t, nil)
	for i := 1; i <= 3; i++ {
		mustAppend(t, l, testRec(i))
	}
	mustClose(t, l)

	for round := 0; round < 2; round++ {
		l = env.mustOpen(t, nil)
		want := uint64(4 + round)
		if got := l.NextOffset(); got != want {
			t.Fatalf("round %d: NextOffset = %d, want %d", round, got, want)
		}
		if off := mustAppend(t, l, testRec(int(want))); off != want {
			t.Fatalf("round %d: Append offset = %d, want %d", round, off, want)
		}
		mustClose(t, l)
	}
	if got := walFiles(t, env.dir); !slices.Equal(got, []string{"wal-00000000000000000001.log"}) {
		t.Errorf("wal files = %v, want a single segment", got)
	}
	l = env.mustOpen(t, nil)
	got := readAll(t, l, 1)
	if len(got) != 5 {
		t.Fatalf("read %d records, want 5", len(got))
	}
	for i, r := range got {
		if r.Offset != uint64(i)+1 || r.ID != testRec(i+1).ID {
			t.Errorf("record %d = offset %d id %q", i, r.Offset, r.ID)
		}
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	recSize := recordSize(ptr(testRec(1))) // same for every testRec index
	end2 := segmentHeaderSize + 2*recSize  // end of record 2
	end3 := end2 + recSize                 // end of record 3 (file end)
	tests := []struct {
		name     string
		damage   func(t *testing.T, path string)
		wantNext uint64
		wantSize int64
	}{
		{
			name:     "truncated in record body",
			damage:   func(t *testing.T, path string) { truncateFile(t, path, end3-5) },
			wantNext: 3, wantSize: end2,
		},
		{
			name:     "truncated in record header",
			damage:   func(t *testing.T, path string) { truncateFile(t, path, end2+3) },
			wantNext: 3, wantSize: end2,
		},
		{
			name:     "crc damage in final record",
			damage:   func(t *testing.T, path string) { flipByte(t, path, end3-2) },
			wantNext: 3, wantSize: end2,
		},
		{
			name:     "garbage length in final record",
			damage:   func(t *testing.T, path string) { writeAt(t, path, end2, []byte{0xff, 0xff, 0xff, 0x7f}) },
			wantNext: 3, wantSize: end2,
		},
		{
			name:     "zero-filled tail after final record",
			damage:   func(t *testing.T, path string) { writeAt(t, path, end3, make([]byte, 4096)) },
			wantNext: 4, wantSize: end3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newEnv(t, 0)
			l := env.mustOpen(t, nil)
			for i := 1; i <= 3; i++ {
				mustAppend(t, l, testRec(i))
			}
			mustClose(t, l)
			path := filepath.Join(env.dir, segmentFileName(1))
			if got := fileSize(t, path); got != end3 {
				t.Fatalf("segment size = %d, want %d", got, end3)
			}
			tt.damage(t, path)

			l, err := env.open(t, nil)
			if err != nil {
				t.Fatalf("Open after torn tail: %v", err)
			}
			if got := l.NextOffset(); got != tt.wantNext {
				t.Errorf("NextOffset = %d, want %d", got, tt.wantNext)
			}
			if got := fileSize(t, path); got != tt.wantSize {
				t.Errorf("segment size after open = %d, want %d", got, tt.wantSize)
			}
			if !strings.Contains(env.logs.String(), "torn tail") {
				t.Errorf("no torn-tail warning logged; logs:\n%s", env.logs.String())
			}
			if off := mustAppend(t, l, testRec(100)); off != tt.wantNext {
				t.Errorf("Append offset = %d, want %d", off, tt.wantNext)
			}
			mustClose(t, l)
			l = env.mustOpen(t, nil)
			got := readAll(t, l, 1)
			if uint64(len(got)) != tt.wantNext {
				t.Fatalf("read %d records, want %d", len(got), tt.wantNext)
			}
			if last := got[len(got)-1]; last.ID != testRec(100).ID {
				t.Errorf("last record id = %q, want %q", last.ID, testRec(100).ID)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestTornHeaderOfNewestSegment(t *testing.T) {
	tests := []struct {
		name     string
		contents []byte
	}{
		{name: "partial header", contents: []byte("CDB")},
		{name: "empty file", contents: nil},
		{name: "zero-filled", contents: make([]byte, 64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newEnv(t, 0)
			writeSegment(t, env.dir, 1, 1, 2)
			torn := filepath.Join(env.dir, segmentFileName(3))
			if err := os.WriteFile(torn, tt.contents, 0o644); err != nil {
				t.Fatal(err)
			}
			l := env.mustOpen(t, nil)
			if got := l.NextOffset(); got != 3 {
				t.Errorf("NextOffset = %d, want 3", got)
			}
			if got := fileSize(t, torn); got != segmentHeaderSize {
				t.Errorf("torn segment size = %d, want %d", got, segmentHeaderSize)
			}
			if off := mustAppend(t, l, testRec(3)); off != 3 {
				t.Errorf("Append offset = %d, want 3", off)
			}
			if got := readAll(t, l, 1); len(got) != 3 {
				t.Errorf("read %d records, want 3", len(got))
			}
		})
	}
}

func TestOpenDetectsCorruption(t *testing.T) {
	recSize := recordSize(ptr(testRec(1)))
	tests := []struct {
		name  string
		setup func(t *testing.T, dir string)
	}{
		{
			name: "crc damage in earlier record of newest segment",
			setup: func(t *testing.T, dir string) {
				p := writeSegment(t, dir, 1, 1, 2, 3)
				flipByte(t, p, segmentHeaderSize+recSize-2) // inside record 1
			},
		},
		{
			name: "crc damage in final record of older segment",
			setup: func(t *testing.T, dir string) {
				p := writeSegment(t, dir, 1, 1, 2)
				writeSegment(t, dir, 3, 3)
				flipByte(t, p, segmentHeaderSize+2*recSize-2)
			},
		},
		{
			name: "truncated older segment",
			setup: func(t *testing.T, dir string) {
				p := writeSegment(t, dir, 1, 1, 2)
				writeSegment(t, dir, 3, 3)
				truncateFile(t, p, segmentHeaderSize+2*recSize-3)
			},
		},
		{
			name: "offset gap inside a segment",
			setup: func(t *testing.T, dir string) {
				writeSegment(t, dir, 1, 1, 2, 4)
			},
		},
		{
			name: "missing middle segment",
			setup: func(t *testing.T, dir string) {
				writeSegment(t, dir, 1, 1, 2)
				writeSegment(t, dir, 5, 5, 6)
			},
		},
		{
			name: "segment name does not match first record",
			setup: func(t *testing.T, dir string) {
				writeSegment(t, dir, 1, 2, 3)
			},
		},
		{
			name: "segment named for offset zero",
			setup: func(t *testing.T, dir string) {
				writeSegment(t, dir, 0, 0, 1)
			},
		},
		{
			name: "bad header magic in older segment",
			setup: func(t *testing.T, dir string) {
				p := writeSegment(t, dir, 1, 1)
				writeSegment(t, dir, 2, 2)
				writeAt(t, p, 0, []byte("XXXX"))
			},
		},
		{
			name: "non-zero garbage header in newest segment",
			setup: func(t *testing.T, dir string) {
				p := writeSegment(t, dir, 1, 1)
				writeAt(t, p, 0, []byte("XXXX"))
			},
		},
		{
			name: "unsupported segment version",
			setup: func(t *testing.T, dir string) {
				p := writeSegment(t, dir, 1, 1)
				writeAt(t, p, 4, []byte{2, 0})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newEnv(t, 0)
			tt.setup(t, env.dir)
			_, err := env.open(t, nil)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open error = %v, want ErrCorrupt", err)
			}
			if !strings.Contains(err.Error(), "wal-") || !strings.Contains(err.Error(), "byte") {
				t.Errorf("error %q should name the file and byte offset", err)
			}
		})
	}
}

func TestRotation(t *testing.T) {
	recSize := recordSize(ptr(testRec(1)))
	env := newEnv(t, segmentHeaderSize+2*recSize) // exactly two records per segment
	l := env.mustOpen(t, nil)
	for i := 1; i <= 5; i++ {
		mustAppend(t, l, testRec(i))
	}
	want := []string{
		"wal-00000000000000000001.log",
		"wal-00000000000000000003.log",
		"wal-00000000000000000005.log",
	}
	if got := walFiles(t, env.dir); !slices.Equal(got, want) {
		t.Fatalf("wal files = %v, want %v", got, want)
	}
	nameRE := regexp.MustCompile(`^wal-\d{20}\.log$`)
	for _, name := range want {
		if !nameRE.MatchString(name) {
			t.Errorf("segment name %q is not wal-<20 digits>.log", name)
		}
	}

	// Filling segment 5 rotates to an empty segment named for offset 7.
	mustAppend(t, l, testRec(6))

	tests := []struct {
		from uint64
		want []uint64
	}{
		{from: 0, want: []uint64{1, 2, 3, 4, 5, 6}},
		{from: 1, want: []uint64{1, 2, 3, 4, 5, 6}},
		{from: 2, want: []uint64{2, 3, 4, 5, 6}},
		{from: 3, want: []uint64{3, 4, 5, 6}},
		{from: 6, want: []uint64{6}},
		{from: 7, want: nil},
		{from: 100, want: nil},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("ReadFrom(%d)", tt.from), func(t *testing.T) {
			var got []uint64
			for _, r := range readAll(t, l, tt.from) {
				if r.ID != testRec(int(r.Offset)).ID {
					t.Errorf("record %d has id %q", r.Offset, r.ID)
				}
				got = append(got, r.Offset)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("offsets = %v, want %v", got, tt.want)
			}
		})
	}
	// Rotation runs after the batch is acknowledged; Close waits for it.
	mustClose(t, l)
	newest := filepath.Join(env.dir, "wal-00000000000000000007.log")
	if got := fileSize(t, newest); got != segmentHeaderSize {
		t.Errorf("new segment size = %d, want header only", got)
	}

	// Reopen appends to the (empty) newest segment.
	l = env.mustOpen(t, nil)
	if off := mustAppend(t, l, testRec(7)); off != 7 {
		t.Errorf("Append after reopen = %d, want 7", off)
	}
	if got := len(walFiles(t, env.dir)); got != 4 {
		t.Errorf("%d segments after reopen, want 4", got)
	}
}

func TestReadFromStopsOnCallbackError(t *testing.T) {
	env := newEnv(t, 0)
	l := env.mustOpen(t, nil)
	for i := 1; i <= 5; i++ {
		mustAppend(t, l, testRec(i))
	}
	stop := errors.New("stop")
	var seen []uint64
	err := l.ReadFrom(2, func(r Record) error {
		seen = append(seen, r.Offset)
		if r.Offset == 3 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Errorf("ReadFrom error = %v, want the callback error", err)
	}
	if !slices.Equal(seen, []uint64{2, 3}) {
		t.Errorf("seen = %v, want [2 3]", seen)
	}
}

func TestReadFromDetectsMissingSegment(t *testing.T) {
	recSize := recordSize(ptr(testRec(1)))
	env := newEnv(t, segmentHeaderSize+2*recSize)
	l := env.mustOpen(t, nil)
	for i := 1; i <= 5; i++ {
		mustAppend(t, l, testRec(i))
	}
	if err := os.Remove(filepath.Join(env.dir, segmentFileName(3))); err != nil {
		t.Fatal(err)
	}
	err := l.ReadFrom(1, func(Record) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("ReadFrom error = %v, want ErrCorrupt", err)
	}
}

func TestSyncFailureIsSticky(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	errDisk := errors.New("simulated EIO")
	s := &testSyncer{hook: func(k int64) error {
		switch k {
		case 1: // first batch: hold it so the next batch accumulates
			entered <- struct{}{}
			<-release
			return nil
		case 2: // second batch fails
			return errDisk
		default:
			return nil
		}
	}}
	env := newEnv(t, 0)
	l := env.mustOpen(t, s)
	s.arm()

	firstDone := make(chan error, 1)
	go func() {
		_, err := l.Append(context.Background(), testRec(1))
		firstDone <- err
	}()
	<-entered

	const batch = 3
	errs := make(chan error, batch)
	for i := 0; i < batch; i++ {
		go func() {
			_, err := l.Append(context.Background(), testRec(2+i))
			errs <- err
		}()
	}
	waitQueued(t, l, batch)
	close(release)

	if err := <-firstDone; err != nil {
		t.Fatalf("first append: %v", err)
	}
	for i := 0; i < batch; i++ {
		err := <-errs
		if !errors.Is(err, ErrFailed) || !errors.Is(err, errDisk) {
			t.Errorf("batch append error = %v, want ErrFailed wrapping the sync error", err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := l.Append(context.Background(), testRec(10+i)); !errors.Is(err, ErrFailed) {
			t.Errorf("later Append error = %v, want ErrFailed", err)
		}
	}
	if got := l.NextOffset(); got != 2 {
		t.Errorf("NextOffset = %d, want 2", got)
	}
	if got := s.calls.Load(); got != 2 {
		t.Errorf("fsync calls = %d, want 2 (no retry)", got)
	}
	if err := l.Close(); err != nil {
		t.Errorf("Close after failure: %v", err)
	}
}

func TestCloseDrainsAndIsIdempotent(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	s := &testSyncer{hook: func(k int64) error {
		if k == 1 {
			entered <- struct{}{}
			<-release
		}
		return nil
	}}
	env := newEnv(t, 0)
	l := env.mustOpen(t, s)
	s.arm()

	const n = 4
	errs := make(chan error, n)
	go func() {
		_, err := l.Append(context.Background(), testRec(0))
		errs <- err
	}()
	<-entered
	for i := 1; i < n; i++ {
		go func() {
			_, err := l.Append(context.Background(), testRec(i))
			errs <- err
		}()
	}
	waitQueued(t, l, n-1)

	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) while a batch was still syncing", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("queued Append: %v", err)
		}
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, err := l.Append(context.Background(), testRec(9)); !errors.Is(err, ErrClosed) {
		t.Errorf("Append after Close = %v, want ErrClosed", err)
	}

	l = env.mustOpen(t, nil)
	if got := l.NextOffset(); got != n+1 {
		t.Errorf("NextOffset after reopen = %d, want %d (all queued records durable)", got, n+1)
	}
}

func TestAppendContextCancellation(t *testing.T) {
	t.Run("already cancelled: not written", func(t *testing.T) {
		env := newEnv(t, 0)
		l := env.mustOpen(t, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := l.Append(ctx, testRec(1)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Append error = %v, want context.Canceled", err)
		}
		if got := l.NextOffset(); got != 1 {
			t.Errorf("NextOffset = %d, want 1", got)
		}
	})

	t.Run("cancelled while waiting: still written", func(t *testing.T) {
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		s := &testSyncer{hook: func(k int64) error {
			if k == 1 {
				entered <- struct{}{}
				<-release
			}
			return nil
		}}
		env := newEnv(t, 0)
		l := env.mustOpen(t, s)
		s.arm()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := l.Append(ctx, testRec(1))
			done <- err
		}()
		<-entered
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Append error = %v, want context.Canceled", err)
		}
		close(release)
		mustClose(t, l)

		l = env.mustOpen(t, nil)
		got := readAll(t, l, 1)
		if len(got) != 1 || got[0].ID != testRec(1).ID {
			t.Errorf("records after reopen = %+v, want the cancelled record", got)
		}
	})
}

func truncateFile(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func writeAt(t *testing.T, path string, off int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	b[off] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}
