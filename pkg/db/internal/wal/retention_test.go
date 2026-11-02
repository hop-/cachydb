package wal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a goroutine-safe settable clock.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.set(t)
	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) set(t time.Time)         { c.ns.Store(t.UnixNano()) }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// captureHandler is a slog.Handler that records every log record.
type captureHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}

// count returns how many records at level have a message starting with prefix.
func (h *captureHandler) count(level slog.Level, prefix string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.recs {
		if r.Level == level && len(r.Message) >= len(prefix) && r.Message[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

const sizeWarning = "wal: size exceeds retention limit"

var retentionBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// retentionEnv returns an env with two records per segment, a fake clock,
// and a capturing logger.
func retentionEnv(t *testing.T, age time.Duration, size int64) (*testEnv, *fakeClock, *captureHandler) {
	t.Helper()
	env := newEnv(t, segmentHeaderSize+2*recordSize(ptr(testRec(1))))
	clock := newFakeClock(retentionBase)
	h := &captureHandler{}
	env.retentionAge, env.retentionSize = age, size
	env.now, env.logger = clock.now, slog.New(h)
	return env, clock, h
}

// appendHourly appends records first..last, record i stamped retentionBase + i hours.
func appendHourly(t *testing.T, l *Log, clock *fakeClock, first, last int) {
	t.Helper()
	for i := first; i <= last; i++ {
		clock.set(retentionBase.Add(time.Duration(i) * time.Hour))
		if off := mustAppend(t, l, testRec(i)); off != uint64(i) {
			t.Fatalf("Append = %d, want %d", off, i)
		}
	}
}

func segStarts(t *testing.T, dir string) []uint64 {
	t.Helper()
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	var out []uint64
	for _, s := range segs {
		out = append(out, s.start)
	}
	return out
}

func offsets(recs []Record) []uint64 {
	var out []uint64
	for _, r := range recs {
		out = append(out, r.Offset)
	}
	return out
}

func span(from, to uint64) []uint64 {
	var out []uint64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

func TestEnforceRetention(t *testing.T) {
	recSize := recordSize(ptr(testRec(1)))
	sealed := segmentHeaderSize + 2*recSize // S
	active := segmentHeaderSize + recSize   // the active segment holds record 11
	total := 5*sealed + active

	// Records 1..11 at base+1h..base+11h; segments start at 1,3,5,7,9 (sealed)
	// and 11 (active). Pass time is base+12h, so the sealed segments' newest
	// records are 10h, 8h, 6h, 4h and 2h old.
	all := []uint64{1, 3, 5, 7, 9, 11}
	tests := []struct {
		name     string
		age      time.Duration
		size     int64
		lowWater uint64
		want     []uint64 // remaining segment starts
		wantWarn bool
	}{
		{name: "neither", lowWater: 7, want: all},
		{name: "neither/all processed", lowWater: math.MaxUint64, want: all},

		{name: "age only/oldest expired", age: 9*time.Hour + 30*time.Minute, lowWater: 7, want: []uint64{3, 5, 7, 9, 11}},
		{name: "age only/stops at low water", age: time.Hour, lowWater: 7, want: []uint64{7, 9, 11}},
		{name: "age only/nothing processed", age: time.Hour, lowWater: 1, want: all},
		{name: "age only/last offset equal to low water is kept", age: time.Hour, lowWater: 6, want: []uint64{5, 7, 9, 11}},

		{name: "size only/one over", size: total - sealed, lowWater: 7, want: []uint64{3, 5, 7, 9, 11}},
		{name: "size only/two over", size: total - 2*sealed, lowWater: 7, want: []uint64{5, 7, 9, 11}},
		{name: "size only/at limit", size: total, lowWater: 7, want: all},
		{name: "size only/unprocessed kept above limit", size: 1, lowWater: 7, want: []uint64{7, 9, 11}, wantWarn: true},
		{name: "size only/nothing processed", size: 1, lowWater: 1, want: all, wantWarn: true},

		{name: "both/size triggers more", age: 9*time.Hour + 30*time.Minute, size: total - 2*sealed, lowWater: 7, want: []uint64{5, 7, 9, 11}},
		{name: "both/age triggers more", age: 7 * time.Hour, size: total - sealed, lowWater: math.MaxUint64, want: []uint64{5, 7, 9, 11}},
		{name: "both/age alone", age: 9*time.Hour + 30*time.Minute, size: 1 << 40, lowWater: 7, want: []uint64{3, 5, 7, 9, 11}},
		{name: "both/neither limit reached", age: 24 * time.Hour, size: 1 << 40, lowWater: math.MaxUint64, want: all},
		{name: "both/active segment never deleted", age: time.Nanosecond, size: 1, lowWater: math.MaxUint64, want: []uint64{11}, wantWarn: true},
	}
	for _, tt := range tests {
		for _, reopen := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reopen=%v", tt.name, reopen), func(t *testing.T) {
				env, clock, h := retentionEnv(t, tt.age, tt.size)
				l := env.mustOpen(t, nil)
				appendHourly(t, l, clock, 1, 11)
				if got := segStarts(t, env.dir); !slices.Equal(got, all) {
					t.Fatalf("segments before retention = %v, want %v", got, all)
				}
				if reopen { // newest-record times then come from recovery, not rotation
					mustClose(t, l)
					l = env.mustOpen(t, nil)
				}
				clock.set(retentionBase.Add(12 * time.Hour))

				l.StartRetention(func() uint64 { return tt.lowWater })
				if err := l.EnforceRetention(); err != nil {
					t.Fatalf("EnforceRetention: %v", err)
				}
				if got := segStarts(t, env.dir); !slices.Equal(got, tt.want) {
					t.Fatalf("segments = %v, want %v", got, tt.want)
				}
				wantWarns := 0
				if tt.wantWarn {
					wantWarns = 1
				}
				if got := h.count(slog.LevelWarn, sizeWarning); got != wantWarns {
					t.Errorf("%d size warnings, want %d", got, wantWarns)
				}

				// ReadFrom still works after deletions.
				oldest := tt.want[0]
				if got := offsets(readAll(t, l, 1)); !slices.Equal(got, span(oldest, 11)) {
					t.Errorf("ReadFrom(1) = %v, want %v", got, span(oldest, 11))
				}
				if got := offsets(readAll(t, l, 8)); !slices.Equal(got, span(max(8, oldest), 11)) {
					t.Errorf("ReadFrom(8) = %v, want %v", got, span(max(8, oldest), 11))
				}

				// The log keeps working and reopens from the oldest retained
				// segment. (The rotation after record 12 grows the WAL and may
				// trigger another pass.)
				appendHourly(t, l, clock, 12, 12)
				mustClose(t, l)
				l = env.mustOpen(t, nil)
				if got := l.NextOffset(); got != 13 {
					t.Errorf("NextOffset after reopen = %d, want 13", got)
				}
				oldest = segStarts(t, env.dir)[0]
				if oldest < tt.want[0] || oldest > 13 {
					t.Errorf("oldest segment after reopen = %d, want in [%d, 13]", oldest, tt.want[0])
				}
				if got := offsets(readAll(t, l, 1)); !slices.Equal(got, span(oldest, 12)) {
					t.Errorf("ReadFrom(1) after reopen = %v, want %v", got, span(oldest, 12))
				}
			})
		}
	}
}

func TestRetentionWarningRateLimited(t *testing.T) {
	env, clock, h := retentionEnv(t, 0, 1)
	l := env.mustOpen(t, nil)
	appendHourly(t, l, clock, 1, 7)
	before := segStarts(t, env.dir)
	l.StartRetention(func() uint64 { return 1 }) // nothing processed

	steps := []struct {
		advance  time.Duration
		wantWarn int
	}{
		{advance: 0, wantWarn: 1},
		{advance: 0, wantWarn: 1},
		{advance: 30 * time.Second, wantWarn: 1},
		{advance: 29 * time.Second, wantWarn: 1},
		{advance: time.Second, wantWarn: 2}, // a full minute after the first warning
		{advance: 59 * time.Second, wantWarn: 2},
		{advance: 5 * time.Minute, wantWarn: 3},
	}
	for i, st := range steps {
		clock.advance(st.advance)
		if err := l.EnforceRetention(); err != nil {
			t.Fatalf("step %d: EnforceRetention: %v", i, err)
		}
		if got := h.count(slog.LevelWarn, sizeWarning); got != st.wantWarn {
			t.Errorf("step %d: %d warnings, want %d", i, got, st.wantWarn)
		}
	}
	if got := segStarts(t, env.dir); !slices.Equal(got, before) {
		t.Errorf("unprocessed segments deleted: %v, want %v", got, before)
	}
}

func TestRetentionRunsAfterRotationAndStopsOnClose(t *testing.T) {
	env, clock, _ := retentionEnv(t, 0, 1)
	l := env.mustOpen(t, nil)
	l.StartRetention(l.NextOffset)               // everything durable counts as processed
	l.StartRetention(func() uint64 { return 0 }) // ignored: already started

	appendHourly(t, l, clock, 1, 5) // rotations after 2 and 4; segments 1, 3, 5
	// The ticker runs once a minute, so prompt deletion proves the
	// post-rotation pass.
	deadline := time.Now().Add(10 * time.Second)
	for !slices.Equal(segStarts(t, env.dir), []uint64{5}) {
		if time.Now().After(deadline) {
			t.Fatalf("segments = %v, want [5] after rotation-triggered retention", segStarts(t, env.dir))
		}
		time.Sleep(time.Millisecond)
	}
	if got := offsets(readAll(t, l, 1)); !slices.Equal(got, []uint64{5}) {
		t.Errorf("ReadFrom(1) = %v, want [5]", got)
	}

	mustClose(t, l)
	select {
	case <-l.retDone:
	default:
		t.Fatal("retention goroutine still running after Close")
	}
	mustClose(t, l) // idempotent
	if err := l.EnforceRetention(); !errors.Is(err, ErrClosed) {
		t.Errorf("EnforceRetention after Close = %v, want ErrClosed", err)
	}
	l.StartRetention(l.NextOffset) // no-op after Close; must not start a goroutine or panic
}

func TestCloseWithoutRetention(t *testing.T) {
	env, _, _ := retentionEnv(t, time.Hour, 1)
	l := env.mustOpen(t, nil)
	mustClose(t, l)
	if l.retDone != nil {
		t.Error("retention goroutine started without StartRetention")
	}
}

func TestReadFromSurvivesConcurrentRetention(t *testing.T) {
	env, clock, _ := retentionEnv(t, 0, 1)
	l := env.mustOpen(t, nil)
	appendHourly(t, l, clock, 1, 11) // segments 1,3,5,7,9 sealed, 11 active
	// The rotations left a signal pending; drop it so the background pass
	// it would trigger cannot run before or between the reads below and
	// delete what they are about to read (only the explicit passes inside
	// the reads are under test). Without this the test failed under load.
	select {
	case <-l.rotated:
	default:
	}
	l.StartRetention(l.NextOffset)

	// A pass started while ReadFrom(1) runs deletes nothing it still needs.
	var got []uint64
	err := l.ReadFrom(1, func(r Record) error {
		if r.Offset == 1 {
			if err := l.EnforceRetention(); err != nil {
				return err
			}
		}
		got = append(got, r.Offset)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(1): %v", err)
	}
	if !slices.Equal(got, span(1, 11)) {
		t.Errorf("ReadFrom(1) = %v, want 1..11", got)
	}

	// While ReadFrom(5) runs, only segments wholly below 5 may go.
	got = nil
	err = l.ReadFrom(5, func(r Record) error {
		if r.Offset == 5 {
			if err := l.EnforceRetention(); err != nil {
				return err
			}
			if s := segStarts(t, env.dir); !slices.Equal(s, []uint64{5, 7, 9, 11}) {
				t.Errorf("segments during ReadFrom(5) = %v, want [5 7 9 11]", s)
			}
		}
		got = append(got, r.Offset)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(5): %v", err)
	}
	if !slices.Equal(got, span(5, 11)) {
		t.Errorf("ReadFrom(5) = %v, want 5..11", got)
	}

	// With no reader, every processed sealed segment goes.
	if err := l.EnforceRetention(); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if s := segStarts(t, env.dir); !slices.Equal(s, []uint64{11}) {
		t.Errorf("segments = %v, want [11]", s)
	}
	if got := offsets(readAll(t, l, 1)); !slices.Equal(got, []uint64{11}) {
		t.Errorf("ReadFrom(1) = %v, want [11]", got)
	}
}

func TestRetentionConcurrentWithAppendsAndReads(t *testing.T) {
	env := newEnv(t, segmentHeaderSize+2*recordSize(ptr(testRec(1))))
	env.retentionSize = 1
	l := env.mustOpen(t, nil)
	// Everything but the last few records counts as processed.
	lowWater := func() uint64 { return max(l.NextOffset(), 4) - 3 }
	l.StartRetention(lowWater)

	const n = 300
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	errc := make(chan error, 4)
	wg.Add(1)
	go func() { // retention pressure in addition to the post-rotation passes
		defer wg.Done()
		for ctx.Err() == nil {
			if err := l.EnforceRetention(); err != nil {
				errc <- err
				return
			}
		}
	}()
	for range 2 {
		wg.Add(1)
		go func() { // readers start at the low-water mark, as engine recovery would
			defer wg.Done()
			for ctx.Err() == nil {
				from := lowWater()
				prev := uint64(0)
				err := l.ReadFrom(from, func(r Record) error {
					if r.Offset < from || (prev != 0 && r.Offset != prev+1) {
						return fmt.Errorf("ReadFrom(%d) returned offset %d after %d", from, r.Offset, prev)
					}
					prev = r.Offset
					return nil
				})
				if err != nil {
					errc <- err
					return
				}
			}
		}()
	}
	for i := 1; i <= n; i++ {
		mustAppend(t, l, testRec(i))
	}
	cancel()
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
	if err := l.EnforceRetention(); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	mustClose(t, l)
	if got := len(segStarts(t, env.dir)); got > 4 {
		t.Errorf("%d segments left, want retention to have removed processed ones", got)
	}
}

func TestNewestRecordTime(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		offsets []uint64
		want    time.Time
	}{
		{name: "records", offsets: []uint64{4, 5, 6}, want: time.Unix(0, 6)},
		{name: "empty", want: time.Time{}},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeSegment(t, dir, uint64(100*(i+1)), tt.offsets...)
			got, err := newestRecordTime(path)
			if err != nil {
				t.Fatalf("newestRecordTime: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("newestRecordTime = %v, want %v", got, tt.want)
			}
		})
	}
	t.Run("corrupt", func(t *testing.T) {
		path := writeSegment(t, dir, 1000, 1000, 1001)
		flipByte(t, path, segmentHeaderSize+10)
		if _, err := newestRecordTime(path); !errors.Is(err, ErrCorrupt) {
			t.Errorf("newestRecordTime = %v, want ErrCorrupt", err)
		}
	})
	t.Run("used when no time is recorded", func(t *testing.T) {
		env, clock, _ := retentionEnv(t, 9*time.Hour+30*time.Minute, 0)
		l := env.mustOpen(t, nil)
		appendHourly(t, l, clock, 1, 5)
		l.metaMu.Lock()
		clear(l.segTimes)
		l.metaMu.Unlock()
		clock.set(retentionBase.Add(12 * time.Hour))
		l.StartRetention(l.NextOffset)
		if err := l.EnforceRetention(); err != nil {
			t.Fatalf("EnforceRetention: %v", err)
		}
		// Segment 1 (newest record 10h old) goes; segment 3 (8h) stays.
		if got := segStarts(t, env.dir); !slices.Equal(got, []uint64{3, 5}) {
			t.Errorf("segments = %v, want [3 5]", got)
		}
	})
}
