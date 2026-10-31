package wal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

// retentionInterval is how often the retention goroutine runs a pass in
// addition to the pass after each rotation (§3.4).
const retentionInterval = time.Minute

// retentionWarnInterval rate-limits the "WAL over size limit but nothing
// deletable" warning.
const retentionWarnInterval = time.Minute

// StartRetention starts the retention goroutine (§3.4): it runs a pass on
// a 1-minute ticker and after each segment rotation, and exits on Close.
// lowWater returns the smallest offset that is not yet processed; only
// sealed segments whose last offset is below it may be deleted. lowWater
// must be safe to call from another goroutine.
//
// Only the first call has an effect; calls after Close are ignored.
func (l *Log) StartRetention(lowWater func() uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.retStarted {
		return
	}
	l.lowWater = lowWater
	l.retStarted = true
	l.retStop = make(chan struct{})
	l.retDone = make(chan struct{})
	go l.retentionLoop(l.retStop, l.retDone)
}

func (l *Log) retentionLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(retentionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		case <-l.rotated:
		}
		if err := l.enforce(); err != nil {
			l.logger.Warn("wal: retention pass failed", "err", err)
		}
	}
}

// EnforceRetention runs one retention pass (§3.4). Processed sealed
// segments are deleted oldest-first while the oldest one's newest record
// is older than Options.RetentionAge or the total WAL size exceeds
// Options.RetentionSize. The active segment and segments holding offsets
// at or above the low-water mark are never deleted. Before StartRetention
// supplies a low-water function nothing counts as processed.
//
// It returns ErrClosed after Close.
func (l *Log) EnforceRetention() error {
	l.mu.RLock()
	closed := l.closed
	l.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return l.enforce()
}

func (l *Log) enforce() error {
	if l.retentionAge <= 0 && l.retentionSize <= 0 {
		return nil // keep all processed segments forever
	}
	// Ask the engine before taking retMu: lowWater may take engine locks.
	l.mu.RLock()
	lowWaterFn := l.lowWater
	l.mu.RUnlock()
	var lowWater uint64
	if lowWaterFn != nil {
		lowWater = min(lowWaterFn(), l.nextOff.Load())
	}

	l.retMu.Lock()
	defer l.retMu.Unlock()
	for _, p := range l.pins {
		lowWater = min(lowWater, p)
	}

	segs, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	sizes := make([]int64, len(segs))
	var total int64
	for i, s := range segs {
		fi, err := os.Stat(s.path)
		if err != nil {
			return fmt.Errorf("wal: stat %s: %w", s.path, err)
		}
		sizes[i] = fi.Size()
		total += sizes[i]
	}

	now := l.now()
	overSize := func() bool { return l.retentionSize > 0 && total > l.retentionSize }
	var removed int
	var errs []error
	blocked := false // stopped at a segment that is not yet processed (or the active one)
	// The newest listed segment is (or was) the active one: never a candidate.
	for i := 0; i < len(segs)-1; i++ {
		// A sealed segment's last offset is the next segment's start - 1.
		if segs[i+1].start > lowWater {
			blocked = true
			break
		}
		expired := false
		if l.retentionAge > 0 {
			newest, err := l.segmentTime(segs[i])
			if err != nil {
				errs = append(errs, err)
				break
			}
			expired = now.Sub(newest) > l.retentionAge
		}
		if !expired && !overSize() {
			break
		}
		if err := os.Remove(segs[i].path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("wal: retention removing %s: %w", segs[i].path, err))
			break
		}
		l.metaMu.Lock()
		delete(l.segTimes, segs[i].start)
		l.metaMu.Unlock()
		total -= sizes[i]
		removed++
		l.logger.Debug("wal: retention removed segment", "file", segs[i].path, "last_offset", segs[i+1].start-1)
	}
	if len(segs) > 0 && removed == len(segs)-1 {
		blocked = true // only the active segment is left
	}
	if removed > 0 {
		if err := fsutil.SyncDir(l.dir); err != nil {
			errs = append(errs, fmt.Errorf("wal: retention: %w", err))
		}
	}

	if blocked && overSize() && (l.lastWarn.IsZero() || now.Sub(l.lastWarn) >= retentionWarnInterval) {
		l.lastWarn = now
		l.logger.Warn("wal: size exceeds retention limit but the remaining segments are not yet processed; the flusher may be falling behind",
			"wal_bytes", total, "retention_size", l.retentionSize, "low_water", lowWater)
	}
	return errors.Join(errs...)
}

// segmentTime returns the newest record time of a sealed segment, reading
// the segment if recovery or rotation did not record it.
func (l *Log) segmentTime(s segmentInfo) (time.Time, error) {
	l.metaMu.Lock()
	t, ok := l.segTimes[s.start]
	l.metaMu.Unlock()
	if ok {
		return t, nil
	}
	t, err := newestRecordTime(s.path)
	if err != nil {
		return time.Time{}, err
	}
	l.metaMu.Lock()
	l.segTimes[s.start] = t
	l.metaMu.Unlock()
	return t, nil
}

// newestRecordTime scans a sealed segment and returns the time of its last
// record (zero for a segment without records).
func newestRecordTime(path string) (t time.Time, err error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, fmt.Errorf("wal: opening %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("wal: closing %s: %w", path, cerr)
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return time.Time{}, fmt.Errorf("wal: stat %s: %w", path, err)
	}
	size := fi.Size()
	if size < segmentHeaderSize {
		return time.Time{}, corruptErr(path, 0, "incomplete segment header")
	}
	hdr := make([]byte, segmentHeaderSize)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return time.Time{}, fmt.Errorf("wal: reading header of %s: %w", path, err)
	}
	if err := checkSegmentHeader(hdr); err != nil {
		return time.Time{}, corruptErr(path, 0, err.Error())
	}
	rr := newRecordReader(f, size)
	for {
		rec, err := rr.next()
		if err == io.EOF {
			return t, nil
		}
		var re *recordError
		if errors.As(err, &re) {
			return time.Time{}, corruptErr(path, re.pos, re.reason)
		}
		if err != nil {
			return time.Time{}, fmt.Errorf("wal: reading %s: %w", path, err)
		}
		t = rec.Time
	}
}

// pin keeps every segment holding offsets >= from until the returned
// function is called.
func (l *Log) pin(from uint64) (unpin func()) {
	l.retMu.Lock()
	l.pinSeq++
	id := l.pinSeq
	l.pins[id] = from
	l.retMu.Unlock()
	return func() {
		l.retMu.Lock()
		delete(l.pins, id)
		l.retMu.Unlock()
	}
}
