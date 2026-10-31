// Package wal implements the cachydb write-ahead log: a sequence of segment
// files holding self-describing, CRC32C-protected records with consecutive
// offsets, written by a single group-commit goroutine.
//
// Segments live directly in the engine root as wal-<first offset>.log, the
// offset zero-padded to 20 digits. Open scans every segment, truncates a
// torn tail of the newest one, and continues appending to it; damage
// anywhere else is reported as ErrCorrupt. Processed sealed segments are
// deleted by retention (see StartRetention). Formats are specified in §3 of
// the storage-engine design.
package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

// DefaultSegmentSize is used when Options.SegmentSize is not positive.
const DefaultSegmentSize int64 = 64 << 20

// Group-commit batch caps (§3.2). A single record larger than
// maxBatchBytes is still written, alone.
const (
	maxBatchRecords = 1024
	maxBatchBytes   = 4 << 20
)

var (
	ErrCorrupt = errors.New("wal: corrupt") // mid-log damage or offset gap
	ErrFailed  = errors.New("wal: failed")  // after an fsync error; sticky
	ErrClosed  = errors.New("wal: closed")
)

// Options configures a Log.
type Options struct {
	Dir           string        // the engine root (WAL files live directly in it)
	SegmentSize   int64         // rotate once the active segment reaches this size; <= 0 means DefaultSegmentSize
	RetentionAge  time.Duration // delete processed segments whose newest record is older; <= 0 = no age limit
	RetentionSize int64         // delete processed segments while the WAL is larger; <= 0 = no size limit
	Logger        *slog.Logger  // nil means slog.Default()
}

type result struct {
	off uint64
	err error
}

type request struct {
	rec   Record
	size  int64
	reply chan result // buffered (1); the writer never blocks on it
}

type failure struct{ err error }

// Log is an open write-ahead log. It is safe for concurrent use.
type Log struct {
	dir         string
	segmentSize int64
	logger      *slog.Logger
	syncFn      func(*os.File) error // fsync; replaced in tests for fault injection
	now         func() time.Time     // record timestamps and retention age; replaced in tests

	retentionAge  time.Duration
	retentionSize int64

	reqs chan request
	done chan struct{} // closed when the writer goroutine has exited

	mu     sync.RWMutex // guards closed, sending on / closing reqs, and the retention start fields below
	closed bool

	// Retention goroutine (StartRetention); guarded by mu.
	lowWater   func() uint64
	retStarted bool
	retStop    chan struct{} // closed by Close
	retDone    chan struct{} // closed when the retention goroutine has exited
	rotated    chan struct{} // buffered (1); the writer signals each rotation

	// retMu serializes retention passes and guards pins and lastWarn. A
	// pass holds it from computing the deletable set until the deletions
	// are durable, so a reader that pinned its start offset first never
	// loses a segment it still needs.
	retMu    sync.Mutex
	pins     map[uint64]uint64 // reader id -> smallest offset it may still read
	pinSeq   uint64
	lastWarn time.Time

	// segTimes holds the newest record time of each sealed segment, keyed
	// by first offset. Filled by recovery and rotation; guarded by metaMu.
	metaMu   sync.Mutex
	segTimes map[uint64]time.Time

	nextOff atomic.Uint64           // next offset to assign; everything below is durable
	failed  atomic.Pointer[failure] // set once a write or fsync fails

	// Owned by the writer goroutine after Open returns.
	f        *os.File // active segment
	fname    string
	fstart   uint64 // first offset of the active segment
	fsize    int64
	ftime    time.Time // newest record time in the active segment
	next     uint64
	buf      []byte
	carry    *request // received but did not fit into the previous batch
	closeErr error    // written before done is closed
}

// Open scans all segments, truncates a torn tail of the newest one,
// computes the next offset, and starts the group-commit writer.
//
// An empty directory gets a first segment starting at offset 1. Damage
// anywhere but the final record of the newest segment, or a gap in the
// offset sequence, fails with an error wrapping ErrCorrupt.
func Open(opts Options) (*Log, error) {
	return open(opts, syncFile, time.Now)
}

func syncFile(f *os.File) error { return f.Sync() }

func open(opts Options, syncFn func(*os.File) error, now func() time.Time) (*Log, error) {
	if opts.Dir == "" {
		return nil, errors.New("wal: Options.Dir is required")
	}
	l := &Log{
		dir:           opts.Dir,
		segmentSize:   opts.SegmentSize,
		logger:        opts.Logger,
		syncFn:        syncFn,
		now:           now,
		retentionAge:  max(opts.RetentionAge, 0),
		retentionSize: max(opts.RetentionSize, 0),
		pins:          make(map[uint64]uint64),
		segTimes:      make(map[uint64]time.Time),
		rotated:       make(chan struct{}, 1),
	}
	if l.segmentSize <= 0 {
		l.segmentSize = DefaultSegmentSize
	}
	if l.logger == nil {
		l.logger = slog.Default()
	}
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: creating %s: %w", l.dir, err)
	}
	if err := l.recover(); err != nil {
		return nil, err
	}
	l.nextOff.Store(l.next)
	l.reqs = make(chan request, maxBatchRecords)
	l.done = make(chan struct{})
	go l.run()
	return l, nil
}

// recover implements §3.3: it validates every segment, truncates a torn
// tail of the newest one, and leaves the newest segment open for append.
func (l *Log) recover() error {
	segs, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		path := filepath.Join(l.dir, segmentFileName(1))
		f, err := createSegment(path, l.syncFn)
		if err != nil {
			return err
		}
		if err := fsutil.SyncDir(l.dir); err != nil {
			_ = f.Close() // already failing; the sync error is reported
			return fmt.Errorf("wal: %w", err)
		}
		l.f, l.fname, l.fstart, l.fsize, l.next = f, path, 1, segmentHeaderSize, 1
		return nil
	}

	expected := segs[0].start
	if expected == 0 {
		return corruptErr(segs[0].path, 0, "segment named for offset 0")
	}
	for i, s := range segs {
		newest := i == len(segs)-1
		if s.start != expected {
			return corruptErr(s.path, 0, fmt.Sprintf("segment starts at offset %d, expected %d", s.start, expected))
		}
		flag := os.O_RDONLY
		if newest {
			flag = os.O_RDWR
		}
		f, err := os.OpenFile(s.path, flag, 0)
		if err != nil {
			return fmt.Errorf("wal: opening %s: %w", s.path, err)
		}
		var last time.Time
		end, err := l.scanSegment(f, s.path, newest, &expected, &last)
		if err != nil {
			_ = f.Close() // already failing; the scan error is reported
			return err
		}
		if !newest {
			if err := f.Close(); err != nil {
				return fmt.Errorf("wal: closing %s: %w", s.path, err)
			}
			l.segTimes[s.start] = last
			continue
		}
		if _, err := f.Seek(end, io.SeekStart); err != nil {
			_ = f.Close() // already failing; the seek error is reported
			return fmt.Errorf("wal: seeking %s: %w", s.path, err)
		}
		l.f, l.fname, l.fstart, l.fsize, l.ftime = f, s.path, s.start, end, last
	}
	l.next = expected
	return nil
}

// scanSegment validates one segment, advancing *expected past every record.
// For the newest segment a torn tail is truncated (and a torn header
// rewritten). It returns the end of the last valid record and sets *last
// to that record's time (left unchanged if the segment holds no records).
func (l *Log) scanSegment(f *os.File, path string, newest bool, expected *uint64, last *time.Time) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal: stat %s: %w", path, err)
	}
	size := fi.Size()

	var hdrErr error
	if size < segmentHeaderSize {
		hdrErr = errors.New("incomplete segment header")
	} else {
		hdr := make([]byte, segmentHeaderSize)
		if _, err := f.ReadAt(hdr, 0); err != nil {
			return 0, fmt.Errorf("wal: reading header of %s: %w", path, err)
		}
		hdrErr = checkSegmentHeader(hdr)
	}
	if hdrErr != nil {
		if !newest {
			return 0, corruptErr(path, 0, hdrErr.Error())
		}
		// A short or all-zero newest segment is an interrupted segment
		// creation: no record in it was ever acknowledged.
		torn := size < segmentHeaderSize
		if !torn {
			zero, err := allZero(f, 0, size)
			if err != nil {
				return 0, fmt.Errorf("wal: reading %s: %w", path, err)
			}
			torn = zero
		}
		if !torn {
			return 0, corruptErr(path, 0, hdrErr.Error())
		}
		if err := l.truncate(f, path, 0, size); err != nil {
			return 0, err
		}
		if _, err := f.WriteAt(segmentHeader(), 0); err != nil {
			return 0, fmt.Errorf("wal: rewriting header of %s: %w", path, err)
		}
		if err := l.syncFn(f); err != nil {
			return 0, fmt.Errorf("wal: syncing %s: %w", path, err)
		}
		return segmentHeaderSize, nil
	}

	rr := newRecordReader(f, size)
	for {
		rec, err := rr.next()
		if err == io.EOF {
			return size, nil
		}
		var re *recordError
		if errors.As(err, &re) {
			if newest {
				torn, err := isTornTail(f, re, size)
				if err != nil {
					return 0, fmt.Errorf("wal: reading %s: %w", path, err)
				}
				if torn {
					if err := l.truncate(f, path, re.pos, size); err != nil {
						return 0, err
					}
					return re.pos, nil
				}
			}
			return 0, corruptErr(path, re.pos, re.reason)
		}
		if err != nil {
			return 0, fmt.Errorf("wal: reading %s: %w", path, err)
		}
		if rec.Offset != *expected {
			return 0, corruptErr(path, rr.pos-recordSize(&rec), fmt.Sprintf("record offset %d, expected %d", rec.Offset, *expected))
		}
		*expected++
		*last = rec.Time
	}
}

// isTornTail decides whether a damaged record is the torn final write of
// the newest segment: it runs past EOF, its checksum fails and it ends
// exactly at EOF, or everything from it to EOF is zero (unwritten pages).
func isTornTail(f *os.File, re *recordError, size int64) (bool, error) {
	switch {
	case re.kind == errKindShort:
		return true, nil
	case re.kind == errKindCRC && re.end == size:
		return true, nil
	default:
		return allZero(f, re.pos, size)
	}
}

func (l *Log) truncate(f *os.File, path string, at, size int64) error {
	if err := f.Truncate(at); err != nil {
		return fmt.Errorf("wal: truncating %s: %w", path, err)
	}
	if err := l.syncFn(f); err != nil {
		return fmt.Errorf("wal: syncing %s: %w", path, err)
	}
	l.logger.Warn("wal: truncated torn tail", "file", path, "offset", at, "removed_bytes", size-at)
	return nil
}

// Append blocks until r is durable (group commit, §3.2) and returns its
// offset. Offset and Time are assigned by the log. The caller must not
// modify r.Payload after calling Append.
//
// If ctx is already done, Append returns ctx.Err() without writing r. If
// ctx is done while waiting, Append stops waiting and returns ctx.Err(),
// but r may still be written and become durable.
//
// After a write or fsync failure every Append fails with an error wrapping
// ErrFailed; after Close it fails with ErrClosed.
func (l *Log) Append(ctx context.Context, r Record) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validateRecord(&r); err != nil {
		return 0, err
	}
	req := request{rec: r, size: recordSize(&r), reply: make(chan result, 1)}

	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return 0, ErrClosed
	}
	if err := l.failure(); err != nil {
		l.mu.RUnlock()
		return 0, err
	}
	select {
	case l.reqs <- req:
	case <-ctx.Done():
		l.mu.RUnlock()
		return 0, ctx.Err()
	}
	l.mu.RUnlock()

	select {
	case res := <-req.reply:
		return res.off, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// NextOffset returns the offset the next appended record will get. Every
// record below it is durable.
func (l *Log) NextOffset() uint64 { return l.nextOff.Load() }

func (l *Log) failure() error {
	if f := l.failed.Load(); f != nil {
		return f.err
	}
	return nil
}

func (l *Log) fail(err error) {
	l.failed.CompareAndSwap(nil, &failure{err: err})
	l.logger.Error("wal: log failed; restart required", "err", err)
}

// run is the single writer goroutine. It exits after Close closes reqs and
// every queued request has been answered.
func (l *Log) run() {
	defer close(l.done)
	batch := make([]request, 0, maxBatchRecords)
	for {
		var first request
		if l.carry != nil {
			first, l.carry = *l.carry, nil
		} else {
			r, ok := <-l.reqs
			if !ok {
				break
			}
			first = r
		}
		batch = l.fill(append(batch[:0], first), first.size)
		l.commit(batch)
		clear(batch) // drop references to payloads and reply channels
	}
	l.closeErr = l.shutdown()
}

// fill drains already-queued requests without blocking, up to the batch caps.
func (l *Log) fill(batch []request, size int64) []request {
	for len(batch) < maxBatchRecords {
		select {
		case r, ok := <-l.reqs:
			if !ok {
				return batch
			}
			if size+r.size > maxBatchBytes {
				l.carry = &r
				return batch
			}
			batch = append(batch, r)
			size += r.size
		default:
			return batch
		}
	}
	return batch
}

// commit writes and syncs one batch, replies to every waiter, then rotates
// the segment if it has reached the size limit.
func (l *Log) commit(batch []request) {
	if err := l.failure(); err != nil {
		replyAll(batch, err)
		return
	}
	now := l.now()
	buf := l.buf[:0]
	for i := range batch {
		batch[i].rec.Offset = l.next + uint64(i)
		batch[i].rec.Time = now
		buf = appendRecord(buf, &batch[i].rec)
	}
	if cap(buf) <= 2*maxBatchBytes {
		l.buf = buf
	} else {
		l.buf = nil // don't pin an unusually large buffer
	}

	// A failed write may leave a partial record and a failed fsync may have
	// dropped dirty pages: never retry, fail the log (§3.2).
	if _, err := l.f.Write(buf); err != nil {
		l.fail(fmt.Errorf("%w: writing %s: %w", ErrFailed, l.fname, err))
		replyAll(batch, l.failure())
		return
	}
	if err := l.syncFn(l.f); err != nil {
		l.fail(fmt.Errorf("%w: syncing %s: %w", ErrFailed, l.fname, err))
		replyAll(batch, l.failure())
		return
	}

	first := l.next
	l.next += uint64(len(batch))
	l.fsize += int64(len(buf))
	l.ftime = now
	l.nextOff.Store(l.next)
	for i := range batch {
		batch[i].reply <- result{off: first + uint64(i)}
	}

	if l.fsize >= l.segmentSize {
		if err := l.rotate(); err != nil {
			l.fail(fmt.Errorf("%w: rotating segment: %w", ErrFailed, err))
			return
		}
		select {
		case l.rotated <- struct{}{}:
		default: // a retention pass is already pending
		}
	}
}

func replyAll(batch []request, err error) {
	for i := range batch {
		batch[i].reply <- result{err: err}
	}
}

// rotate starts a new segment named after the next offset. The active
// segment was synced by the batch that filled it.
func (l *Log) rotate() error {
	path := filepath.Join(l.dir, segmentFileName(l.next))
	nf, err := createSegment(path, l.syncFn)
	if err != nil {
		return err
	}
	if err := fsutil.SyncDir(l.dir); err != nil {
		_ = nf.Close() // already failing; the sync error is reported
		return fmt.Errorf("wal: %w", err)
	}
	old, oldName := l.f, l.fname
	l.metaMu.Lock()
	l.segTimes[l.fstart] = l.ftime
	l.metaMu.Unlock()
	l.f, l.fname, l.fstart, l.fsize, l.ftime = nf, path, l.next, segmentHeaderSize, time.Time{}
	if err := old.Close(); err != nil {
		// Its contents were already synced; the new segment is in use.
		l.logger.Warn("wal: closing rotated segment", "file", oldName, "err", err)
	}
	return nil
}

// shutdown syncs (unless the log has failed) and closes the active segment.
func (l *Log) shutdown() error {
	var errs []error
	if l.failure() == nil {
		if err := l.syncFn(l.f); err != nil {
			errs = append(errs, fmt.Errorf("wal: syncing %s on close: %w", l.fname, err))
		}
	}
	if err := l.f.Close(); err != nil {
		errs = append(errs, fmt.Errorf("wal: closing %s: %w", l.fname, err))
	}
	return errors.Join(errs...)
}

// Close stops accepting appends, lets the writer finish every queued
// request, syncs and closes the active segment, stops the retention
// goroutine, and waits for both to exit. It is idempotent; later Appends
// return ErrClosed.
func (l *Log) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.reqs)
		if l.retStarted {
			close(l.retStop)
		}
	}
	retDone := l.retDone
	l.mu.Unlock()
	<-l.done
	if retDone != nil {
		<-retDone
	}
	return l.closeErr
}

// ReadFrom streams records with Offset >= from in order; stops on fn error.
//
// Only records that were durable when ReadFrom was called are visible. It
// reads one segment at a time and never loads the whole log into memory.
// An error returned by fn is returned unchanged; damage or an offset gap
// returns an error wrapping ErrCorrupt.
//
// Retention never deletes a segment holding offsets >= from while ReadFrom
// runs. If from is below the oldest retained record, reading starts at the
// oldest retained record. fn may call any Log method.
func (l *Log) ReadFrom(from uint64, fn func(Record) error) error {
	limit := l.nextOff.Load() // exclusive
	if from >= limit {
		return nil
	}
	unpin := l.pin(from)
	defer unpin()
	segs, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return fmt.Errorf("%w: no segments in %s", ErrCorrupt, l.dir)
	}
	i := sort.Search(len(segs), func(i int) bool { return segs[i].start > from }) - 1
	if i < 0 {
		i = 0
	}
	expected := segs[i].start
	for ; i < len(segs) && expected < limit; i++ {
		s := segs[i]
		if s.start != expected {
			return corruptErr(s.path, 0, fmt.Sprintf("segment starts at offset %d, expected %d", s.start, expected))
		}
		if err := readSegment(s.path, from, limit, &expected, fn); err != nil {
			return err
		}
	}
	if expected < limit {
		return fmt.Errorf("%w: log in %s ends before offset %d, expected records up to %d", ErrCorrupt, l.dir, expected, limit-1)
	}
	return nil
}

// readSegment streams the records of one segment with offsets in
// [from, limit) to fn, verifying offsets continue from *expected.
func readSegment(path string, from, limit uint64, expected *uint64, fn func(Record) error) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("wal: opening %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("wal: closing %s: %w", path, cerr)
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat %s: %w", path, err)
	}
	size := fi.Size()
	hdr := make([]byte, segmentHeaderSize)
	if size < segmentHeaderSize {
		return corruptErr(path, 0, "incomplete segment header")
	}
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return fmt.Errorf("wal: reading header of %s: %w", path, err)
	}
	if err := checkSegmentHeader(hdr); err != nil {
		return corruptErr(path, 0, err.Error())
	}

	rr := newRecordReader(f, size)
	for *expected < limit {
		rec, err := rr.next()
		if err == io.EOF {
			return nil
		}
		var re *recordError
		if errors.As(err, &re) {
			return corruptErr(path, re.pos, re.reason)
		}
		if err != nil {
			return fmt.Errorf("wal: reading %s: %w", path, err)
		}
		if rec.Offset != *expected {
			return corruptErr(path, rr.pos-recordSize(&rec), fmt.Sprintf("record offset %d, expected %d", rec.Offset, *expected))
		}
		*expected++
		if rec.Offset >= from {
			if err := fn(rec); err != nil {
				return err
			}
		}
	}
	return nil
}
