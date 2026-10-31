package segment

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
)

var errWriterClosed = errors.New("segment: writer closed")

// Writer appends records to the active data segment. Not safe for
// concurrent use.
//
// Records are written at an explicitly tracked end-of-data position, never
// at the file's current offset, so appending after a reopen can never
// overwrite existing records. A write or fsync error is sticky: every later
// Append, Sync, or Close returns it.
type Writer struct {
	seg       uint32
	path      string
	f         *os.File
	size      int64 // end of the last record (== file size)
	buf       []byte
	err       error // sticky write/fsync failure
	closed    bool
	truncated int64 // bytes removed by OpenActive's torn-tail truncation
}

// Create creates segment seg in dir, writes its header, and fsyncs the file
// and dir. It fails if the segment already exists.
func Create(dir string, seg uint32) (*Writer, error) {
	path := DataPath(dir, seg)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment: creating %s: %w", path, err)
	}
	if _, err := f.Write(fileHeader()); err != nil {
		_ = f.Close() // already failing; the write error is reported
		return nil, fmt.Errorf("segment: writing header of %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close() // already failing; the sync error is reported
		return nil, fmt.Errorf("segment: syncing %s: %w", path, err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		_ = f.Close() // already failing; the sync error is reported
		return nil, fmt.Errorf("segment: creating %s: %w", path, err)
	}
	return &Writer{seg: seg, path: path, f: f, size: headerSize}, nil
}

// OpenActive opens existing segment seg in dir for append. It validates
// every record; a damaged final record (torn write) is truncated and the
// file fsynced. A torn or all-zero header (interrupted Create) is
// rewritten. Damage anywhere else fails with an error wrapping ErrCorrupt.
func OpenActive(dir string, seg uint32) (*Writer, error) {
	path := DataPath(dir, seg)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("segment: opening %s: %w", path, err)
	}
	w := &Writer{seg: seg, path: path, f: f}
	if err := w.recover(); err != nil {
		_ = f.Close() // already failing; the recovery error is reported
		return nil, err
	}
	return w, nil
}

// recover validates the file and sets w.size to the end of the last valid
// record, truncating a torn tail.
func (w *Writer) recover() error {
	fi, err := w.f.Stat()
	if err != nil {
		return fmt.Errorf("segment: stat %s: %w", w.path, err)
	}
	size := fi.Size()

	hdrErr, ioErr := readHeader(w.f, size)
	if ioErr != nil {
		return fmt.Errorf("segment: reading header of %s: %w", w.path, ioErr)
	}
	if hdrErr != nil {
		// A short or all-zero file is an interrupted Create: nothing in it
		// was ever written by Append.
		torn := size < headerSize
		if !torn {
			if torn, err = allZero(w.f, 0, size); err != nil {
				return fmt.Errorf("segment: reading %s: %w", w.path, err)
			}
		}
		if !torn {
			return corruptErr(w.path, 0, hdrErr.Error())
		}
		if err := w.truncate(0, size); err != nil {
			return err
		}
		if _, err := w.f.WriteAt(fileHeader(), 0); err != nil {
			return fmt.Errorf("segment: rewriting header of %s: %w", w.path, err)
		}
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("segment: syncing %s: %w", w.path, err)
		}
		w.size = headerSize
		return nil
	}

	rr := newRecordReader(w.f, size)
	for {
		_, _, err := rr.next()
		if err == io.EOF {
			w.size = size
			return nil
		}
		var re *recordError
		if errors.As(err, &re) {
			torn, terr := isTornTail(w.f, re, size)
			if terr != nil {
				return fmt.Errorf("segment: reading %s: %w", w.path, terr)
			}
			if !torn {
				return corruptErr(w.path, re.pos, re.reason)
			}
			if err := w.truncate(re.pos, size); err != nil {
				return err
			}
			w.size = re.pos
			return nil
		}
		if err != nil {
			return fmt.Errorf("segment: reading %s: %w", w.path, err)
		}
	}
}

func (w *Writer) truncate(at, size int64) error {
	if err := w.f.Truncate(at); err != nil {
		return fmt.Errorf("segment: truncating %s: %w", w.path, err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("segment: syncing %s: %w", w.path, err)
	}
	w.truncated = size - at
	return nil
}

func (w *Writer) usable() error {
	if w.closed {
		return errWriterClosed
	}
	return w.err
}

// Append encodes recs and writes them with a single write at the end of the
// segment. It does not fsync; call Sync. On a validation or encoding error
// nothing is written. The returned locations correspond to recs by index.
func (w *Writer) Append(recs []Record) ([]Location, error) {
	if err := w.usable(); err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, nil
	}
	for i := range recs {
		if err := validateRecord(&recs[i]); err != nil {
			return nil, err
		}
	}
	buf := w.buf[:0]
	locs := make([]Location, len(recs))
	for i := range recs {
		start := len(buf)
		var err error
		if buf, err = appendRecord(buf, &recs[i]); err != nil {
			return nil, err
		}
		locs[i] = Location{Seg: w.seg, Pos: uint64(w.size) + uint64(start), Size: uint32(len(buf) - start)}
	}
	w.buf = buf

	if _, err := w.f.WriteAt(buf, w.size); err != nil {
		// Drop any partially written bytes so the file never holds a
		// half-written record past w.size; best effort, the writer is
		// failed either way.
		_ = w.f.Truncate(w.size)
		w.err = fmt.Errorf("segment: writing %s: %w", w.path, err)
		return nil, w.err
	}
	w.size += int64(len(buf))
	if cap(w.buf) > 4<<20 {
		w.buf = nil // don't pin a large batch buffer
	}
	return locs, nil
}

// Sync fsyncs the segment file.
func (w *Writer) Sync() error {
	if err := w.usable(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("segment: syncing %s: %w", w.path, err)
		return w.err
	}
	return nil
}

// Size returns the segment size in bytes (header included).
func (w *Writer) Size() int64 { return w.size }

// Seg returns the segment id.
func (w *Writer) Seg() uint32 { return w.seg }

// TruncatedBytes returns how many bytes OpenActive removed as a torn tail
// (0 if none), so the caller can log it.
func (w *Writer) TruncatedBytes() int64 { return w.truncated }

// Close fsyncs and closes the segment file. The file is closed even if the
// fsync fails or the writer had failed earlier; the first error is
// returned.
func (w *Writer) Close() error {
	if w.closed {
		return errWriterClosed
	}
	w.closed = true
	err := w.err
	if err == nil {
		if serr := w.f.Sync(); serr != nil {
			err = fmt.Errorf("segment: syncing %s: %w", w.path, serr)
		}
	}
	if cerr := w.f.Close(); cerr != nil && err == nil {
		err = fmt.Errorf("segment: closing %s: %w", w.path, cerr)
	}
	return err
}
