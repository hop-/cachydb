package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Reader gives shared, concurrency-safe random access to one data segment
// through a single *os.File (ReadAt is safe for concurrent use).
//
// Lifetime is reference counted. The owner (the collection's segment
// table) holds an implicit reference; each reader of the segment brackets
// its Read calls with Acquire/Release. Retire drops the owner's reference
// and marks the segment for deletion: no new Acquire succeeds, and the file
// is closed and removed once the last outstanding reference is released.
// Close is the same without removing the file (engine shutdown).
type Reader struct {
	seg  uint32
	path string
	f    *os.File

	mu        sync.Mutex
	refs      int
	closing   bool // Retire or Close called: Acquire fails, file closes at refs == 0
	retired   bool // Retire called: the file is removed when closed
	finalized bool // the file has been closed
}

// OpenReader opens segment seg in dir for reading and validates its header.
func OpenReader(dir string, seg uint32) (*Reader, error) {
	path := DataPath(dir, seg)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment: opening %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close() // already failing; the stat error is reported
		return nil, fmt.Errorf("segment: stat %s: %w", path, err)
	}
	hdrErr, ioErr := readHeader(f, fi.Size())
	if ioErr == nil && hdrErr != nil {
		ioErr = corruptErr(path, 0, hdrErr.Error())
	} else if ioErr != nil {
		ioErr = fmt.Errorf("segment: reading header of %s: %w", path, ioErr)
	}
	if ioErr != nil {
		_ = f.Close() // already failing; the header error is reported
		return nil, ioErr
	}
	return &Reader{seg: seg, path: path, f: f}, nil
}

// Seg returns the segment id.
func (r *Reader) Seg() uint32 { return r.seg }

// Read reads the record at loc, verifies its checksum, and decompresses
// the document. Damaged data yields an error wrapping ErrCorrupt. The
// caller must hold a reference (Acquire) for the duration of the call.
func (r *Reader) Read(loc Location) (Record, error) {
	if loc.Seg != r.seg {
		return Record{}, fmt.Errorf("segment: location for segment %d read from segment %d", loc.Seg, r.seg)
	}
	pos := int64(loc.Pos)
	if loc.Pos > uint64(1<<63-1) || loc.Size < recordHeaderSize+recordFixedBody || pos < headerSize {
		return Record{}, corruptErr(r.path, pos, fmt.Sprintf("invalid record location (pos %d, size %d)", loc.Pos, loc.Size))
	}
	buf := make([]byte, loc.Size)
	if _, err := r.f.ReadAt(buf, pos); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, corruptErr(r.path, pos, "record extends past end of file")
		}
		return Record{}, fmt.Errorf("segment: reading %s at byte %d: %w", r.path, pos, err)
	}
	le := binary.LittleEndian
	if length := le.Uint32(buf[0:]); uint64(length)+recordHeaderSize != uint64(loc.Size) {
		return Record{}, corruptErr(r.path, pos, fmt.Sprintf("record length %d does not match location size %d", length, loc.Size))
	}
	body := buf[recordHeaderSize:]
	if crc32.Checksum(body, crcTable) != le.Uint32(buf[4:]) {
		return Record{}, corruptErr(r.path, pos, "record checksum mismatch")
	}
	rec, err := decodeBody(body)
	if err != nil {
		if errors.Is(err, errBadBody) {
			return Record{}, corruptErr(r.path, pos, err.Error())
		}
		return Record{}, err
	}
	return rec, nil
}

// Acquire takes a reference for reading. It returns false once the reader
// is retired or closed; the caller must then not use it.
func (r *Reader) Acquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return false
	}
	r.refs++
	return true
}

// Release drops a reference taken by Acquire. When the reader is retired
// or closed and this was the last reference, the file is closed (and, if
// retired, removed). A Release without a matching Acquire is ignored.
func (r *Reader) Release() {
	r.mu.Lock()
	if r.refs == 0 {
		r.mu.Unlock()
		return
	}
	r.refs--
	fin := r.closing && r.refs == 0 && !r.finalized
	if fin {
		r.finalized = true
	}
	remove := r.retired
	r.mu.Unlock()
	if fin {
		r.finalize(remove)
	}
}

// Retire marks the segment as obsolete: Acquire fails from now on, and the
// file is closed and removed once the refcount reaches 0 (immediately if
// no reference is held). Calling it again has no effect.
//
// Close and remove errors cannot be reported here: a failed removal leaves
// an orphaned segment file, which holds only records superseded by newer
// copies (last writer wins by WAL offset), so it is harmless.
func (r *Reader) Retire() {
	r.mu.Lock()
	if r.retired {
		r.mu.Unlock()
		return
	}
	r.retired = true
	r.closing = true
	alreadyClosed := r.finalized
	fin := r.refs == 0 && !r.finalized
	if fin {
		r.finalized = true
	}
	r.mu.Unlock()
	switch {
	case fin:
		r.finalize(true)
	case alreadyClosed:
		// Closed earlier by Close; only the removal is left. Best effort,
		// see above.
		_ = os.Remove(r.path)
	}
}

// Close stops new Acquires and closes the file once the refcount reaches 0
// (immediately if no reference is held, returning the close error). The
// file is not removed. Calling it again, or after Retire, returns nil.
func (r *Reader) Close() error {
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return nil
	}
	r.closing = true
	fin := r.refs == 0
	if fin {
		r.finalized = true
	}
	r.mu.Unlock()
	if !fin {
		return nil
	}
	if err := r.f.Close(); err != nil {
		return fmt.Errorf("segment: closing %s: %w", r.path, err)
	}
	return nil
}

// finalize closes the file and optionally removes it. It runs exactly once
// (guarded by r.finalized) and outside r.mu.
func (r *Reader) finalize(remove bool) {
	// Best effort: the file is read-only, so a close error loses nothing.
	_ = r.f.Close()
	if remove {
		// Best effort, see Retire.
		_ = os.Remove(r.path)
	}
}
