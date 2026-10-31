// Package segment implements the append-only data segment files of a
// cachydb collection (approach §4): a record codec with per-record zstd
// compression and CRC32C checksums, the active-segment Writer, a shared
// ref-counted Reader, sequential Scan, and the hint (§4.2) and index
// snapshot (§8) sidecar files (sidecar.go, hint.go, snapshot.go).
//
// Segment files are named seg-<n, at least 6 digits>.data and start with a
// 16-byte header: magic "CDBD" · u16 format version · u8 codec · 9
// reserved bytes. Records follow back to back; see record.go.
package segment

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// ErrCorrupt is returned (wrapped) when a segment holds damaged data: a bad
// header, a checksum mismatch, or a malformed record.
var ErrCorrupt = errors.New("segment: corrupt")

const (
	headerSize    = 16
	headerMagic   = "CDBD"
	formatVersion = 1
	dataPrefix    = "seg-"
	dataSuffix    = ".data"
)

// DataPath returns the path of data segment seg in dir: <dir>/seg-%06d.data.
func DataPath(dir string, seg uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d%s", dataPrefix, seg, dataSuffix))
}

// parseDataName returns the segment id encoded in a data file name. Only
// the canonical form produced by DataPath is accepted.
func parseDataName(name string) (uint32, bool) {
	if !strings.HasPrefix(name, dataPrefix) || !strings.HasSuffix(name, dataSuffix) {
		return 0, false
	}
	digits := name[len(dataPrefix) : len(name)-len(dataSuffix)]
	if len(digits) < 6 {
		return 0, false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return 0, false
	}
	if fmt.Sprintf("%06d", n) != digits { // reject non-canonical zero padding
		return 0, false
	}
	return uint32(n), true
}

// ListSegments returns the ids of the data segments in dir, sorted
// ascending.
func ListSegments(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("segment: listing %s: %w", dir, err)
	}
	var segs []uint32
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		if seg, ok := parseDataName(e.Name()); ok {
			segs = append(segs, seg)
		}
	}
	slices.Sort(segs)
	return segs, nil
}

func fileHeader() []byte {
	h := make([]byte, headerSize)
	copy(h, headerMagic)
	binary.LittleEndian.PutUint16(h[len(headerMagic):], formatVersion)
	h[len(headerMagic)+2] = codecZstd
	return h
}

func checkHeader(h []byte) error {
	if len(h) < headerSize || !bytes.Equal(h[:len(headerMagic)], []byte(headerMagic)) {
		return errors.New("bad segment header magic")
	}
	if v := binary.LittleEndian.Uint16(h[len(headerMagic):]); v != formatVersion {
		return fmt.Errorf("unsupported segment format version %d", v)
	}
	if c := h[len(headerMagic)+2]; c != codecNone && c != codecZstd {
		return fmt.Errorf("unknown segment codec %d", c)
	}
	return nil
}

// readHeader reads and validates the header of f, whose size is size.
// Validation failures are returned as plain errors (not ErrCorrupt) so the
// caller can decide whether the header is merely torn.
func readHeader(f *os.File, size int64) (hdrErr, ioErr error) {
	if size < headerSize {
		return errors.New("incomplete segment header"), nil
	}
	h := make([]byte, headerSize)
	if _, err := f.ReadAt(h, 0); err != nil {
		return nil, err
	}
	return checkHeader(h), nil
}

// corruptErr builds an ErrCorrupt error naming the file and byte offset.
func corruptErr(path string, pos int64, reason string) error {
	return fmt.Errorf("%w: %s at byte %d: %s", ErrCorrupt, path, pos, reason)
}

type recordErrKind int

const (
	errKindShort   recordErrKind = iota + 1 // record extends past the end of the file
	errKindCRC                              // checksum mismatch
	errKindInvalid                          // impossible length or malformed body
)

// recordError describes a damaged record at pos. end is where the record
// claims to end (pos + header + length), when known.
type recordError struct {
	kind   recordErrKind
	pos    int64
	end    int64
	reason string
}

func (e *recordError) Error() string { return e.reason }

// recordReader reads records sequentially from a segment file, starting
// right after the header, up to size bytes.
type recordReader struct {
	r    *bufio.Reader
	pos  int64 // file position of the next record
	size int64 // file size the reader treats as EOF
	buf  []byte
}

func newRecordReader(f *os.File, size int64) *recordReader {
	sec := io.NewSectionReader(f, headerSize, size-headerSize)
	return &recordReader{r: bufio.NewReaderSize(sec, 64<<10), pos: headerSize, size: size}
}

// next returns the next record and its location (Seg left zero). It
// returns io.EOF exactly at the end of the file, a *recordError for a
// damaged record, or another error for I/O failures.
func (rr *recordReader) next() (Record, Location, error) {
	start := rr.pos
	if start == rr.size {
		return Record{}, Location{}, io.EOF
	}
	if rr.size-start < recordHeaderSize {
		return Record{}, Location{}, &recordError{kind: errKindShort, pos: start, end: rr.size, reason: "incomplete record header"}
	}
	var hdr [recordHeaderSize]byte
	if _, err := io.ReadFull(rr.r, hdr[:]); err != nil {
		return Record{}, Location{}, fmt.Errorf("reading record header at byte %d: %w", start, err)
	}
	length := int64(binary.LittleEndian.Uint32(hdr[0:]))
	want := binary.LittleEndian.Uint32(hdr[4:])
	end := start + recordHeaderSize + length
	if length < recordFixedBody || length > maxBodyLen {
		return Record{}, Location{}, &recordError{kind: errKindInvalid, pos: start, end: end, reason: fmt.Sprintf("record length %d out of range", length)}
	}
	if end > rr.size {
		return Record{}, Location{}, &recordError{kind: errKindShort, pos: start, end: end, reason: "record extends past end of file"}
	}
	if int64(cap(rr.buf)) < length {
		rr.buf = make([]byte, length)
	}
	body := rr.buf[:length]
	if _, err := io.ReadFull(rr.r, body); err != nil {
		return Record{}, Location{}, fmt.Errorf("reading record at byte %d: %w", start, err)
	}
	if got := crc32.Checksum(body, crcTable); got != want {
		return Record{}, Location{}, &recordError{kind: errKindCRC, pos: start, end: end, reason: "record checksum mismatch"}
	}
	rec, err := decodeBody(body)
	if err != nil {
		if errors.Is(err, errBadBody) {
			return Record{}, Location{}, &recordError{kind: errKindInvalid, pos: start, end: end, reason: err.Error()}
		}
		return Record{}, Location{}, err
	}
	rr.pos = end
	return rec, Location{Pos: uint64(start), Size: uint32(end - start)}, nil
}

// isTornTail decides whether a damaged record is the torn final write of
// the active segment: it runs past EOF, its checksum fails and it ends
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

// allZero reports whether f holds only zero bytes in [from, to).
func allZero(f *os.File, from, to int64) (bool, error) {
	buf := make([]byte, 32<<10)
	for from < to {
		n := min(int64(len(buf)), to-from)
		if _, err := f.ReadAt(buf[:n], from); err != nil {
			return false, err
		}
		for _, b := range buf[:n] {
			if b != 0 {
				return false, nil
			}
		}
		from += n
	}
	return true, nil
}

// Scan calls fn for every valid record of segment seg in dir, in file
// order, and returns the end of the last valid record.
//
// Scan never modifies the file. When it meets a damaged record (including
// a torn tail) it stops and returns the end of the last valid record with
// an error wrapping ErrCorrupt; callers that want a torn tail of the active
// segment removed open it with OpenActive first. If fn returns an error,
// Scan stops and returns that error unwrapped, with validEnd at the end of
// the last record fn accepted.
func Scan(dir string, seg uint32, fn func(Record, Location) error) (validEnd int64, err error) {
	path := DataPath(dir, seg)
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("segment: opening %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("segment: closing %s: %w", path, cerr)
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("segment: stat %s: %w", path, err)
	}
	size := fi.Size()
	hdrErr, ioErr := readHeader(f, size)
	if ioErr != nil {
		return 0, fmt.Errorf("segment: reading header of %s: %w", path, ioErr)
	}
	if hdrErr != nil {
		return 0, corruptErr(path, 0, hdrErr.Error())
	}

	rr := newRecordReader(f, size)
	validEnd = headerSize
	for {
		rec, loc, err := rr.next()
		if err == io.EOF {
			return validEnd, nil
		}
		var re *recordError
		if errors.As(err, &re) {
			return validEnd, corruptErr(path, re.pos, re.reason)
		}
		if err != nil {
			return validEnd, fmt.Errorf("segment: reading %s: %w", path, err)
		}
		loc.Seg = seg
		if err := fn(rec, loc); err != nil {
			return validEnd, err
		}
		validEnd = int64(loc.Pos) + int64(loc.Size)
	}
}
