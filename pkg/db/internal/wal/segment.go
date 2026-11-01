package wal

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

// Segment files are named wal-<first offset, 20 digits>.log and start with
// a 16-byte header: magic "CDBW" · u16 format version · 10 reserved bytes.
const (
	segmentHeaderSize = 16
	segmentMagic      = "CDBW"
	segmentVersion    = 1
	segmentPrefix     = "wal-"
	segmentSuffix     = ".log"
	segmentDigits     = 20
)

// FormatVersion is the WAL segment format version this build reads and
// writes (the version field of the segment header).
const FormatVersion = segmentVersion

type segmentInfo struct {
	start uint64 // offset of the first record (the name)
	path  string
}

func segmentFileName(start uint64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentDigits, start, segmentSuffix)
}

// parseSegmentName returns the first offset encoded in a segment file name.
// Names that are not exactly wal-<20 digits>.log are not segments.
func parseSegmentName(name string) (uint64, bool) {
	if len(name) != len(segmentPrefix)+segmentDigits+len(segmentSuffix) ||
		!strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	digits := name[len(segmentPrefix) : len(segmentPrefix)+segmentDigits]
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// listSegments returns the WAL segments in dir sorted by first offset.
func listSegments(dir string) ([]segmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: listing segments in %s: %w", dir, err)
	}
	var segs []segmentInfo
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		start, ok := parseSegmentName(e.Name())
		if !ok {
			continue
		}
		segs = append(segs, segmentInfo{start: start, path: filepath.Join(dir, e.Name())})
	}
	slices.SortFunc(segs, func(a, b segmentInfo) int {
		switch {
		case a.start < b.start:
			return -1
		case a.start > b.start:
			return 1
		default:
			return 0
		}
	})
	return segs, nil
}

func segmentHeader() []byte {
	h := make([]byte, segmentHeaderSize)
	copy(h, segmentMagic)
	binary.LittleEndian.PutUint16(h[len(segmentMagic):], segmentVersion)
	return h
}

func checkSegmentHeader(h []byte) error {
	if len(h) < segmentHeaderSize || !bytes.Equal(h[:len(segmentMagic)], []byte(segmentMagic)) {
		return errors.New("bad segment header magic")
	}
	if v := binary.LittleEndian.Uint16(h[len(segmentMagic):]); v != segmentVersion {
		return fmt.Errorf("unsupported segment format version %d", v)
	}
	return nil
}

// createSegment creates a new segment file, writes its header, and syncs
// it. The caller must fsync the directory.
func createSegment(path string, syncFn func(*os.File) error) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: creating segment %s: %w", path, err)
	}
	if _, err := f.Write(segmentHeader()); err != nil {
		_ = f.Close() // already failing; the write error is reported
		return nil, fmt.Errorf("wal: writing header of %s: %w", path, err)
	}
	if err := syncFn(f); err != nil {
		_ = f.Close() // already failing; the sync error is reported
		return nil, fmt.Errorf("wal: syncing %s: %w", path, err)
	}
	return f, nil
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
	sec := io.NewSectionReader(f, segmentHeaderSize, size-segmentHeaderSize)
	return &recordReader{r: bufio.NewReaderSize(sec, 64<<10), pos: segmentHeaderSize, size: size}
}

// next returns the next record. It returns io.EOF exactly at the end of the
// file, a *recordError for a damaged record, or another error for I/O
// failures.
func (rr *recordReader) next() (Record, error) {
	start := rr.pos
	if start == rr.size {
		return Record{}, io.EOF
	}
	if rr.size-start < recordHeaderSize {
		return Record{}, &recordError{kind: errKindShort, pos: start, end: rr.size, reason: "incomplete record header"}
	}
	var hdr [recordHeaderSize]byte
	if _, err := io.ReadFull(rr.r, hdr[:]); err != nil {
		return Record{}, fmt.Errorf("reading record header at byte %d: %w", start, err)
	}
	length := int64(binary.LittleEndian.Uint32(hdr[0:]))
	want := binary.LittleEndian.Uint32(hdr[4:])
	end := start + recordHeaderSize + length
	if length < recordFixedBody {
		return Record{}, &recordError{kind: errKindInvalid, pos: start, end: end, reason: fmt.Sprintf("record length %d too small", length)}
	}
	if end > rr.size {
		return Record{}, &recordError{kind: errKindShort, pos: start, end: end, reason: "record extends past end of file"}
	}
	if int64(cap(rr.buf)) < length {
		rr.buf = make([]byte, length)
	}
	body := rr.buf[:length]
	if _, err := io.ReadFull(rr.r, body); err != nil {
		return Record{}, fmt.Errorf("reading record at byte %d: %w", start, err)
	}
	if got := crc32.Checksum(body, crcTable); got != want {
		return Record{}, &recordError{kind: errKindCRC, pos: start, end: end, reason: "record checksum mismatch"}
	}
	rec, err := decodeBody(body)
	if err != nil {
		return Record{}, &recordError{kind: errKindInvalid, pos: start, end: end, reason: err.Error()}
	}
	rr.pos = end
	return rec, nil
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
