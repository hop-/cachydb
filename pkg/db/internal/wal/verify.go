package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// Finding is one result of Verify: a file, the byte position in it, and
// what is wrong there.
type Finding struct {
	Path   string
	Pos    int64
	Reason string
}

// VerifyResult is what Verify found in a WAL directory.
type VerifyResult struct {
	Segments int   // segment files
	Records  int64 // valid records read
	// First is the offset of the oldest record the log holds (the first
	// segment's name), and Next the offset the next record would get: one
	// past the newest valid record. With no segment both are 1, as Open
	// would start a new log.
	First, Next uint64
	// Problems is damage Open fails on: a bad record or header anywhere
	// but at the end of the newest segment, an offset gap, a segment named
	// for offset 0.
	Problems []Finding
	// Notices is what Open repairs by itself: a torn tail (or torn header)
	// of the newest segment, which it truncates.
	Notices []Finding
}

// Verify checks every segment of the WAL in dir the way Open does (§3.3:
// headers, checksums, offset continuity across all segments) without
// changing anything, and calls fn for every valid record in offset order
// with its file and byte position (fn may be nil; an error it returns
// stops Verify and is returned as it is). Unlike Open it does not stop at the first damage: it
// reports it and goes on with the next segment, whose first offset it
// then takes from the file name. A damaged record ends the scan of its
// segment, since the records behind it cannot be found.
//
// The returned error is an I/O failure or fn's; damage is reported in the
// result.
func Verify(dir string, fn func(path string, pos int64, r Record) error) (VerifyResult, error) {
	res := VerifyResult{First: 1, Next: 1}
	segs, err := listSegments(dir)
	if err != nil {
		return res, err
	}
	res.Segments = len(segs)
	if len(segs) == 0 {
		return res, nil
	}
	res.First = segs[0].start
	expected, known := segs[0].start, true
	for i, s := range segs {
		newest := i == len(segs)-1
		switch {
		case s.start == 0:
			res.Problems = append(res.Problems, Finding{Path: s.path, Reason: "segment named for offset 0"})
		case known && s.start != expected:
			res.Problems = append(res.Problems, Finding{Path: s.path,
				Reason: fmt.Sprintf("segment starts at offset %d, expected %d (an offset gap: records are missing)", s.start, expected)})
		}
		expected = s.start
		ok, err := verifySegment(s.path, newest, &expected, &res, fn)
		if err != nil {
			return res, err
		}
		known = ok
	}
	res.Next = expected
	return res, nil
}

// verifySegment checks one segment, advancing *expected past every valid
// record. It reports false when damage ended the scan before the end of
// the file, so the offset after it is unknown.
func verifySegment(path string, newest bool, expected *uint64, res *VerifyResult, fn func(string, int64, Record) error) (complete bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("wal: opening %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("wal: closing %s: %w", path, cerr)
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("wal: stat %s: %w", path, err)
	}
	size := fi.Size()

	var hdrErr error
	if size < segmentHeaderSize {
		hdrErr = errors.New("incomplete segment header")
	} else {
		hdr := make([]byte, segmentHeaderSize)
		if _, err := f.ReadAt(hdr, 0); err != nil {
			return false, fmt.Errorf("wal: reading header of %s: %w", path, err)
		}
		hdrErr = checkSegmentHeader(hdr)
	}
	if hdrErr != nil {
		torn := false
		if newest { // the same rule as Open (scanSegment)
			torn = size < segmentHeaderSize
			if !torn {
				if torn, err = allZero(f, 0, size); err != nil {
					return false, fmt.Errorf("wal: reading %s: %w", path, err)
				}
			}
		}
		if torn {
			res.Notices = append(res.Notices, Finding{Path: path,
				Reason: fmt.Sprintf("torn segment header (%d bytes, an interrupted segment creation); Open rewrites it", size)})
			return true, nil
		}
		res.Problems = append(res.Problems, Finding{Path: path, Reason: hdrErr.Error()})
		return false, nil
	}

	rr := newRecordReader(f, size)
	for {
		rec, err := rr.next()
		if err == io.EOF {
			return true, nil
		}
		if re, ok := errors.AsType[*recordError](err); ok {
			if newest {
				torn, err := isTornTail(f, re, size)
				if err != nil {
					return false, fmt.Errorf("wal: reading %s: %w", path, err)
				}
				if torn {
					res.Notices = append(res.Notices, Finding{Path: path, Pos: re.pos,
						Reason: fmt.Sprintf("torn tail of the newest segment (%s, %d bytes); Open truncates it", re.reason, size-re.pos)})
					return true, nil
				}
			}
			res.Problems = append(res.Problems, Finding{Path: path, Pos: re.pos, Reason: re.reason})
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("wal: reading %s: %w", path, err)
		}
		pos := rr.pos - recordSize(&rec)
		if rec.Offset != *expected {
			res.Problems = append(res.Problems, Finding{Path: path, Pos: pos,
				Reason: fmt.Sprintf("record offset %d, expected %d", rec.Offset, *expected)})
			return false, nil
		}
		*expected++
		res.Records++
		if fn != nil {
			if err := fn(path, pos, rec); err != nil {
				return false, err
			}
		}
	}
}
