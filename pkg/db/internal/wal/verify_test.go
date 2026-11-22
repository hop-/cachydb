package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dirSnapshot returns every file of dir with its contents.
func dirSnapshot(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string][]byte, len(ents))
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = b
	}
	return out
}

// verifyDir runs Verify on dir, checks that it changed no file, and
// returns its result and the offsets fn saw.
func verifyDir(t *testing.T, dir string) (VerifyResult, []uint64) {
	t.Helper()
	before := dirSnapshot(t, dir)
	var seen []uint64
	res, err := Verify(dir, func(path string, pos int64, r Record) error {
		// fn gets the record's file and byte position.
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		enc := appendRecord(nil, &r)
		if pos < segmentHeaderSize || pos+int64(len(enc)) > int64(len(b)) || !bytes.Equal(b[pos:pos+int64(len(enc))], enc) {
			t.Errorf("record %d is not at %s byte %d", r.Offset, filepath.Base(path), pos)
		}
		seen = append(seen, r.Offset)
		return nil
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	after := dirSnapshot(t, dir)
	if len(after) != len(before) {
		t.Errorf("Verify changed the directory: %d files before, %d after", len(before), len(after))
	}
	for name, b := range before {
		if !bytes.Equal(after[name], b) {
			t.Errorf("Verify changed %s", name)
		}
	}
	return res, seen
}

func findingsMention(fs []Finding, file, text string) bool {
	for _, f := range fs {
		if filepath.Base(f.Path) == file && strings.Contains(f.Reason, text) {
			return true
		}
	}
	return false
}

func TestVerifyEmptyDir(t *testing.T) {
	res, seen := verifyDir(t, t.TempDir())
	if res.Segments != 0 || res.Records != 0 || res.First != 1 || res.Next != 1 || len(res.Problems)+len(res.Notices) != 0 || len(seen) != 0 {
		t.Errorf("Verify of an empty dir = %+v", res)
	}
	if _, err := Verify(filepath.Join(t.TempDir(), "missing"), nil); err == nil {
		t.Error("Verify of a missing dir succeeded, want the I/O error")
	}
}

func TestVerifyHealthyLog(t *testing.T) {
	env := newEnv(t, 400) // rotates every few records
	l := env.mustOpen(t, nil)
	for i := range 20 {
		mustAppend(t, l, testRec(i))
	}
	mustClose(t, l)
	files := walFiles(t, env.dir)
	if len(files) < 3 {
		t.Fatalf("want several segments, got %v", files)
	}
	res, seen := verifyDir(t, env.dir)
	if res.Segments != len(files) || res.Records != 20 || res.First != 1 || res.Next != 21 {
		t.Errorf("Verify = %+v, want %d segments, 20 records, offsets 1..21", res, len(files))
	}
	if len(res.Problems)+len(res.Notices) != 0 {
		t.Errorf("findings on a healthy log: %+v %+v", res.Problems, res.Notices)
	}
	if len(seen) != 20 || seen[0] != 1 || seen[19] != 20 {
		t.Errorf("fn saw offsets %v", seen)
	}

	// An error of fn stops Verify and is returned as it is.
	stop := errors.New("stop")
	n := 0
	_, err := Verify(env.dir, func(string, int64, Record) error {
		if n++; n == 3 {
			return stop
		}
		return nil
	})
	if err != stop || n != 3 {
		t.Errorf("Verify with a failing fn = %v after %d calls, want stop after 3", err, n)
	}
	// fn may be nil.
	if res, err := Verify(env.dir, nil); err != nil || res.Records != 20 {
		t.Errorf("Verify(nil fn) = %+v, %v", res, err)
	}
}

func TestVerifyDamage(t *testing.T) {
	tests := []struct {
		name string
		// build writes the segments into dir and returns the file whose
		// finding is expected.
		build      func(t *testing.T, dir string) string
		problem    string // substring of the expected problem ("" = none)
		notice     string // substring of the expected notice ("" = none)
		records    int64
		first      uint64
		next       uint64
		wantOffset []uint64
	}{
		{
			name: "flipped byte in an older segment",
			build: func(t *testing.T, dir string) string {
				p := writeSegment(t, dir, 1, 1, 2, 3)
				writeSegment(t, dir, 4, 4, 5)
				r := testRec(1)
				flipByte(t, p, segmentHeaderSize+recordSize(&r)+12) // inside record 2
				return filepath.Base(p)
			},
			problem: "", // any reason
			records: 3, first: 1, next: 6,
		},
		{
			name: "flipped byte in the middle of the newest segment",
			build: func(t *testing.T, dir string) string {
				p := writeSegment(t, dir, 1, 1, 2, 3, 4, 5)
				r := testRec(2)
				flipByte(t, p, segmentHeaderSize+recordSize(&r)+12) // inside record 2
				return filepath.Base(p)
			},
			records: 1, first: 1, next: 2,
		},
		{
			name: "torn tail of the newest segment",
			build: func(t *testing.T, dir string) string {
				p := writeSegment(t, dir, 1, 1, 2, 3)
				truncateFile(t, p, fileSize(t, p)-4)
				return filepath.Base(p)
			},
			notice:  "torn tail of the newest segment",
			records: 2, first: 1, next: 3,
		},
		{
			name: "torn tail of an older segment",
			build: func(t *testing.T, dir string) string {
				p := writeSegment(t, dir, 1, 1, 2, 3)
				truncateFile(t, p, fileSize(t, p)-4)
				writeSegment(t, dir, 4, 4)
				return filepath.Base(p)
			},
			problem: "",
			records: 3, first: 1, next: 5,
		},
		{
			name: "offset gap between segments",
			build: func(t *testing.T, dir string) string {
				writeSegment(t, dir, 1, 1, 2)
				return filepath.Base(writeSegment(t, dir, 5, 5, 6))
			},
			problem: "an offset gap",
			records: 4, first: 1, next: 7,
		},
		{
			name: "record offset out of sequence",
			build: func(t *testing.T, dir string) string {
				return filepath.Base(writeSegment(t, dir, 1, 1, 2, 4))
			},
			problem: "record offset 4, expected 3",
			records: 2, first: 1, next: 3,
		},
		{
			name: "segment named for offset 0",
			build: func(t *testing.T, dir string) string {
				return filepath.Base(writeSegment(t, dir, 0))
			},
			problem: "segment named for offset 0",
			first:   0, next: 0,
		},
		{
			name: "bad header of an older segment",
			build: func(t *testing.T, dir string) string {
				p := writeSegment(t, dir, 1, 1)
				flipByte(t, p, 0)
				writeSegment(t, dir, 2, 2)
				return filepath.Base(p)
			},
			problem: "",
			records: 1, first: 1, next: 3,
		},
		{
			name: "short header of the newest segment",
			build: func(t *testing.T, dir string) string {
				writeSegment(t, dir, 1, 1)
				p := filepath.Join(dir, segmentFileName(2))
				if err := os.WriteFile(p, segmentHeader()[:5], 0o644); err != nil {
					t.Fatal(err)
				}
				return filepath.Base(p)
			},
			notice:  "torn segment header",
			records: 1, first: 1, next: 2,
		},
		{
			name: "zeroed header of the newest segment",
			build: func(t *testing.T, dir string) string {
				writeSegment(t, dir, 1, 1)
				p := filepath.Join(dir, segmentFileName(2))
				if err := os.WriteFile(p, make([]byte, segmentHeaderSize+10), 0o644); err != nil {
					t.Fatal(err)
				}
				return filepath.Base(p)
			},
			notice:  "torn segment header",
			records: 1, first: 1, next: 2,
		},
		{
			name: "short header of an older segment",
			build: func(t *testing.T, dir string) string {
				p := filepath.Join(dir, segmentFileName(1))
				if err := os.WriteFile(p, segmentHeader()[:5], 0o644); err != nil {
					t.Fatal(err)
				}
				writeSegment(t, dir, 2, 2)
				return filepath.Base(p)
			},
			problem: "incomplete segment header",
			records: 1, first: 1, next: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			file := tt.build(t, dir)
			res, _ := verifyDir(t, dir)
			wantProblem := tt.notice == ""
			switch {
			case wantProblem && len(res.Problems) != 1:
				t.Errorf("problems = %+v, want one in %s", res.Problems, file)
			case wantProblem && !findingsMention(res.Problems, file, tt.problem):
				t.Errorf("problems = %+v, want one in %s mentioning %q", res.Problems, file, tt.problem)
			case !wantProblem && len(res.Problems) != 0:
				t.Errorf("problems = %+v, want none", res.Problems)
			}
			if !wantProblem && (len(res.Notices) != 1 || !findingsMention(res.Notices, file, tt.notice)) {
				t.Errorf("notices = %+v, want one in %s mentioning %q", res.Notices, file, tt.notice)
			}
			if wantProblem && len(res.Notices) != 0 {
				t.Errorf("notices = %+v, want none", res.Notices)
			}
			if res.Records != tt.records || res.First != tt.first || res.Next != tt.next {
				t.Errorf("records %d, first %d, next %d; want %d, %d, %d", res.Records, res.First, res.Next, tt.records, tt.first, tt.next)
			}
		})
	}
}

// TestVerifyMatchesOpen: Verify's problems are exactly the damage Open
// fails on, and its notices what Open repairs.
func TestVerifyMatchesOpen(t *testing.T) {
	for _, tt := range []struct {
		name   string
		damage func(t *testing.T, p string)
	}{
		{"flipped byte", func(t *testing.T, p string) { flipByte(t, p, fileSize(t, p)/2) }},
		{"torn tail", func(t *testing.T, p string) { truncateFile(t, p, fileSize(t, p)-3) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeSegment(t, dir, 1, 1, 2, 3, 4)
			tt.damage(t, filepath.Join(dir, segmentFileName(1)))
			res, _ := verifyDir(t, dir)
			env := &testEnv{dir: dir, logs: &syncBuffer{}}
			l, err := env.open(t, nil)
			if err == nil {
				mustClose(t, l)
			}
			switch {
			case len(res.Problems) > 0 && err == nil:
				t.Errorf("Verify reports %+v, but Open succeeds", res.Problems)
			case len(res.Problems) == 0 && err != nil:
				t.Errorf("Verify reports no problem, but Open fails: %v", err)
			case err != nil && !errors.Is(err, ErrCorrupt):
				t.Errorf("Open: %v", err)
			}
		})
	}
}
