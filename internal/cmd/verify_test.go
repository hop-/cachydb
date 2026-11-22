package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/hop-/cachydb/pkg/db"
)

// Tests of "cachydb utils verify" (plan 013 §3.3, AC-14). The CLI cannot
// reach the index-file package (pkg/db/internal), so index.db damage is
// made with raw byte patches; pkg/db's verify tests cover every damage
// kind through the store itself.

// verifyDocs is the number of documents seedVerify writes.
const verifyDocs = 120

// seedVerify writes database kb, collection notes with an index on the
// unique-per-document field "val", in several sessions so that there are
// sealed data segments, and returns the directory.
func seedVerify(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	opts := db.Options{Dir: dir, DataSegmentSize: 4096}
	for s := range 4 {
		e, err := db.Open(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		if s == 0 {
			d, err := e.CreateDatabase(ctx, "kb")
			if err != nil {
				t.Fatal(err)
			}
			if err := d.CreateCollection(ctx, "notes", nil); err != nil {
				t.Fatal(err)
			}
		}
		d, err := e.Database("kb")
		if err != nil {
			t.Fatal(err)
		}
		c, err := d.Collection("notes")
		if err != nil {
			t.Fatal(err)
		}
		if s == 0 {
			if err := c.CreateIndex(ctx, "by_val", "val"); err != nil {
				t.Fatal(err)
			}
		}
		for i := s * verifyDocs / 4; i < (s+1)*verifyDocs/4; i++ {
			doc := &db.Document{ID: fmt.Sprintf("doc-%04d", i), Data: map[string]any{
				"val": fmt.Sprintf("val-%04d", i), "pad": strings.Repeat("p", 80)}}
			if _, err := c.Insert(ctx, doc); err != nil {
				t.Fatal(err)
			}
		}
		closeEngine(t, e)
	}
	// A last session without writes: a flush that fills the active segment
	// leaves the next one created but empty and unregistered, which Open
	// deletes (and verify notes); this one has nothing to flush.
	closeEngine(t, openEngine(t, dir))
	if segs, _ := filepath.Glob(filepath.Join(dir, "kb", "notes", "seg-*.data")); len(segs) < 2 {
		t.Fatalf("want sealed data segments, have %v", segs)
	}
	return dir
}

// readAll returns every document of kb/notes in dir.
func readAll(t *testing.T, dir string) []*db.Document {
	t.Helper()
	var out []*db.Document
	err := withEngine(context.Background(), db.Options{Dir: dir}, func(e *db.Engine) error {
		d, err := e.Database("kb")
		if err != nil {
			return err
		}
		c, err := d.Collection("notes")
		if err != nil {
			return err
		}
		out, err = c.Find(context.Background(), &db.Query{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// patchFile replaces every occurrence of old in path by repl (same
// length), failing unless there are exactly want occurrences (0 = any
// number but none).
func patchFile(t *testing.T, path string, old, repl []byte, want int) {
	t.Helper()
	if len(old) != len(repl) {
		t.Fatal("patchFile: lengths differ")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := bytes.Count(b, old)
	if n == 0 || (want > 0 && n != want) {
		t.Fatalf("%q occurs %d times in %s, want %d", old, n, path, want)
	}
	if err := os.WriteFile(path, bytes.ReplaceAll(b, old, repl), 0o644); err != nil {
		t.Fatal(err)
	}
}

// keyEntry returns the 24-byte key-table value (segment, position, size,
// WAL offset) stored right after id's key in the index file b. bbolt keeps
// a leaf's keys and values next to each other; the entry is recognised
// by a plausible segment and size, so an index key that ends with the id
// does not match.
func keyEntry(t *testing.T, b []byte, id string) []byte {
	t.Helper()
	var found [][]byte
	for i := 0; ; {
		j := bytes.Index(b[i:], []byte(id))
		if j < 0 {
			break
		}
		at := i + j + len(id)
		i = at
		if at+24 > len(b) {
			break
		}
		v := b[at : at+24]
		seg, size := binary.BigEndian.Uint32(v), binary.BigEndian.Uint32(v[12:])
		if seg >= 1 && seg < 100 && size > 0 && size < 4096 {
			found = append(found, v)
		}
	}
	if len(found) == 0 {
		t.Fatalf("no key-table entry of %q", id)
	}
	for _, v := range found[1:] {
		if !bytes.Equal(v, found[0]) {
			t.Fatalf("differing key-table entries of %q", id)
		}
	}
	return slices.Clone(found[0])
}

// verifyGlobals restores verify's flag variables when t ends.
func verifyGlobals(t *testing.T) {
	d, c, deep, rebuild, format := verifyDatabase, verifyCollection, verifyDeep, verifyRebuild, verifyFormat
	t.Cleanup(func() {
		verifyDatabase, verifyCollection, verifyDeep, verifyRebuild, verifyFormat = d, c, deep, rebuild, format
	})
}

// runVerifyCLI runs "cachydb utils verify -R dir" with every verify flag
// set (flags keep their values between in-process runs) plus extra.
func runVerifyCLI(t *testing.T, dir string, extra ...string) (int, string, string) {
	t.Helper()
	verifyGlobals(t)
	args := append([]string{"utils", "verify", "-R", dir, "-d", "", "-c", "", "--deep=false", "--rebuild-index=false", "-f", "text"}, extra...)
	return runCLI(t, context.Background(), args...)
}

var summaryLine = "verify: ok: 1 database(s), 1 collection(s), 120 document(s), "

func TestVerifyCommandHealthy(t *testing.T) {
	dir := seedVerify(t)
	code, out, errOut := runVerifyCLI(t, dir)
	if code != 0 || errOut != "" {
		t.Fatalf("exit %d, stderr %q, stdout %q", code, errOut, out)
	}
	if strings.Count(out, "\n") != 1 || !strings.HasPrefix(out, summaryLine) || !strings.Contains(out, "WAL segment(s)") {
		t.Errorf("healthy output is not a one-line summary: %q", out)
	}
	code, out, _ = runVerifyCLI(t, dir, "--deep")
	if code != 0 || strings.Count(out, "\n") != 1 || !strings.HasSuffix(out, ", 1 index(es) checked entry by entry\n") {
		t.Errorf("--deep: exit %d, %q", code, out)
	}
	// JSON: one object with the report's fields.
	code, out, _ = runVerifyCLI(t, dir, "-f", "json", "--deep")
	var rep db.VerifyReport
	if err := json.Unmarshal([]byte(out), &rep); err != nil || code != 0 {
		t.Fatalf("json: exit %d, %v: %q", code, err, out)
	}
	if !rep.OK || !rep.Deep || rep.Documents != verifyDocs || rep.Collections != 1 || rep.Indexes != 1 || len(rep.Problems) != 0 {
		t.Errorf("json report: %+v", rep)
	}
	// Scoped to one collection.
	if code, out, _ := runVerifyCLI(t, dir, "-d", "kb", "-c", "notes"); code != 0 || !strings.HasPrefix(out, summaryLine) {
		t.Errorf("-d kb -c notes: exit %d, %q", code, out)
	}
}

// TestVerifyCommandDamage: every AC-14 damage exits 1 with at least one
// line naming the file or collection and the problem (task 03 Amendment 1)
// and a summary counting the problems.
func TestVerifyCommandDamage(t *testing.T) {
	base := seedVerify(t)
	notes := filepath.Join("kb", "notes")
	index := filepath.Join(notes, "index.db")
	tests := []struct {
		name   string
		flags  []string
		damage func(t *testing.T, dir string)
		line   []string // texts one output line holds
	}{
		{
			name: "flipped byte in a sealed data segment",
			damage: func(t *testing.T, dir string) {
				flipMiddle(t, filepath.Join(dir, notes, "seg-000001.data"))
			},
			line: []string{"kb/notes/seg-000001.data: ", "corrupt"},
		},
		{
			name: "flipped byte in the middle of a WAL segment",
			damage: func(t *testing.T, dir string) {
				wals, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
				if err != nil || len(wals) == 0 {
					t.Fatalf("WAL files: %v, %v", wals, err)
				}
				flipMiddle(t, wals[0])
			},
			line: []string{"wal-00000000000000000001.log: ", "Open fails"},
		},
		{
			name: "key-table entry pointing at another document's record",
			damage: func(t *testing.T, dir string) {
				p := filepath.Join(dir, index)
				b, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				from, to := keyEntry(t, b, "doc-0010"), keyEntry(t, b, "doc-0011")
				patchFile(t, p, append([]byte("doc-0011"), to...), append([]byte("doc-0011"), from...), 0)
			},
			line: []string{"kb/notes/index.db: ", `"doc-0011"`, `"doc-0010"`},
		},
		{
			name: "wrong persisted count",
			damage: func(t *testing.T, dir string) {
				count := func(n uint64) []byte { return binary.BigEndian.AppendUint64([]byte("count"), n) }
				patchFile(t, filepath.Join(dir, index), count(verifyDocs), count(verifyDocs+7), 0)
			},
			line: []string{"kb/notes/index.db: ", "count is 127", "120"},
		},
		{
			name:  "missing index entry (deep)",
			flags: []string{"--deep"},
			damage: func(t *testing.T, dir string) {
				// The entry of doc-0042 now names doc-0043, which exists
				// (so the key table agrees) but does not hold the value:
				// doc-0042 has no entry, and doc-0043 an extra one.
				key := func(id string) []byte { return append([]byte("\x40val-0042\x00\x00"), id...) }
				patchFile(t, filepath.Join(dir, index), key("doc-0042"), key("doc-0043"), 0)
			},
			line: []string{"kb/notes/index.db: ", `index "by_val" has no entry for document "doc-0042"`},
		},
		{
			name:  "extra index entry (deep)",
			flags: []string{"--deep"},
			damage: func(t *testing.T, dir string) {
				key := func(id string) []byte { return append([]byte("\x40val-0042\x00\x00"), id...) }
				patchFile(t, filepath.Join(dir, index), key("doc-0042"), key("doc-0043"), 0)
			},
			line: []string{"kb/notes/index.db: ", `index "by_val" has an entry for document "doc-0043" with value "val-0042", which the document does not hold`},
		},
		{
			name:   "deleted index.db",
			damage: func(t *testing.T, dir string) { removeFile(t, filepath.Join(dir, index)) },
			line:   []string{"kb/notes/index.db: ", "--rebuild-index"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := copyDir(t, base)
			tt.damage(t, dir)
			code, out, errOut := runVerifyCLI(t, dir, tt.flags...)
			if code != 1 {
				t.Fatalf("exit %d, want 1; stdout %q, stderr %q", code, out, errOut)
			}
			lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
			if !slices.ContainsFunc(lines, func(l string) bool {
				for _, s := range tt.line {
					if !strings.Contains(l, s) {
						return false
					}
				}
				return true
			}) {
				t.Errorf("no line with %q in:\n%s", tt.line, out)
			}
			problems := 0
			for _, l := range lines[:len(lines)-1] {
				if !strings.HasPrefix(l, "note: ") {
					problems++
				}
			}
			summary := lines[len(lines)-1]
			if !strings.HasPrefix(summary, fmt.Sprintf("verify: %d problem(s): ", problems)) {
				t.Errorf("summary %q, want %d problem(s)", summary, problems)
			}
			if !strings.Contains(errOut, fmt.Sprintf("Error: verify found problems: %d", problems)) {
				t.Errorf("stderr %q", errOut)
			}
			// JSON: the same verdict, machine-readable.
			code, js, _ := runVerifyCLI(t, dir, append(slices.Clone(tt.flags), "-f", "json")...)
			var rep db.VerifyReport
			if err := json.Unmarshal([]byte(js), &rep); err != nil || code != 1 || rep.OK || len(rep.Problems) != problems {
				t.Errorf("json: exit %d, %v, report %+v", code, err, rep)
			}
		})
	}
	// The deep-only damage passes the shallow check.
	dir := copyDir(t, base)
	tests[4].damage(t, dir)
	if code, out, _ := runVerifyCLI(t, dir); code != 0 {
		t.Errorf("index-entry damage fails the shallow check: %q", out)
	}
}

// TestVerifyCommandRebuildIndex: after index.db is deleted by hand,
// --rebuild-index restores a directory that reads the same data.
func TestVerifyCommandRebuildIndex(t *testing.T) {
	base := seedVerify(t)
	want := readAll(t, copyDir(t, base))
	for _, flags := range [][]string{
		{"--rebuild-index"},
		{"--rebuild-index", "-d", "kb", "-c", "notes", "--deep"},
	} {
		t.Run(strings.Join(flags, " "), func(t *testing.T) {
			dir := copyDir(t, base)
			removeFile(t, filepath.Join(dir, "kb", "notes", "index.db"))
			code, out, errOut := runVerifyCLI(t, dir, flags...)
			if code != 0 || errOut != "" {
				t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
			}
			lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
			if len(lines) != 2 || lines[0] != "rebuilt: kb/notes/index.db" || !strings.HasPrefix(lines[1], summaryLine) {
				t.Errorf("output %q", out)
			}
			if _, err := os.Stat(filepath.Join(dir, "kb", "notes", "index.db")); err != nil {
				t.Fatal(err)
			}
			if code, out, _ := runVerifyCLI(t, dir, "--deep"); code != 0 {
				t.Errorf("verify after the rebuild: exit %d, %q", code, out)
			}
			if got := readAll(t, dir); !reflect.DeepEqual(got, want) {
				t.Errorf("the rebuilt directory reads %d documents, want the %d before", len(got), len(want))
			}
		})
	}
	// JSON output names what was rebuilt.
	dir := copyDir(t, base)
	code, js, _ := runVerifyCLI(t, dir, "--rebuild-index", "-f", "json")
	var rep db.VerifyReport
	if err := json.Unmarshal([]byte(js), &rep); err != nil || code != 0 || !rep.OK || !reflect.DeepEqual(rep.Rebuilt, []string{"kb/notes"}) {
		t.Errorf("json: exit %d, %v, %+v", code, err, rep)
	}
}

func TestVerifyCommandArguments(t *testing.T) {
	dir := seedVerify(t)
	for _, tt := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"collection without database", []string{"-c", "notes"}, "-c/--collection requires -d/--database"},
		{"bad format", []string{"-f", "yaml"}, `invalid --format "yaml"`},
		{"unknown database", []string{"-d", "nope"}, `database "nope"`},
		{"unknown collection", []string{"-d", "kb", "-c", "nope"}, `collection "nope"`},
		{"positional argument", []string{"extra"}, "unknown command"},
		{"missing directory", []string{"-R", filepath.Join(t.TempDir(), "none")}, "does not exist"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, out, errOut := runVerifyCLI(t, dir, tt.args...)
			if code != 1 || !strings.Contains(errOut, tt.wantErr) {
				t.Errorf("exit %d, stdout %q, stderr %q; want 1 and %q", code, out, errOut, tt.wantErr)
			}
		})
	}
}

// TestVerifyCommandWhileServerRuns: a locked directory gives
// ErrDataDirInUse with a hint to stop the server.
func TestVerifyCommandWhileServerRuns(t *testing.T) {
	dir := seedVerify(t)
	e := openEngine(t, dir)
	defer closeEngine(t, e)
	err := verifyOffline(context.Background(), io.Discard, db.Options{Dir: dir}, db.VerifyOptions{}, "text")
	if !errors.Is(err, db.ErrDataDirInUse) || !strings.Contains(err.Error(), "stop it first") {
		t.Fatalf("err = %v, want ErrDataDirInUse with a hint", err)
	}
}

// TestVerifyCommandHelp: the command is registered under utils with help
// text and a multi-line Example (TestHelpExamples checks every command).
func TestVerifyCommandHelp(t *testing.T) {
	c, _, err := rootCmd.Find([]string{"utils", "verify"})
	if err != nil || c != verifyCmd {
		t.Fatalf("utils verify: %v, %v", c, err)
	}
	if c.Short == "" || !strings.Contains(c.Long, "--rebuild-index") || !strings.Contains(c.Example, "cachydb utils verify") {
		t.Errorf("help: short %q, example %q", c.Short, c.Example)
	}
	for _, f := range []string{"database", "collection", "deep", "rebuild-index", "format"} {
		if c.Flags().Lookup(f) == nil {
			t.Errorf("flag --%s missing", f)
		}
	}
}

func TestWriteVerifyText(t *testing.T) {
	r := &db.VerifyReport{
		Databases: 1, Collections: 2, Documents: 3, DataSegments: 4, DataRecords: 5, WALSegments: 6, WALRecords: 7,
		Rebuilt:  []string{"a/b"},
		Problems: []db.VerifyFinding{{File: "a/b/index.db", Message: "bad"}},
		Notices:  []db.VerifyFinding{{Database: "a", Collection: "b", Message: "fine"}},
		Omitted:  2,
	}
	var buf bytes.Buffer
	if err := writeVerifyText(&buf, r); err != nil {
		t.Fatal(err)
	}
	want := "rebuilt: a/b/index.db\na/b/index.db: bad\n... 2 more problem(s) not shown\nnote: a/b: fine\n" +
		"verify: 3 problem(s): 1 database(s), 2 collection(s), 3 document(s), 4 data segment(s) with 5 record(s), 6 WAL segment(s) with 7 record(s)\n"
	if buf.String() != want {
		t.Errorf("text =\n%s\nwant\n%s", buf.String(), want)
	}
	if err := writeVerifyText(failWriter{}, r); err == nil {
		t.Error("a write error is not returned")
	}
	if err := verifyOffline(context.Background(), failWriter{}, db.Options{Dir: t.TempDir()}, db.VerifyOptions{}, "json"); err == nil {
		t.Error("a JSON write error is not returned")
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

// flipMiddle inverts the byte in the middle of path.
func flipMiddle(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func removeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

// copyDir returns a copy of the data directory src.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "data")
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	return dst
}
