package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
	"github.com/hop-/cachydb/pkg/db/internal/segment"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Tests of the offline Verify (plan 013 §3.3, AC-14).

// verifyBase is the healthy base directory, built once per test binary
// (TestMain removes it); tests work on copies of it.
var verifyBase struct {
	once sync.Once
	dir  string
	err  error
}

func verifyBaseDir() (string, error) {
	verifyBase.once.Do(func() {
		verifyBase.dir, verifyBase.err = os.MkdirTemp("", "cachydb-verify-base-")
		if verifyBase.err == nil {
			verifyBase.err = buildVerifyBase(verifyBase.dir)
		}
	})
	return verifyBase.dir, verifyBase.err
}

// buildVerifyBase writes the base directory: database app with
// collection c (indexes by_v, by_tags, and by_pad created last; several
// sealed data segments; deletes; a batch) and database other with
// collection x.
func buildVerifyBase(dir string) error {
	ctx := context.Background()
	opts := testOptions(dir)
	opts.DataSegmentSize = 4096
	e, err := Open(ctx, opts)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_ = e.Close(ctx)
		return err
	}
	d, err := e.CreateDatabase(ctx, "app")
	if err != nil {
		return fail(err)
	}
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		return fail(err)
	}
	c, err := d.Collection("c")
	if err != nil {
		return fail(err)
	}
	for _, ix := range [][2]string{{"by_v", "v"}, {"by_tags", "tags"}} {
		if err := c.CreateIndex(ctx, ix[0], ix[1]); err != nil {
			return fail(err)
		}
	}
	for i := range 300 {
		data := map[string]any{"v": float64(i % 17), "tags": []any{"a", fmt.Sprint(i % 3)}, "pad": strings.Repeat("x", 50)}
		if _, err := c.Insert(ctx, &Document{ID: fmt.Sprintf("id%03d", i), Data: data}); err != nil {
			return fail(err)
		}
		if i%50 == 49 {
			if err := c.flush(); err != nil {
				return fail(err)
			}
		}
	}
	for i := range 40 {
		if err := c.Delete(ctx, fmt.Sprintf("id%03d", i*3)); err != nil {
			return fail(err)
		}
	}
	var ops []WriteOp
	for i := range 30 {
		ops = append(ops, WriteOp{Kind: OpReplace, ID: fmt.Sprintf("id%03d", 200+i), Document: &Document{Data: map[string]any{"v": "s", "o": map[string]any{"k": 1.0}}}})
	}
	if _, err := c.Batch(ctx, ops); err != nil {
		return fail(err)
	}
	if err := c.flush(); err != nil {
		return fail(err)
	}
	if err := c.CreateIndex(ctx, "by_pad", "pad"); err != nil {
		return fail(err)
	}
	o, err := e.CreateDatabase(ctx, "other")
	if err != nil {
		return fail(err)
	}
	if err := o.CreateCollection(ctx, "x", nil); err != nil {
		return fail(err)
	}
	x, err := o.Collection("x")
	if err != nil {
		return fail(err)
	}
	for i := range 5 {
		if _, err := x.Insert(ctx, &Document{ID: fmt.Sprint(i), Data: map[string]any{"i": float64(i)}}); err != nil {
			return fail(err)
		}
	}
	return e.Close(ctx)
}

func TestMain(m *testing.M) {
	code := m.Run()
	if verifyBase.dir != "" {
		_ = os.RemoveAll(verifyBase.dir) // a temporary directory; nothing to report
	}
	os.Exit(code)
}

// verifyCopy returns a fresh copy of the base directory.
func verifyCopy(t *testing.T) string {
	t.Helper()
	base, err := verifyBaseDir()
	if err != nil {
		t.Fatalf("building the verify base: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "data")
	if err := os.CopyFS(dst, os.DirFS(base)); err != nil {
		t.Fatal(err)
	}
	return dst
}

// snapshotFiles returns every file under dir (but the lock file) with its
// contents.
func snapshotFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() == lockFileName {
			return err
		}
		b, err := os.ReadFile(p)
		out[p] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// runVerify runs Verify on dir and logs the findings.
func runVerify(t *testing.T, dir string, vo VerifyOptions) *VerifyReport {
	t.Helper()
	r, err := Verify(context.Background(), testOptions(dir), vo)
	if err != nil {
		t.Fatalf("Verify(%+v): %v", vo, err)
	}
	for _, f := range r.Problems {
		t.Logf("problem: %s", f)
	}
	for _, f := range r.Notices {
		t.Logf("notice: %s", f)
	}
	if r.OK != (len(r.Problems) == 0 && r.Omitted == 0) {
		t.Errorf("OK = %v with %d problems, %d omitted", r.OK, len(r.Problems), r.Omitted)
	}
	return r
}

// findingAt reports whether one of fs names file (relative to the data
// directory; "" = any) and contains every text.
func findingAt(fs []VerifyFinding, file string, texts ...string) bool {
	for _, f := range fs {
		if file != "" && f.File != file {
			continue
		}
		ok := true
		for _, s := range texts {
			ok = ok && strings.Contains(f.String(), s)
		}
		if ok {
			return true
		}
	}
	return false
}

// wantProblem checks that r failed with a problem naming file and texts.
func wantProblem(t *testing.T, r *VerifyReport, file string, texts ...string) {
	t.Helper()
	if r.OK {
		t.Errorf("Verify reports OK, want a problem in %s", file)
	}
	if !findingAt(r.Problems, file, texts...) {
		t.Errorf("no problem in %s mentioning %q among %v", file, texts, r.Problems)
	}
}

// liveDocs opens dir and returns every document of every collection.
func liveDocs(t *testing.T, dir string) map[string]map[string]map[string]any {
	t.Helper()
	e := openEngine(t, testOptions(dir))
	out := map[string]map[string]map[string]any{}
	for _, dn := range e.ListDatabases() {
		d := mustDB(t, e, dn)
		for _, cn := range d.ListCollections() {
			c := mustColl(t, d, cn)
			out[dn+"/"+cn] = docsOf(t, c)
			d.mu.RLock()
			checkIndexEntries(t, c, mustFind(t, c))
			d.mu.RUnlock()
		}
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	return out
}

const (
	vColl      = "app/c"
	vIndexFile = "app/c/index.db"
)

// collSegments returns the data segment ids of app/c in dir.
func collSegments(t *testing.T, dir string) []uint32 {
	t.Helper()
	segs, err := segment.ListSegments(filepath.Join(dir, vColl))
	if err != nil || len(segs) < 3 {
		t.Fatalf("segments of %s: %v, %v", vColl, segs, err)
	}
	return segs
}

// updateIndexFile changes app/c's index file behind the engine's back.
func updateIndexFile(t *testing.T, dir string, fn func(st *idxstore.Store, u *idxstore.WTx) error) {
	t.Helper()
	st, err := idxstore.Open(filepath.Join(dir, vColl), idxstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(u *idxstore.WTx) error { return fn(st, u) }); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// flipFileByte inverts the byte at off of path (from the end if negative).
func flipFileByte(t *testing.T, path string, off int64) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if off < 0 {
		off += int64(len(b))
	}
	b[off] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyHealthy(t *testing.T) {
	dir := verifyCopy(t)
	before := snapshotFiles(t, dir)
	r := runVerify(t, dir, VerifyOptions{})
	if !r.OK || len(r.Problems) != 0 || len(r.Notices) != 0 {
		t.Fatalf("healthy directory: %+v", r)
	}
	if r.Format != FormatVersion || r.Databases != 2 || r.Collections != 2 || r.Documents != 265 ||
		r.DataSegments < 4 || r.DataRecords < 300 || r.WALSegments != 1 || r.WALRecords == 0 || r.WALFirst != 1 || r.WALNext != uint64(r.WALRecords)+1 {
		t.Errorf("healthy report: %+v", r)
	}
	if r.Deep || r.Indexes != 0 || len(r.Rebuilt) != 0 {
		t.Errorf("shallow report: deep %v, %d indexes, rebuilt %v", r.Deep, r.Indexes, r.Rebuilt)
	}
	deep := runVerify(t, dir, VerifyOptions{Deep: true})
	if !deep.OK || len(deep.Notices) != 0 || !deep.Deep || deep.Indexes != 3 {
		t.Errorf("deep verify of a healthy directory: %+v", deep)
	}
	// Verify writes nothing.
	after := snapshotFiles(t, dir)
	if !reflect.DeepEqual(slices.Sorted(maps.Keys(after)), slices.Sorted(maps.Keys(before))) {
		t.Errorf("Verify changed the file list")
	}
	for p, b := range before {
		if !bytes.Equal(after[p], b) {
			t.Errorf("Verify changed %s", p)
		}
	}
	// The report's JSON names its fields.
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{`"ok":true`, `"format":3`, `"documents":265`, `"problems":[]`, `"notices":[]`, `"wal_next"`} {
		if !bytes.Contains(b, []byte(k)) {
			t.Errorf("report JSON %s lacks %s", b, k)
		}
	}
}

func TestVerifyScope(t *testing.T) {
	ctx := context.Background()
	dir := verifyCopy(t)
	r := runVerify(t, dir, VerifyOptions{Database: "app"})
	if !r.OK || r.Databases != 1 || r.Collections != 1 || r.Documents != 260 {
		t.Errorf("-d app: %+v", r)
	}
	r = runVerify(t, dir, VerifyOptions{Database: "other", Collection: "x", Deep: true})
	if !r.OK || r.Databases != 1 || r.Collections != 1 || r.Documents != 5 || r.Indexes != 0 {
		t.Errorf("-d other -c x: %+v", r)
	}
	for _, tt := range []struct {
		vo      VerifyOptions
		wantErr error
		text    string
	}{
		{VerifyOptions{Database: "nope"}, ErrNotFound, `database "nope"`},
		{VerifyOptions{Database: "app", Collection: "nope"}, ErrNotFound, `collection "nope"`},
		{VerifyOptions{Collection: "c"}, nil, "a collection needs its database"},
	} {
		_, err := Verify(ctx, testOptions(dir), tt.vo)
		if err == nil || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) || !strings.Contains(err.Error(), tt.text) {
			t.Errorf("Verify(%+v) = %v", tt.vo, err)
		}
	}
	// Damage outside the scope is not reported, but the WAL always is.
	flipFileByte(t, filepath.Join(dir, vColl, "index.db"), 5000)
	if r := runVerify(t, dir, VerifyOptions{Database: "other"}); !r.OK {
		t.Errorf("damage in app/c fails a check of other: %v", r.Problems)
	}
}

func TestVerifyErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := Verify(ctx, Options{}, VerifyOptions{}); err == nil {
		t.Error("Verify without a directory succeeded")
	}
	if _, err := Verify(ctx, testOptions(filepath.Join(t.TempDir(), "missing")), VerifyOptions{}); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Verify of a missing directory: %v", err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, testOptions(file), VerifyOptions{}); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("Verify of a file: %v", err)
	}
	// A directory an Engine has open.
	dir := verifyCopy(t)
	e := openEngine(t, testOptions(dir))
	if _, err := Verify(ctx, testOptions(dir), VerifyOptions{}); !errors.Is(err, ErrDataDirInUse) {
		t.Errorf("Verify of an open directory: %v", err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// An older layout Open refuses.
	old := t.TempDir()
	if err := os.WriteFile(filepath.Join(old, "wal.checkpoint"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, testOptions(old), VerifyOptions{}); !errors.Is(err, ErrIncompatibleDataDir) {
		t.Errorf("Verify of an old layout: %v", err)
	}
	// A format file of an unknown version.
	bad := t.TempDir()
	if err := os.WriteFile(filepath.Join(bad, formatFileName), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, testOptions(bad), VerifyOptions{}); err == nil {
		t.Error("Verify of a directory with a damaged format file succeeded")
	}
	// A cancelled context.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Verify(cctx, testOptions(verifyCopy(t)), VerifyOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("Verify with a cancelled context: %v", err)
	}
}

func TestVerifyEmptyAndNewDirectories(t *testing.T) {
	// An empty directory (what Open would initialize) is fine.
	empty := t.TempDir()
	r := runVerify(t, empty, VerifyOptions{})
	if !r.OK || len(r.Notices) != 0 || r.WALNext != 1 || r.Databases != 0 {
		t.Errorf("empty directory: %+v", r)
	}
	// Files without a format file: a notice.
	nofmt := verifyCopy(t)
	if err := os.Remove(filepath.Join(nofmt, formatFileName)); err != nil {
		t.Fatal(err)
	}
	r = runVerify(t, nofmt, VerifyOptions{})
	if !r.OK || !findingAt(r.Notices, formatFileName, "missing; Open writes it") {
		t.Errorf("missing format file: %+v", r.Notices)
	}
}

// TestVerifyDamage injects each damage AC-14 names (and more) into a copy
// of the base directory and checks that Verify fails with a finding that
// names the file or collection and the problem.
func TestVerifyDamage(t *testing.T) {
	tests := []struct {
		name   string
		vo     VerifyOptions
		damage func(t *testing.T, dir string)
		file   string // the problem's file
		texts  []string
	}{
		{
			name: "flipped byte in a sealed data segment",
			damage: func(t *testing.T, dir string) {
				seg := collSegments(t, dir)[0]
				p := segment.DataPath(filepath.Join(dir, vColl), seg)
				fi, err := os.Stat(p)
				if err != nil {
					t.Fatal(err)
				}
				flipFileByte(t, p, fi.Size()/2)
			},
			file:  vColl + "/seg-000001.data",
			texts: []string{"the records after it cannot be scanned"},
		},
		{
			name: "flipped byte in the middle of the WAL",
			damage: func(t *testing.T, dir string) {
				p := walFiles(t, dir)[0]
				fi, err := os.Stat(p)
				if err != nil {
					t.Fatal(err)
				}
				flipFileByte(t, p, fi.Size()/2)
			},
			file:  filepath.Base(walFiles(t, verifyCopy(t))[0]),
			texts: []string{"Open fails"},
		},
		{
			name: "key-table entry pointing at another document's record",
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					e, ok, err := u.Key("id100")
					if err != nil || !ok {
						return fmt.Errorf("key id100: %v, %v", ok, err)
					}
					return u.PutKey("id101", e)
				})
			},
			file:  vIndexFile,
			texts: []string{`"id101"`, `of "id100"`},
		},
		{
			name: "key-table entry with the wrong size",
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					e, _, err := u.Key("id100")
					e.Size++
					if err == nil {
						err = u.PutKey("id100", e)
					}
					return err
				})
			},
			file:  vIndexFile,
			texts: []string{`the key-table entry of "id100" has size`},
		},
		{
			name: "key-table entry pointing into the middle of a record",
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					e, _, err := u.Key("id101")
					e.Pos += 3
					if err == nil {
						err = u.PutKey("id101", e)
					}
					return err
				})
			},
			file:  vIndexFile,
			texts: []string{`the key-table entry of "id101" points at`, "no valid record"},
		},
		{
			name:   "wrong persisted count",
			damage: func(t *testing.T, dir string) { patchIndexCount(t, filepath.Join(dir, vColl), 260, 265) },
			file:   vIndexFile,
			texts:  []string{"count is 265, the key table has 260 entries"},
		},
		{
			name: "missing index entry (deep)",
			vo:   VerifyOptions{Deep: true},
			damage: func(t *testing.T, dir string) {
				enc, _, _ := idxstore.EncodeValue(float64(100 % 17))
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					w, err := u.Index("by_v")
					if err != nil {
						return err
					}
					return w.Delete(enc, "id100")
				})
			},
			file:  vIndexFile,
			texts: []string{`index "by_v" has no entry for document "id100" with value 15`},
		},
		{
			name: "extra index entry (deep)",
			vo:   VerifyOptions{Deep: true},
			damage: func(t *testing.T, dir string) {
				enc, _, _ := idxstore.EncodeValue("bogus")
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					w, err := u.Index("by_tags")
					if err != nil {
						return err
					}
					return w.Put(enc, "id101", 0)
				})
			},
			file:  vIndexFile,
			texts: []string{`index "by_tags" has an entry for document "id101" with value "bogus", which the document does not hold`},
		},
		{
			name: "wrong flags of an index entry (deep)",
			vo:   VerifyOptions{Deep: true},
			damage: func(t *testing.T, dir string) {
				enc, _, _ := idxstore.EncodeValue("a")
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					w, err := u.Index("by_tags")
					if err != nil {
						return err
					}
					return w.Put(enc, "id101", 0) // an array element has FlagElement
				})
			},
			file:  vIndexFile,
			texts: []string{`index "by_tags": the entry for document "id101" with value "a" has flags`},
		},
		{
			name: "wrong complex counter (deep)",
			vo:   VerifyOptions{Deep: true},
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					w, err := u.Index("by_v")
					if err != nil {
						return err
					}
					return w.AddComplex(1)
				})
			},
			file:  vIndexFile,
			texts: []string{`index "by_v": the complex counter is`},
		},
		{
			name: "wrong arrays counter (deep)",
			vo:   VerifyOptions{Deep: true},
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					w, err := u.Index("by_tags")
					if err != nil {
						return err
					}
					return w.AddArrays(-1)
				})
			},
			file:  vIndexFile,
			texts: []string{`index "by_tags": the arrays counter is`},
		},
		{
			name:   "missing index file",
			damage: func(t *testing.T, dir string) { mustRemove(t, filepath.Join(dir, vIndexFile)) },
			file:   vIndexFile,
			texts:  []string{"--rebuild-index"},
		},
		{
			name:   "index file whose meta pages are damaged",
			damage: func(t *testing.T, dir string) { overwriteIndexPages(t, filepath.Join(dir, vIndexFile)) },
			file:   vIndexFile,
			texts:  []string{"--rebuild-index"},
		},
		{
			name: "missing registered data segment",
			damage: func(t *testing.T, dir string) {
				mustRemove(t, segment.DataPath(filepath.Join(dir, vColl), collSegments(t, dir)[1]))
			},
			file:  vColl + "/seg-000002.data",
			texts: []string{"registered in index.db, but the file is missing"},
		},
		{
			name: "covered segment shorter than index.db covers",
			damage: func(t *testing.T, dir string) {
				segs := collSegments(t, dir)
				p := segment.DataPath(filepath.Join(dir, vColl), segs[len(segs)-1])
				fi, err := os.Stat(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(p, fi.Size()-10); err != nil {
					t.Fatal(err)
				}
			},
			file:  "",
			texts: []string{"but index.db covers", "data is missing"},
		},
		{
			name: "undecodable document record in the WAL",
			damage: func(t *testing.T, dir string) {
				appendWALRecords(t, dir, wal.Record{Op: wal.OpPutDoc, DB: "app", Coll: "c", ID: "bad", Payload: []byte("not json")})
			},
			file:  "",
			texts: []string{"put_doc record", "for app/c", "replay fails"},
		},
		{
			name: "undecodable item of a batch record in the WAL",
			damage: func(t *testing.T, dir string) {
				p, err := wal.AppendBatch(nil, []wal.BatchItem{{Op: wal.OpPutDoc, ID: "ok", Doc: []byte(`{"_id":"ok"}`)}, {Op: wal.OpDeleteDoc, ID: "del"}, {Op: wal.OpPutDoc, ID: "bad", Doc: []byte(`[1]`)}})
				if err != nil {
					t.Fatal(err)
				}
				appendWALRecords(t, dir, wal.Record{Op: wal.OpBatch, DB: "app", Coll: "c", Payload: p})
			},
			file:  "",
			texts: []string{"batch record", "item 2", "replay fails"},
		},
		{
			name: "malformed batch payload in the WAL",
			damage: func(t *testing.T, dir string) {
				appendWALRecords(t, dir, wal.Record{Op: wal.OpBatch, DB: "app", Coll: "c", Payload: []byte{9, 0, 0, 0, 1}})
			},
			file:  "",
			texts: []string{"batch record", "malformed batch payload"},
		},
		{
			name: "metadata record that is not JSON",
			damage: func(t *testing.T, dir string) {
				appendWALRecords(t, dir, wal.Record{Op: wal.OpCreateIndex, DB: "app", Coll: "c", Payload: []byte("{")})
			},
			file:  "",
			texts: []string{"create_index record", "not valid JSON"},
		},
		{
			name: "unreadable db.meta",
			damage: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, "other", dbMetaFile), []byte("garbage"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			file:  "other",
			texts: []string{"Open fails"},
		},
		{
			name: "WAL files missing",
			damage: func(t *testing.T, dir string) {
				for _, p := range walFiles(t, dir) {
					mustRemove(t, p)
				}
			},
			file:  vIndexFile,
			texts: []string{"references WAL offset", "WAL files are missing"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := verifyCopy(t)
			tt.damage(t, dir)
			before := snapshotFiles(t, dir)
			r := runVerify(t, dir, tt.vo)
			wantProblem(t, r, tt.file, tt.texts...)
			for _, f := range r.Problems {
				if f.File == "" && f.Collection == "" {
					t.Errorf("finding %q names neither a file nor a collection", f)
				}
			}
			if after := snapshotFiles(t, dir); !reflect.DeepEqual(after, before) {
				t.Error("Verify changed the damaged directory")
			}
		})
	}
}

// TestVerifyDeepOnlyDamage: index-entry damage passes the shallow check
// and fails the deep one.
func TestVerifyDeepOnlyDamage(t *testing.T) {
	dir := verifyCopy(t)
	enc, _, _ := idxstore.EncodeValue(float64(100 % 17))
	bogus, _, _ := idxstore.EncodeValue("bogus")
	updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
		w, err := u.Index("by_v")
		if err != nil {
			return err
		}
		if err := w.Delete(enc, "id100"); err != nil {
			return err
		}
		return w.Put(bogus, "id101", 0)
	})
	if r := runVerify(t, dir, VerifyOptions{}); !r.OK {
		t.Errorf("shallow verify fails on index-entry damage: %v", r.Problems)
	}
	r := runVerify(t, dir, VerifyOptions{Deep: true})
	wantProblem(t, r, vIndexFile, `has no entry for document "id100"`)
	wantProblem(t, r, vIndexFile, `entry for document "id101" with value "bogus"`)
	if len(r.Problems) != 2 {
		t.Errorf("%d problems, want 2: %v", len(r.Problems), r.Problems)
	}
}

// TestVerifyNotices: what Open repairs by itself does not fail the check.
func TestVerifyNotices(t *testing.T) {
	tests := []struct {
		name   string
		damage func(t *testing.T, dir string)
		file   string
		text   string
	}{
		{
			name: "torn tail of the WAL",
			damage: func(t *testing.T, dir string) {
				p := walFiles(t, dir)[0]
				f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte{40, 0, 0, 0, 1, 2, 3}); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			},
			text: "torn tail of the newest segment",
		},
		{
			name: "unregistered data segment",
			damage: func(t *testing.T, dir string) {
				c := filepath.Join(dir, vColl)
				b, err := os.ReadFile(segment.DataPath(c, collSegments(t, dir)[0]))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(segment.DataPath(c, 999), b, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			file: vColl + "/seg-000999.data",
			text: "not registered in index.db",
		},
		{
			name: "bytes past the covered end",
			damage: func(t *testing.T, dir string) {
				segs := collSegments(t, dir)
				p := segment.DataPath(filepath.Join(dir, vColl), segs[len(segs)-1])
				f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			},
			text: "past the end index.db covers",
		},
		{
			name: "index entries of an index coll.meta does not define",
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error { return u.CreateIndex("ghost") })
			},
			file: vIndexFile,
			text: `index "ghost", which coll.meta does not define`,
		},
		{
			name: "an index coll.meta defines has no entries",
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error { return u.DropIndex("by_pad") })
			},
			file: vIndexFile,
			text: `has no entries for index "by_pad"`,
		},
		{
			name: "an index that is not completely built",
			damage: func(t *testing.T, dir string) {
				updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
					if err := u.DropIndex("by_pad"); err != nil {
						return err
					}
					return u.CreateIndex("by_pad")
				})
			},
			file: vIndexFile,
			text: `index "by_pad" is not completely built`,
		},
		{
			name: "unusable metadata checkpoint",
			damage: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, metaCheckpointFileName), []byte("garbage"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			file: metaCheckpointFileName,
			text: "Open ignores it",
		},
		{
			name: "database directory without db.meta",
			damage: func(t *testing.T, dir string) {
				if err := os.Mkdir(filepath.Join(dir, "half"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
			file: "half",
			text: "directory without db.meta",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := verifyCopy(t)
			tt.damage(t, dir)
			r := runVerify(t, dir, VerifyOptions{Deep: true})
			if !r.OK {
				t.Errorf("a repairable state fails the check: %v", r.Problems)
			}
			if !findingAt(r.Notices, tt.file, tt.text) {
				t.Errorf("no notice in %q mentioning %q among %v", tt.file, tt.text, r.Notices)
			}
			// Open agrees: it opens the directory and reads the same data.
			liveDocs(t, dir)
		})
	}
}

// TestVerifyRebuildIndex: after an index.db is deleted by hand,
// RebuildIndex restores a directory that reads the same data, and verify
// then passes.
func TestVerifyRebuildIndex(t *testing.T) {
	base := verifyCopy(t)
	want := liveDocs(t, base)
	for _, tt := range []struct {
		name    string
		vo      VerifyOptions
		rebuilt []string
	}{
		{"all", VerifyOptions{RebuildIndex: true}, []string{"app/c", "other/x"}},
		{"one database", VerifyOptions{RebuildIndex: true, Database: "app"}, []string{"app/c"}},
		{"one collection", VerifyOptions{RebuildIndex: true, Database: "app", Collection: "c", Deep: true}, []string{"app/c"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := verifyCopy(t)
			mustRemove(t, filepath.Join(dir, vIndexFile))
			wantProblem(t, runVerify(t, dir, VerifyOptions{}), vIndexFile)
			r := runVerify(t, dir, tt.vo)
			if !r.OK || len(r.Notices) != 0 {
				t.Fatalf("verify after the rebuild: %+v", r)
			}
			if !reflect.DeepEqual(r.Rebuilt, tt.rebuilt) {
				t.Errorf("Rebuilt = %v, want %v", r.Rebuilt, tt.rebuilt)
			}
			if r := runVerify(t, dir, VerifyOptions{Deep: true}); !r.OK || len(r.Notices) != 0 {
				t.Errorf("deep verify after the rebuild: %v %v", r.Problems, r.Notices)
			}
			if got := liveDocs(t, dir); !reflect.DeepEqual(got, want) {
				t.Errorf("the rebuilt directory reads different data")
			}
		})
	}
	// A damaged page is repaired the same way.
	dir := verifyCopy(t)
	overwriteIndexPages(t, filepath.Join(dir, vIndexFile))
	if r := runVerify(t, dir, VerifyOptions{RebuildIndex: true, Deep: true}); !r.OK {
		t.Errorf("verify after rebuilding a damaged index.db: %v", r.Problems)
	}
	if got := liveDocs(t, dir); !reflect.DeepEqual(got, want) {
		t.Errorf("the rebuilt directory reads different data")
	}
}

// TestVerifyStaleIndex: an index.db older than the data segments is a
// notice while the WAL still replays the changes, and a problem once it
// does not (data Open would discard, follow-up F-010); RebuildIndex
// recovers the data.
func TestVerifyStaleIndex(t *testing.T) {
	ctx := context.Background()
	write := func(t *testing.T, dir string, mod func(*Options), n int, retention bool) {
		t.Helper()
		opts := testOptions(dir)
		opts.DataSegmentSize = 4096
		if mod != nil {
			mod(&opts)
		}
		e, err := Open(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		c := mustColl(t, mustDB(t, e, "app"), "c")
		for i := range n {
			mustInsert(t, c, fmt.Sprintf("new%03d", i), map[string]any{"v": 1.0, "pad": strings.Repeat("y", 100)})
			if retention && i%20 == 0 {
				mustFlush(t, c)
				e.wal.EnforceRetention()
			}
		}
		if _, err := c.Update(ctx, "id101", map[string]any{"v": "changed"}); err != nil {
			t.Fatal(err)
		}
		mustFlush(t, c)
		if retention {
			e.wal.EnforceRetention()
		}
		if err := e.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("WAL still replays", func(t *testing.T) {
		dir := verifyCopy(t)
		old, err := os.ReadFile(filepath.Join(dir, vIndexFile))
		if err != nil {
			t.Fatal(err)
		}
		write(t, dir, nil, 100, false)
		if err := os.WriteFile(filepath.Join(dir, vIndexFile), old, 0o644); err != nil {
			t.Fatal(err)
		}
		r := runVerify(t, dir, VerifyOptions{})
		if !r.OK || len(r.Notices) == 0 {
			t.Errorf("stale index.db with the WAL intact: OK %v, notices %v", r.OK, r.Notices)
		}
		if got := liveDocs(t, dir)["app/c"]; len(got) != 360 || got["id101"]["v"] != "changed" {
			t.Errorf("Open of the stale directory: %d documents, id101 = %v", len(got), got["id101"])
		}
	})
	t.Run("WAL no longer replays", func(t *testing.T) {
		dir := verifyCopy(t)
		old, err := os.ReadFile(filepath.Join(dir, vIndexFile))
		if err != nil {
			t.Fatal(err)
		}
		write(t, dir, func(o *Options) {
			o.WALSegmentSize = 4096
			o.WALRetentionSize = 1
			o.WALRetentionAge = -1
		}, 300, true)
		if err := os.WriteFile(filepath.Join(dir, vIndexFile), old, 0o644); err != nil {
			t.Fatal(err)
		}
		r := runVerify(t, dir, VerifyOptions{})
		wantProblem(t, r, vIndexFile, "opening would discard", "--rebuild-index")
		r = runVerify(t, dir, VerifyOptions{RebuildIndex: true, Deep: true})
		if !r.OK {
			t.Errorf("verify after the rebuild: %v", r.Problems)
		}
		if got := liveDocs(t, dir)["app/c"]; len(got) != 560 || got["id101"]["v"] != "changed" {
			t.Errorf("after the rebuild: %d documents, id101 = %v; want 560 and the change", len(got), got["id101"])
		}
	})
	t.Run("covered data newer than the key table", func(t *testing.T) {
		// A flush whose segment append is in index.db's covered range but
		// whose key-table changes are lost: the restored key table is
		// older than the data it covers.
		dir := verifyCopy(t)
		old, err := os.ReadFile(filepath.Join(dir, vIndexFile))
		if err != nil {
			t.Fatal(err)
		}
		write(t, dir, nil, 10, false)
		st, err := idxstore.Open(filepath.Join(dir, vColl), idxstore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		var covered idxstore.Position
		var segs []idxstore.SegmentStats
		if err := st.View(func(tx *idxstore.Tx) error {
			if covered, err = tx.Covered(); err != nil {
				return err
			}
			segs, err = tx.Segments()
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, vIndexFile), old, 0o644); err != nil {
			t.Fatal(err)
		}
		updateIndexFile(t, dir, func(st *idxstore.Store, u *idxstore.WTx) error {
			for _, s := range segs {
				if err := u.SetSegment(s); err != nil {
					return err
				}
			}
			return u.SetCovered(covered)
		})
		r := runVerify(t, dir, VerifyOptions{})
		wantProblem(t, r, vIndexFile, "is older than the data segments", "newer than its key table")
	})
}

// TestVerifyMaxProblems: problems beyond MaxProblems are counted, not
// listed, and fail the check.
func TestVerifyMaxProblems(t *testing.T) {
	dir := verifyCopy(t)
	for _, seg := range collSegments(t, dir) {
		p := segment.DataPath(filepath.Join(dir, vColl), seg)
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		flipFileByte(t, p, fi.Size()/2)
	}
	all := runVerify(t, dir, VerifyOptions{})
	if len(all.Problems) < 3 {
		t.Fatalf("want several problems, got %v", all.Problems)
	}
	r := runVerify(t, dir, VerifyOptions{MaxProblems: 1})
	if r.OK || len(r.Problems) != 1 || r.Omitted != len(all.Problems)-1 {
		t.Errorf("MaxProblems 1: %d listed, %d omitted, OK %v; want 1 and %d", len(r.Problems), r.Omitted, r.OK, len(all.Problems)-1)
	}
}

// TestVerifyFormat2: a format-2 directory (a released fixture, copied) is
// a notice, and RebuildIndex refuses it.
func TestVerifyFormat2(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.CopyFS(dir, os.DirFS(filepath.Join("testdata", "compat", "v0.0.6", "data"))); err != nil {
		t.Fatal(err)
	}
	r := runVerify(t, dir, VerifyOptions{Deep: true})
	if !r.OK || r.Format != upgradableFormatVersion || !findingAt(r.Notices, formatFileName, "format version 2") {
		t.Errorf("format-2 directory: %+v", r)
	}
	if _, err := Verify(ctx, testOptions(dir), VerifyOptions{RebuildIndex: true}); err == nil || !strings.Contains(err.Error(), "format version 2") {
		t.Errorf("RebuildIndex of a format-2 directory: %v", err)
	}
	// Open upgrades it; verify then checks its index files.
	liveDocs(t, dir)
	if r := runVerify(t, dir, VerifyOptions{Deep: true}); !r.OK || r.Format != FormatVersion || r.Indexes == 0 {
		t.Errorf("after the upgrade: %+v", r)
	}
}

func TestVerifyFindingString(t *testing.T) {
	for _, tt := range []struct {
		f    VerifyFinding
		want string
	}{
		{VerifyFinding{File: "app/c/index.db", Database: "app", Collection: "c", Message: "m"}, "app/c/index.db: m"},
		{VerifyFinding{Database: "app", Collection: "c", Message: "m"}, "app/c: m"},
		{VerifyFinding{Database: "app", Message: "m"}, "app: m"},
		{VerifyFinding{Message: "m"}, "m"},
	} {
		if got := tt.f.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
	if got := withoutPath(errors.New("segment: bad record: /x/y"), "/x/y"); got != "segment: bad record" {
		t.Errorf("withoutPath = %q", got)
	}
	if got := withoutPath(errors.New("/x/y is bad"), "/x/y"); got != "is bad" {
		t.Errorf("withoutPath = %q", got)
	}
	if got := describeIndexValue([]byte{0xff, 0xfe}); !strings.Contains(got, "undecodable") {
		t.Errorf("describeIndexValue of garbage = %q", got)
	}
}

// ---- damage helpers ----

// patchIndexCount rewrites the persisted document count of the index file
// in collection directory dir from have to want, behind the store's back:
// only idxstore may write the file through bbolt, so the test changes the
// bytes of the "count" meta key's value in place (bbolt keeps a key and
// its value next to each other in a leaf page, and leaf pages carry no
// checksum).
func patchIndexCount(t *testing.T, dir string, have, want uint64) {
	t.Helper()
	path := idxstore.Path(dir)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	old := binary.BigEndian.AppendUint64([]byte("count"), have)
	repl := binary.BigEndian.AppendUint64([]byte("count"), want)
	if n := bytes.Count(b, old); n == 0 {
		t.Fatalf("no %q meta value %d in %s", "count", have, path)
	}
	if err := os.WriteFile(path, bytes.ReplaceAll(b, old, repl), 0o644); err != nil {
		t.Fatal(err)
	}
	// The store still opens and reads the new count.
	st, err := idxstore.Open(dir, idxstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	var got int64
	if err := st.View(func(tx *idxstore.Tx) error {
		got, err = tx.Count()
		return err
	}); err != nil || got != int64(want) {
		t.Fatalf("count after the patch = %d, %v; want %d", got, err, want)
	}
}

// overwriteIndexPages overwrites both meta pages of the index file, so it
// no longer opens.
func overwriteIndexPages(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copy(b, bytes.Repeat([]byte{0xA5}, 2*os.Getpagesize()))
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// appendWALRecords appends records to the WAL of the closed directory dir.
func appendWALRecords(t *testing.T, dir string, recs ...wal.Record) {
	t.Helper()
	l, err := wal.Open(wal.Options{Dir: dir, Logger: testOptions(dir).Logger})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if _, err := l.Append(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---- code-review-1 advisories ----

// TestVerifyReplayIDRules: a WAL document record whose id replay refuses
// (longer than maxIDLen, which the WAL format can still hold) makes Open
// fail, so Verify must report it; and a batch record that repeats an id,
// which the format forbids, is a problem too. The expected behaviour is
// the one replay has (recovery.go replayItem) and the batch format states
// (plan 013 §3.1: an id appears at most once per batch).
func TestVerifyReplayIDRules(t *testing.T) {
	long := strings.Repeat("L", maxIDLen+1)
	batch := func(t *testing.T, items ...wal.BatchItem) []byte {
		t.Helper()
		p, err := wal.AppendBatch(nil, items)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	for _, tt := range []struct {
		name     string
		rec      func(t *testing.T) wal.Record
		openFail bool // Open refuses the directory
	}{
		{
			name: "put_doc with an id longer than maxIDLen",
			rec: func(t *testing.T) wal.Record {
				return wal.Record{Op: wal.OpPutDoc, DB: "app", Coll: "c", ID: long, Payload: []byte(`{"_id":"` + long + `","v":1}`)}
			},
			openFail: true,
		},
		{
			name: "delete_doc with an id longer than maxIDLen",
			rec: func(t *testing.T) wal.Record {
				return wal.Record{Op: wal.OpDeleteDoc, DB: "app", Coll: "c", ID: long}
			},
			openFail: true,
		},
		{
			name: "batch item with an id longer than maxIDLen",
			rec: func(t *testing.T) wal.Record {
				return wal.Record{Op: wal.OpBatch, DB: "app", Coll: "c", Payload: batch(t,
					wal.BatchItem{Op: wal.OpPutDoc, ID: "fine", Doc: []byte(`{"_id":"fine","v":1}`)},
					wal.BatchItem{Op: wal.OpPutDoc, ID: long, Doc: []byte(`{"_id":"` + long + `","v":2}`)})}
			},
			openFail: true,
		},
		{
			name: "batch record that repeats an id",
			rec: func(t *testing.T) wal.Record {
				return wal.Record{Op: wal.OpBatch, DB: "app", Coll: "c", Payload: batch(t,
					wal.BatchItem{Op: wal.OpPutDoc, ID: "twice", Doc: []byte(`{"_id":"twice","v":1}`)},
					wal.BatchItem{Op: wal.OpDeleteDoc, ID: "twice"})}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := verifyCopy(t)
			appendWALRecords(t, dir, tt.rec(t))
			walFile := filepath.Base(walFiles(t, dir)[len(walFiles(t, dir))-1])
			if tt.openFail {
				// The premise: Open fails on this WAL.
				cp := filepath.Join(t.TempDir(), "data")
				if err := os.CopyFS(cp, os.DirFS(dir)); err != nil {
					t.Fatal(err)
				}
				if e, err := Open(context.Background(), testOptions(cp)); err == nil {
					_ = e.Close(context.Background())
					t.Fatal("Open accepted the record; the test premise is wrong")
				}
			}
			r := runVerify(t, dir, VerifyOptions{})
			wantProblem(t, r, walFile, "app/c")
		})
	}
}

// TestVerifyLeavesIndexFilesUntouched: without RebuildIndex, Verify (also
// deep) leaves every index.db byte-identical with its modification time
// unchanged, healthy or damaged.
func TestVerifyLeavesIndexFilesUntouched(t *testing.T) {
	for _, tt := range []struct {
		name   string
		damage func(t *testing.T, dir string)
	}{
		{"healthy", func(t *testing.T, dir string) {}},
		{"wrong persisted count", func(t *testing.T, dir string) { patchIndexCount(t, filepath.Join(dir, vColl), 260, 261) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := verifyCopy(t)
			tt.damage(t, dir)
			files := []string{filepath.Join(dir, vIndexFile), filepath.Join(dir, "other", "x", "index.db")}
			type state struct {
				b   []byte
				fi  os.FileInfo
				err error
			}
			snap := func() []state {
				var out []state
				for _, p := range files {
					b, err := os.ReadFile(p)
					fi, serr := os.Stat(p)
					out = append(out, state{b, fi, errors.Join(err, serr)})
				}
				return out
			}
			before := snap()
			for _, s := range before {
				if s.err != nil {
					t.Fatal(s.err)
				}
			}
			runVerify(t, dir, VerifyOptions{})
			runVerify(t, dir, VerifyOptions{Deep: true})
			for i, s := range snap() {
				if s.err != nil {
					t.Fatal(s.err)
				}
				if !bytes.Equal(s.b, before[i].b) {
					t.Errorf("%s changed", files[i])
				}
				if !s.fi.ModTime().Equal(before[i].fi.ModTime()) || s.fi.Size() != before[i].fi.Size() {
					t.Errorf("%s: mtime %v size %d, before %v size %d", files[i], s.fi.ModTime(), s.fi.Size(), before[i].fi.ModTime(), before[i].fi.Size())
				}
			}
		})
	}
}

// appendTornRecord appends the start of a record (a length past the end
// of the file) to path: what a crash in the middle of a segment write
// leaves.
func appendTornRecord(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{40, 0, 0, 0, 1, 2, 3}); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestVerifyTornSegmentWithoutIndex: with no usable index.db, a torn tail
// of a collection's newest data segment is something Open (the format-2
// upgrade, rebuildIndex) truncates by itself, so it is a notice, not a
// problem; damage elsewhere stays a problem.
func TestVerifyTornSegmentWithoutIndex(t *testing.T) {
	t.Run("missing index.db", func(t *testing.T) {
		dir := verifyCopy(t)
		segs := collSegments(t, dir)
		newest := segment.DataPath(filepath.Join(dir, vColl), segs[len(segs)-1])
		appendTornRecord(t, newest)
		mustRemove(t, filepath.Join(dir, vIndexFile))
		r := runVerify(t, dir, VerifyOptions{})
		wantProblem(t, r, vIndexFile, "--rebuild-index")
		rel, err := filepath.Rel(dir, newest)
		if err != nil {
			t.Fatal(err)
		}
		if findingAt(r.Problems, rel) {
			t.Errorf("the torn tail of %s is reported as a problem: %v", rel, r.Problems)
		}
		// Rebuilding truncates it, and the directory reads like the base.
		if r := runVerify(t, dir, VerifyOptions{RebuildIndex: true, Deep: true}); !r.OK {
			t.Errorf("verify after the rebuild: %v", r.Problems)
		}
		if got, want := liveDocs(t, dir), liveDocs(t, verifyCopy(t)); !reflect.DeepEqual(got, want) {
			t.Error("the rebuilt directory reads different data")
		}
	})
	t.Run("format-2 directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		if err := os.CopyFS(dir, os.DirFS(filepath.Join("testdata", "compat", "v0.0.6", "data"))); err != nil {
			t.Fatal(err)
		}
		newest := filepath.Join(dir, "kb", "notes", "seg-000002.data")
		appendTornRecord(t, newest)
		// The premise: Open upgrades the directory without complaint.
		cp := filepath.Join(t.TempDir(), "data")
		if err := os.CopyFS(cp, os.DirFS(dir)); err != nil {
			t.Fatal(err)
		}
		liveDocs(t, cp)
		r := runVerify(t, dir, VerifyOptions{})
		if !r.OK {
			t.Errorf("a format-2 directory with a torn segment tail fails verify: %v", r.Problems)
		}
	})
	t.Run("damage in a sealed segment stays a problem", func(t *testing.T) {
		dir := verifyCopy(t)
		p := segment.DataPath(filepath.Join(dir, vColl), collSegments(t, dir)[0])
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		flipFileByte(t, p, fi.Size()/2)
		mustRemove(t, filepath.Join(dir, vIndexFile))
		wantProblem(t, runVerify(t, dir, VerifyOptions{}), vColl+"/seg-000001.data")
	})
}

// TestVerifyCleanCloseAfterSegmentRoll: a flush that fills the active data
// segment seals it and creates the next one, header only and not yet
// registered in index.db (flush.go sealActive). That is the normal state of
// a cleanly closed directory, not "an interrupted flush", so a healthy
// directory in that state verifies with no finding at all: AC-14's
// one-line summary.
func TestVerifyCleanCloseAfterSegmentRoll(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.DataSegmentSize = 4096
	e := openEngine(t, opts)
	d, err := e.CreateDatabase(ctx, "app")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateCollection(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "c")
	for i := range 60 {
		mustInsert(t, c, fmt.Sprintf("id%03d", i), map[string]any{"pad": strings.Repeat("x", 80)})
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	segs, err := segment.ListSegments(filepath.Join(dir, "app", "c"))
	if err != nil || len(segs) != 2 {
		t.Fatalf("segments %v, %v; want the full one and the next, empty one", segs, err)
	}
	r := runVerify(t, dir, VerifyOptions{Deep: true})
	if !r.OK || len(r.Notices) != 0 {
		t.Errorf("a cleanly closed directory: OK %v, notices %v; want no finding", r.OK, r.Notices)
	}
}
