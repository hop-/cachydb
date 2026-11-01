package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hop-/cachydb/pkg/db/internal/fsutil"
	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

func testOptions(dir string) Options {
	return Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// openEngine opens dir and closes the engine at test cleanup.
func openEngine(t *testing.T, opts Options) *Engine {
	t.Helper()
	logDirOnFailure(t, opts.Dir)
	e, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", opts.Dir, err)
	}
	t.Cleanup(func() {
		if err := e.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e
}

// dirLogged dedups logDirOnFailure registrations per (test, directory).
var dirLogged sync.Map

// logDirOnFailure lists every file under dir (path and size) at the end of
// a failed test, once per test and directory, so an intermittent failure
// leaves the on-disk state that led to it in the log.
func logDirOnFailure(t *testing.T, dir string) {
	t.Helper()
	type key struct {
		t   *testing.T
		dir string
	}
	k := key{t, dir}
	if _, loaded := dirLogged.LoadOrStore(k, true); loaded {
		return
	}
	t.Cleanup(func() {
		defer dirLogged.Delete(k)
		if !t.Failed() {
			return
		}
		var b strings.Builder
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(dir, path) // path is under dir
			fmt.Fprintf(&b, "\n  %s %d", rel, info.Size())
			return nil
		})
		t.Logf("data directory %s at the end of the failed test (walk error: %v):%s", dir, err, b.String())
	})
}

// reopen closes e and opens its directory again.
func reopen(t *testing.T, e *Engine) *Engine {
	t.Helper()
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return openEngine(t, e.opts)
}

func mustCreateDB(t *testing.T, e *Engine, name string) *Database {
	t.Helper()
	d, err := e.CreateDatabase(context.Background(), name)
	if err != nil {
		t.Fatalf("CreateDatabase(%q): %v", name, err)
	}
	return d
}

func mustDB(t *testing.T, e *Engine, name string) *Database {
	t.Helper()
	d, err := e.Database(name)
	if err != nil {
		t.Fatalf("Database(%q): %v", name, err)
	}
	return d
}

func mustColl(t *testing.T, d *Database, name string) *Collection {
	t.Helper()
	c, err := d.Collection(name)
	if err != nil {
		t.Fatalf("Collection(%q): %v", name, err)
	}
	return c
}

// walRecord returns the WAL record at off.
func walRecord(t *testing.T, e *Engine, off uint64) wal.Record {
	t.Helper()
	var got *wal.Record
	err := e.wal.ReadFrom(off, func(r wal.Record) error {
		if r.Offset == off {
			got = &r
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(%d): %v", off, err)
	}
	if got == nil {
		t.Fatalf("no WAL record at offset %d", off)
	}
	return *got
}

func readDBMetaFile(t *testing.T, path string) dbMeta {
	t.Helper()
	var m dbMeta
	if err := readMeta(path, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func readCollMetaFile(t *testing.T, path string) collMeta {
	t.Helper()
	var m collMeta
	if err := readMeta(path, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func deletedEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), deletedPrefix) {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestOpenLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("data-directory locking is unsupported on windows")
	}
	dir := t.TempDir()
	e, err := Open(context.Background(), testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), testOptions(dir)); !errors.Is(err, ErrDataDirInUse) {
		t.Fatalf("second Open: got %v, want ErrDataDirInUse", err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e2, err := Open(context.Background(), testOptions(dir))
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := e2.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestOpenErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, dir string)
		wantErr error
	}{
		{
			name: "old layout",
			setup: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, "wal.checkpoint"), []byte("1"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: ErrIncompatibleDataDir,
		},
		{
			name: "schema too new",
			setup: func(t *testing.T, dir string) {
				e := openEngine(t, testOptions(dir))
				mustCreateDB(t, e, "app")
				if err := e.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				m := dbMeta{Name: "app", SchemaVersion: CurrentSchemaVersion + 1, WALOffset: 1}
				if err := writeDBMeta(dbDir(dir, "app"), m); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: ErrSchemaTooNew,
		},
		{
			name: "corrupt meta",
			setup: func(t *testing.T, dir string) {
				e := openEngine(t, testOptions(dir))
				mustCreateDB(t, e, "app")
				if err := e.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "app", dbMetaFile), []byte("{"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.setup(t, dir)
			_, err := Open(context.Background(), testOptions(dir))
			if err == nil {
				t.Fatal("Open succeeded, want error")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("Open: got %v, want %v", err, tt.wantErr)
			}
			// A failed Open releases the lock.
			if runtime.GOOS != "windows" {
				lock, err := fsutil.AcquireLock(filepath.Join(dir, lockFileName))
				if err != nil {
					t.Fatalf("lock still held after failed Open: %v", err)
				}
				if err := lock.Release(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestOpenOptionsInvalid(t *testing.T) {
	if _, err := Open(context.Background(), Options{}); err == nil {
		t.Fatal("Open without Dir succeeded")
	}
}

func TestOpenRemovesDeletedLeftovers(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))
	mustCreateDB(t, e, "app")
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	leftovers := []string{
		filepath.Join(dir, ".deleted-7", "coll", "seg-000001.data"),
		filepath.Join(dir, "app", ".deleted-9", "seg-000001.data"),
	}
	for _, p := range leftovers {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	e = openEngine(t, testOptions(dir))
	for _, d := range []string{dir, filepath.Join(dir, "app")} {
		if got := deletedEntries(t, d); len(got) != 0 {
			t.Errorf("%s: leftovers %v not removed", d, got)
		}
	}
	if got := e.ListDatabases(); !slices.Equal(got, []string{"app"}) {
		t.Errorf("ListDatabases = %v, want [app]", got)
	}
}

func TestCloseIdempotent(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "users", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "users")
	if e.Closed() {
		t.Fatal("Closed() = true before Close")
	}
	for i := range 3 {
		if err := e.Close(ctx); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
	if !e.Closed() {
		t.Fatal("Closed() = false after Close")
	}

	checks := []struct {
		name string
		call func() error
	}{
		{"CreateDatabase", func() error { _, err := e.CreateDatabase(ctx, "other"); return err }},
		{"Database", func() error { _, err := e.Database("app"); return err }},
		{"DropDatabase", func() error { return e.DropDatabase(ctx, "app") }},
		{"CreateCollection", func() error { return d.CreateCollection(ctx, "c2", nil) }},
		{"Collection", func() error { _, err := d.Collection("users"); return err }},
		{"DropCollection", func() error { return d.DropCollection(ctx, "users") }},
		{"CreateIndex", func() error { return c.CreateIndex(ctx, "by_age", "age") }},
		{"DropIndex", func() error { return c.DropIndex(ctx, "by_age") }},
		{"MigrateDatabase", func() error { return e.MigrateDatabase(ctx, "app", 1) }},
		{"MigrateAll", func() error { return e.MigrateAll(ctx, 1) }},
	}
	for _, tt := range checks {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, ErrClosed) {
				t.Fatalf("got %v, want ErrClosed", err)
			}
		})
	}
}

func TestCreateDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))

	before := e.wal.NextOffset()
	d := mustCreateDB(t, e, "app")
	if got := e.wal.NextOffset(); got != before+1 {
		t.Fatalf("NextOffset = %d, want %d", got, before+1)
	}
	rec := walRecord(t, e, before)
	if rec.Op != wal.OpCreateDB || rec.DB != "app" {
		t.Fatalf("WAL record = %v %q, want create_db app", rec.Op, rec.DB)
	}
	m := readDBMetaFile(t, filepath.Join(dir, "app", dbMetaFile))
	if want := (dbMeta{Name: "app", SchemaVersion: CurrentSchemaVersion, WALOffset: before}); m != want {
		t.Fatalf("db.meta = %+v, want %+v", m, want)
	}
	if d.Name() != "app" || d.SchemaVersion() != CurrentSchemaVersion {
		t.Fatalf("database = %q v%d", d.Name(), d.SchemaVersion())
	}

	// Existing name: same database, no WAL record.
	d2, err := e.CreateDatabase(ctx, "app")
	if err != nil {
		t.Fatal(err)
	}
	if d2 != d {
		t.Error("CreateDatabase of an existing name returned a different *Database")
	}
	if got := e.wal.NextOffset(); got != before+1 {
		t.Errorf("CreateDatabase of an existing name wrote a WAL record (NextOffset %d)", got)
	}

	mustCreateDB(t, e, "Beta-2")
	e = reopen(t, e)
	if got := e.ListDatabases(); !slices.Equal(got, []string{"Beta-2", "app"}) {
		t.Fatalf("ListDatabases after reopen = %v", got)
	}
	if v := mustDB(t, e, "app").SchemaVersion(); v != CurrentSchemaVersion {
		t.Errorf("SchemaVersion after reopen = %d", v)
	}
	// Existing name after reopen: still no WAL record.
	before = e.wal.NextOffset()
	mustCreateDB(t, e, "app")
	if got := e.wal.NextOffset(); got != before {
		t.Errorf("CreateDatabase of a reloaded name wrote a WAL record")
	}
	if _, err := e.Database("missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Database(missing): got %v, want ErrNotFound", err)
	}
}

func TestDropDatabase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "users", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "users")
	mustCreateDB(t, e, "keep")

	off := e.wal.NextOffset()
	if err := e.DropDatabase(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	if rec := walRecord(t, e, off); rec.Op != wal.OpDropDB || rec.DB != "app" {
		t.Fatalf("WAL record = %v %q, want drop_db app", rec.Op, rec.DB)
	}
	if _, err := os.Stat(filepath.Join(dir, "app")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("database directory still present: %v", err)
	}
	if _, err := e.Database("app"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Database after drop: got %v, want ErrNotFound", err)
	}
	if err := e.DropDatabase(ctx, "app"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second DropDatabase: got %v, want ErrNotFound", err)
	}
	// Stale handles report the drop.
	if err := d.CreateCollection(ctx, "x", nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("CreateCollection on dropped database: got %v, want ErrNotFound", err)
	}
	if err := c.CreateIndex(ctx, "by_age", "age"); !errors.Is(err, ErrNotFound) {
		t.Errorf("CreateIndex on collection of dropped database: got %v, want ErrNotFound", err)
	}
	if d.ListCollections() != nil {
		t.Errorf("ListCollections on dropped database = %v", d.ListCollections())
	}

	e = reopen(t, e) // joins the background removal
	if got := deletedEntries(t, dir); len(got) != 0 {
		t.Errorf("renamed directories not removed: %v", got)
	}
	if got := e.ListDatabases(); !slices.Equal(got, []string{"keep"}) {
		t.Fatalf("ListDatabases after reopen = %v", got)
	}

	// The name can be reused.
	d = mustCreateDB(t, e, "app")
	if got := d.ListCollections(); len(got) != 0 {
		t.Errorf("recreated database has collections %v", got)
	}
}

func TestCollections(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))
	d := mustCreateDB(t, e, "app")

	schema := &Schema{Fields: map[string]Field{
		"name": {Type: TypeString, Required: true},
		"age":  {Type: TypeNumber},
	}}
	tests := []struct {
		name   string
		schema *Schema
	}{
		{"users", schema},
		{"logs", nil},
	}
	for _, tt := range tests {
		off := e.wal.NextOffset()
		if err := d.CreateCollection(ctx, tt.name, tt.schema); err != nil {
			t.Fatalf("CreateCollection(%q): %v", tt.name, err)
		}
		rec := walRecord(t, e, off)
		if rec.Op != wal.OpCreateColl || rec.DB != "app" || rec.Coll != tt.name {
			t.Fatalf("WAL record = %v %q/%q", rec.Op, rec.DB, rec.Coll)
		}
		var p createCollPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			t.Fatalf("payload %s: %v", rec.Payload, err)
		}
		if (p.Schema == nil) != (tt.schema == nil) {
			t.Fatalf("payload schema = %s", rec.Payload)
		}
		m := readCollMetaFile(t, filepath.Join(dir, "app", tt.name, collMetaFile))
		if m.Name != tt.name || m.WALOffset != off || m.Indexes == nil || len(m.Indexes) != 0 {
			t.Fatalf("coll.meta = %+v", m)
		}
	}
	// The stored schema does not alias the caller's.
	schema.Fields["extra"] = Field{Type: TypeBoolean}

	if err := d.CreateCollection(ctx, "users", nil); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate CreateCollection: got %v, want ErrExists", err)
	}
	bad := &Schema{Fields: map[string]Field{"x": {Type: "weird"}}}
	if err := d.CreateCollection(ctx, "bad", bad); !errors.Is(err, ErrSchemaValidation) {
		t.Fatalf("CreateCollection with invalid schema: got %v, want ErrSchemaValidation", err)
	}

	e = reopen(t, e)
	d = mustDB(t, e, "app")
	if got := d.ListCollections(); !slices.Equal(got, []string{"logs", "users"}) {
		t.Fatalf("ListCollections after reopen = %v", got)
	}
	users := mustColl(t, d, "users")
	if users.Name() != "users" || users.schema == nil || len(users.schema.Fields) != 2 ||
		users.schema.Fields["name"] != (Field{Type: TypeString, Required: true}) {
		t.Fatalf("users schema after reopen = %+v", users.schema)
	}
	if mustColl(t, d, "logs").schema != nil {
		t.Fatal("schemaless collection has a schema after reopen")
	}

	off := e.wal.NextOffset()
	if err := d.DropCollection(ctx, "logs"); err != nil {
		t.Fatal(err)
	}
	if rec := walRecord(t, e, off); rec.Op != wal.OpDropColl || rec.Coll != "logs" {
		t.Fatalf("WAL record = %v %q", rec.Op, rec.Coll)
	}
	if _, err := os.Stat(filepath.Join(dir, "app", "logs")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("collection directory still present: %v", err)
	}
	if _, err := d.Collection("logs"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Collection after drop: got %v, want ErrNotFound", err)
	}
	if err := d.DropCollection(ctx, "logs"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second DropCollection: got %v, want ErrNotFound", err)
	}

	e = reopen(t, e)
	if got := deletedEntries(t, filepath.Join(dir, "app")); len(got) != 0 {
		t.Errorf("renamed directories not removed: %v", got)
	}
	if got := mustDB(t, e, "app").ListCollections(); !slices.Equal(got, []string{"users"}) {
		t.Fatalf("ListCollections after reopen = %v", got)
	}
}

func TestIndexDefinitions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "users", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "users")
	metaPath := filepath.Join(dir, "app", "users", collMetaFile)

	for _, def := range []indexDef{{"by_name", "name"}, {"by_age", "age"}} {
		off := e.wal.NextOffset()
		if err := c.CreateIndex(ctx, def.Name, def.Field); err != nil {
			t.Fatalf("CreateIndex(%q): %v", def.Name, err)
		}
		rec := walRecord(t, e, off)
		var p indexDef
		if err := json.Unmarshal(rec.Payload, &p); err != nil || rec.Op != wal.OpCreateIndex || p != def {
			t.Fatalf("WAL record = %v %s (%v)", rec.Op, rec.Payload, err)
		}
		if m := readCollMetaFile(t, metaPath); m.WALOffset != off {
			t.Fatalf("coll.meta wal_offset = %d, want %d", m.WALOffset, off)
		}
	}
	if err := c.CreateIndex(ctx, "by_age", "other"); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate CreateIndex: got %v, want ErrExists", err)
	}
	if err := c.CreateIndex(ctx, "no_field", ""); err == nil {
		t.Fatal("CreateIndex with empty field succeeded")
	}

	e = reopen(t, e)
	c = mustColl(t, mustDB(t, e, "app"), "users")
	want := []indexDef{{"by_age", "age"}, {"by_name", "name"}}
	if got := readCollMetaFile(t, metaPath).Indexes; !slices.Equal(got, want) {
		t.Fatalf("coll.meta indexes = %v, want %v", got, want)
	}
	if got := sortedIndexDefs(c.indexDefs()); !slices.Equal(got, want) {
		t.Fatalf("indexes after reopen = %v, want %v", got, want)
	}

	off := e.wal.NextOffset()
	if err := c.DropIndex(ctx, "by_name"); err != nil {
		t.Fatal(err)
	}
	if rec := walRecord(t, e, off); rec.Op != wal.OpDropIndex || string(rec.Payload) != `{"name":"by_name"}` {
		t.Fatalf("WAL record = %v %s", rec.Op, rec.Payload)
	}
	if err := c.DropIndex(ctx, "by_name"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second DropIndex: got %v, want ErrNotFound", err)
	}

	e = reopen(t, e)
	c = mustColl(t, mustDB(t, e, "app"), "users")
	if got := sortedIndexDefs(c.indexDefs()); !slices.Equal(got, []indexDef{{"by_age", "age"}}) {
		t.Fatalf("indexes after drop and reopen = %v", got)
	}
	if m := readCollMetaFile(t, metaPath); m.WALOffset != off {
		t.Fatalf("coll.meta wal_offset = %d, want %d", m.WALOffset, off)
	}
}

func TestMetadataNamesValidated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))
	d := mustCreateDB(t, e, "app")
	if err := d.CreateCollection(ctx, "users", nil); err != nil {
		t.Fatal(err)
	}
	c := mustColl(t, d, "users")
	before := e.wal.NextOffset()

	for _, name := range []string{"../x", "a/b", ".x", "LOCK.x", "", "_x", strings.Repeat("a", 65)} {
		ops := []struct {
			op   string
			call func() error
		}{
			{"CreateDatabase", func() error { _, err := e.CreateDatabase(ctx, name); return err }},
			{"Database", func() error { _, err := e.Database(name); return err }},
			{"DropDatabase", func() error { return e.DropDatabase(ctx, name) }},
			{"CreateCollection", func() error { return d.CreateCollection(ctx, name, nil) }},
			{"Collection", func() error { _, err := d.Collection(name); return err }},
			{"DropCollection", func() error { return d.DropCollection(ctx, name) }},
			{"CreateIndex", func() error { return c.CreateIndex(ctx, name, "f") }},
			{"DropIndex", func() error { return c.DropIndex(ctx, name) }},
		}
		for _, tt := range ops {
			t.Run(tt.op+"/"+name, func(t *testing.T) {
				if err := tt.call(); !errors.Is(err, ErrInvalidName) {
					t.Fatalf("got %v, want ErrInvalidName", err)
				}
			})
		}
	}
	if got := e.wal.NextOffset(); got != before {
		t.Errorf("invalid names wrote %d WAL records", got-before)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "x")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("path traversal created %s", filepath.Join(filepath.Dir(dir), "x"))
	}
}

// TestHookMetaWrite checks that every metadata operation calls the
// during-meta-write hook after its WAL record is durable and before the
// meta file change, and that a simulated crash there leaves memory and
// files untouched.
func TestHookMetaWrite(t *testing.T) {
	ctx := context.Background()
	type state struct {
		next  uint64
		files map[string]bool // path -> exists
	}

	tests := []struct {
		name string
		// setup prepares the engine; run performs the operation.
		setup func(t *testing.T, e *Engine)
		run   func(e *Engine) error
		// path is the file or directory the operation changes after the hook.
		path func(dir string) string
		// visible reports whether the operation's effect is visible in memory.
		visible func(e *Engine) bool
	}{
		{
			name:    "create database",
			setup:   func(*testing.T, *Engine) {},
			run:     func(e *Engine) error { _, err := e.CreateDatabase(ctx, "app"); return err },
			path:    func(dir string) string { return filepath.Join(dir, "app", dbMetaFile) },
			visible: func(e *Engine) bool { _, err := e.Database("app"); return err == nil },
		},
		{
			name:    "drop database",
			setup:   func(t *testing.T, e *Engine) { mustCreateDB(t, e, "app") },
			run:     func(e *Engine) error { return e.DropDatabase(ctx, "app") },
			path:    func(dir string) string { return filepath.Join(dir, "app") },
			visible: func(e *Engine) bool { _, err := e.Database("app"); return err != nil },
		},
		{
			name:  "create collection",
			setup: func(t *testing.T, e *Engine) { mustCreateDB(t, e, "app") },
			run: func(e *Engine) error {
				return e.databases["app"].CreateCollection(ctx, "users", nil)
			},
			path: func(dir string) string { return filepath.Join(dir, "app", "users", collMetaFile) },
			visible: func(e *Engine) bool {
				_, err := e.databases["app"].Collection("users")
				return err == nil
			},
		},
		{
			name: "drop collection",
			setup: func(t *testing.T, e *Engine) {
				if err := mustCreateDB(t, e, "app").CreateCollection(ctx, "users", nil); err != nil {
					t.Fatal(err)
				}
			},
			run:  func(e *Engine) error { return e.databases["app"].DropCollection(ctx, "users") },
			path: func(dir string) string { return filepath.Join(dir, "app", "users") },
			visible: func(e *Engine) bool {
				_, err := e.databases["app"].Collection("users")
				return err != nil
			},
		},
		{
			name: "create index",
			setup: func(t *testing.T, e *Engine) {
				if err := mustCreateDB(t, e, "app").CreateCollection(ctx, "users", nil); err != nil {
					t.Fatal(err)
				}
			},
			run: func(e *Engine) error {
				return e.databases["app"].collections["users"].CreateIndex(ctx, "by_age", "age")
			},
			path: func(dir string) string { return filepath.Join(dir, "app", "users", collMetaFile) },
			visible: func(e *Engine) bool {
				_, ok := e.databases["app"].collections["users"].indexes["by_age"]
				return ok
			},
		},
		{
			name: "drop index",
			setup: func(t *testing.T, e *Engine) {
				if err := mustCreateDB(t, e, "app").CreateCollection(ctx, "users", nil); err != nil {
					t.Fatal(err)
				}
				if err := e.databases["app"].collections["users"].CreateIndex(ctx, "by_age", "age"); err != nil {
					t.Fatal(err)
				}
			},
			run: func(e *Engine) error {
				return e.databases["app"].collections["users"].DropIndex(ctx, "by_age")
			},
			path: func(dir string) string { return filepath.Join(dir, "app", "users", collMetaFile) },
			visible: func(e *Engine) bool {
				_, ok := e.databases["app"].collections["users"].indexes["by_age"]
				return !ok
			},
		},
	}

	for _, tt := range tests {
		for _, crash := range []bool{false, true} {
			name := tt.name
			if crash {
				name += "/crash"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				opts := testOptions(dir)
				var (
					armed   bool
					e       *Engine
					called  int
					atHook  state
					preNext uint64
					preData []byte
				)
				snapshot := func() state {
					p := tt.path(dir)
					_, err := os.Stat(p)
					return state{next: e.wal.NextOffset(), files: map[string]bool{p: err == nil}}
				}
				opts.testHook = func(p hookPoint) error {
					if p != hookMetaWrite || !armed { // p first: the flusher calls the hook from another goroutine
						return nil
					}
					called++
					atHook = snapshot()
					if data, _ := os.ReadFile(tt.path(dir)); !bytes.Equal(data, preData) {
						t.Errorf("%s changed before the hook", tt.path(dir))
					}
					if crash {
						return errSimulatedCrash
					}
					return nil
				}
				e = openEngine(t, opts)
				tt.setup(t, e)
				pre := snapshot()
				preNext = pre.next
				preData, _ = os.ReadFile(tt.path(dir)) // missing => nil

				armed = true
				err := tt.run(e)
				armed = false

				if called != 1 {
					t.Fatalf("hook called %d times, want 1", called)
				}
				if atHook.next != preNext+1 {
					t.Errorf("at hook NextOffset = %d, want %d (WAL record durable before the hook)", atHook.next, preNext+1)
				}
				for p, existed := range pre.files {
					if atHook.files[p] != existed {
						t.Errorf("at hook %s existence changed (%v -> %v)", p, existed, atHook.files[p])
					}
				}
				if crash {
					if !errors.Is(err, errSimulatedCrash) {
						t.Fatalf("got %v, want errSimulatedCrash", err)
					}
					if tt.visible(e) {
						t.Error("operation visible in memory after a crash at the meta write")
					}
					after := snapshot()
					for p, existed := range pre.files {
						if after.files[p] != existed {
							t.Errorf("%s existence changed after crash", p)
						}
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !tt.visible(e) {
					t.Error("operation not visible after success")
				}
			})
		}
	}
}

func TestHookNilAndWrapping(t *testing.T) {
	e := &Engine{}
	if err := e.hook(hookMetaWrite); err != nil {
		t.Fatalf("hook without testHook = %v", err)
	}
	e.opts.testHook = func(hookPoint) error { return errors.New("boom") }
	if err := e.hook(hookMetaWrite); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("hook = %v, want errSimulatedCrash", err)
	}
}

func TestMigrations(t *testing.T) {
	ctx := context.Background()
	var calls []string
	RegisterMigration(1, func(ctx context.Context, e *Engine, dbName string) error {
		calls = append(calls, dbName)
		d, err := e.Database(dbName)
		if err != nil {
			return err
		}
		return d.CreateCollection(ctx, "migrated", nil)
	})
	t.Cleanup(func() {
		registry.mu.Lock()
		delete(registry.migrations, 1)
		registry.mu.Unlock()
	})
	if got := ListMigrations(); !slices.Equal(got, []int{1}) {
		t.Fatalf("ListMigrations = %v", got)
	}

	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))
	mustCreateDB(t, e, "app")
	mustCreateDB(t, e, "other")

	if err := e.MigrateDatabase(ctx, "app", 3); err == nil {
		t.Fatal("migration to 3 without a 2->3 migration succeeded")
	}
	// The 1->2 step of the failed run is persisted.
	if v := mustDB(t, e, "app").SchemaVersion(); v != 2 {
		t.Fatalf("SchemaVersion after partial migration = %d, want 2", v)
	}
	if err := e.MigrateDatabase(ctx, "app", 2); err != nil {
		t.Fatalf("MigrateDatabase to the current version: %v", err)
	}
	if err := e.MigrateDatabase(ctx, "app", 1); err == nil {
		t.Fatal("downgrade succeeded")
	}
	if err := e.MigrateDatabase(ctx, "missing", 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MigrateDatabase(missing): got %v, want ErrNotFound", err)
	}
	if err := e.MigrateAll(ctx, 2); err != nil {
		t.Fatalf("MigrateAll: %v", err)
	}
	if !slices.Equal(calls, []string{"app", "other"}) {
		t.Fatalf("migration calls = %v", calls)
	}
	for _, name := range []string{"app", "other"} {
		d := mustDB(t, e, name)
		if d.SchemaVersion() != 2 {
			t.Errorf("%s SchemaVersion = %d, want 2", name, d.SchemaVersion())
		}
		if _, err := d.Collection("migrated"); err != nil {
			t.Errorf("%s: migration's collection missing: %v", name, err)
		}
		m := readDBMetaFile(t, filepath.Join(dir, name, dbMetaFile))
		if m.SchemaVersion != 2 {
			t.Errorf("%s db.meta schema_version = %d, want 2", name, m.SchemaVersion)
		}
	}

	// Version 2 is newer than this build supports.
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, testOptions(dir)); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("reopen after migrating past CurrentSchemaVersion: got %v, want ErrSchemaTooNew", err)
	}
}

func TestConcurrentMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	e := openEngine(t, testOptions(dir))
	before := e.wal.NextOffset()

	const n = 16
	var wg sync.WaitGroup
	dbs := make([]*Database, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { dbs[i], errs[i] = e.CreateDatabase(ctx, "app") })
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if dbs[i] != dbs[0] {
			t.Fatal("concurrent CreateDatabase returned different databases")
		}
	}
	if got := e.wal.NextOffset(); got != before+1 {
		t.Fatalf("concurrent CreateDatabase wrote %d records, want 1", got-before)
	}

	d := dbs[0]
	names := []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"}
	for _, name := range names {
		wg.Go(func() {
			if err := d.CreateCollection(ctx, name, nil); err != nil {
				t.Error(err)
				return
			}
			c, err := d.Collection(name)
			if err != nil {
				t.Error(err)
				return
			}
			if err := c.CreateIndex(ctx, "ix", "f"); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() { _ = e.ListDatabases(); _ = d.ListCollections() })
	}
	wg.Wait()

	e = reopen(t, e)
	d = mustDB(t, e, "app")
	if got := d.ListCollections(); !slices.Equal(got, names) {
		t.Fatalf("ListCollections after reopen = %v", got)
	}
	for _, name := range names {
		if _, ok := mustColl(t, d, name).indexes["ix"]; !ok {
			t.Errorf("%s: index missing after reopen", name)
		}
	}
}
