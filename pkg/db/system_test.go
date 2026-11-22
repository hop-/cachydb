package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// Tests of the reserved system database (SystemDatabaseName), which holds
// internal/auth's users and tokens: durable like any database, never
// reachable through a public name.

// TestSystemDatabaseHiddenAndRejected: SystemDatabase creates the database
// once and returns the same one after; HasSystemDatabase sees it without
// creating it; ListDatabases leaves it out; Database, CreateDatabase, and
// DropDatabase reject its name.
func TestSystemDatabaseHiddenAndRejected(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, testOptions(t.TempDir()))
	mustCreateDB(t, e, "app")

	if e.HasSystemDatabase() {
		t.Fatal("HasSystemDatabase = true before SystemDatabase")
	}
	if _, err := os.Stat(filepath.Join(e.root, SystemDatabaseName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("HasSystemDatabase created %s: stat err %v", SystemDatabaseName, err)
	}
	d, err := e.SystemDatabase(ctx)
	if err != nil {
		t.Fatalf("SystemDatabase: %v", err)
	}
	if d.Name() != SystemDatabaseName {
		t.Fatalf("Name = %q, want %q", d.Name(), SystemDatabaseName)
	}
	again, err := e.SystemDatabase(ctx)
	if err != nil || again != d {
		t.Fatalf("second SystemDatabase = %p, %v; want the same database %p", again, err, d)
	}
	if !e.HasSystemDatabase() {
		t.Fatal("HasSystemDatabase = false after SystemDatabase")
	}

	if got := e.ListDatabases(); !slices.Equal(got, []string{"app"}) {
		t.Fatalf("ListDatabases = %v, want [app]", got)
	}
	if _, err := e.Database(SystemDatabaseName); !errors.Is(err, ErrInvalidName) {
		t.Errorf("Database(%q) err = %v, want ErrInvalidName", SystemDatabaseName, err)
	}
	if _, err := e.CreateDatabase(ctx, SystemDatabaseName); !errors.Is(err, ErrInvalidName) {
		t.Errorf("CreateDatabase(%q) err = %v, want ErrInvalidName", SystemDatabaseName, err)
	}
	if err := e.DropDatabase(ctx, SystemDatabaseName); !errors.Is(err, ErrInvalidName) {
		t.Errorf("DropDatabase(%q) err = %v, want ErrInvalidName", SystemDatabaseName, err)
	}
	// Still there after the refused drop.
	if !e.HasSystemDatabase() {
		t.Fatal("system database gone after a refused DropDatabase")
	}
	// Other underscore names stay invalid: only the one reserved name is
	// accepted by the engine's own machinery.
	if validDatabaseDir("_other") || !validDatabaseDir(SystemDatabaseName) || !validDatabaseDir("app") {
		t.Error("validDatabaseDir accepts the wrong names")
	}
}

// seedSystem writes one user document into the system database.
func seedSystem(t *testing.T, e *Engine) *Collection {
	t.Helper()
	ctx := context.Background()
	d, err := e.SystemDatabase(ctx)
	if err != nil {
		t.Fatalf("SystemDatabase: %v", err)
	}
	if err := d.CreateCollection(ctx, "users", nil); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	c := mustColl(t, d, "users")
	mustInsert(t, c, "alice", map[string]any{"server_admin": true})
	return c
}

// checkSystem checks that the reopened engine e has the system database
// with the user document of seedSystem, and still hides it.
func checkSystem(t *testing.T, e *Engine) {
	t.Helper()
	if !e.HasSystemDatabase() {
		t.Fatal("system database missing after reopen")
	}
	d, err := e.SystemDatabase(context.Background())
	if err != nil {
		t.Fatalf("SystemDatabase after reopen: %v", err)
	}
	c := mustColl(t, d, "users")
	doc, err := c.Get(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Get(alice) after reopen: %v", err)
	}
	if doc.Data["server_admin"] != true {
		t.Fatalf("alice after reopen = %v", doc.Data)
	}
	if slices.Contains(e.ListDatabases(), SystemDatabaseName) {
		t.Fatalf("ListDatabases after reopen = %v, lists the system database", e.ListDatabases())
	}
}

// TestSystemDatabaseSurvivesReopen: a clean close and reopen keeps the
// system database and its documents.
func TestSystemDatabaseSurvivesReopen(t *testing.T) {
	e := openEngine(t, crashOpts(t.TempDir()))
	c := seedSystem(t, e)
	mustFlush(t, c)
	e = reopen(t, e)
	checkSystem(t, e)
}

// TestSystemDatabaseCrashReplay: a crash before any flush leaves the
// system database's documents only in the WAL; replay restores them.
func TestSystemDatabaseCrashReplay(t *testing.T) {
	opts := crashOpts(t.TempDir())
	e := openEngine(t, opts)
	seedSystem(t, e)
	e.abandon()

	e = openEngine(t, opts)
	checkSystem(t, e)
	if got := e.recov.replayApplied.Load(); got == 0 {
		t.Error("reopen applied no WAL records; the document did not come from replay")
	}
}

// TestSystemDatabaseCrashBeforeDirectory: a crash after the create record
// reached the WAL but before the directory was written replays the
// create record of the reserved name (replayCreateDB accepts it) and
// everything logged after it.
func TestSystemDatabaseCrashBeforeDirectory(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	crashed := opts
	crashAt(t, &crashed, hookMetaWrite)
	e := openEngine(t, crashed)
	if _, err := e.SystemDatabase(ctx); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("SystemDatabase err = %v, want the simulated crash", err)
	}
	e.abandon()
	if _, err := os.Stat(filepath.Join(opts.Dir, SystemDatabaseName, dbMetaFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("database metadata written before the crash: stat err %v", err)
	}

	e = openEngine(t, opts)
	if !e.HasSystemDatabase() {
		t.Fatal("WAL replay did not recreate the system database")
	}
	if got := e.ListDatabases(); len(got) != 0 {
		t.Fatalf("ListDatabases after replay = %v, want none", got)
	}
}

// TestSystemDatabaseVerify: the offline verifier checks the system
// database like any other (it counts it and its collection).
func TestSystemDatabaseVerify(t *testing.T) {
	ctx := context.Background()
	opts := crashOpts(t.TempDir())
	e := openEngine(t, opts)
	mustCreateDB(t, e, "app")
	c := seedSystem(t, e)
	mustFlush(t, c)
	if err := e.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rep, err := Verify(ctx, opts, VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !rep.OK {
		t.Fatalf("Verify not OK: %+v", rep.Problems)
	}
	if rep.Databases != 2 || rep.Collections != 1 || rep.Documents != 1 {
		t.Fatalf("Verify counted %d databases, %d collections, %d documents; want 2, 1, 1",
			rep.Databases, rep.Collections, rep.Documents)
	}
}

// TestSystemDatabaseNotMigrated: MigrateAll enumerates ListDatabases, so
// it never touches the system database (whose layout is internal/auth's).
func TestSystemDatabaseNotMigrated(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, testOptions(t.TempDir()))
	mustCreateDB(t, e, "app")
	seedSystem(t, e)
	if err := e.MigrateAll(ctx, CurrentSchemaVersion); err != nil {
		t.Fatalf("MigrateAll: %v", err)
	}
	checkSystem(t, e)
}
