package exportfmt

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

// TestExportSkipsSystemDatabase: the engine source lists databases
// through ListDatabases, so a whole-engine export leaves the reserved
// system database (users and token digests) out; naming it in a scope
// fails, and an import cannot write into it.
func TestExportSkipsSystemDatabase(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, t.TempDir())
	seedEngine(t, e)
	sys, err := e.SystemDatabase(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sys.CreateCollection(ctx, "tokens", nil); err != nil {
		t.Fatal(err)
	}
	tokens, err := sys.Collection("tokens")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tokens.Insert(ctx, &db.Document{ID: "secret-digest", Data: map[string]any{"user": "alice"}}); err != nil {
		t.Fatal(err)
	}

	dbs, err := Engine{E: e}.Databases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range dbs {
		if name == db.SystemDatabaseName {
			t.Fatalf("Databases = %v, lists the system database", dbs)
		}
	}
	all := export(t, e, Scope{})
	for _, leak := range []string{db.SystemDatabaseName, "secret-digest", `"tokens"`} {
		if strings.Contains(all, leak) {
			t.Errorf("export contains %q:\n%s", leak, all)
		}
	}
	if err := Export(ctx, &strings.Builder{}, Engine{E: e}, Scope{Database: db.SystemDatabaseName}, "v", time.Now()); !errors.Is(err, db.ErrInvalidName) {
		t.Errorf("export of the system database err = %v, want ErrInvalidName", err)
	}

	// An export whose database record names the system database is refused.
	data := strings.ReplaceAll(export(t, e, Scope{Database: "kb"}), `"kb"`, `"`+db.SystemDatabaseName+`"`)
	dst := openEngine(t, t.TempDir())
	if _, err := Import(ctx, strings.NewReader(data), Engine{E: dst}, Fail); err == nil {
		t.Fatal("import into the system database succeeded")
	}
	if dst.HasSystemDatabase() {
		t.Fatal("import created the system database")
	}
}
