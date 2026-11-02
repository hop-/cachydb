// Package exportfmt implements the cachydb export format (plan 004 §4.1):
// JSON Lines, UTF-8. A header line comes first, then for each database
// (sorted by name) its line, then for each of its collections (sorted) a
// collection line followed by its documents sorted by _id:
//
//	{"type":"cachydb-export","version":1,"cachydb":"<version>","created":"<RFC3339>"}
//	{"type":"database","name":"kb","schema_version":1}
//	{"type":"collection","database":"kb","name":"notes","schema":{...}|null,"indexes":[{"name":"by_type","field":"type"}]}
//	{"type":"document","database":"kb","collection":"notes","doc":{"_id":"...",...}}
//
// The same format is written and read offline (cachydb utils
// export/import, against the engine) and through a running server
// (cachydb client export/import, through the MCP tools): Export and Import
// work on a Source and a Target that each side implements.
package exportfmt

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

// Version is the export format version this package writes and reads.
const Version = 1

// Line types.
const (
	TypeHeader     = "cachydb-export"
	TypeDatabase   = "database"
	TypeCollection = "collection"
	TypeDocument   = "document"
)

// Header is the first line of an export.
type Header struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	Cachydb string `json:"cachydb"`
	Created string `json:"created"`
}

// DatabaseLine starts a database.
type DatabaseLine struct {
	Type          string `json:"type"`
	Name          string `json:"name"`
	SchemaVersion int    `json:"schema_version"`
}

// CollectionLine starts a collection of the current database.
type CollectionLine struct {
	Type     string         `json:"type"`
	Database string         `json:"database"`
	Name     string         `json:"name"`
	Schema   *db.Schema     `json:"schema"`
	Indexes  []db.IndexInfo `json:"indexes"`
}

// DocumentLine is one document of the current collection.
type DocumentLine struct {
	Type       string         `json:"type"`
	Database   string         `json:"database"`
	Collection string         `json:"collection"`
	Doc        map[string]any `json:"doc"`
}

// Scope limits an export to one database (Database) or one collection
// (Database and Collection). The zero Scope exports everything.
type Scope struct {
	Database, Collection string
}

// Validate checks that a collection scope names its database.
func (s Scope) Validate() error {
	if s.Collection != "" && s.Database == "" {
		return errors.New("a collection needs its database (-c requires -d)")
	}
	return nil
}

// Source is what Export reads.
type Source interface {
	// Databases returns every database name.
	Databases(ctx context.Context) ([]string, error)
	// SchemaVersion returns the schema version of a database.
	SchemaVersion(ctx context.Context, database string) (int, error)
	// Collections returns the collection names of a database.
	Collections(ctx context.Context, database string) ([]string, error)
	// Describe returns a collection's schema (nil if none) and indexes.
	Describe(ctx context.Context, database, collection string) (*db.Schema, []db.IndexInfo, error)
	// Documents calls fn for every document of a collection in ascending
	// _id order; each document is a flat object with its _id.
	Documents(ctx context.Context, database, collection string, fn func(doc map[string]any) error) error
}

// Export writes the data of src selected by scope to w. version is the
// cachydb version for the header; created is its creation time.
func Export(ctx context.Context, w io.Writer, src Source, scope Scope, version string, created time.Time) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	line := func(v any) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		data = append(data, '\n')
		_, err = bw.Write(data)
		return err
	}
	if err := line(Header{Type: TypeHeader, Version: Version, Cachydb: version, Created: created.UTC().Format(time.RFC3339)}); err != nil {
		return err
	}

	dbs := []string{scope.Database}
	if scope.Database == "" {
		all, err := src.Databases(ctx)
		if err != nil {
			return err
		}
		dbs = slices.Sorted(slices.Values(all))
	}
	for _, dbName := range dbs {
		sv, err := src.SchemaVersion(ctx, dbName)
		if err != nil {
			return err
		}
		if err := line(DatabaseLine{Type: TypeDatabase, Name: dbName, SchemaVersion: sv}); err != nil {
			return err
		}
		colls := []string{scope.Collection}
		if scope.Collection == "" {
			all, err := src.Collections(ctx, dbName)
			if err != nil {
				return err
			}
			colls = slices.Sorted(slices.Values(all))
		}
		for _, collName := range colls {
			schema, indexes, err := src.Describe(ctx, dbName, collName)
			if err != nil {
				return err
			}
			if indexes == nil {
				indexes = []db.IndexInfo{}
			}
			slices.SortFunc(indexes, func(a, b db.IndexInfo) int { return cmp.Compare(a.Name, b.Name) })
			if err := line(CollectionLine{Type: TypeCollection, Database: dbName, Name: collName, Schema: schema, Indexes: indexes}); err != nil {
				return err
			}
			err = src.Documents(ctx, dbName, collName, func(doc map[string]any) error {
				return line(DocumentLine{Type: TypeDocument, Database: dbName, Collection: collName, Doc: doc})
			})
			if err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}

// OnConflict says what Import does with an existing document or a
// collection whose schema or indexes differ.
type OnConflict string

// Conflict modes.
const (
	// Fail stops at the first existing _id (and at a collection whose
	// schema or index set differs).
	Fail OnConflict = "fail"
	// Skip keeps existing documents (a differing collection still fails).
	Skip OnConflict = "skip"
	// Replace replaces existing documents, updates a differing schema
	// (validated against the existing documents), and adds missing
	// indexes.
	Replace OnConflict = "replace"
)

// ParseOnConflict parses a conflict mode name.
func ParseOnConflict(s string) (OnConflict, error) {
	switch m := OnConflict(s); m {
	case Fail, Skip, Replace:
		return m, nil
	}
	return "", fmt.Errorf("invalid --on-conflict %q: use fail, skip, or replace", s)
}

// ErrDocumentExists is returned by Target.InsertDocument for an existing
// _id.
var ErrDocumentExists = errors.New("document already exists")

// Target is what Import writes to.
type Target interface {
	// EnsureDatabase creates the database if it does not exist.
	EnsureDatabase(ctx context.Context, name string) error
	// Lookup returns a collection's schema and indexes, and false if the
	// collection does not exist.
	Lookup(ctx context.Context, database, collection string) (*db.Schema, []db.IndexInfo, bool, error)
	// CreateCollection creates a collection with a schema (nil = none).
	CreateCollection(ctx context.Context, database, collection string, schema *db.Schema) error
	// UpdateSchema replaces a collection's schema.
	UpdateSchema(ctx context.Context, database, collection string, schema *db.Schema) error
	// CreateIndex and DropIndex manage a secondary index.
	CreateIndex(ctx context.Context, database, collection, name, field string) error
	DropIndex(ctx context.Context, database, collection, name string) error
	// InsertDocument inserts a document (a flat object with its _id); an
	// existing _id returns an error wrapping ErrDocumentExists.
	InsertDocument(ctx context.Context, database, collection string, doc map[string]any) error
	// ReplaceDocument replaces the existing document with doc's _id.
	ReplaceDocument(ctx context.Context, database, collection string, doc map[string]any) error
}

// Stats counts what Import did.
type Stats struct {
	Lines       int // lines read and applied (header included)
	Databases   int // database lines
	Collections int // collection lines
	Inserted    int // documents inserted
	Replaced    int // documents replaced (Replace mode)
	Skipped     int // existing documents kept (Skip mode)
}

// Import reads an export from r and applies it to t: it creates missing
// databases and collections (with schema and indexes) and inserts the
// documents, handling conflicts per mode. On error, the returned Stats
// tell how many lines were applied before it; the error names the line.
func Import(ctx context.Context, r io.Reader, t Target, mode OnConflict) (Stats, error) {
	var st Stats
	if _, err := ParseOnConflict(string(mode)); err != nil {
		return st, err
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<30)
	lineNo := 0
	fail := func(err error) (Stats, error) {
		return st, fmt.Errorf("import line %d (%d lines applied): %w", lineNo, st.Lines, err)
	}
	for sc.Scan() {
		lineNo++
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var kind struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &kind); err != nil {
			return fail(fmt.Errorf("invalid JSON: %w", err))
		}
		if st.Lines == 0 && kind.Type != TypeHeader {
			return fail(errors.New("not a cachydb export: the first line must be the cachydb-export header"))
		}
		var err error
		switch kind.Type {
		case TypeHeader:
			err = checkHeader(raw, st.Lines)
		case TypeDatabase:
			err = importDatabase(ctx, raw, t, &st)
		case TypeCollection:
			err = importCollection(ctx, raw, t, mode, &st)
		case TypeDocument:
			err = importDocument(ctx, raw, t, mode, &st)
		default:
			err = fmt.Errorf("unknown line type %q", kind.Type)
		}
		if err != nil {
			return fail(err)
		}
		st.Lines++
	}
	if err := sc.Err(); err != nil {
		return fail(err)
	}
	if st.Lines == 0 {
		return st, errors.New("import: empty input; not a cachydb export")
	}
	return st, nil
}

func checkHeader(raw []byte, linesBefore int) error {
	if linesBefore != 0 {
		return errors.New("a second header")
	}
	var h Header
	if err := json.Unmarshal(raw, &h); err != nil {
		return fmt.Errorf("invalid header: %w", err)
	}
	if h.Version != Version {
		return fmt.Errorf("unsupported export format version %d (this cachydb reads version %d)", h.Version, Version)
	}
	return nil
}

func importDatabase(ctx context.Context, raw []byte, t Target, st *Stats) error {
	var l DatabaseLine
	if err := json.Unmarshal(raw, &l); err != nil {
		return fmt.Errorf("invalid database line: %w", err)
	}
	if err := t.EnsureDatabase(ctx, l.Name); err != nil {
		return fmt.Errorf("database %q: %w", l.Name, err)
	}
	st.Databases++
	return nil
}

func importCollection(ctx context.Context, raw []byte, t Target, mode OnConflict, st *Stats) error {
	var l CollectionLine
	if err := json.Unmarshal(raw, &l); err != nil {
		return fmt.Errorf("invalid collection line: %w", err)
	}
	where := fmt.Sprintf("collection %s/%s", l.Database, l.Name)
	schema, indexes, exists, err := t.Lookup(ctx, l.Database, l.Name)
	if err != nil {
		return fmt.Errorf("%s: %w", where, err)
	}
	if !exists {
		if err := t.CreateCollection(ctx, l.Database, l.Name, l.Schema); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
		indexes = nil
	} else if !sameSchema(schema, l.Schema) {
		if mode != Replace {
			return fmt.Errorf("%s exists with a different schema (use --on-conflict replace to update it)", where)
		}
		if err := t.UpdateSchema(ctx, l.Database, l.Name, l.Schema); err != nil {
			return fmt.Errorf("%s: updating the schema: %w", where, err)
		}
	}
	have := make(map[string]string, len(indexes))
	for _, ix := range indexes {
		have[ix.Name] = ix.Field
	}
	want := make(map[string]string, len(l.Indexes))
	for _, ix := range l.Indexes {
		want[ix.Name] = ix.Field
	}
	if exists && mode != Replace && !maps.Equal(have, want) {
		return fmt.Errorf("%s exists with a different index set (use --on-conflict replace to add the missing indexes)", where)
	}
	for _, ix := range l.Indexes {
		field, ok := have[ix.Name]
		if ok && field == ix.Field {
			continue
		}
		if ok {
			if err := t.DropIndex(ctx, l.Database, l.Name, ix.Name); err != nil {
				return fmt.Errorf("%s: replacing index %q: %w", where, ix.Name, err)
			}
		}
		if err := t.CreateIndex(ctx, l.Database, l.Name, ix.Name, ix.Field); err != nil {
			return fmt.Errorf("%s: index %q: %w", where, ix.Name, err)
		}
	}
	st.Collections++
	return nil
}

// sameSchema reports whether two schemas are equal (nil = none).
func sameSchema(a, b *db.Schema) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return maps.Equal(a.Fields, b.Fields)
}

func importDocument(ctx context.Context, raw []byte, t Target, mode OnConflict, st *Stats) error {
	var l DocumentLine
	if err := json.Unmarshal(raw, &l); err != nil {
		return fmt.Errorf("invalid document line: %w", err)
	}
	id, _ := l.Doc["_id"].(string)
	if id == "" {
		return errors.New("document without a string _id")
	}
	where := fmt.Sprintf("document %q in %s/%s", id, l.Database, l.Collection)
	err := t.InsertDocument(ctx, l.Database, l.Collection, l.Doc)
	switch {
	case err == nil:
		st.Inserted++
		return nil
	case !errors.Is(err, ErrDocumentExists):
		return fmt.Errorf("%s: %w", where, err)
	}
	switch mode {
	case Skip:
		st.Skipped++
		return nil
	case Replace:
		if err := t.ReplaceDocument(ctx, l.Database, l.Collection, l.Doc); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
		st.Replaced++
		return nil
	}
	return fmt.Errorf("%s already exists (use --on-conflict skip or replace)", where)
}
