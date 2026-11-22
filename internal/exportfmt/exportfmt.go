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

// BatchTarget is a Target that can also write many documents of one
// collection at once. Import uses it, when the target implements it, for
// each run of document lines of one collection: the engine adapter (one
// atomic batch, one WAL fsync) and the MCP client adapter
// (insert_documents) do.
type BatchTarget interface {
	Target
	// InsertDocuments writes docs (flat objects with distinct _ids) to one
	// collection atomically: every document or none. An existing _id
	// fails the call with an error wrapping ErrDocumentExists (Fail),
	// keeps the stored document (Skip), or is replaced (Replace). It
	// returns the counts of Inserted, Replaced, and Skipped. An error that
	// concerns one document is a *db.BatchItemError whose Index is the
	// document's position in docs; an error wrapping db.ErrBatchTooLarge
	// means docs must be split.
	InsertDocuments(ctx context.Context, database, collection string, docs []map[string]any, mode OnConflict) (Stats, error)
}

// Batching of document lines for a BatchTarget: a run is written once it
// holds importBatchDocs documents or importBatchBytes bytes of lines,
// both below the engine's default batch limits (db.Options.MaxBatchDocs,
// MaxBatchBytes). A run over the target's limits is split in halves.
const (
	importBatchDocs  = 1000
	importBatchBytes = 4 << 20
)

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
// documents, handling conflicts per mode. When t is a BatchTarget, the
// document lines of one collection are written in atomic batches (see
// importBatchDocs). On error, the returned Stats tell how many lines were
// applied before it (every line before the failing one, except the lines
// of a failing batch); the error names the line.
func Import(ctx context.Context, r io.Reader, t Target, mode OnConflict) (Stats, error) {
	var st Stats
	if _, err := ParseOnConflict(string(mode)); err != nil {
		return st, err
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<30)
	lineNo := 0
	failAt := func(line int, err error) (Stats, error) {
		return st, fmt.Errorf("import line %d (%d lines applied): %w", line, st.Lines, err)
	}
	bt, _ := t.(BatchTarget)
	run := docRun{t: bt, mode: mode, st: &st}
	// fail reports err at the current line once the pending document
	// lines before it are applied, as they would be without batching.
	fail := func(err error) (Stats, error) {
		if line, ferr := run.flush(ctx); ferr != nil {
			return failAt(line, ferr)
		}
		return failAt(lineNo, err)
	}
	for sc.Scan() {
		lineNo++
		if err := ctx.Err(); err != nil {
			return failAt(lineNo, err)
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
		if kind.Type == TypeDocument && bt != nil {
			if line, err := run.add(ctx, raw, lineNo); err != nil {
				return failAt(line, err)
			}
			continue
		}
		if line, err := run.flush(ctx); err != nil {
			return failAt(line, err)
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
	if line, err := run.flush(ctx); err != nil {
		return failAt(line, err)
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

// parseDocumentLine decodes a document line and returns it with the
// document's _id.
func parseDocumentLine(raw []byte) (DocumentLine, string, error) {
	var l DocumentLine
	if err := json.Unmarshal(raw, &l); err != nil {
		return l, "", fmt.Errorf("invalid document line: %w", err)
	}
	id, _ := l.Doc["_id"].(string)
	if id == "" {
		return l, "", errors.New("document without a string _id")
	}
	return l, id, nil
}

func importDocument(ctx context.Context, raw []byte, t Target, mode OnConflict, st *Stats) error {
	l, id, err := parseDocumentLine(raw)
	if err != nil {
		return err
	}
	return applyDocument(ctx, t, l.Database, l.Collection, id, l.Doc, mode, st)
}

// applyDocument inserts the document id of database/collection through
// t, handling an existing _id per mode, and counts it in st.
func applyDocument(ctx context.Context, t Target, database, collection, id string, doc map[string]any, mode OnConflict, st *Stats) error {
	where := docWhere(id, database, collection)
	err := t.InsertDocument(ctx, database, collection, doc)
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
		if err := t.ReplaceDocument(ctx, database, collection, doc); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
		st.Replaced++
		return nil
	}
	return existsErr(where)
}

// docWhere names a document in errors.
func docWhere(id, database, collection string) string {
	return fmt.Sprintf("document %q in %s/%s", id, database, collection)
}

// existsErr is the error of an existing document in Fail mode.
func existsErr(where string) error {
	return fmt.Errorf("%s already exists (use --on-conflict skip or replace)", where)
}

// docRun holds the pending document lines of one collection for a
// BatchTarget (t; nil without batching).
type docRun struct {
	t    BatchTarget
	mode OnConflict
	st   *Stats

	database, collection string
	docs                 []map[string]any
	lines                []int // the line number of each document
	ids                  map[string]struct{}
	bytes                int
}

// add queues the document line raw (line number line). The pending lines
// are written first when the document belongs to another collection or
// repeats an _id of the run, and after it when the run is full. On error
// it returns the line that failed.
func (r *docRun) add(ctx context.Context, raw []byte, line int) (int, error) {
	l, id, err := parseDocumentLine(raw)
	if err != nil {
		// The pending lines come first, as without batching.
		if at, ferr := r.flush(ctx); ferr != nil {
			return at, ferr
		}
		return line, err
	}
	if len(r.docs) > 0 {
		_, dup := r.ids[id]
		if dup || l.Database != r.database || l.Collection != r.collection {
			if at, err := r.flush(ctx); err != nil {
				return at, err
			}
		}
	}
	if len(r.docs) == 0 {
		r.database, r.collection = l.Database, l.Collection
		if r.ids == nil {
			r.ids = make(map[string]struct{})
		}
	}
	r.docs = append(r.docs, l.Doc)
	r.lines = append(r.lines, line)
	r.ids[id] = struct{}{}
	r.bytes += len(raw)
	if len(r.docs) >= importBatchDocs || r.bytes >= importBatchBytes {
		return r.flush(ctx)
	}
	return 0, nil
}

// flush writes the pending documents and empties the run. On error it
// returns the line of the document that failed (the first one's when the
// error concerns no single document).
func (r *docRun) flush(ctx context.Context) (int, error) {
	if len(r.docs) == 0 {
		return 0, nil
	}
	line, err := r.write(ctx, 0, len(r.docs))
	clear(r.docs)
	clear(r.ids)
	r.docs, r.lines, r.bytes = r.docs[:0], r.lines[:0], 0
	return line, err
}

// write writes r.docs[from:to] as one batch, splitting it in halves while
// it is over the target's batch limits; a single document over them goes
// through the one-document path (applyDocument).
func (r *docRun) write(ctx context.Context, from, to int) (int, error) {
	docs := r.docs[from:to]
	got, err := r.t.InsertDocuments(ctx, r.database, r.collection, docs, r.mode)
	switch {
	case err == nil:
		r.st.Inserted += got.Inserted
		r.st.Replaced += got.Replaced
		r.st.Skipped += got.Skipped
		r.st.Lines += len(docs)
		return 0, nil
	case errors.Is(err, db.ErrBatchTooLarge) && len(docs) > 1:
		mid := from + len(docs)/2
		if line, err := r.write(ctx, from, mid); err != nil {
			return line, err
		}
		return r.write(ctx, mid, to)
	case errors.Is(err, db.ErrBatchTooLarge):
		id, _ := docs[0]["_id"].(string)
		if err := applyDocument(ctx, r.t, r.database, r.collection, id, docs[0], r.mode, r.st); err != nil {
			return r.lines[from], err
		}
		r.st.Lines++
		return 0, nil
	}
	i := 0
	if bie, ok := errors.AsType[*db.BatchItemError](err); ok && bie.Index >= 0 && bie.Index < len(docs) {
		i, err = bie.Index, bie.Err
	}
	id, _ := docs[i]["_id"].(string)
	where := docWhere(id, r.database, r.collection)
	if r.mode == Fail && errors.Is(err, ErrDocumentExists) {
		return r.lines[from+i], existsErr(where)
	}
	return r.lines[from+i], fmt.Errorf("%s: %w", where, err)
}
