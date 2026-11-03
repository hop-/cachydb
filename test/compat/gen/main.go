// Command gen writes a compatibility fixture: a small data directory
// written by the cachydb engine it is built with, and expected.jsonl, the
// expected logical content in the export format (plan 004), computed from
// the operations it performs, not by exporting.
//
// It uses only the engine API that exists in every release since v0.0.4
// (db.Open, CreateDatabase, CreateCollection, Collection, Insert, Update,
// Delete, CreateIndex, Close), so the same file is copied into a worktree
// of an older release tag and run there. See
// pkg/db/testdata/compat/README.md.
//
//	go run ./test/compat/gen -out /tmp/fixture -version v0.0.5
//
// The data directory ends with an unflushed tail: the last operations are
// only in the write-ahead log, because the program exits without Close
// (as a crash would), so opening the fixture also replays the old WAL.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

func main() {
	out := flag.String("out", "", "fixture directory to create (must not exist)")
	version := flag.String("version", "", "cachydb release the fixture is written with, e.g. v0.0.5")
	flag.Parse()
	if *out == "" || *version == "" {
		fmt.Fprintln(os.Stderr, "usage: gen -out DIR -version vX.Y.Z")
		os.Exit(2)
	}
	if _, err := os.Stat(*out); err == nil {
		fmt.Fprintf(os.Stderr, "gen: %s exists\n", *out)
		os.Exit(1)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	g, err := generate(context.Background(), filepath.Join(*out, "data"), *version)
	if err == nil {
		err = g.writeExpected(filepath.Join(*out, "expected.jsonl"))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	// Exit without g.engine.Close: the tail stays in the WAL only.
	os.Exit(0)
}

type coll struct {
	schema  *db.Schema
	indexes map[string]string // name -> field
	docs    map[string]map[string]any
}

// gen performs operations on the engine and on its own model of the
// expected content.
type gen struct {
	engine  *db.Engine
	version string
	dbs     map[string]map[string]*coll
}

func (g *gen) coll(ctx context.Context, database, name string) (*db.Collection, *coll, error) {
	d, err := g.engine.Database(database)
	if err != nil {
		return nil, nil, err
	}
	c, err := d.Collection(name)
	if err != nil {
		return nil, nil, err
	}
	return c, g.dbs[database][name], nil
}

func (g *gen) createDB(ctx context.Context, name string) error {
	if _, err := g.engine.CreateDatabase(ctx, name); err != nil {
		return fmt.Errorf("creating database %s: %w", name, err)
	}
	g.dbs[name] = map[string]*coll{}
	return nil
}

func (g *gen) createColl(ctx context.Context, database, name string, s *db.Schema) error {
	d, err := g.engine.Database(database)
	if err != nil {
		return err
	}
	if err := d.CreateCollection(ctx, name, s); err != nil {
		return fmt.Errorf("creating collection %s/%s: %w", database, name, err)
	}
	g.dbs[database][name] = &coll{schema: s, indexes: map[string]string{}, docs: map[string]map[string]any{}}
	return nil
}

func (g *gen) createIndex(ctx context.Context, database, name, index, field string) error {
	c, m, err := g.coll(ctx, database, name)
	if err != nil {
		return err
	}
	if err := c.CreateIndex(ctx, index, field); err != nil {
		return fmt.Errorf("creating index %s on %s/%s: %w", index, database, name, err)
	}
	m.indexes[index] = field
	return nil
}

func (g *gen) insert(ctx context.Context, database, name, id string, data map[string]any) error {
	c, m, err := g.coll(ctx, database, name)
	if err != nil {
		return err
	}
	if _, err := c.Insert(ctx, &db.Document{ID: id, Data: data}); err != nil {
		return fmt.Errorf("inserting %s into %s/%s: %w", id, database, name, err)
	}
	m.docs[id] = normalize(data)
	return nil
}

func (g *gen) update(ctx context.Context, database, name, id string, set map[string]any) error {
	c, m, err := g.coll(ctx, database, name)
	if err != nil {
		return err
	}
	if _, err := c.Update(ctx, id, set); err != nil {
		return fmt.Errorf("updating %s in %s/%s: %w", id, database, name, err)
	}
	for k, v := range normalize(set) {
		m.docs[id][k] = v
	}
	return nil
}

func (g *gen) delete(ctx context.Context, database, name, id string) error {
	c, m, err := g.coll(ctx, database, name)
	if err != nil {
		return err
	}
	if err := c.Delete(ctx, id); err != nil {
		return fmt.Errorf("deleting %s from %s/%s: %w", id, database, name, err)
	}
	delete(m.docs, id)
	return nil
}

// normalize round-trips v through JSON, as the engine stores it.
func normalize(v map[string]any) map[string]any {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err) // the generator only builds plain JSON values
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return out
}

// generate writes the fixture data directory dir and returns the
// generator with the engine still open (the caller decides whether to
// Close it).
func generate(ctx context.Context, dir, version string) (*gen, error) {
	opts := db.Options{
		Dir:             dir,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		DataSegmentSize: 4 << 10,              // several sealed segments
		FlushInterval:   5 * time.Millisecond, // flush between the phases
	}
	engine, err := db.Open(ctx, opts)
	if err != nil {
		return nil, err
	}
	g := &gen{engine: engine, version: version, dbs: map[string]map[string]*coll{}}
	settle := func() { time.Sleep(200 * time.Millisecond) } // let the flusher write segments

	steps := []func() error{
		func() error { return g.createDB(ctx, "kb") },
		func() error { return g.createDB(ctx, "shop") },
		func() error {
			return g.createColl(ctx, "kb", "notes", &db.Schema{Fields: map[string]db.Field{
				"title": {Type: db.TypeString, Required: true},
				"prio":  {Type: db.TypeNumber},
			}})
		},
		func() error { return g.createIndex(ctx, "kb", "notes", "by_tags", "tags") }, // array field
		func() error { return g.createIndex(ctx, "kb", "notes", "by_prio", "prio") }, // scalar field
		func() error { return g.createColl(ctx, "kb", "log", nil) },
		func() error { return g.createColl(ctx, "shop", "items", nil) },
		func() error { return g.createIndex(ctx, "shop", "items", "by_cat", "cat") },
		func() error {
			return g.createColl(ctx, "shop", "empty", &db.Schema{Fields: map[string]db.Field{"name": {Type: db.TypeString, Required: true}}})
		},
	}
	tagSets := [][]any{{"wal"}, {"storage", "wal"}, {}, {"index", 1, true}, {"query"}}
	for i := range 40 {
		steps = append(steps, func() error {
			return g.insert(ctx, "kb", "notes", fmt.Sprintf("n%02d", i), map[string]any{
				"title": fmt.Sprintf("Note %d", i),
				"prio":  i % 5,
				"tags":  tagSets[i%len(tagSets)],
				"meta":  map[string]any{"author": fmt.Sprintf("a%d", i%3), "rev": i, "nested": map[string]any{"deep": []any{i, "x", nil}}},
				"body":  strings.Repeat(fmt.Sprintf("line %d of note %d. ", i, i), 8),
			})
		})
	}
	for i := range 10 {
		steps = append(steps, func() error {
			return g.insert(ctx, "kb", "log", fmt.Sprintf("l%02d", i), map[string]any{
				"level": []any{"info", "warn", nil}[i%3],
				"ok":    i%2 == 0,
				"items": []any{map[string]any{"k": i}, []any{i, i + 1}},
				"ratio": float64(i) / 4,
			})
		})
	}
	for i := range 30 {
		steps = append(steps, func() error {
			return g.insert(ctx, "shop", "items", fmt.Sprintf("i%02d", i), map[string]any{
				"cat":   fmt.Sprintf("c%d", i%4),
				"price": float64(i) * 1.25,
				"name":  fmt.Sprintf("item %d — ünïcode", i),
				"stock": map[string]any{"count": i * 3, "where": []any{"a", "b"}},
			})
		})
	}
	for i := 0; i < 40; i += 7 {
		steps = append(steps, func() error {
			return g.update(ctx, "kb", "notes", fmt.Sprintf("n%02d", i), map[string]any{"prio": 10, "tags": []any{"updated", "wal"}})
		})
	}
	for i := 4; i < 40; i += 9 {
		steps = append(steps, func() error { return g.delete(ctx, "kb", "notes", fmt.Sprintf("n%02d", i)) })
	}
	for i := 0; i < 30; i += 5 {
		steps = append(steps, func() error { return g.delete(ctx, "shop", "items", fmt.Sprintf("i%02d", i)) })
	}
	for i, step := range steps {
		if err := step(); err != nil {
			return g, err
		}
		if i%20 == 19 {
			settle()
		}
	}
	settle()

	// The unflushed tail: reopened with a long flush interval, these stay
	// in the WAL until the engine is closed (for the fixture, never).
	if err := engine.Close(ctx); err != nil {
		return g, err
	}
	opts.FlushInterval = time.Hour
	if g.engine, err = db.Open(ctx, opts); err != nil {
		return g, err
	}
	tail := []func() error{
		func() error {
			return g.update(ctx, "kb", "notes", "n01", map[string]any{"title": "Note 1 (tail)", "tags": []any{"tail"}})
		},
		func() error { return g.delete(ctx, "shop", "items", "i01") },
		func() error {
			return g.insert(ctx, "shop", "items", "i99", map[string]any{"cat": "c9", "price": 9.5, "name": "tail item"})
		},
	}
	for _, step := range tail {
		if err := step(); err != nil {
			return g, err
		}
	}
	return g, nil
}

// writeExpected writes the expected export of the generated content.
func (g *gen) writeExpected(path string) error {
	var b strings.Builder
	line := func(v any) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		b.Write(data)
		b.WriteByte('\n')
		return nil
	}
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	if err := line(map[string]any{"type": "cachydb-export", "version": 1, "cachydb": g.version, "created": created}); err != nil {
		return err
	}
	for _, dbName := range sortedKeys(g.dbs) {
		if err := line(map[string]any{"type": "database", "name": dbName, "schema_version": db.CurrentSchemaVersion}); err != nil {
			return err
		}
		for _, collName := range sortedKeys(g.dbs[dbName]) {
			c := g.dbs[dbName][collName]
			indexes := []map[string]string{}
			for _, name := range sortedKeys(c.indexes) {
				indexes = append(indexes, map[string]string{"name": name, "field": c.indexes[name]})
			}
			if err := line(map[string]any{"type": "collection", "database": dbName, "name": collName, "schema": c.schema, "indexes": indexes}); err != nil {
				return err
			}
			for _, id := range sortedKeys(c.docs) {
				doc := map[string]any{"_id": id}
				for k, v := range c.docs[id] {
					doc[k] = v
				}
				if err := line(map[string]any{"type": "document", "database": dbName, "collection": collName, "doc": doc}); err != nil {
					return err
				}
			}
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
