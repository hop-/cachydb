package exportfmt

import (
	"context"
	"errors"

	"github.com/hop-/cachydb/pkg/db"
)

// Engine adapts an open *db.Engine as a Source and a Target (offline
// export and import).
type Engine struct {
	E *db.Engine
}

var (
	_ Source = Engine{}
	_ Target = Engine{}
)

func (a Engine) coll(database, collection string) (*db.Collection, error) {
	d, err := a.E.Database(database)
	if err != nil {
		return nil, err
	}
	return d.Collection(collection)
}

// Databases implements Source.
func (a Engine) Databases(context.Context) ([]string, error) { return a.E.ListDatabases(), nil }

// SchemaVersion implements Source.
func (a Engine) SchemaVersion(_ context.Context, database string) (int, error) {
	d, err := a.E.Database(database)
	if err != nil {
		return 0, err
	}
	return d.SchemaVersion(), nil
}

// Collections implements Source.
func (a Engine) Collections(_ context.Context, database string) ([]string, error) {
	d, err := a.E.Database(database)
	if err != nil {
		return nil, err
	}
	return d.ListCollections(), nil
}

// Describe implements Source.
func (a Engine) Describe(_ context.Context, database, collection string) (*db.Schema, []db.IndexInfo, error) {
	c, err := a.coll(database, collection)
	if err != nil {
		return nil, nil, err
	}
	return c.Schema(), c.Indexes(), nil
}

// Documents implements Source.
func (a Engine) Documents(ctx context.Context, database, collection string, fn func(map[string]any) error) error {
	c, err := a.coll(database, collection)
	if err != nil {
		return err
	}
	docs, err := c.Find(ctx, nil)
	if err != nil {
		return err
	}
	for _, d := range docs {
		if err := fn(DocMap(d)); err != nil {
			return err
		}
	}
	return nil
}

// DocMap returns d as a flat object with its _id.
func DocMap(d *db.Document) map[string]any {
	m := make(map[string]any, len(d.Data)+1)
	for k, v := range d.Data {
		m[k] = v
	}
	m["_id"] = d.ID
	return m
}

// docFromMap splits a flat object into id and data.
func docFromMap(m map[string]any) *db.Document {
	d := &db.Document{Data: make(map[string]any, len(m))}
	for k, v := range m {
		if k == "_id" {
			if id, ok := v.(string); ok {
				d.ID = id
				continue
			}
		}
		d.Data[k] = v
	}
	return d
}

// EnsureDatabase implements Target.
func (a Engine) EnsureDatabase(ctx context.Context, name string) error {
	_, err := a.E.CreateDatabase(ctx, name)
	return err
}

// Lookup implements Target.
func (a Engine) Lookup(_ context.Context, database, collection string) (*db.Schema, []db.IndexInfo, bool, error) {
	c, err := a.coll(database, collection)
	if errors.Is(err, db.ErrNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	return c.Schema(), c.Indexes(), true, nil
}

// CreateCollection implements Target.
func (a Engine) CreateCollection(ctx context.Context, database, collection string, schema *db.Schema) error {
	d, err := a.E.Database(database)
	if err != nil {
		return err
	}
	return d.CreateCollection(ctx, collection, schema)
}

// UpdateSchema implements Target.
func (a Engine) UpdateSchema(ctx context.Context, database, collection string, schema *db.Schema) error {
	c, err := a.coll(database, collection)
	if err != nil {
		return err
	}
	return c.UpdateSchema(ctx, schema)
}

// CreateIndex implements Target.
func (a Engine) CreateIndex(ctx context.Context, database, collection, name, field string) error {
	c, err := a.coll(database, collection)
	if err != nil {
		return err
	}
	return c.CreateIndex(ctx, name, field)
}

// DropIndex implements Target.
func (a Engine) DropIndex(ctx context.Context, database, collection, name string) error {
	c, err := a.coll(database, collection)
	if err != nil {
		return err
	}
	return c.DropIndex(ctx, name)
}

// InsertDocument implements Target.
func (a Engine) InsertDocument(ctx context.Context, database, collection string, doc map[string]any) error {
	c, err := a.coll(database, collection)
	if err != nil {
		return err
	}
	_, err = c.Insert(ctx, docFromMap(doc))
	if errors.Is(err, db.ErrExists) {
		return errors.Join(ErrDocumentExists, err)
	}
	return err
}

// ReplaceDocument implements Target.
func (a Engine) ReplaceDocument(ctx context.Context, database, collection string, doc map[string]any) error {
	c, err := a.coll(database, collection)
	if err != nil {
		return err
	}
	d := docFromMap(doc)
	_, err = c.Replace(ctx, d.ID, d, nil)
	return err
}
