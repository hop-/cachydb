// Command basic shows how to embed CachyDB as a Go library: open an
// engine on a data directory, create databases, collections, schemas, and
// indexes, write and query documents (conditional updates, $now, tag
// queries with projection and sort, count and distinct), then close the
// engine and reopen the directory to show the data is durable.
//
// Usage:
//
//	go run ./examples/basic           # uses a fresh temporary directory
//	go run ./examples/basic -dir DIR  # uses (and keeps) DIR
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/hop-/cachydb/pkg/db"
)

func main() {
	dir := flag.String("dir", "", "data directory (default: a temporary directory removed on exit)")
	flag.Parse()

	if err := run(context.Background(), *dir); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, dir string) error {
	if dir == "" {
		tmp, err := os.MkdirTemp("", "cachydb-example-*")
		if err != nil {
			return fmt.Errorf("creating temp dir: %w", err)
		}
		defer func() { _ = os.RemoveAll(tmp) }() // best effort: it's a temp dir
		dir = tmp
	}
	fmt.Println("Data directory:", dir)

	// Open the engine. Every write is durable (fsynced to the WAL) before
	// the call returns; only Dir is required, the rest has defaults.
	engine, err := db.Open(ctx, db.Options{Dir: dir})
	if err != nil {
		return fmt.Errorf("opening engine: %w", err)
	}
	// Close flushes everything to the data segments. The deferred call
	// covers early returns; the explicit Close below reports its error.
	closed := false
	defer func() {
		if !closed {
			_ = engine.Close(ctx) // already returning an error
		}
	}()

	userDB, err := engine.CreateDatabase(ctx, "users_db")
	if err != nil {
		return err
	}
	productsDB, err := engine.CreateDatabase(ctx, "products_db")
	if err != nil {
		return err
	}
	fmt.Println("Databases:", engine.ListDatabases())

	// === users_db: a collection with a schema and an index ===

	userSchema := &db.Schema{
		Fields: map[string]db.Field{
			"name":  {Type: db.TypeString, Required: true},
			"email": {Type: db.TypeString, Required: true},
			"age":   {Type: db.TypeNumber},
		},
	}
	if err := userDB.CreateCollection(ctx, "users", userSchema); err != nil {
		return err
	}
	users, err := userDB.Collection("users")
	if err != nil {
		return err
	}

	aliceID, err := users.Insert(ctx, &db.Document{Data: map[string]any{
		"name":  "Alice Smith",
		"email": "alice@example.com",
		"age":   28,
	}})
	if err != nil {
		return err
	}
	fmt.Println("Inserted Alice with generated ID:", aliceID)

	// A caller-chosen ID works too.
	bobID, err := users.Insert(ctx, &db.Document{ID: "bob", Data: map[string]any{
		"name":  "Bob Johnson",
		"email": "bob@example.com",
		"age":   35,
	}})
	if err != nil {
		return err
	}
	fmt.Println("Inserted Bob with ID:", bobID)

	// Schema violations are reported with ErrSchemaValidation.
	_, err = users.Insert(ctx, &db.Document{Data: map[string]any{"name": "No Email"}})
	if !errors.Is(err, db.ErrSchemaValidation) {
		return fmt.Errorf("expected a schema validation error, got %v", err)
	}
	fmt.Println("Rejected invalid document:", err)

	if err := users.CreateIndex(ctx, "email_idx", "email"); err != nil {
		return err
	}
	fmt.Println("Created index email_idx on email")

	all, err := users.Find(ctx, &db.Query{})
	if err != nil {
		return err
	}
	printDocs(fmt.Sprintf("All users (%d), sorted by ID", len(all)), all)

	// An eq filter on an indexed field uses the index.
	byEmail, err := users.Find(ctx, &db.Query{Filters: []db.QueryFilter{
		{Field: "email", Operator: "eq", Value: "alice@example.com"},
	}})
	if err != nil {
		return err
	}
	printDocs("Users with email = alice@example.com", byEmail)

	// Numbers compare by value whatever their Go type (int, float64, ...).
	older, err := users.Find(ctx, &db.Query{Filters: []db.QueryFilter{
		{Field: "age", Operator: "gte", Value: 30},
	}})
	if err != nil {
		return err
	}
	printDocs("Users with age >= 30", older)

	updated, err := users.Update(ctx, aliceID, map[string]any{"age": 29, "city": "New York"})
	if err != nil {
		return err
	}
	printDocs("Updated Alice", []*db.Document{updated})

	// A conditional update (compare-and-set): it is written only if the
	// stored document still matches If. {"$now": true} becomes the server's
	// current time (RFC 3339, UTC).
	onlyIfAge29 := db.Condition{{Field: "age", Operator: "eq", Value: 29}}
	updated, _, err = users.UpdateWith(ctx, aliceID, db.UpdateSpec{
		Set:   map[string]any{"age": 30, "updated": map[string]any{"$now": true}},
		Unset: []string{"city"},
		If:    onlyIfAge29,
	})
	if err != nil {
		return err
	}
	printDocs("Alice after a conditional update (age 29 -> 30, city removed)", []*db.Document{updated})

	// The same condition no longer matches, so nothing is written. Another
	// agent that read the old version gets this error and re-reads.
	_, _, err = users.UpdateWith(ctx, aliceID, db.UpdateSpec{
		Set: map[string]any{"age": 99},
		If:  onlyIfAge29,
	})
	if !errors.Is(err, db.ErrConditionFailed) {
		return fmt.Errorf("expected a condition failure, got %v", err)
	}
	fmt.Println("\nSecond conditional update rejected:", err)

	// Describe the collection: its schema and indexes.
	fmt.Println("\nusers indexes:", users.Indexes())
	if s := users.Schema(); s != nil {
		fmt.Println("users required fields:", requiredFields(s))
	}

	if err := users.Delete(ctx, bobID); err != nil {
		return err
	}
	if _, err := users.Get(ctx, bobID); !errors.Is(err, db.ErrNotFound) {
		return fmt.Errorf("expected Bob to be gone, got %v", err)
	}
	fmt.Println("\nDeleted Bob")

	// === products_db: a schemaless collection ===

	if err := productsDB.CreateCollection(ctx, "products", nil); err != nil {
		return err
	}
	products, err := productsDB.Collection("products")
	if err != nil {
		return err
	}
	for _, p := range []map[string]any{
		{"name": "Laptop", "price": 999.99, "stock": 50, "tags": []any{"electronics", "computers"}},
		{"name": "Phone", "price": 599, "stock": 120, "tags": []any{"electronics", "mobile"}},
		{"name": "Desk", "price": 249.5, "stock": 8, "tags": []any{"furniture"}},
	} {
		id, err := products.Insert(ctx, &db.Document{Data: p})
		if err != nil {
			return err
		}
		fmt.Printf("Inserted product %s: %s\n", p["name"], id)
	}

	// An index on an array field indexes each element, so "has" queries on
	// tags use it.
	if err := products.CreateIndex(ctx, "tags_idx", "tags"); err != nil {
		return err
	}
	hasElectronics := []db.QueryFilter{{Field: "tags", Operator: "has", Value: "electronics"}}

	// Only the listed fields come back (plus _id), most expensive first.
	electronics, err := products.Find(ctx, &db.Query{
		Filters: hasElectronics,
		Fields:  []string{"name", "price"},
		Sort:    []db.SortKey{{Field: "price", Order: "desc"}},
	})
	if err != nil {
		return err
	}
	printDocs("Electronics (name and price, by price descending)", electronics)

	// Case-insensitive text search.
	byName, err := products.Find(ctx, &db.Query{Filters: []db.QueryFilter{
		{Field: "name", Operator: "contains", Value: "LAP"},
	}})
	if err != nil {
		return err
	}
	printDocs(`Products whose name contains "lap"`, byName)

	n, err := products.CountDocuments(ctx, &db.Query{Filters: hasElectronics})
	if err != nil {
		return err
	}
	tags, err := products.Distinct(ctx, "tags", nil)
	if err != nil {
		return err
	}
	fmt.Printf("\nElectronics count: %d; all tags: %v\n", n, tags)

	// The example doubles as a smoke test (main_test.go), so check what it
	// printed.
	var first *db.Document
	if len(electronics) > 0 {
		first = electronics[0]
	}
	if n != 2 || len(electronics) != 2 || first.Data["name"] != "Laptop" ||
		len(byName) != 1 || len(tags) != 4 {
		return fmt.Errorf("unexpected query results: count=%d electronics=%d byName=%d tags=%v",
			n, len(electronics), len(byName), tags)
	}
	if _, hasStock := first.Data["stock"]; hasStock {
		return errors.New("projection returned a field that was not requested")
	}

	closed = true
	if err := engine.Close(ctx); err != nil {
		return fmt.Errorf("closing engine: %w", err)
	}
	fmt.Println("\nEngine closed (all changes flushed)")

	// === Reopen the directory: the data survived ===

	engine, err = db.Open(ctx, db.Options{Dir: dir})
	if err != nil {
		return fmt.Errorf("reopening engine: %w", err)
	}
	closed = false
	fmt.Println("Reopened; databases:", engine.ListDatabases())

	for _, name := range engine.ListDatabases() {
		d, err := engine.Database(name)
		if err != nil {
			return err
		}
		for _, cname := range d.ListCollections() {
			c, err := d.Collection(cname)
			if err != nil {
				return err
			}
			fmt.Printf("  %s.%s: %d documents\n", name, cname, c.Count())
		}
	}

	reloaded, err := engine.Database("users_db")
	if err != nil {
		return err
	}
	users, err = reloaded.Collection("users")
	if err != nil {
		return err
	}
	alice, err := users.Get(ctx, aliceID)
	if err != nil {
		return err
	}
	printDocs("Alice after reopen", []*db.Document{alice})

	closed = true
	if err := engine.Close(ctx); err != nil {
		return fmt.Errorf("closing engine: %w", err)
	}
	return nil
}

// requiredFields returns the names of a schema's required fields, sorted.
func requiredFields(s *db.Schema) []string {
	var names []string
	for name, f := range s.Fields {
		if f.Required {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func printDocs(title string, docs []*db.Document) {
	fmt.Printf("\n%s:\n", title)
	for _, doc := range docs {
		data, err := json.MarshalIndent(doc, "  ", "  ")
		if err != nil {
			fmt.Printf("  <unprintable document %s: %v>\n", doc.ID, err)
			continue
		}
		fmt.Println(" ", string(data))
	}
}
