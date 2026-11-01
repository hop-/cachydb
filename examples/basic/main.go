// Command basic shows how to embed CachyDB as a Go library: open an
// engine on a data directory, create databases, collections, schemas, and
// indexes, write and query documents, then close the engine and reopen
// the directory to show the data is durable.
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
	laptopID, err := products.Insert(ctx, &db.Document{Data: map[string]any{
		"name":  "Laptop",
		"price": 999.99,
		"stock": 50,
		"tags":  []any{"electronics", "computers"},
	}})
	if err != nil {
		return err
	}
	fmt.Println("Inserted product:", laptopID)

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
