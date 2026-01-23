package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/hop-/cachydb/pkg/db"
)

func main() {
	// Create a database
	database := db.NewDatabase("example")
	storage := db.NewStorageManager("/tmp/cachydb-example")

	// Create a schema for users collection
	userSchema := &db.Schema{
		Fields: map[string]db.Field{
			"name": {
				Type:     db.TypeString,
				Required: true,
			},
			"email": {
				Type:     db.TypeString,
				Required: true,
			},
			"age": {
				Type:     db.TypeNumber,
				Required: false,
			},
		},
	}

	// Create collection with schema
	err := database.CreateCollection("users", userSchema)
	if err != nil {
		log.Fatal(err)
	}

	// Get the collection
	users, err := database.GetCollection("users")
	if err != nil {
		log.Fatal(err)
	}

	// Insert documents
	doc1 := &db.Document{
		Data: map[string]any{
			"name":  "Alice Smith",
			"email": "alice@example.com",
			"age":   28.0,
		},
	}
	err = users.Insert(doc1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Inserted document with ID: %s\n", doc1.ID)

	doc2 := &db.Document{
		Data: map[string]any{
			"name":  "Bob Johnson",
			"email": "bob@example.com",
			"age":   35.0,
		},
	}
	err = users.Insert(doc2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Inserted document with ID: %s\n", doc2.ID)

	// Create an index on email
	err = users.CreateIndex("email_idx", "email")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created index on email field")

	// Find all documents
	allDocs, err := users.Find(&db.Query{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nAll documents (%d):\n", len(allDocs))
	printDocs(allDocs)

	// Find documents with filter (using index)
	query := &db.Query{
		Filters: []db.QueryFilter{
			{
				Field:    "email",
				Operator: "eq",
				Value:    "alice@example.com",
			},
		},
	}
	docs, err := users.Find(query)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nDocuments with email='alice@example.com' (%d):\n", len(docs))
	printDocs(docs)

	// Find documents with age filter
	query = &db.Query{
		Filters: []db.QueryFilter{
			{
				Field:    "age",
				Operator: "gte",
				Value:    30.0,
			},
		},
	}
	docs, err = users.Find(query)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nDocuments with age >= 30 (%d):\n", len(docs))
	printDocs(docs)

	// Update a document
	err = users.Update(doc1.ID, map[string]any{
		"age":  29.0,
		"city": "New York",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nUpdated document %s\n", doc1.ID)

	// Find by ID
	updatedDoc, err := users.FindByID(doc1.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Updated document:")
	printDocs([]*db.Document{updatedDoc})

	// Save to disk
	err = storage.SaveDatabase(database)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nDatabase saved to disk")

	// Load from disk
	loadedDB, err := storage.LoadDatabase("example")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Database loaded from disk. Collections: %v\n", loadedDB.ListCollections())

	// Delete a document
	err = users.Delete(doc2.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nDeleted document %s\n", doc2.ID)

	// Final count
	fmt.Printf("Final document count: %d\n", users.Count())
}

func printDocs(docs []*db.Document) {
	for _, doc := range docs {
		jsonData, _ := json.MarshalIndent(doc, "", "  ")
		fmt.Println(string(jsonData))
	}
}
