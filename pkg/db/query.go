package db

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Insert inserts a document into the collection
func (c *Collection) Insert(doc *Document) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Generate ID if not provided
	if doc.ID == "" {
		doc.ID = uuid.New().String()
	}

	// Check if document already exists
	if _, exists := c.Documents[doc.ID]; exists {
		return fmt.Errorf("document with ID '%s' already exists", doc.ID)
	}

	// Validate against schema
	if c.Schema != nil {
		if err := c.Schema.ValidateDocument(doc); err != nil {
			return fmt.Errorf("schema validation failed: %w", err)
		}
	}

	// Add document
	c.Documents[doc.ID] = doc

	// Update indexes
	if err := c.updateIndexes(nil, doc); err != nil {
		delete(c.Documents, doc.ID)
		return fmt.Errorf("failed to update indexes: %w", err)
	}

	return nil
}

// FindByID finds a document by ID
func (c *Collection) FindByID(id string) (*Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	doc, exists := c.Documents[id]
	if !exists {
		return nil, fmt.Errorf("document with ID '%s' not found", id)
	}

	return doc.Clone(), nil
}

// Find finds documents matching a query
func (c *Collection) Find(query *Query) ([]*Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	results := make([]*Document, 0)

	// If no filters, return all documents
	if len(query.Filters) == 0 {
		for _, doc := range c.Documents {
			results = append(results, doc.Clone())
		}
	} else {
		// Try to use index for first filter if possible
		firstFilter := query.Filters[0]
		if firstFilter.Operator == "eq" {
			// Check if there's an index for this field
			var candidateDocs []*Document
			indexFound := false

			for _, idx := range c.Indexes {
				if idx.FieldName == firstFilter.Field {
					docID, found := idx.Find(firstFilter.Value)
					if found {
						if doc, exists := c.Documents[docID]; exists {
							candidateDocs = []*Document{doc}
							indexFound = true
							break
						}
					} else {
						// Index exists but no match found
						return results, nil
					}
				}
			}

			if !indexFound {
				// No index, scan all documents
				for _, doc := range c.Documents {
					candidateDocs = append(candidateDocs, doc)
				}
			}

			// Apply all filters
			for _, doc := range candidateDocs {
				if matchesAllFilters(doc, query.Filters) {
					results = append(results, doc.Clone())
				}
			}
		} else {
			// Non-equality first filter, scan all documents
			for _, doc := range c.Documents {
				if matchesAllFilters(doc, query.Filters) {
					results = append(results, doc.Clone())
				}
			}
		}
	}

	// Apply skip and limit
	if query.Skip > 0 {
		if query.Skip >= len(results) {
			return []*Document{}, nil
		}
		results = results[query.Skip:]
	}

	if query.Limit > 0 && query.Limit < len(results) {
		results = results[:query.Limit]
	}

	return results, nil
}

// Update updates a document
func (c *Collection) Update(id string, updates map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	doc, exists := c.Documents[id]
	if !exists {
		return fmt.Errorf("document with ID '%s' not found", id)
	}

	oldDoc := doc.Clone()

	// Apply updates
	for key, value := range updates {
		if key == "_id" {
			return fmt.Errorf("cannot update _id field")
		}
		doc.Data[key] = value
	}

	// Validate against schema
	if c.Schema != nil {
		if err := c.Schema.ValidateDocument(doc); err != nil {
			// Rollback
			c.Documents[id] = oldDoc
			return fmt.Errorf("schema validation failed: %w", err)
		}
	}

	// Update indexes
	if err := c.updateIndexes(oldDoc, doc); err != nil {
		// Rollback
		c.Documents[id] = oldDoc
		return fmt.Errorf("failed to update indexes: %w", err)
	}

	return nil
}

// Delete deletes a document by ID
func (c *Collection) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	doc, exists := c.Documents[id]
	if !exists {
		return fmt.Errorf("document with ID '%s' not found", id)
	}

	// Update indexes
	if err := c.updateIndexes(doc, nil); err != nil {
		return fmt.Errorf("failed to update indexes: %w", err)
	}

	delete(c.Documents, id)
	return nil
}

// Count returns the number of documents in the collection
func (c *Collection) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.Documents)
}

// matchesAllFilters checks if a document matches all filters
func matchesAllFilters(doc *Document, filters []QueryFilter) bool {
	for _, filter := range filters {
		if !matchesFilter(doc, filter) {
			return false
		}
	}
	return true
}

// matchesFilter checks if a document matches a single filter
func matchesFilter(doc *Document, filter QueryFilter) bool {
	value, exists := doc.GetValue(filter.Field)
	if !exists {
		return false
	}

	switch filter.Operator {
	case "eq":
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", filter.Value)
	case "ne":
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", filter.Value)
	case "gt":
		return compareValues(value, filter.Value) > 0
	case "gte":
		return compareValues(value, filter.Value) >= 0
	case "lt":
		return compareValues(value, filter.Value) < 0
	case "lte":
		return compareValues(value, filter.Value) <= 0
	case "in":
		// Check if value is in the filter.Value array
		if arr, ok := filter.Value.([]any); ok {
			valueStr := fmt.Sprintf("%v", value)
			for _, item := range arr {
				if fmt.Sprintf("%v", item) == valueStr {
					return true
				}
			}
		}
		return false
	}

	return false
}

// compareValues compares two values (simple numeric/string comparison)
func compareValues(a, b any) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return strings.Compare(aStr, bStr)
}

// CreateCollection creates a new collection in the database
func (db *Database) CreateCollection(name string, schema *Schema) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Collections[name]; exists {
		return fmt.Errorf("collection '%s' already exists", name)
	}

	if schema != nil {
		if err := schema.Validate(); err != nil {
			return fmt.Errorf("invalid schema: %w", err)
		}
	}

	db.Collections[name] = NewCollection(name, schema)
	return nil
}

// DropCollection drops a collection from the database
func (db *Database) DropCollection(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Collections[name]; !exists {
		return fmt.Errorf("collection '%s' does not exist", name)
	}

	delete(db.Collections, name)
	return nil
}

// GetCollection gets a collection by name
func (db *Database) GetCollection(name string) (*Collection, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	coll, exists := db.Collections[name]
	if !exists {
		return nil, fmt.Errorf("collection '%s' does not exist", name)
	}

	return coll, nil
}

// ListCollections returns a list of all collection names
func (db *Database) ListCollections() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.Collections))
	for name := range db.Collections {
		names = append(names, name)
	}
	return names
}
