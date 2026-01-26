package db

import (
	"fmt"
)

// AddToIndex adds a document to an index
func (idx *Index) AddToIndex(doc *Document) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	value, exists := doc.GetValue(idx.FieldName)
	if !exists {
		return nil // Field doesn't exist in document, skip indexing
	}

	// Convert value to string for hash-based indexing
	key := fmt.Sprintf("%v", value)
	idx.Data[key] = doc.ID

	return nil
}

// RemoveFromIndex removes a document from an index
func (idx *Index) RemoveFromIndex(doc *Document) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	value, exists := doc.GetValue(idx.FieldName)
	if !exists {
		return nil
	}

	key := fmt.Sprintf("%v", value)
	delete(idx.Data, key)

	return nil
}

// Find finds a document ID by indexed field value
func (idx *Index) Find(value any) (string, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := fmt.Sprintf("%v", value)
	docID, exists := idx.Data[key]
	return docID, exists
}

// CreateIndex creates a new index on a collection
func (c *Collection) CreateIndex(indexName, fieldName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.Indexes[indexName]; exists {
		return fmt.Errorf("index '%s' already exists", indexName)
	}

	idx := NewIndex(indexName, fieldName)

	// Build index from existing documents
	for _, doc := range c.Documents {
		if err := idx.AddToIndex(doc); err != nil {
			return fmt.Errorf("failed to add document to index: %w", err)
		}
	}

	c.Indexes[indexName] = idx
	return nil
}

// DropIndex removes an index from a collection
func (c *Collection) DropIndex(indexName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if indexName == "_id" {
		return fmt.Errorf("cannot drop the automatic _id index")
	}

	if _, exists := c.Indexes[indexName]; !exists {
		return fmt.Errorf("index '%s' does not exist", indexName)
	}

	delete(c.Indexes, indexName)
	return nil
}

// updateIndexes updates all indexes when a document is modified
func (c *Collection) updateIndexes(oldDoc, newDoc *Document) error {
	for _, idx := range c.Indexes {
		if oldDoc != nil {
			if err := idx.RemoveFromIndex(oldDoc); err != nil {
				return err
			}
		}
		if newDoc != nil {
			if err := idx.AddToIndex(newDoc); err != nil {
				return err
			}
		}
	}
	return nil
}
