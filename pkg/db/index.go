package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// IndexData represents the serializable format of an index
type IndexData struct {
	Name      string            `json:"name"`
	FieldName string            `json:"field_name"`
	Data      map[string]string `json:"data"`
}

// Serialize converts an index to its serializable format
func (idx *Index) Serialize() (*IndexData, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return &IndexData{
		Name:      idx.Name,
		FieldName: idx.FieldName,
		Data:      idx.Data,
	}, nil
}

// Deserialize loads an index from its serialized format
func (idx *Index) Deserialize(data *IndexData) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.Name = data.Name
	idx.FieldName = data.FieldName
	idx.Data = data.Data

	return nil
}

// SaveToDisk saves an index to a file
func (idx *Index) SaveToDisk(dataDir, dbName, collName string) error {
	data, err := idx.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize index: %w", err)
	}

	// Create directory structure: dataDir/dbName/collName/indexes/
	indexDir := filepath.Join(dataDir, dbName, collName, "indexes")
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	// Save to file: indexName.json
	indexPath := filepath.Join(indexDir, idx.Name+".json")
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	if err := os.WriteFile(indexPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write index file: %w", err)
	}

	return nil
}

// LoadFromDisk loads an index from a file
func LoadIndexFromDisk(dataDir, dbName, collName, indexName string) (*Index, error) {
	indexPath := filepath.Join(dataDir, dbName, collName, "indexes", indexName+".json")

	jsonData, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file: %w", err)
	}

	var data IndexData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index: %w", err)
	}

	idx := NewIndex(data.Name, data.FieldName)
	if err := idx.Deserialize(&data); err != nil {
		return nil, fmt.Errorf("failed to deserialize index: %w", err)
	}

	return idx, nil
}

// LoadAllIndexes loads all indexes for a collection from disk
func LoadAllIndexes(dataDir, dbName, collName string) (map[string]*Index, error) {
	indexDir := filepath.Join(dataDir, dbName, collName, "indexes")

	// Check if index directory exists
	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		return make(map[string]*Index), nil // No indexes yet
	}

	entries, err := os.ReadDir(indexDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read index directory: %w", err)
	}

	indexes := make(map[string]*Index)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		indexName := entry.Name()[:len(entry.Name())-5] // Remove .json extension
		idx, err := LoadIndexFromDisk(dataDir, dbName, collName, indexName)
		if err != nil {
			return nil, fmt.Errorf("failed to load index %s: %w", indexName, err)
		}

		indexes[indexName] = idx
	}

	return indexes, nil
}
