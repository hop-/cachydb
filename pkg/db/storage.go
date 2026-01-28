package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// StorageManager handles persistence
type StorageManager struct {
	RootDir string
}

// NewStorageManager creates a new storage manager
func NewStorageManager(rootDir string) *StorageManager {
	return &StorageManager{RootDir: rootDir}
}

// SaveDatabase saves the entire database to disk
func (sm *StorageManager) SaveDatabase(db *Database) error {
	dbDir := filepath.Join(sm.RootDir, db.Name)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Save database metadata
	metaPath := filepath.Join(dbDir, "db.meta.json")
	metaData := map[string]any{
		"name": db.Name,
	}
	if err := sm.writeJSON(metaPath, metaData); err != nil {
		return fmt.Errorf("failed to save database metadata: %w", err)
	}

	// Save each collection
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, coll := range db.Collections {
		if err := sm.SaveCollection(db.Name, coll); err != nil {
			return fmt.Errorf("failed to save collection '%s': %w", coll.Name, err)
		}
	}

	return nil
}

// SaveCollection saves a collection to disk
func (sm *StorageManager) SaveCollection(dbName string, coll *Collection) error {
	collDir := filepath.Join(sm.RootDir, dbName, coll.Name)
	if err := os.MkdirAll(collDir, 0755); err != nil {
		return fmt.Errorf("failed to create collection directory: %w", err)
	}

	coll.mu.RLock()
	defer coll.mu.RUnlock()

	// Save collection metadata (schema and index definitions)
	metaPath := filepath.Join(collDir, "collection.meta.json")
	meta := struct {
		Name    string            `json:"name"`
		Schema  *Schema           `json:"schema,omitempty"`
		Indexes map[string]string `json:"indexes"` // index name -> field name
	}{
		Name:    coll.Name,
		Schema:  coll.Schema,
		Indexes: make(map[string]string),
	}

	for name, idx := range coll.Indexes {
		meta.Indexes[name] = idx.FieldName
	}

	if err := sm.writeJSON(metaPath, meta); err != nil {
		return fmt.Errorf("failed to save collection metadata: %w", err)
	}

	// Save all documents
	docsPath := filepath.Join(collDir, "documents.json")
	docs := make([]*Document, 0, len(coll.Documents))
	for _, doc := range coll.Documents {
		docs = append(docs, doc)
	}

	if err := sm.writeJSON(docsPath, docs); err != nil {
		return fmt.Errorf("failed to save documents: %w", err)
	}

	return nil
}

// LoadDatabase loads a database from disk
func (sm *StorageManager) LoadDatabase(dbName string) (*Database, error) {
	dbDir := filepath.Join(sm.RootDir, dbName)

	// Check if database exists
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("database '%s' does not exist", dbName)
	}

	db := NewDatabase(dbName)

	// Load collections
	entries, err := os.ReadDir(dbDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read database directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			coll, err := sm.LoadCollection(dbName, entry.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to load collection '%s': %w", entry.Name(), err)
			}
			db.Collections[coll.Name] = coll
		}
	}

	return db, nil
}

// LoadCollection loads a collection from disk
func (sm *StorageManager) LoadCollection(dbName, collName string) (*Collection, error) {
	collDir := filepath.Join(sm.RootDir, dbName, collName)

	// Load metadata
	metaPath := filepath.Join(collDir, "collection.meta.json")
	var meta struct {
		Name    string            `json:"name"`
		Schema  *Schema           `json:"schema,omitempty"`
		Indexes map[string]string `json:"indexes"`
	}

	if err := sm.readJSON(metaPath, &meta); err != nil {
		return nil, fmt.Errorf("failed to load collection metadata: %w", err)
	}

	coll := NewCollection(meta.Name, meta.Schema)

	// Load documents
	docsPath := filepath.Join(collDir, "documents.json")
	var docs []*Document
	if err := sm.readJSON(docsPath, &docs); err != nil {
		// If file doesn't exist, it's ok (empty collection)
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load documents: %w", err)
		}
	}

	// Restore documents and indexes
	for _, doc := range docs {
		coll.Documents[doc.ID] = doc
	}

	// Recreate indexes (except _id which already exists)
	for indexName, fieldName := range meta.Indexes {
		if indexName != "_id" {
			idx := NewIndex(indexName, fieldName)
			for _, doc := range coll.Documents {
				idx.AddToIndex(doc)
			}
			coll.Indexes[indexName] = idx
		} else {
			// Rebuild _id index
			for _, doc := range coll.Documents {
				coll.Indexes["_id"].AddToIndex(doc)
			}
		}
	}

	return coll, nil
}

// DatabaseExists checks if a database exists on disk
func (sm *StorageManager) DatabaseExists(dbName string) bool {
	dbDir := filepath.Join(sm.RootDir, dbName)
	_, err := os.Stat(dbDir)
	return err == nil
}

// DeleteDatabase deletes a database from disk
func (sm *StorageManager) DeleteDatabase(dbName string) error {
	dbDir := filepath.Join(sm.RootDir, dbName)
	return os.RemoveAll(dbDir)
}

// LoadAllDatabases loads all databases from disk into a DatabaseManager
func (sm *StorageManager) LoadAllDatabases() (*DatabaseManager, error) {
	dm := NewDatabaseManager()

	// Create root dir if it doesn't exist
	if err := os.MkdirAll(sm.RootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	// Read all subdirectories (each is a database)
	entries, err := os.ReadDir(sm.RootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read root directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			db, err := sm.LoadDatabase(entry.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to load database '%s': %w", entry.Name(), err)
			}
			dm.Databases[db.Name] = db
		}
	}

	return dm, nil
}

// SaveAllDatabases saves all databases from a DatabaseManager
func (sm *StorageManager) SaveAllDatabases(dm *DatabaseManager) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	for _, db := range dm.Databases {
		if err := sm.SaveDatabase(db); err != nil {
			return fmt.Errorf("failed to save database '%s': %w", db.Name, err)
		}
	}

	return nil
}

// Helper functions
func (sm *StorageManager) writeJSON(path string, data any) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func (sm *StorageManager) readJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(target)
}
