package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// StorageFormat represents the storage format type
type StorageFormat string

const (
	FormatJSON   StorageFormat = "json"
	FormatBinary StorageFormat = "binary"
)

// StorageManager handles persistence
type StorageManager struct {
	RootDir string
	WAL     *WALManager
	Format  StorageFormat // Default format for new data
}

// NewStorageManager creates a new storage manager
func NewStorageManager(rootDir string) (*StorageManager, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	wal, err := NewWALManager(rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL manager: %w", err)
	}

	return &StorageManager{
		RootDir: rootDir,
		WAL:     wal,
		Format:  FormatBinary, // Use binary format by default
	}, nil
}

// Close closes the storage manager and flushes WAL
func (sm *StorageManager) Close() error {
	if sm.WAL != nil {
		return sm.WAL.Close()
	}
	return nil
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
		Format  StorageFormat     `json:"format"`  // Storage format
	}{
		Name:    coll.Name,
		Schema:  coll.Schema,
		Indexes: make(map[string]string),
		Format:  sm.Format,
	}

	for name, idx := range coll.Indexes {
		meta.Indexes[name] = idx.FieldName
	}

	if err := sm.writeJSON(metaPath, meta); err != nil {
		return fmt.Errorf("failed to save collection metadata: %w", err)
	}

	// Save based on format
	if sm.Format == FormatBinary {
		// Save to binary format with compression
		writer, err := NewBinaryCollectionWriter(sm.RootDir, dbName, coll.Name)
		if err != nil {
			return fmt.Errorf("failed to create binary writer: %w", err)
		}
		defer writer.Close(sm.RootDir, dbName, coll.Name)

		for _, doc := range coll.Documents {
			if err := writer.WriteDocument(doc); err != nil {
				return fmt.Errorf("failed to write document: %w", err)
			}
		}

		if err := writer.Flush(sm.RootDir, dbName, coll.Name); err != nil {
			return fmt.Errorf("failed to flush writer: %w", err)
		}

		// Save indexes to disk
		for _, idx := range coll.Indexes {
			if err := idx.SaveToDisk(sm.RootDir, dbName, coll.Name); err != nil {
				return fmt.Errorf("failed to save index %s: %w", idx.Name, err)
			}
		}
	} else {
		// Save to JSON format (legacy)
		docsPath := filepath.Join(collDir, "documents.json")
		docs := make([]*Document, 0, len(coll.Documents))
		for _, doc := range coll.Documents {
			docs = append(docs, doc)
		}

		if err := sm.writeJSON(docsPath, docs); err != nil {
			return fmt.Errorf("failed to save documents: %w", err)
		}
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
		Format  StorageFormat     `json:"format"`
	}

	if err := sm.readJSON(metaPath, &meta); err != nil {
		return nil, fmt.Errorf("failed to load collection metadata: %w", err)
	}

	// Default to JSON if not specified (for backward compatibility)
	if meta.Format == "" {
		meta.Format = FormatJSON
	}

	coll := NewCollection(meta.Name, meta.Schema)

	// Load based on format
	if meta.Format == FormatBinary {
		// Load from binary format
		reader, err := NewBinaryCollectionReader(sm.RootDir, dbName, collName)
		if err != nil {
			// If binary file doesn't exist yet, it's ok (empty collection)
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to create binary reader: %w", err)
			}
		} else {
			defer reader.Close()

			docs, err := reader.ReadAllDocuments()
			if err != nil {
				return nil, fmt.Errorf("failed to read documents: %w", err)
			}

			for _, doc := range docs {
				coll.Documents[doc.ID] = doc
			}
		}

		// Load indexes from disk
		indexes, err := LoadAllIndexes(sm.RootDir, dbName, collName)
		if err != nil {
			return nil, fmt.Errorf("failed to load indexes: %w", err)
		}

		// Replace default _id index if it was loaded
		for name, idx := range indexes {
			coll.Indexes[name] = idx
		}

		// If _id index wasn't loaded, rebuild it
		if _, exists := indexes["_id"]; !exists {
			for _, doc := range coll.Documents {
				coll.Indexes["_id"].AddToIndex(doc)
			}
		}
	} else {
		// Load from JSON format (legacy)
		docsPath := filepath.Join(collDir, "documents.json")
		var docs []*Document
		if err := sm.readJSON(docsPath, &docs); err != nil {
			// If file doesn't exist, it's ok (empty collection)
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to load documents: %w", err)
			}
		}

		// Restore documents
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
		// Skip the WAL directory
		if entry.Name() == "wal" {
			continue
		}

		if entry.IsDir() {
			db, err := sm.LoadDatabase(entry.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to load database '%s': %w", entry.Name(), err)
			}
			dm.Databases[db.Name] = db
		}
	}

	// Replay WAL to restore any operations not yet persisted
	if err := sm.WAL.Replay(dm, sm); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
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

// WAL Integration Methods

// LogInsert logs an insert operation to WAL
func (sm *StorageManager) LogInsert(dbName, collName string, doc *Document) error {
	docData, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	entry := &WALEntry{
		Database:   dbName,
		Collection: collName,
		Operation:  WALOpInsert,
		DocumentID: doc.ID,
		Data:       docData,
	}

	return sm.WAL.AppendEntry(entry)
}

// LogUpdate logs an update operation to WAL
func (sm *StorageManager) LogUpdate(dbName, collName string, doc *Document) error {
	docData, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	entry := &WALEntry{
		Database:   dbName,
		Collection: collName,
		Operation:  WALOpUpdate,
		DocumentID: doc.ID,
		Data:       docData,
	}

	return sm.WAL.AppendEntry(entry)
}

// LogDelete logs a delete operation to WAL
func (sm *StorageManager) LogDelete(dbName, collName, docID string) error {
	entry := &WALEntry{
		Database:   dbName,
		Collection: collName,
		Operation:  WALOpDelete,
		DocumentID: docID,
	}

	return sm.WAL.AppendEntry(entry)
}

// LogCreateDatabase logs a create database operation to WAL
func (sm *StorageManager) LogCreateDatabase(dbName string) error {
	entry := &WALEntry{
		Database:  dbName,
		Operation: WALOpCreateDatabase,
	}

	return sm.WAL.AppendEntry(entry)
}

// LogDeleteDatabase logs a delete database operation to WAL
func (sm *StorageManager) LogDeleteDatabase(dbName string) error {
	entry := &WALEntry{
		Database:  dbName,
		Operation: WALOpDeleteDatabase,
	}

	return sm.WAL.AppendEntry(entry)
}

// LogCreateCollection logs a create collection operation to WAL
func (sm *StorageManager) LogCreateCollection(dbName, collName string, schema *Schema) error {
	var schemaData []byte
	var err error
	if schema != nil {
		schemaData, err = json.Marshal(schema)
		if err != nil {
			return fmt.Errorf("failed to marshal schema: %w", err)
		}
	}

	entry := &WALEntry{
		Database:   dbName,
		Collection: collName,
		Operation:  WALOpCreateCollection,
		Data:       schemaData,
	}

	return sm.WAL.AppendEntry(entry)
}

// LogCreateIndex logs a create index operation to WAL
func (sm *StorageManager) LogCreateIndex(dbName, collName, indexName, fieldName string) error {
	indexData := map[string]string{
		"index_name": indexName,
		"field_name": fieldName,
	}
	data, err := json.Marshal(indexData)
	if err != nil {
		return fmt.Errorf("failed to marshal index data: %w", err)
	}

	entry := &WALEntry{
		Database:   dbName,
		Collection: collName,
		Operation:  WALOpCreateIndex,
		Data:       data,
	}

	return sm.WAL.AppendEntry(entry)
}

// Checkpoint creates a checkpoint in the WAL at the current offset
func (sm *StorageManager) Checkpoint() error {
	sm.WAL.mu.RLock()
	currentOffset := sm.WAL.currentOffset
	sm.WAL.mu.RUnlock()

	return sm.WAL.Checkpoint(currentOffset)
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
