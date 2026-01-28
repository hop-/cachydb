package db

import (
	"encoding/json"
	"sync"
	"time"
)

// Document represents a document in the database
type Document struct {
	ID   string         `json:"_id"`
	Data map[string]any `json:"data"`
}

// FieldType represents the type of a field in the schema
type FieldType string

// FieldTypes
const (
	TypeString  FieldType = "string"
	TypeNumber  FieldType = "number"
	TypeBoolean FieldType = "boolean"
	TypeObject  FieldType = "object"
	TypeArray   FieldType = "array"
	TypeDate    FieldType = "date"
)

// Field represents a field definition in a schema
type Field struct {
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
}

// Schema represents a collection schema
type Schema struct {
	Fields map[string]Field `json:"fields"`
}

// Index represents an index on a collection
type Index struct {
	Name      string            `json:"name"`
	FieldName string            `json:"field_name"`
	Data      map[string]string `json:"-"` // maps field value to document ID
	mu        sync.RWMutex
}

// Collection represents a collection of documents
type Collection struct {
	Name      string               `json:"name"`
	Schema    *Schema              `json:"schema,omitempty"`
	Documents map[string]*Document `json:"-"` // maps document ID to document
	Indexes   map[string]*Index    `json:"indexes"`
	mu        sync.RWMutex
}

// Database represents the database
type Database struct {
	Name        string                 `json:"name"`
	Collections map[string]*Collection `json:"collections"`
	mu          sync.RWMutex
}

// DatabaseManager manages multiple databases
type DatabaseManager struct {
	Databases map[string]*Database `json:"databases"`
	mu        sync.RWMutex
}

// QueryFilter represents a query filter
type QueryFilter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"` // "eq", "ne", "gt", "lt", "gte", "lte", "in"
	Value    any    `json:"value"`
}

// Query represents a query
type Query struct {
	Filters []QueryFilter `json:"filters"`
	Limit   int           `json:"limit"`
	Skip    int           `json:"skip"`
}

// MarshalJSON customizes JSON marshaling for Document
func (d *Document) MarshalJSON() ([]byte, error) {
	combined := make(map[string]any)
	combined["_id"] = d.ID
	for k, v := range d.Data {
		combined[k] = v
	}
	return json.Marshal(combined)
}

// UnmarshalJSON customizes JSON unmarshaling for Document
func (d *Document) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if id, ok := raw["_id"].(string); ok {
		d.ID = id
		delete(raw, "_id")
	}

	d.Data = raw
	return nil
}

// NewIndex creates a new index
func NewIndex(name, fieldName string) *Index {
	return &Index{
		Name:      name,
		FieldName: fieldName,
		Data:      make(map[string]string),
	}
}

// NewCollection creates a new collection
func NewCollection(name string, schema *Schema) *Collection {
	coll := &Collection{
		Name:      name,
		Schema:    schema,
		Documents: make(map[string]*Document),
		Indexes:   make(map[string]*Index),
	}

	// Create automatic ID index
	coll.Indexes["_id"] = NewIndex("_id", "_id")

	return coll
}

// NewDatabase creates a new database
func NewDatabase(name string) *Database {
	return &Database{
		Name:        name,
		Collections: make(map[string]*Collection),
	}
}

// NewDatabaseManager creates a new database manager
func NewDatabaseManager() *DatabaseManager {
	return &DatabaseManager{
		Databases: make(map[string]*Database),
	}
}

// GetDatabase gets a database by name, returns nil if not found
func (dm *DatabaseManager) GetDatabase(name string) *Database {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.Databases[name]
}

// CreateDatabase creates a new database or returns existing one
func (dm *DatabaseManager) CreateDatabase(name string) *Database {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if db, exists := dm.Databases[name]; exists {
		return db
	}

	db := NewDatabase(name)
	dm.Databases[name] = db
	return db
}

// ListDatabases returns a list of all database names
func (dm *DatabaseManager) ListDatabases() []string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	names := make([]string, 0, len(dm.Databases))
	for name := range dm.Databases {
		names = append(names, name)
	}
	return names
}

// DeleteDatabase removes a database
func (dm *DatabaseManager) DeleteDatabase(name string) bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if _, exists := dm.Databases[name]; exists {
		delete(dm.Databases, name)
		return true
	}
	return false
}

// GetValue safely extracts a value from a document by field name
func (d *Document) GetValue(fieldName string) (any, bool) {
	if fieldName == "_id" {
		return d.ID, true
	}
	val, ok := d.Data[fieldName]
	return val, ok
}

// Clone creates a deep copy of the document
func (d *Document) Clone() *Document {
	clone := &Document{
		ID:   d.ID,
		Data: make(map[string]any),
	}
	for k, v := range d.Data {
		clone.Data[k] = v
	}
	return clone
}

// ValidateType checks if a value matches the expected field type
func ValidateType(value any, fieldType FieldType) bool {
	switch fieldType {
	case TypeString:
		_, ok := value.(string)
		return ok
	case TypeNumber:
		switch value.(type) {
		case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		}
		return false
	case TypeBoolean:
		_, ok := value.(bool)
		return ok
	case TypeObject:
		_, ok := value.(map[string]any)
		return ok
	case TypeArray:
		switch value.(type) {
		case []any, []string, []int, []float64:
			return true
		}
		return false
	case TypeDate:
		switch value.(type) {
		case string, time.Time:
			return true
		}
		return false
	}
	return false
}
