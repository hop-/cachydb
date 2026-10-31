package db

import (
	"encoding/json"
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
