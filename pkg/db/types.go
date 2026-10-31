package db

import (
	"encoding/json"
	"reflect"
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

// Clone returns a deep copy of the document: nested maps, slices, and
// arrays are copied recursively, so mutating the clone never changes the
// original. Pointers, structs, and other values are copied shallowly.
// Clone of a nil document is nil.
func (d *Document) Clone() *Document {
	if d == nil {
		return nil
	}
	clone := &Document{
		ID:   d.ID,
		Data: make(map[string]any, len(d.Data)),
	}
	for k, v := range d.Data {
		clone.Data[k] = deepCopy(v)
	}
	return clone
}

// deepCopy returns a copy of v that shares no map, slice, or array storage
// with it. The common JSON shapes take a fast path; other map, slice, and
// array types are copied through reflection with their types preserved.
func deepCopy(v any) any {
	switch t := v.(type) {
	case nil, string, bool, float64, json.Number:
		return v
	case map[string]any:
		if t == nil {
			return t
		}
		m := make(map[string]any, len(t))
		for k, x := range t {
			m[k] = deepCopy(x)
		}
		return m
	case []any:
		if t == nil {
			return t
		}
		s := make([]any, len(t))
		for i, x := range t {
			s[i] = deepCopy(x)
		}
		return s
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		return deepCopyValue(rv).Interface()
	}
	return v
}

// deepCopyValue is deepCopy for reflect values.
func deepCopyValue(rv reflect.Value) reflect.Value {
	switch rv.Kind() {
	case reflect.Interface:
		if rv.IsNil() {
			return rv
		}
		out := reflect.New(rv.Type()).Elem()
		out.Set(deepCopyValue(rv.Elem()))
		return out
	case reflect.Map:
		if rv.IsNil() {
			return rv
		}
		out := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), deepCopyValue(iter.Value()))
		}
		return out
	case reflect.Slice:
		if rv.IsNil() {
			return rv
		}
		out := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := range rv.Len() {
			out.Index(i).Set(deepCopyValue(rv.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(rv.Type()).Elem()
		for i := range rv.Len() {
			out.Index(i).Set(deepCopyValue(rv.Index(i)))
		}
		return out
	}
	return rv
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
