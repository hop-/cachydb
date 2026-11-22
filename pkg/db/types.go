package db

import (
	"encoding/json"
	"maps"
	"reflect"
	"strings"
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
	// Field is a field name or a nested path ("meta.author"); see
	// Document.GetPath for how it resolves.
	Field string `json:"field"`
	// Operator is one of "eq", "ne", "gt", "gte", "lt", "lte", "in",
	// "contains", "has", "has_any", "has_all", "exists" (Value is a
	// boolean), "starts_with" (Value is a string). Any other operator is an
	// error.
	Operator string `json:"operator"`
	Value    any    `json:"value"`
}

// Query represents a query. A document matches when it matches every
// filter of Filters and, if Any is not empty, every filter of at least one
// of its groups.
type Query struct {
	Filters []QueryFilter   `json:"filters"`
	Any     [][]QueryFilter `json:"any,omitempty"`    // OR of AND groups; empty = no constraint
	Fields  []string        `json:"fields,omitempty"` // projection: fields or nested paths to return, a path's value nested under it (_id always); empty = all
	Sort    []SortKey       `json:"sort,omitempty"`   // sort keys in order, then _id ascending; empty = _id ascending
	Limit   int             `json:"limit"`
	Skip    int             `json:"skip"`
}

// SortKey is one sort key of a Query.
type SortKey struct {
	Field string `json:"field"`           // a field name or a nested path (Document.GetPath)
	Order string `json:"order,omitempty"` // "asc" (default) or "desc"
}

// Condition is the "if" of a conditional write: filters, with the same
// shape and operators as Query.Filters, that the stored document must all
// match for the write to happen (plan 004 §1.2).
type Condition = []QueryFilter

// UpdateSpec describes an update for Collection.UpdateWith.
// The operators Inc, Push, AddToSet, and Pull apply after Set and Unset,
// each a map of top-level field to value (see update.go); a field may be
// named by at most one of Set, Unset, and the operators.
type UpdateSpec struct {
	Set    map[string]any // top-level fields to replace or add
	Unset  []string       // top-level fields to remove (after Set)
	If     Condition      // the stored version must match all of these
	Upsert bool           // create {_id: id} + Set (then the operators) if the document is missing

	Inc      map[string]any // add the number to a numeric field; a missing field starts at 0
	Push     map[string]any // append the value to an array field; a missing field becomes [value]
	AddToSet map[string]any // as Push, unless an equal element exists
	Pull     map[string]any // remove every element equal to the value; a missing field is left alone
}

// MarshalJSON customizes JSON marshaling for Document
func (d *Document) MarshalJSON() ([]byte, error) {
	combined := make(map[string]any)
	combined["_id"] = d.ID
	maps.Copy(combined, d.Data)
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

// GetValue safely extracts a value from a document by top-level field name
// ("_id" is the id). It does not follow dotted paths; see GetPath. Schemas
// use it: schema fields are top-level.
func (d *Document) GetValue(fieldName string) (any, bool) {
	if fieldName == "_id" {
		return d.ID, true
	}
	val, ok := d.Data[fieldName]
	return val, ok
}

// GetPath returns the value at a field path, as filters, sort keys,
// projection, Distinct, conditions, and indexes resolve field names
// (approach-index-and-query §6): "_id" is the id; a top-level key equal to
// the whole path wins (a literal "a.b" key keeps working); otherwise the
// path is split on "." and followed through nested objects. A path that
// crosses an array or a value that is not an object is missing (ok false).
func (d *Document) GetPath(path string) (any, bool) {
	if path == "_id" {
		return d.ID, true
	}
	if v, ok := d.Data[path]; ok {
		return v, true
	}
	head, rest, dotted := strings.Cut(path, ".")
	if !dotted {
		return nil, false
	}
	v, ok := d.Data[head]
	for ok {
		m, isObject := v.(map[string]any)
		if !isObject {
			return nil, false
		}
		head, rest, dotted = strings.Cut(rest, ".")
		v, ok = m[head]
		if !dotted {
			return v, ok
		}
	}
	return nil, false
}

// fieldPath is a field name split once for resolution (GetPath's rules).
type fieldPath struct {
	name  string
	parts []string // name split on "."; nil when name has no dot
}

func compilePath(name string) fieldPath {
	p := fieldPath{name: name}
	if name != "_id" && strings.Contains(name, ".") {
		p.parts = strings.Split(name, ".")
	}
	return p
}

func compilePaths(names []string) []fieldPath {
	if len(names) == 0 {
		return nil
	}
	out := make([]fieldPath, len(names))
	for i, n := range names {
		out[i] = compilePath(n)
	}
	return out
}

// get resolves p in d exactly as d.GetPath(p.name).
func (p *fieldPath) get(d *Document) (any, bool) {
	if p.name == "_id" {
		return d.ID, true
	}
	if v, ok := d.Data[p.name]; ok || p.parts == nil {
		return v, ok
	}
	m := d.Data
	last := len(p.parts) - 1
	for i, part := range p.parts {
		v, ok := m[part]
		if !ok || i == last {
			return v, ok
		}
		if m, ok = v.(map[string]any); !ok {
			return nil, false
		}
	}
	return nil, false
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
