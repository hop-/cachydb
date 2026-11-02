package db

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// SchemaError reports every way a document fails its collection's schema
// (plan 004 §3.1). It wraps ErrSchemaValidation, so errors.Is(err,
// ErrSchemaValidation) holds; errors.As gives the individual violations.
type SchemaError struct {
	// Violations lists one entry per violation: first the missing
	// required fields, then the fields with a wrong type, each group in
	// field-name order. Entries read "required field missing: a" and
	// "wrong type: c (expected string, got number)".
	Violations []string

	missing []string // missing required field names
	wrong   []string // "c (expected string, got number)"
}

// Error returns "schema validation failed: required field(s) missing: a,
// b; wrong type: c (expected string, got number)".
func (e *SchemaError) Error() string {
	var parts []string
	if len(e.missing) > 0 || len(e.wrong) > 0 {
		if len(e.missing) > 0 {
			parts = append(parts, "required field(s) missing: "+strings.Join(e.missing, ", "))
		}
		if len(e.wrong) > 0 {
			parts = append(parts, "wrong type: "+strings.Join(e.wrong, ", "))
		}
	} else {
		parts = e.Violations
	}
	if len(parts) == 0 {
		return ErrSchemaValidation.Error()
	}
	return ErrSchemaValidation.Error() + ": " + strings.Join(parts, "; ")
}

// Unwrap returns ErrSchemaValidation.
func (e *SchemaError) Unwrap() error { return ErrSchemaValidation }

// ValidateDocument checks doc against the schema and returns a
// *SchemaError listing every violation, or nil. A nil schema accepts
// everything.
func (s *Schema) ValidateDocument(doc *Document) error {
	if s == nil {
		return nil // No schema, no validation
	}
	var se SchemaError
	names := slices.Sorted(maps.Keys(s.Fields))
	for _, name := range names {
		if _, exists := doc.GetValue(name); !exists && s.Fields[name].Required {
			se.missing = append(se.missing, name)
			se.Violations = append(se.Violations, "required field missing: "+name)
		}
	}
	for _, name := range names {
		field := s.Fields[name]
		value, exists := doc.GetValue(name)
		if exists && !ValidateType(value, field.Type) {
			w := fmt.Sprintf("%s (expected %s, got %s)", name, field.Type, jsonTypeName(value))
			se.wrong = append(se.wrong, w)
			se.Violations = append(se.Violations, "wrong type: "+w)
		}
	}
	if len(se.Violations) == 0 {
		return nil
	}
	return &se
}

// jsonTypeName names the JSON type of v for error messages.
func jsonTypeName(v any) string {
	if v == nil {
		return "null"
	}
	if _, ok := toFloat(v); ok {
		return "number"
	}
	switch v.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	}
	return fmt.Sprintf("%T", v)
}

// ValidateSchema validates the schema structure itself
func (s *Schema) Validate() error {
	if s == nil {
		return nil
	}

	if len(s.Fields) == 0 {
		return fmt.Errorf("schema must have at least one field")
	}

	for fieldName, field := range s.Fields {
		if fieldName == "" {
			return fmt.Errorf("field name cannot be empty")
		}

		if fieldName == "_id" {
			return fmt.Errorf("field name '_id' is reserved")
		}

		switch field.Type {
		case TypeString, TypeNumber, TypeBoolean, TypeObject, TypeArray, TypeDate:
			// Valid types
		default:
			return fmt.Errorf("invalid field type '%s' for field '%s'", field.Type, fieldName)
		}
	}

	return nil
}
