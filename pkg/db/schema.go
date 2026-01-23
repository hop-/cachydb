package db

import (
	"fmt"
)

// ValidateDocument validates a document against a schema
func (s *Schema) ValidateDocument(doc *Document) error {
	if s == nil {
		return nil // No schema, no validation
	}

	// Check required fields
	for fieldName, field := range s.Fields {
		value, exists := doc.GetValue(fieldName)

		if field.Required && !exists {
			return fmt.Errorf("required field '%s' is missing", fieldName)
		}

		if exists {
			if !ValidateType(value, field.Type) {
				return fmt.Errorf("field '%s' has invalid type, expected %s", fieldName, field.Type)
			}
		}
	}

	return nil
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
