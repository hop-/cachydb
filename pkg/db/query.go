package db

import (
	"fmt"
	"strings"
)

// Legacy filter helpers. Task 08 replaces them with the type-aware R5
// semantics (compare, valuesEqual, matchesFilter); the engine wires query
// execution into Collection.Find in task 09.

// matchesAllFilters checks if a document matches all filters
func matchesAllFilters(doc *Document, filters []QueryFilter) bool {
	for _, filter := range filters {
		if !matchesFilter(doc, filter) {
			return false
		}
	}
	return true
}

// matchesFilter checks if a document matches a single filter
func matchesFilter(doc *Document, filter QueryFilter) bool {
	value, exists := doc.GetValue(filter.Field)
	if !exists {
		return false
	}

	switch filter.Operator {
	case "eq":
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", filter.Value)
	case "ne":
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", filter.Value)
	case "gt":
		return compareValues(value, filter.Value) > 0
	case "gte":
		return compareValues(value, filter.Value) >= 0
	case "lt":
		return compareValues(value, filter.Value) < 0
	case "lte":
		return compareValues(value, filter.Value) <= 0
	case "in":
		// Check if value is in the filter.Value array
		if arr, ok := filter.Value.([]any); ok {
			valueStr := fmt.Sprintf("%v", value)
			for _, item := range arr {
				if fmt.Sprintf("%v", item) == valueStr {
					return true
				}
			}
		}
		return false
	}

	return false
}

// compareValues compares two values (simple numeric/string comparison)
func compareValues(a, b any) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return strings.Compare(aStr, bStr)
}
