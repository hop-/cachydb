package db

import (
	"cmp"
	"encoding/json"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
)

// Query semantics (R5). Values are compared by type, never through their
// string form:
//
//   - every Go numeric kind and json.Number is compared as a float64;
//   - strings compare lexically (byte order) with strings;
//   - bools and nil support equality only;
//   - objects and arrays support equality through reflect.DeepEqual after
//     numeric normalization;
//   - values of different types are not comparable: gt/gte/lt/lte and eq
//     are false for them, ne is true.

// toFloat reports v as a float64 if it is a Go numeric value or a valid
// json.Number.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

// compare orders a and b. ok is false when the pair has no ordering: the
// types differ, either side is a bool, nil, object, or array, or a number
// is NaN. When ok is true the result is -1, 0, or +1.
func compare(a, b any) (int, bool) {
	if fa, ok := toFloat(a); ok {
		fb, ok := toFloat(b)
		if !ok || math.IsNaN(fa) || math.IsNaN(fb) {
			return 0, false
		}
		return cmp.Compare(fa, fb), true
	}
	if sa, ok := a.(string); ok {
		sb, ok := b.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(sa, sb), true
	}
	return 0, false
}

// valuesEqual reports whether a and b are equal under R5 semantics. It is
// the equality used by eq, ne, and in.
func valuesEqual(a, b any) bool {
	return reflect.DeepEqual(normalize(a), normalize(b))
}

// normalize rewrites v so that reflect.DeepEqual implements R5 equality:
// numbers become float64, string-keyed maps become map[string]any, and
// slices and arrays become []any, recursively. Other values are returned
// unchanged.
func normalize(v any) any {
	if v == nil {
		return nil
	}
	if f, ok := toFloat(v); ok {
		return f
	}
	switch t := v.(type) {
	case string, bool:
		return v
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, x := range t {
			m[k] = normalize(x)
		}
		return m
	case []any:
		s := make([]any, len(t))
		for i, x := range t {
			s[i] = normalize(x)
		}
		return s
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return v
		}
		m := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			m[iter.Key().String()] = normalize(iter.Value().Interface())
		}
		return m
	case reflect.Slice, reflect.Array:
		s := make([]any, rv.Len())
		for i := range s {
			s[i] = normalize(rv.Index(i).Interface())
		}
		return s
	}
	return v
}

// matchesFilter reports whether doc satisfies f. A document that lacks the
// filtered field matches no operator. Unknown operators match nothing. For
// "in", f.Value must be a slice or array; the document matches when its
// value equals any element. The element operators (plan 004 §2.2) are:
//
//   - contains: a string field contains f.Value (a string),
//     case-insensitively; an array field has a string element that does;
//   - has: an array field has an element equal to f.Value;
//   - has_any: an array field has an element equal to some value of the
//     array f.Value;
//   - has_all: every value of the array f.Value equals some element of
//     an array field.
//
// eq, ne, and in compare arrays as whole values.
func matchesFilter(doc *Document, f QueryFilter) bool {
	value, exists := doc.GetValue(f.Field)
	if !exists {
		return false
	}

	switch f.Operator {
	case "eq":
		return valuesEqual(value, f.Value)
	case "ne":
		return !valuesEqual(value, f.Value)
	case "gt":
		c, ok := compare(value, f.Value)
		return ok && c > 0
	case "gte":
		c, ok := compare(value, f.Value)
		return ok && c >= 0
	case "lt":
		c, ok := compare(value, f.Value)
		return ok && c < 0
	case "lte":
		c, ok := compare(value, f.Value)
		return ok && c <= 0
	case "in":
		vals, ok := listValues(f.Value)
		if !ok {
			return false
		}
		for _, v := range vals {
			if valuesEqual(value, v) {
				return true
			}
		}
		return false
	case "contains":
		return matchContains(value, f.Value)
	case "has":
		elems, ok := listValues(value)
		return ok && containsEqual(elems, f.Value)
	case "has_any":
		elems, ok := listValues(value)
		want, wok := listValues(f.Value)
		if !ok || !wok {
			return false
		}
		for _, w := range want {
			if containsEqual(elems, w) {
				return true
			}
		}
		return false
	case "has_all":
		elems, ok := listValues(value)
		want, wok := listValues(f.Value)
		if !ok || !wok {
			return false
		}
		for _, w := range want {
			if !containsEqual(elems, w) {
				return false
			}
		}
		return true
	}
	return false
}

// matchContains implements "contains": value is a string containing sub
// case-insensitively, or an array with such a string element. sub must be
// a string; the empty string matches every string and array field.
func matchContains(value, sub any) bool {
	s, ok := sub.(string)
	if !ok {
		return false
	}
	s = strings.ToLower(s)
	if v, ok := value.(string); ok {
		return strings.Contains(strings.ToLower(v), s)
	}
	elems, ok := listValues(value)
	if !ok {
		return false
	}
	if s == "" {
		return true
	}
	for _, e := range elems {
		if v, ok := e.(string); ok && strings.Contains(strings.ToLower(v), s) {
			return true
		}
	}
	return false
}

// containsEqual reports whether some element of elems equals v.
func containsEqual(elems []any, v any) bool {
	for _, e := range elems {
		if valuesEqual(e, v) {
			return true
		}
	}
	return false
}

// listValues returns the elements of v if it is a slice or array (but not
// a byte string), or false.
func listValues(v any) ([]any, bool) {
	switch t := v.(type) {
	case nil:
		return nil, false
	case []any:
		return t, true
	case []byte:
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

// matchesAll reports whether doc satisfies every filter.
func matchesAll(doc *Document, filters []QueryFilter) bool {
	for _, f := range filters {
		if !matchesFilter(doc, f) {
			return false
		}
	}
	return true
}

// matchesQuery reports whether doc matches every filter of filters and,
// when anyOf is not empty, every filter of at least one of its groups
// (plan 004 §2.1).
func matchesQuery(doc *Document, filters []QueryFilter, anyOf [][]QueryFilter) bool {
	if !matchesAll(doc, filters) {
		return false
	}
	if len(anyOf) == 0 {
		return true
	}
	for _, g := range anyOf {
		if matchesAll(doc, g) {
			return true
		}
	}
	return false
}

// $now values (plan 004 §1.1). In document data and in filter values, a
// value that is exactly the object {"$now": true} stands for the server's
// current time, as an RFC 3339 UTC string with nanoseconds. Any other
// object with a "$now" key is ordinary data.

// nowKey is the only key of a $now value.
const nowKey = "$now"

// isNowValue reports whether v is exactly {"$now": true}.
func isNowValue(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	b, ok := m[nowKey].(bool)
	return ok && b
}

// nowString formats now the way $now values are stored.
func nowString(now time.Time) string { return now.UTC().Format(time.RFC3339Nano) }

// ResolveNow returns v with every {"$now": true} value, at any depth in
// objects ([map[string]any]) and arrays ([]any), replaced by now formatted
// as an RFC 3339 UTC string with nanoseconds. v is not modified: the parts
// that change are copied, the rest is shared. The engine applies it to
// document data and filter values itself; callers use it to resolve
// values the same way.
func ResolveNow(v any, now time.Time) any {
	out, _ := resolveNow(v, nowString(now))
	return out
}

// resolveNow replaces $now values in v with ts and reports whether it
// replaced any. Unchanged subtrees are returned as they are.
func resolveNow(v any, ts string) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		if isNowValue(t) {
			return ts, true
		}
		var out map[string]any
		for k, x := range t {
			nx, changed := resolveNow(x, ts)
			if !changed {
				continue
			}
			if out == nil {
				out = maps.Clone(t)
			}
			out[k] = nx
		}
		if out == nil {
			return v, false
		}
		return out, true
	case []any:
		var out []any
		for i, x := range t {
			nx, changed := resolveNow(x, ts)
			if !changed {
				continue
			}
			if out == nil {
				out = slices.Clone(t)
			}
			out[i] = nx
		}
		if out == nil {
			return v, false
		}
		return out, true
	}
	return v, false
}

// resolveNowData returns data with its $now values replaced by ts (data
// itself when it has none).
func resolveNowData(data map[string]any, ts string) map[string]any {
	if data == nil {
		return nil
	}
	out, _ := resolveNow(data, ts)
	return out.(map[string]any)
}

// resolveNowFilters returns filters with $now filter values replaced by
// ts (filters itself when there are none).
func resolveNowFilters(filters []QueryFilter, ts string) []QueryFilter {
	out, _ := resolveNowFiltersChanged(filters, ts)
	return out
}

// resolveNowFiltersChanged is resolveNowFilters that also reports whether
// it replaced anything.
func resolveNowFiltersChanged(filters []QueryFilter, ts string) ([]QueryFilter, bool) {
	var out []QueryFilter
	for i, f := range filters {
		v, changed := resolveNow(f.Value, ts)
		if !changed {
			continue
		}
		if out == nil {
			out = slices.Clone(filters)
		}
		out[i].Value = v
	}
	if out == nil {
		return filters, false
	}
	return out, true
}

// resolveNowGroups returns groups with their $now filter values replaced
// by ts (groups itself when there are none).
func resolveNowGroups(groups [][]QueryFilter, ts string) [][]QueryFilter {
	var out [][]QueryFilter
	for i, g := range groups {
		r, changed := resolveNowFiltersChanged(g, ts)
		if !changed {
			continue
		}
		if out == nil {
			out = slices.Clone(groups)
		}
		out[i] = r
	}
	if out == nil {
		return groups
	}
	return out
}
