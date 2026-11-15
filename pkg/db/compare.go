package db

import (
	"cmp"
	"encoding/json"
	"fmt"
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
//     numeric normalization (scalars are compared directly, see
//     filterValue);
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

// Operators. A QueryFilter's Operator string is compiled into one of these
// once per call (compileFilter); an unknown operator is an error.
type opCode uint8

const (
	opEq opCode = iota + 1
	opNe
	opGt
	opGte
	opLt
	opLte
	opIn
	opContains
	opHas
	opHasAny
	opHasAll
	opExists
	opStartsWith
)

// operatorNames lists the operators in the order the docs give them.
var operatorNames = []string{"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "has", "has_any", "has_all", "exists", "starts_with"}

// operatorCodes maps an operator name to its code.
var operatorCodes = map[string]opCode{
	"eq": opEq, "ne": opNe, "gt": opGt, "gte": opGte, "lt": opLt, "lte": opLte, "in": opIn,
	"contains": opContains, "has": opHas, "has_any": opHasAny, "has_all": opHasAll,
	"exists": opExists, "starts_with": opStartsWith,
}

// valueKind classifies a filter value for equality.
type valueKind uint8

const (
	kindNull valueKind = iota
	kindBool
	kindNumber
	kindString
	kindOther // objects, arrays, and every other type: deep equality after normalize
)

// filterValue is a filter value normalized once, so that matching a
// document compares scalars directly instead of through reflect.DeepEqual.
type filterValue struct {
	kind  valueKind
	b     bool
	n     float64
	s     string
	other any // normalize(v), for kindOther
}

func newFilterValue(v any) filterValue {
	if v == nil {
		return filterValue{kind: kindNull}
	}
	if f, ok := toFloat(v); ok {
		return filterValue{kind: kindNumber, n: f}
	}
	switch t := v.(type) {
	case string:
		return filterValue{kind: kindString, s: t}
	case bool:
		return filterValue{kind: kindBool, b: t}
	}
	return filterValue{kind: kindOther, other: normalize(v)}
}

// equals reports whether the document value v equals fv under R5 semantics:
// exactly valuesEqual(v, the filter value).
func (fv *filterValue) equals(v any) bool {
	switch fv.kind {
	case kindNull:
		return v == nil
	case kindBool:
		b, ok := v.(bool)
		return ok && b == fv.b
	case kindNumber:
		f, ok := toFloat(v)
		return ok && f == fv.n // NaN equals nothing, as with reflect.DeepEqual
	case kindString:
		s, ok := v.(string)
		return ok && s == fv.s
	}
	return reflect.DeepEqual(fv.other, normalize(v))
}

// compiledFilter is a QueryFilter compiled once per call: the operator as
// a code and the value normalized for it.
type compiledFilter struct {
	field string
	op    opCode
	value any // the filter value as given ($now resolved); the index plans encode it

	val    filterValue   // eq, ne, has
	list   []filterValue // in, has_any, has_all
	isList bool          // the value is an array (in, has_any, has_all)

	// Ranges (gt, gte, lt, lte): the value orders as a number (n) or a
	// string (s); otherwise the filter matches nothing.
	ordered bool
	num     bool
	n       float64
	s       string // also the lowered substring of contains and the prefix of starts_with
	isStr   bool   // contains: the value is a string

	exists bool // exists: want the field present (true) or missing (false)
}

// compileFilter compiles f. It fails for an unknown operator, for exists
// with a value that is not a boolean, and for starts_with with a value that
// is not a string. Values of the wrong type for the other operators match
// nothing, as before.
func compileFilter(f QueryFilter) (compiledFilter, error) {
	op, ok := operatorCodes[f.Operator]
	if !ok {
		return compiledFilter{}, fmt.Errorf("filter on field %q: unknown operator %q (want one of %s)",
			f.Field, f.Operator, strings.Join(operatorNames, " "))
	}
	cf := compiledFilter{field: f.Field, op: op, value: f.Value}
	switch op {
	case opEq, opNe, opHas:
		cf.val = newFilterValue(f.Value)
	case opIn, opHasAny, opHasAll:
		vals, ok := listValues(f.Value)
		cf.isList = ok
		cf.list = make([]filterValue, len(vals))
		for i, v := range vals {
			cf.list[i] = newFilterValue(v)
		}
	case opGt, opGte, opLt, opLte:
		if n, ok := toFloat(f.Value); ok {
			cf.ordered, cf.num, cf.n = !math.IsNaN(n), true, n
		} else if s, ok := f.Value.(string); ok {
			cf.ordered, cf.s = true, s
		}
	case opContains:
		if s, ok := f.Value.(string); ok {
			cf.isStr, cf.s = true, strings.ToLower(s)
		}
	case opExists:
		b, ok := f.Value.(bool)
		if !ok {
			return compiledFilter{}, fmt.Errorf("filter on field %q: exists takes a boolean, got %s", f.Field, describeValue(f.Value))
		}
		cf.exists = b
	case opStartsWith:
		s, ok := f.Value.(string)
		if !ok {
			return compiledFilter{}, fmt.Errorf("filter on field %q: starts_with takes a string, got %s", f.Field, describeValue(f.Value))
		}
		cf.s = s
	}
	return cf, nil
}

// describeValue names the JSON type of v for error messages.
func describeValue(v any) string {
	if v == nil {
		return "null"
	}
	if _, ok := toFloat(v); ok {
		return "a number"
	}
	switch v.(type) {
	case string:
		return "a string"
	case bool:
		return "a boolean"
	}
	if _, ok := listValues(v); ok {
		return "an array"
	}
	return "an object"
}

// compileFilters compiles every filter of fs (nil for none).
func compileFilters(fs []QueryFilter) ([]compiledFilter, error) {
	if len(fs) == 0 {
		return nil, nil
	}
	out := make([]compiledFilter, len(fs))
	for i, f := range fs {
		cf, err := compileFilter(f)
		if err != nil {
			return nil, err
		}
		out[i] = cf
	}
	return out, nil
}

// never reports whether f matches no document at all, whatever the data:
// a range whose value does not order, in or has_any without values or with
// a value that is not an array, has_all with a value that is not an array,
// contains with a value that is not a string, and eq on _id with a value
// that is not a string.
func (f *compiledFilter) never() bool {
	switch f.op {
	case opGt, opGte, opLt, opLte:
		return !f.ordered
	case opIn, opHasAny:
		return len(f.list) == 0
	case opHasAll:
		return !f.isList
	case opContains:
		return !f.isStr
	case opEq:
		return f.field == "_id" && f.val.kind != kindString
	}
	return false
}

// match reports whether doc satisfies f. A document that lacks the
// filtered field matches no operator except exists false. For "in", the
// value must be an array; the document matches when its value equals any
// element. The element operators (plan 004 §2.2) are:
//
//   - contains: a string field contains the value (a string),
//     case-insensitively; an array field has a string element that does;
//   - has: an array field has an element equal to the value;
//   - has_any: an array field has an element equal to some value of the
//     array value;
//   - has_all: every value of the array value equals some element of an
//     array field.
//
// exists true matches a document that has the field (null included),
// exists false one that does not. starts_with matches a string field that
// begins with the value (case-sensitive, byte-wise). eq, ne, and in compare
// arrays as whole values.
func (f *compiledFilter) match(doc *Document) bool {
	value, exists := doc.GetValue(f.field)
	if f.op == opExists {
		return exists == f.exists
	}
	if !exists {
		return false
	}
	switch f.op {
	case opEq:
		return f.val.equals(value)
	case opNe:
		return !f.val.equals(value)
	case opGt, opGte, opLt, opLte:
		c, ok := f.compareTo(value)
		if !ok {
			return false
		}
		switch f.op {
		case opGt:
			return c > 0
		case opGte:
			return c >= 0
		case opLt:
			return c < 0
		}
		return c <= 0
	case opIn:
		for i := range f.list {
			if f.list[i].equals(value) {
				return true
			}
		}
		return false
	case opContains:
		return f.isStr && matchContains(value, f.s)
	case opHas:
		elems, ok := listValues(value)
		return ok && containsEqual(elems, &f.val)
	case opHasAny, opHasAll:
		if !f.isList {
			return false
		}
		elems, ok := listValues(value)
		if !ok {
			return false
		}
		for i := range f.list {
			found := containsEqual(elems, &f.list[i])
			if f.op == opHasAny && found {
				return true
			}
			if f.op == opHasAll && !found {
				return false
			}
		}
		return f.op == opHasAll
	case opStartsWith:
		s, ok := value.(string)
		return ok && strings.HasPrefix(s, f.s)
	}
	return false
}

// compareTo orders the document value v against a range filter's value,
// as compare(v, value) does; ok is false when the pair has no ordering.
func (f *compiledFilter) compareTo(v any) (int, bool) {
	if !f.ordered {
		return 0, false
	}
	if f.num {
		n, ok := toFloat(v)
		if !ok || math.IsNaN(n) {
			return 0, false
		}
		return cmp.Compare(n, f.n), true
	}
	s, ok := v.(string)
	if !ok {
		return 0, false
	}
	return strings.Compare(s, f.s), true
}

// matchContains implements "contains": value is a string containing the
// lowered substring sub case-insensitively, or an array with such a string
// element. The empty string matches every string and array field.
func matchContains(value any, sub string) bool {
	if v, ok := value.(string); ok {
		return strings.Contains(strings.ToLower(v), sub)
	}
	elems, ok := listValues(value)
	if !ok {
		return false
	}
	if sub == "" {
		return true
	}
	for _, e := range elems {
		if v, ok := e.(string); ok && strings.Contains(strings.ToLower(v), sub) {
			return true
		}
	}
	return false
}

// containsEqual reports whether some element of elems equals v.
func containsEqual(elems []any, v *filterValue) bool {
	for _, e := range elems {
		if v.equals(e) {
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

// matchAll reports whether doc satisfies every filter.
func matchAll(doc *Document, filters []compiledFilter) bool {
	for i := range filters {
		if !filters[i].match(doc) {
			return false
		}
	}
	return true
}

// matchQuery reports whether doc matches every filter of filters and,
// when anyOf is not empty, every filter of at least one of its groups
// (plan 004 §2.1).
func matchQuery(doc *Document, filters []compiledFilter, anyOf [][]compiledFilter) bool {
	if !matchAll(doc, filters) {
		return false
	}
	if len(anyOf) == 0 {
		return true
	}
	for _, g := range anyOf {
		if matchAll(doc, g) {
			return true
		}
	}
	return false
}

// matchesFilter compiles f and matches doc against it; a filter that does
// not compile matches nothing. The engine compiles once per call instead.
func matchesFilter(doc *Document, f QueryFilter) bool {
	cf, err := compileFilter(f)
	return err == nil && cf.match(doc)
}

// matchesQuery is matchQuery for filters that are not compiled yet; a
// filter that does not compile matches nothing.
func matchesQuery(doc *Document, filters []QueryFilter, anyOf [][]QueryFilter) bool {
	cfs, err := compileFilters(filters)
	if err != nil {
		return false
	}
	groups := make([][]compiledFilter, len(anyOf))
	for i, g := range anyOf {
		if groups[i], err = compileFilters(g); err != nil {
			return false
		}
	}
	return matchQuery(doc, cfs, groups)
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
