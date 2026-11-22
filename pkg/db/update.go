package db

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
)

// Update operators (plan 013, approach-agent-interface §4). Besides Set
// and Unset, an UpdateSpec may change fields relative to their stored
// value: Inc adds to a number, Push appends to an array, AddToSet appends
// unless an equal element exists, and Pull removes every equal element.
// They apply after Set and Unset, to top-level fields only, inside the
// write's build callback (under the document lock), so they are atomic
// and combine with If and Upsert. A field may be named by at most one of
// Set, Unset, and the operators. Element equality is the engine's eq
// (numbers compare as float64).

// opFields returns the operator maps of u with their names, in the order
// they apply.
func (u *UpdateSpec) opFields() []struct {
	name string
	m    map[string]any
} {
	return []struct {
		name string
		m    map[string]any
	}{{"inc", u.Inc}, {"push", u.Push}, {"add_to_set", u.AddToSet}, {"pull", u.Pull}}
}

// hasOps reports whether u uses an update operator.
func (u *UpdateSpec) hasOps() bool {
	return len(u.Inc) > 0 || len(u.Push) > 0 || len(u.AddToSet) > 0 || len(u.Pull) > 0
}

// checkUpdateOps checks the operators of u before any document is read: no
// operator names "_id" or a field that Set, Unset, or another operator
// also names, and every Inc value is a finite number. The error wraps
// ErrInvalidUpdate. what names the call ("updating document "x"").
func checkUpdateOps(u *UpdateSpec, what string) error {
	if !u.hasOps() {
		return nil
	}
	owner := make(map[string]string, len(u.Set)+len(u.Unset))
	for f := range u.Set {
		owner[f] = "updates"
	}
	for _, f := range u.Unset {
		owner[f] = "unset"
	}
	for _, op := range u.opFields() {
		for _, f := range slices.Sorted(maps.Keys(op.m)) { // deterministic error text
			if f == "_id" {
				return fmt.Errorf("%w: %s: %s: field \"_id\" cannot be changed", ErrInvalidUpdate, what, op.name)
			}
			if prev, ok := owner[f]; ok {
				return fmt.Errorf("%w: %s: field %q in both %s and %s; name a field in at most one of updates, unset, inc, push, add_to_set, and pull", ErrInvalidUpdate, what, f, prev, op.name)
			}
			owner[f] = op.name
		}
	}
	for _, f := range slices.Sorted(maps.Keys(u.Inc)) {
		n, ok := toFloat(u.Inc[f])
		if !ok || math.IsNaN(n) || math.IsInf(n, 0) {
			return fmt.Errorf("%w: %s: inc: the value for field %q must be a number, got %s", ErrInvalidUpdate, what, f, describeValue(u.Inc[f]))
		}
	}
	return nil
}

// resolveNowUpdate returns u with the $now values of Set, Push, AddToSet,
// and Pull resolved to ts. The maps of u are not modified.
func resolveNowUpdate(u UpdateSpec, ts string) UpdateSpec {
	u.Set = resolveNowData(u.Set, ts)
	u.Push = resolveNowData(u.Push, ts)
	u.AddToSet = resolveNowData(u.AddToSet, ts)
	u.Pull = resolveNowData(u.Pull, ts)
	return u
}

// applyUpdate returns the data of cur (nil: a document being upserted)
// after u (with $now resolved): Set, then Unset, then the operators. It
// builds a new top-level map and new arrays: cur is shared (memtable) and
// must not change. Nested values of cur and u are only read; the write path
// re-encodes the after-image into fresh values. An operator applied to a
// field of the wrong type fails with ErrInvalidUpdate. id names the
// document in errors.
func applyUpdate(id string, cur *Document, u *UpdateSpec) (map[string]any, error) {
	var data map[string]any
	if cur != nil {
		data = make(map[string]any, len(cur.Data)+len(u.Set))
		maps.Copy(data, cur.Data)
	} else {
		data = make(map[string]any, len(u.Set))
	}
	maps.Copy(data, u.Set)
	for _, k := range u.Unset {
		delete(data, k)
	}
	if !u.hasOps() {
		return data, nil
	}
	fail := func(op, field string, v any, want string) error {
		return fmt.Errorf("%w: updating document %q: %s: field %q is %s, not %s", ErrInvalidUpdate, id, op, field, describeValue(v), want)
	}
	for _, f := range slices.Sorted(maps.Keys(u.Inc)) { // sorted: the same error every time
		add, _ := toFloat(u.Inc[f]) // checked by checkUpdateOps
		v, ok := data[f]
		if !ok {
			data[f] = add
			continue
		}
		n, isNum := toFloat(v)
		if !isNum {
			return nil, fail("inc", f, v, "a number")
		}
		sum := n + add
		if math.IsInf(sum, 0) || math.IsNaN(sum) {
			return nil, fmt.Errorf("%w: updating document %q: inc: field %q would overflow (%v + %v)", ErrInvalidUpdate, id, f, n, add)
		}
		data[f] = sum
	}
	for _, f := range slices.Sorted(maps.Keys(u.Push)) {
		el := u.Push[f]
		arr, err := arrayField(data, f)
		if err != nil {
			return nil, fail("push", f, data[f], "an array")
		}
		data[f] = append(slices.Clip(arr), el)
	}
	for _, f := range slices.Sorted(maps.Keys(u.AddToSet)) {
		el := u.AddToSet[f]
		arr, err := arrayField(data, f)
		if err != nil {
			return nil, fail("add_to_set", f, data[f], "an array")
		}
		if !slices.ContainsFunc(arr, func(x any) bool { return valuesEqual(x, el) }) {
			arr = append(slices.Clip(arr), el)
		}
		data[f] = arr
	}
	for _, f := range slices.Sorted(maps.Keys(u.Pull)) {
		el := u.Pull[f]
		v, ok := data[f]
		if !ok {
			continue // pull on a missing field changes nothing
		}
		arr, err := arrayField(data, f)
		if err != nil {
			return nil, fail("pull", f, v, "an array")
		}
		data[f] = slices.DeleteFunc(slices.Clone(arr), func(x any) bool { return valuesEqual(x, el) })
	}
	return data, nil
}

// errNotArray is returned by arrayField for a field that is not an array.
var errNotArray = errors.New("not an array")

// arrayField returns the array value of field f of data, an empty array
// if f is missing, and errNotArray if it holds anything else. The result
// may share its backing array with data[f]: callers clip or clone it
// before changing it.
func arrayField(data map[string]any, f string) ([]any, error) {
	v, ok := data[f]
	if !ok {
		return []any{}, nil
	}
	if arr, ok := listValues(v); ok {
		return arr, nil
	}
	return nil, errNotArray
}
