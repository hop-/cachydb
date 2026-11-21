package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Model-based randomized test (plan 005 §4).
//
// A seeded PRNG drives random operations against the engine and against a
// plain in-memory reference model whose query semantics are implemented
// naively and independently of the engine's: every operator, OR groups,
// the total sort order, projection, count and distinct, and the write
// semantics (if, unset, upsert, replace, delete, schemas, index metadata,
// unique indexes). Fields are also addressed by nested paths ("o.x"), and
// documents sometimes hold a literal top-level "o.x" key, which wins over
// the path. Queries include ones shaped for each query plan (an index-served
// filter, an OR of index-served filters, a sort on an indexed field with a
// limit), and every state check also compares the index file's entries
// with the documents (checkIndexEntries).
// The key and value spaces are small so collisions and edge cases are
// frequent. Reopens, simulated crashes (crashAt + abandon), flushes and
// compactions are interleaved; tiny data segments make flushes seal and
// compactions merge. After every read the engine's result must equal the
// model's exactly; after every reopen the whole state must.
//
// Knobs: CACHYDB_MODEL_SEEDS (default 2) seeds of CACHYDB_MODEL_OPS
// (default 300) operations each; CACHYDB_MODEL_SEED=<n> runs that one
// seed (to reproduce a failure, with the same CACHYDB_MODEL_OPS).

const (
	modelDB          = "m"
	modelDefaultOps  = 300
	modelDefaultSeed = 2
)

var (
	modelColls  = []string{"c1", "c2"}
	modelFields = []string{"a", "b", "t", "o", "o.x"} // "o.x": a literal top-level key that hides the path o.x
	modelIndex  = []string{"i1", "i2", "i3"}
	modelOps    = []string{"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "has", "has_any", "has_all", "exists", "starts_with"}
	// modelQueryFields are the fields queries, sorts, projections, distinct,
	// and indexes use: the top-level fields, the nested path o.x, _id, and a
	// field no document has.
	modelQueryFields = []string{"a", "b", "t", "o", "o.x", "_id", "missing"}
	modelIndexFields = []string{"a", "b", "t", "o", "o.x"}
	// modelSchemaFields are the fields schemas name (top-level only).
	modelSchemaFields = []string{"a", "b", "t", "o"}
)

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q: want a positive integer", name, s)
	}
	return n
}

func TestModel(t *testing.T) {
	ops := envInt(t, "CACHYDB_MODEL_OPS", modelDefaultOps)
	var seeds []uint64
	if s := os.Getenv("CACHYDB_MODEL_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("CACHYDB_MODEL_SEED=%q: %v", s, err)
		}
		seeds = []uint64{seed}
	} else {
		base := uint64(time.Now().UnixNano())
		for i := range envInt(t, "CACHYDB_MODEL_SEEDS", modelDefaultSeed) {
			seeds = append(seeds, base+uint64(i)*7919)
		}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Logf("model run: seed %d, %d operations (reproduce with CACHYDB_MODEL_SEED=%d CACHYDB_MODEL_OPS=%d)", seed, ops, seed, ops)
			r := newModelRun(t, seed)
			for r.step = 0; r.step < ops; r.step++ {
				r.randomOp()
				if t.Failed() {
					return
				}
			}
			r.reopen("final reopen")
			t.Logf("operations: %v", r.stats)
		})
	}
}

// ---- the reference model ----

type mColl struct {
	schema  *Schema
	indexes map[string]string         // name -> field
	unique  map[string]bool           // name -> the index is unique
	docs    map[string]map[string]any // id -> data
}

type mState map[string]*mColl // collection -> state

func (s mState) clone() mState {
	out := make(mState, len(s))
	for name, c := range s {
		docs := make(map[string]map[string]any, len(c.docs))
		for id, d := range c.docs {
			docs[id] = maps.Clone(d) // values are never mutated in place
		}
		out[name] = &mColl{schema: cloneSchema(c.schema), indexes: maps.Clone(c.indexes), unique: maps.Clone(c.unique), docs: docs}
	}
	return out
}

// dump renders s deterministically for comparisons.
func (s mState) dump() string {
	var b strings.Builder
	for _, name := range slices.Sorted(maps.Keys(s)) {
		c := s[name]
		schema, _ := json.Marshal(c.schema) // plain data: cannot fail
		fmt.Fprintf(&b, "%s schema=%s\n", name, schema)
		for _, ix := range slices.Sorted(maps.Keys(c.indexes)) {
			fmt.Fprintf(&b, "%s index %s=%s unique=%v\n", name, ix, c.indexes[ix], c.unique[ix])
		}
		for _, id := range slices.Sorted(maps.Keys(c.docs)) {
			data, _ := json.Marshal(c.docs[id]) // plain data: cannot fail
			fmt.Fprintf(&b, "%s doc %s %s\n", name, id, data)
		}
	}
	return b.String()
}

// mValue returns the value of field in a document (id, data): _id, a
// top-level key equal to the whole name, or else the dotted path through
// objects (an array or any other value on the way means missing).
func mValue(id string, data map[string]any, field string) (any, bool) {
	if field == "_id" {
		return id, true
	}
	if v, ok := data[field]; ok {
		return v, true
	}
	parts := strings.Split(field, ".")
	if len(parts) == 1 {
		return nil, false
	}
	var cur any = data
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// mProject returns data limited to fields: a top-level key equal to the
// field name as it is, otherwise the value of the path nested under the
// path (merged into an object already there, skipped under a non-object).
func mProject(id string, data map[string]any, fields []string) map[string]any {
	p := map[string]any{}
	for _, f := range fields {
		if f == "_id" {
			continue
		}
		if v, ok := data[f]; ok {
			p[f] = v
			continue
		}
		v, ok := mValue(id, data, f)
		if !ok {
			continue
		}
		parts := strings.Split(f, ".")
		m := p
		placed := true
		for _, part := range parts[:len(parts)-1] {
			next, ok := m[part]
			if !ok {
				child := map[string]any{}
				m[part] = child
				m = child
				continue
			}
			child, isObj := next.(map[string]any)
			if !isObj {
				placed = false
				break
			}
			// Copy before extending: values in the model are shared.
			child = maps.Clone(child)
			m[part] = child
			m = child
		}
		if placed {
			m[parts[len(parts)-1]] = v
		}
	}
	return p
}

// mConstrained reports whether a unique index constrains v: null, a bool,
// a number, or a string.
func mConstrained(v any) bool {
	switch v.(type) {
	case nil, bool, float64, string:
		return true
	}
	return false
}

// mUniqueViolation reports whether storing data under id would give a
// unique index's value to two documents.
func (c *mColl) mUniqueViolation(id string, data map[string]any) bool {
	for name, field := range c.indexes {
		if !c.unique[name] {
			continue
		}
		v, ok := mValue(id, data, field)
		if !ok || !mConstrained(v) {
			continue
		}
		for other, od := range c.docs {
			if other == id {
				continue
			}
			if ov, ok := mValue(other, od, field); ok && mEqual(ov, v) {
				return true
			}
		}
	}
	return false
}

// mHasDuplicates reports whether two documents share a constrained value of
// field (a unique index on it cannot be created).
func (c *mColl) mHasDuplicates(field string) bool {
	var seen []any
	for id, data := range c.docs {
		v, ok := mValue(id, data, field)
		if !ok || !mConstrained(v) {
			continue
		}
		for _, s := range seen {
			if mEqual(s, v) {
				return true
			}
		}
		seen = append(seen, v)
	}
	return false
}

// mEqual is equality of decoded JSON values (float64, string, bool, nil,
// []any, map[string]any).
func mEqual(a, b any) bool { return reflect.DeepEqual(a, b) }

// mOrder orders two numbers or two strings.
func mOrder(a, b any) (int, bool) {
	switch x := a.(type) {
	case float64:
		if y, ok := b.(float64); ok {
			switch {
			case x < y:
				return -1, true
			case x > y:
				return 1, true
			}
			return 0, true
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), true
		}
	}
	return 0, false
}

func mMatch(id string, data map[string]any, f QueryFilter) bool {
	v, ok := mValue(id, data, f.Field)
	switch f.Operator {
	case "exists":
		return ok == f.Value.(bool)
	case "starts_with":
		s, isStr := v.(string)
		return ok && isStr && strings.HasPrefix(s, f.Value.(string))
	}
	if !ok {
		return false
	}
	arr, isArr := v.([]any)
	want, wantArr := f.Value.([]any)
	anyEqual := func(elems []any, x any) bool {
		for _, e := range elems {
			if mEqual(e, x) {
				return true
			}
		}
		return false
	}
	switch f.Operator {
	case "eq":
		return mEqual(v, f.Value)
	case "ne":
		return !mEqual(v, f.Value)
	case "gt", "gte", "lt", "lte":
		c, ok := mOrder(v, f.Value)
		if !ok {
			return false
		}
		return map[string]bool{"gt": c > 0, "gte": c >= 0, "lt": c < 0, "lte": c <= 0}[f.Operator]
	case "in":
		return wantArr && anyEqual(want, v)
	case "contains":
		sub, ok := f.Value.(string)
		if !ok {
			return false
		}
		sub = strings.ToLower(sub)
		if s, ok := v.(string); ok {
			return strings.Contains(strings.ToLower(s), sub)
		}
		if !isArr {
			return false
		}
		if sub == "" {
			return true
		}
		for _, e := range arr {
			if s, ok := e.(string); ok && strings.Contains(strings.ToLower(s), sub) {
				return true
			}
		}
		return false
	case "has":
		return isArr && anyEqual(arr, f.Value)
	case "has_any":
		if !isArr || !wantArr {
			return false
		}
		for _, w := range want {
			if anyEqual(arr, w) {
				return true
			}
		}
		return false
	case "has_all":
		if !isArr || !wantArr {
			return false
		}
		for _, w := range want {
			if !anyEqual(arr, w) {
				return false
			}
		}
		return true
	}
	return false
}

func mMatchAll(id string, data map[string]any, fs []QueryFilter) bool {
	for _, f := range fs {
		if !mMatch(id, data, f) {
			return false
		}
	}
	return true
}

func mMatchQuery(id string, data map[string]any, q *Query) bool {
	if !mMatchAll(id, data, q.Filters) {
		return false
	}
	if len(q.Any) == 0 {
		return true
	}
	for _, g := range q.Any {
		if mMatchAll(id, data, g) {
			return true
		}
	}
	return false
}

// mRank and mSortCmp implement the documented total sort order: missing
// < null < false < true < numbers < strings < arrays < objects; arrays and
// objects by their JSON with sorted keys.
func mRank(v any, ok bool) int {
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case nil:
		return 1
	case bool:
		if x {
			return 3
		}
		return 2
	case float64:
		return 4
	case string:
		return 5
	case []any:
		return 6
	}
	return 7
}

func mSortCmp(a any, aok bool, b any, bok bool) int {
	ra, rb := mRank(a, aok), mRank(b, bok)
	if ra != rb {
		return ra - rb
	}
	switch ra {
	case 4, 5:
		c, _ := mOrder(a, b)
		return c
	case 6, 7:
		ja, _ := json.Marshal(a) // plain data: cannot fail
		jb, _ := json.Marshal(b)
		return strings.Compare(string(ja), string(jb))
	}
	return 0
}

// mFind evaluates q on c: ids in result order and the projected documents.
func (c *mColl) find(q *Query) []*Document {
	var ids []string
	for id, data := range c.docs {
		if mMatchQuery(id, data, q) {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(x, y string) int {
		for _, k := range q.Sort {
			vx, okx := mValue(x, c.docs[x], k.Field)
			vy, oky := mValue(y, c.docs[y], k.Field)
			r := mSortCmp(vx, okx, vy, oky)
			if k.Order == "desc" {
				r = -r
			}
			if r != 0 {
				return r
			}
		}
		return strings.Compare(x, y)
	})
	if q.Skip >= len(ids) {
		ids = nil
	} else {
		ids = ids[q.Skip:]
	}
	if q.Limit > 0 && len(ids) > q.Limit {
		ids = ids[:q.Limit]
	}
	out := make([]*Document, 0, len(ids))
	for _, id := range ids {
		data := c.docs[id]
		if len(q.Fields) > 0 {
			data = mProject(id, data, q.Fields)
		}
		out = append(out, &Document{ID: id, Data: maps.Clone(data)})
	}
	return out
}

func (c *mColl) distinct(field string, q *Query) []any {
	var vals []any
	add := func(v any) {
		for _, have := range vals {
			if mEqual(have, v) {
				return
			}
		}
		vals = append(vals, v)
	}
	for id, data := range c.docs {
		if !mMatchQuery(id, data, q) {
			continue
		}
		v, ok := mValue(id, data, field)
		if !ok {
			continue
		}
		if arr, isArr := v.([]any); isArr {
			for _, e := range arr {
				add(e)
			}
		} else {
			add(v)
		}
	}
	slices.SortFunc(vals, func(a, b any) int { return mSortCmp(a, true, b, true) })
	if vals == nil {
		vals = []any{}
	}
	return vals
}

// mValid reports whether data satisfies the schema s.
func mValid(s *Schema, data map[string]any) bool {
	if s == nil {
		return true
	}
	for name, f := range s.Fields {
		v, ok := data[name]
		if !ok {
			if f.Required {
				return false
			}
			continue
		}
		okType := false
		switch f.Type {
		case TypeString, TypeDate:
			_, okType = v.(string)
		case TypeNumber:
			_, okType = v.(float64)
		case TypeBoolean:
			_, okType = v.(bool)
		case TypeObject:
			_, okType = v.(map[string]any)
		case TypeArray:
			_, okType = v.([]any)
		}
		if !okType {
			return false
		}
	}
	return true
}

// ---- the run ----

type modelRun struct {
	t     *testing.T
	seed  uint64
	rng   *rand.Rand
	opts  Options
	e     *Engine
	m     mState
	step  int
	trail []string       // recent operations, for failure messages
	stats map[string]int // operation outcomes, logged at the end
}

func newModelRun(t *testing.T, seed uint64) *modelRun {
	opts := testOptions(t.TempDir())
	opts.DataSegmentSize = 2 << 10
	opts.CompactionRatio = 0.3
	opts.FlushInterval = time.Hour // flushes are explicit operations
	r := &modelRun{t: t, seed: seed, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), opts: opts, m: mState{}, stats: map[string]int{}}
	r.e = openEngine(t, opts)
	d := mustCreateDB(t, r.e, modelDB)
	for _, name := range modelColls {
		if err := d.CreateCollection(context.Background(), name, nil); err != nil {
			t.Fatal(err)
		}
		r.m[name] = &mColl{indexes: map[string]string{}, unique: map[string]bool{}, docs: map[string]map[string]any{}}
		for _, ix := range modelIndex[:r.pick(3)] {
			field := modelIndexFields[r.pick(len(modelIndexFields))]
			unique := r.chance(0.5) // on an empty collection: always possible
			if err := mustColl(t, d, name).CreateIndex(context.Background(), ix, field, Unique(unique)); err != nil {
				t.Fatal(err)
			}
			r.m[name].indexes[ix] = field
			r.m[name].unique[ix] = unique
		}
	}
	return r
}

func (r *modelRun) note(format string, args ...any) {
	r.stats[strings.Fields(fmt.Sprintf(format, args...))[0]]++
	r.trail = append(r.trail, fmt.Sprintf("%4d ", r.step)+fmt.Sprintf(format, args...))
	if len(r.trail) > 25 {
		r.trail = r.trail[1:]
	}
}

func (r *modelRun) fail(format string, args ...any) {
	r.t.Helper()
	r.t.Errorf("seed %d, operation %d: %s\nreproduce: CACHYDB_MODEL_SEED=%d go test -run TestModel ./pkg/db/\nrecent operations:\n  %s",
		r.seed, r.step, fmt.Sprintf(format, args...), r.seed, strings.Join(r.trail, "\n  "))
}

func (r *modelRun) coll(name string) *Collection {
	d, err := r.e.Database(modelDB)
	if err != nil {
		r.t.Fatalf("Database: %v", err)
	}
	c, err := d.Collection(name)
	if err != nil {
		r.t.Fatalf("Collection(%s): %v", name, err)
	}
	return c
}

func (r *modelRun) pick(n int) int { return r.rng.IntN(n) }

func (r *modelRun) chance(p float64) bool { return r.rng.Float64() < p }

func (r *modelRun) scalar() any {
	vals := []any{nil, true, false, 0.0, 1.0, 2.0, 1.5, -1.0, "a", "b", "A", "ab", ""}
	return vals[r.pick(len(vals))]
}

func (r *modelRun) array() []any {
	out := make([]any, r.pick(4))
	for i := range out {
		out[i] = r.scalar()
	}
	return out
}

func (r *modelRun) value(field string) any {
	p := r.rng.Float64()
	switch field {
	case "t":
		p = 1 - p // "t" holds arrays most of the time
	case "o":
		if r.chance(0.5) { // "o" holds objects half of the time: the path o.x
			if r.chance(0.2) {
				return map[string]any{"x": r.array(), "y": r.scalar()}
			}
			return map[string]any{"x": r.scalar()}
		}
	}
	switch {
	case p < 0.65:
		return r.scalar()
	case p < 0.9:
		return r.array()
	default:
		return map[string]any{"x": r.scalar()}
	}
}

func (r *modelRun) id() string { return fmt.Sprintf("k%d", r.pick(10)) }

func (r *modelRun) data() map[string]any {
	d := map[string]any{}
	for _, f := range modelFields {
		p := 0.6
		if f == "o.x" {
			p = 0.15 // the literal key is rarer than the path
		}
		if r.chance(p) {
			d[f] = r.value(f)
		}
	}
	return d
}

// storedValue returns the value of field in a random stored document (an
// element of it, sometimes, for arrays), so filters hit real values often.
func (r *modelRun) storedValue(field string) (any, bool) {
	var vals []any
	for _, cn := range modelColls {
		docs := r.m[cn].docs
		for _, id := range slices.Sorted(maps.Keys(docs)) { // deterministic for the seed
			if v, ok := mValue(id, docs[id], field); ok {
				vals = append(vals, v)
			}
		}
	}
	if len(vals) == 0 {
		return nil, false
	}
	v := vals[r.pick(len(vals))]
	if arr, ok := v.([]any); ok && len(arr) > 0 && r.chance(0.5) {
		v = arr[r.pick(len(arr))]
	}
	return v, true
}

// indexedField returns the field of a random index of collection cn.
func (r *modelRun) indexedField(cn string) (string, bool) {
	ix := r.m[cn].indexes
	if len(ix) == 0 {
		return "", false
	}
	names := slices.Sorted(maps.Keys(ix))
	return ix[names[r.pick(len(names))]], true
}

func (r *modelRun) filter(cn string) QueryFilter {
	f := QueryFilter{Field: modelQueryFields[r.pick(len(modelQueryFields))], Operator: modelOps[r.pick(len(modelOps))]}
	if field, ok := r.indexedField(cn); ok && r.chance(0.4) {
		f.Field = field // index-backed candidates
	}
	return r.filterValue(f)
}

// filterOn returns a filter on field with operator op and a fitting value.
func (r *modelRun) filterOn(field, op string) QueryFilter {
	return r.filterValue(QueryFilter{Field: field, Operator: op})
}

// filterValue fills in a value for f: often one a stored document holds.
func (r *modelRun) filterValue(f QueryFilter) QueryFilter {
	switch f.Operator {
	case "exists":
		f.Value = r.chance(0.5)
		return f
	case "starts_with":
		f.Value = []string{"a", "A", "", "ab", "b", "abc"}[r.pick(6)]
		if v, ok := r.storedValue(f.Field); ok && r.chance(0.5) {
			if s, isStr := v.(string); isStr {
				f.Value = s[:r.pick(len(s)+1)]
			}
		}
		return f
	}
	if v, ok := r.storedValue(f.Field); ok && r.chance(0.5) {
		f.Value = v
		if f.Operator == "in" || f.Operator == "has_any" || f.Operator == "has_all" {
			f.Value = append(r.array(), v)
		}
		return f
	}
	switch {
	case f.Field == "_id" && r.chance(0.7):
		f.Value = r.id()
	case f.Operator == "in" || f.Operator == "has_any" || f.Operator == "has_all":
		if r.chance(0.9) {
			f.Value = r.array()
		} else {
			f.Value = r.scalar()
		}
	case f.Operator == "contains":
		f.Value = []any{"a", "b", "A", "", 1.0}[r.pick(5)]
	default:
		f.Value = r.value(f.Field)
	}
	return f
}

func (r *modelRun) filters(cn string, maxN int) []QueryFilter {
	var out []QueryFilter
	for range r.pick(maxN + 1) {
		out = append(out, r.filter(cn))
	}
	return out
}

func (r *modelRun) query(cn string) *Query {
	if r.chance(0.3) {
		return r.planQuery(cn)
	}
	q := &Query{Filters: r.filters(cn, 2)}
	if r.chance(0.3) {
		for range 1 + r.pick(2) {
			q.Any = append(q.Any, r.filters(cn, 2))
		}
	}
	if r.chance(0.5) {
		sortFields := modelQueryFields
		for range 1 + r.pick(2) {
			k := SortKey{Field: sortFields[r.pick(len(sortFields))]}
			if r.chance(0.5) {
				k.Order = "desc"
			} else if r.chance(0.5) {
				k.Order = "asc"
			}
			q.Sort = append(q.Sort, k)
		}
	}
	if r.chance(0.3) {
		for range 1 + r.pick(2) {
			q.Fields = append(q.Fields, modelQueryFields[r.pick(len(modelQueryFields))])
		}
	}
	if r.chance(0.3) {
		q.Skip = r.pick(4)
	}
	if r.chance(0.4) {
		q.Limit = r.pick(6)
	}
	return q
}

// planQuery returns a query shaped for one of the query plans, on an
// indexed field when the collection has one: a single index-served filter
// (sometimes with a sort on the same field), an OR of index-served
// filters, or a single sort key with a limit.
func (r *modelRun) planQuery(cn string) *Query {
	field, ok := r.indexedField(cn)
	if !ok {
		field = modelIndexFields[r.pick(len(modelIndexFields))]
	}
	served := []string{"eq", "in", "gt", "gte", "lt", "lte", "starts_with", "exists", "has", "has_any", "has_all"}
	q := &Query{}
	switch r.pick(3) {
	case 0:
		q.Filters = []QueryFilter{r.filterOn(field, served[r.pick(len(served))])}
		if r.chance(0.3) { // a second bound on the same field
			q.Filters = append(q.Filters, r.filterOn(field, []string{"gt", "gte", "lt", "lte"}[r.pick(4)]))
		}
		if r.chance(0.5) {
			q.Sort = []SortKey{{Field: field, Order: []string{"", "asc", "desc"}[r.pick(3)]}}
		}
	case 1:
		for range 2 + r.pick(2) {
			g := field
			if f, ok := r.indexedField(cn); ok {
				g = f
			}
			q.Any = append(q.Any, []QueryFilter{r.filterOn(g, served[r.pick(len(served))])})
		}
	default:
		q.Sort = []SortKey{{Field: field, Order: []string{"", "asc", "desc"}[r.pick(3)]}}
		if r.chance(0.3) {
			q.Filters = []QueryFilter{r.filterOn(field, []string{"gt", "gte", "lt", "lte", "starts_with"}[r.pick(5)])}
		}
	}
	if r.chance(0.8) {
		q.Limit = 1 + r.pick(5)
	}
	if r.chance(0.3) {
		q.Skip = r.pick(4)
	}
	return q
}

func (r *modelRun) schema() *Schema {
	if r.chance(0.4) {
		return nil
	}
	types := []FieldType{TypeString, TypeNumber, TypeBoolean, TypeArray, TypeObject, TypeDate}
	s := &Schema{Fields: map[string]Field{}}
	for range 1 + r.pick(2) {
		s.Fields[modelSchemaFields[r.pick(len(modelSchemaFields))]] = Field{Type: types[r.pick(len(types))], Required: r.chance(0.5)}
	}
	return s
}

func js(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%#v", v)
	}
	return string(b)
}

// errClass maps an error to the sentinel the model predicts.
func errClass(err error) string {
	for _, s := range []struct {
		err  error
		name string
	}{
		{ErrExists, "exists"}, {ErrNotFound, "not found"}, {ErrConditionFailed, "condition failed"}, {ErrSchemaValidation, "schema"},
		{ErrUniqueViolation, "unique"},
	} {
		if errors.Is(err, s.err) {
			return s.name
		}
	}
	if err == nil {
		return "ok"
	}
	return "other: " + err.Error()
}

// writeOp is a document or metadata write: how to run it on the engine,
// and the model's predicted error class and next state.
type writeOp struct {
	desc  string
	run   func(ctx context.Context) error
	want  string
	apply func(m mState) // applied to a clone when want is "ok"
}

// docItem is one random document write of id in collection cn: the
// operation (a batch operation; docWrite runs it as the matching single
// write), and the model's prediction leaving unique indexes aside: the
// error class, and the after-image when it is "ok" (del: the document is
// deleted).
type docItem struct {
	desc string
	op   WriteOp
	want string
	next map[string]any
	del  bool
}

func (r *modelRun) docItem(cn, id string) docItem {
	mc := r.m[cn]
	cur, exists := mc.docs[id]
	var cond Condition
	if r.chance(0.35) {
		cond = r.filters(cn, 2)
		if len(cond) == 0 {
			cond = Condition{r.filter(cn)}
		}
	}
	condOK := len(cond) == 0 || (exists && mMatchAll(id, cur, cond))

	switch r.pick(4) {
	case 0: // insert
		data := r.data()
		want := "ok"
		switch {
		case exists:
			want = "exists"
		case !mValid(mc.schema, data):
			want = "schema"
		}
		return docItem{
			desc: fmt.Sprintf("insert %s/%s %s", cn, id, js(data)),
			op:   WriteOp{Kind: OpInsert, Document: &Document{ID: id, Data: data}},
			want: want,
			next: data,
		}
	case 1: // update
		u := UpdateSpec{Set: map[string]any{}, Upsert: r.chance(0.3)}
		for _, f := range modelFields {
			switch {
			case r.chance(0.35):
				u.Set[f] = r.value(f)
			case r.chance(0.2):
				u.Unset = append(u.Unset, f)
			}
		}
		op := WriteOp{Kind: OpUpdate, ID: id, Update: u}
		if r.chance(0.5) {
			op.Update.If = cond // UpdateSpec.If; otherwise WriteOp.If
		} else {
			op.If = cond
		}
		var next map[string]any
		want := "ok"
		switch {
		case !condOK:
			want = "condition failed"
		case !exists && !u.Upsert:
			want = "not found"
		case !exists:
			next = maps.Clone(u.Set)
		default:
			next = maps.Clone(cur)
			maps.Copy(next, u.Set)
			for _, f := range u.Unset {
				delete(next, f)
			}
		}
		if want == "ok" && !mValid(mc.schema, next) {
			want = "schema"
		}
		return docItem{
			desc: fmt.Sprintf("update %s/%s set=%s unset=%v if=%s upsert=%v", cn, id, js(u.Set), u.Unset, js(cond), u.Upsert),
			op:   op,
			want: want,
			next: next,
		}
	case 2: // replace
		data := r.data()
		want := "ok"
		switch {
		case !condOK:
			want = "condition failed"
		case !exists:
			want = "not found"
		case !mValid(mc.schema, data):
			want = "schema"
		}
		return docItem{
			desc: fmt.Sprintf("replace %s/%s %s if=%s", cn, id, js(data), js(cond)),
			op:   WriteOp{Kind: OpReplace, ID: id, Document: &Document{Data: data}, If: cond},
			want: want,
			next: data,
		}
	default: // delete
		want := "ok"
		switch {
		case !condOK:
			want = "condition failed"
		case !exists:
			want = "not found"
		}
		return docItem{
			desc: fmt.Sprintf("delete %s/%s if=%s", cn, id, js(cond)),
			op:   WriteOp{Kind: OpDelete, ID: id, If: cond},
			want: want,
			del:  true,
		}
	}
}

// docWrite is one random single-document write.
func (r *modelRun) docWrite() writeOp {
	cn := modelColls[r.pick(len(modelColls))]
	mc := r.m[cn]
	id := r.id()
	it := r.docItem(cn, id)
	want := it.want
	if want == "ok" && !it.del && mc.mUniqueViolation(id, it.next) {
		want = "unique"
	}
	op := it.op
	return writeOp{
		desc: it.desc,
		run: func(ctx context.Context) error {
			c := r.coll(cn)
			var err error
			switch op.Kind {
			case OpInsert:
				_, err = c.Insert(ctx, op.Document)
			case OpUpdate:
				u := op.Update
				if len(op.If) > 0 {
					u.If = op.If
				}
				_, _, err = c.UpdateWith(ctx, id, u)
			case OpReplace:
				_, err = c.Replace(ctx, id, op.Document, op.If)
			default:
				err = c.DeleteWith(ctx, id, op.If)
			}
			return err
		},
		want: want,
		apply: func(m mState) {
			if it.del {
				delete(m[cn].docs, id)
			} else {
				m[cn].docs[id] = it.next
			}
		},
	}
}

// mUniqueBroken reports whether docs, the documents of c after a change,
// give a unique index's value to two documents.
func (c *mColl) mUniqueBroken(docs map[string]map[string]any) bool {
	after := &mColl{indexes: c.indexes, unique: c.unique, docs: docs}
	for name, field := range c.indexes {
		if c.unique[name] && after.mHasDuplicates(field) {
			return true
		}
	}
	return false
}

// batchWrite is one random Collection.Batch of 1 to 4 document writes of
// one collection (plan 013 §3.1): every operation applies or none. The
// model's prediction follows the documented order: an id repeated in the
// batch fails that operation with ErrExists; otherwise the first operation
// that fails (in order) fails the batch; otherwise unique indexes are
// checked on the batch's final state. A failure must be a *BatchItemError
// naming the failing operation.
func (r *modelRun) batchWrite() writeOp {
	cn := modelColls[r.pick(len(modelColls))]
	mc := r.m[cn]
	items := make([]docItem, 1+r.pick(4))
	ops := make([]WriteOp, len(items))
	descs := make([]string, len(items))
	seen := map[string]bool{}
	dupAt := -1
	for i := range items {
		id := r.id()
		for try := 0; seen[id] && try < 3 && r.chance(0.8); try++ {
			id = r.id() // repeated ids are rarer than random picks make them
		}
		items[i] = r.docItem(cn, id)
		for try := 0; items[i].want != "ok" && try < 3 && r.chance(0.7); try++ {
			items[i] = r.docItem(cn, id) // most batches should apply
		}
		ops[i], descs[i] = items[i].op, items[i].desc
		if seen[id] && dupAt < 0 {
			dupAt = i
		}
		seen[id] = true
	}
	want, failAt := "ok", -1
	if dupAt >= 0 {
		want, failAt = "exists", dupAt
	} else {
		for i, it := range items {
			if it.want != "ok" {
				want, failAt = it.want, i
				break
			}
		}
	}
	final := maps.Clone(mc.docs) // documents are never changed in place
	if want == "ok" {
		for i, it := range items {
			if it.del {
				delete(final, ops[i].ID)
			} else {
				final[docItemID(&ops[i])] = it.next
			}
		}
		if mc.mUniqueBroken(final) {
			want = "unique"
		}
	}
	return writeOp{
		desc: fmt.Sprintf("batch %s [%s]", cn, strings.Join(descs, "; ")),
		run: func(ctx context.Context) error {
			res, err := r.coll(cn).Batch(ctx, ops)
			switch {
			case errors.Is(err, errSimulatedCrash):
				return err
			case err == nil && len(res.IDs) != len(ops):
				return fmt.Errorf("batch result has %d ids for %d operations", len(res.IDs), len(ops))
			case err == nil:
				return nil
			}
			bie, ok := errors.AsType[*BatchItemError](err)
			switch {
			case !ok:
				return fmt.Errorf("batch error is not a *BatchItemError: %s", err.Error())
			case failAt >= 0 && bie.Index != failAt:
				return fmt.Errorf("batch failed at operation %d, want %d: %s", bie.Index, failAt, err.Error())
			}
			return err
		},
		want: want,
		apply: func(m mState) {
			for i, it := range items {
				if it.del {
					delete(m[cn].docs, ops[i].ID)
				} else {
					m[cn].docs[docItemID(&ops[i])] = it.next
				}
			}
		},
	}
}

// docItemID returns the id a docItem operation writes.
func docItemID(op *WriteOp) string {
	if op.Kind == OpInsert {
		return op.Document.ID
	}
	return op.ID
}

// manyWrite is one random UpdateMany or DeleteMany (plan 013 §3.1, "Writes
// by filter"): every matching document, or the first limit of them in _id
// order, is changed by one batch. The counts must equal the matches.
func (r *modelRun) manyWrite() writeOp {
	cn := modelColls[r.pick(len(modelColls))]
	mc := r.m[cn]
	q := &Query{Filters: r.filters(cn, 2)}
	if r.chance(0.3) {
		for range 1 + r.pick(2) {
			q.Any = append(q.Any, r.filters(cn, 2))
		}
	}
	limit := 0
	if r.chance(0.4) {
		limit = 1 + r.pick(4)
	}
	match := func(extra Condition) []string {
		var ids []string
		for _, id := range slices.Sorted(maps.Keys(mc.docs)) {
			if mMatchQuery(id, mc.docs[id], q) && mMatchAll(id, mc.docs[id], extra) {
				ids = append(ids, id)
			}
		}
		if limit > 0 && len(ids) > limit {
			ids = ids[:limit]
		}
		return ids
	}
	counts := func(ids []string, run func(ctx context.Context) (int, int, error)) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			matched, changed, err := run(ctx)
			if err == nil && (matched != len(ids) || changed != len(ids)) {
				return fmt.Errorf("matched %d, changed %d; want %d, %d (%v)", matched, changed, len(ids), len(ids), ids)
			}
			return err
		}
	}
	if r.chance(0.4) {
		ids := match(nil)
		return writeOp{
			desc: fmt.Sprintf("delete_many %s %s limit=%d -> %v", cn, js(q), limit, ids),
			run: counts(ids, func(ctx context.Context) (int, int, error) {
				return r.coll(cn).DeleteMany(ctx, q, limit)
			}),
			want: "ok",
			apply: func(m mState) {
				for _, id := range ids {
					delete(m[cn].docs, id)
				}
			},
		}
	}
	u := UpdateSpec{Set: map[string]any{}}
	for _, f := range modelFields {
		switch {
		case r.chance(0.35):
			u.Set[f] = r.value(f)
		case r.chance(0.2):
			u.Unset = append(u.Unset, f)
		}
	}
	if r.chance(0.2) {
		u.If = r.filters(cn, 1)
	}
	ids := match(u.If)
	final := maps.Clone(mc.docs)
	want := "ok"
	for _, id := range ids {
		next := maps.Clone(mc.docs[id])
		maps.Copy(next, u.Set)
		for _, f := range u.Unset {
			delete(next, f)
		}
		if !mValid(mc.schema, next) {
			want = "schema"
			break
		}
		final[id] = next
	}
	if want == "ok" && mc.mUniqueBroken(final) {
		want = "unique"
	}
	return writeOp{
		desc: fmt.Sprintf("update_many %s %s set=%s unset=%v if=%s limit=%d -> %v", cn, js(q), js(u.Set), u.Unset, js(u.If), limit, ids),
		run: counts(ids, func(ctx context.Context) (int, int, error) {
			return r.coll(cn).UpdateMany(ctx, q, u, limit)
		}),
		want: want,
		apply: func(m mState) {
			for _, id := range ids {
				m[cn].docs[id] = final[id]
			}
		},
	}
}

// anyDocWrite picks a single write, a batch, or a write by filter.
func (r *modelRun) anyDocWrite() writeOp {
	switch n := r.pick(100); {
	case n < 70:
		return r.docWrite()
	case n < 88:
		return r.batchWrite()
	default:
		return r.manyWrite()
	}
}

func (r *modelRun) metaWrite() writeOp {
	cn := modelColls[r.pick(len(modelColls))]
	mc := r.m[cn]
	switch r.pick(3) {
	case 0: // create_index
		name := modelIndex[r.pick(len(modelIndex))]
		field := modelIndexFields[r.pick(len(modelIndexFields))]
		unique := r.chance(0.4)
		if unique && r.chance(0.6) {
			field = "o.x" // often missing: a unique index on it can be created more often
		}
		want := "ok"
		if _, ok := mc.indexes[name]; ok {
			want = "exists"
		} else if unique && mc.mHasDuplicates(field) {
			want = "unique"
		}
		return writeOp{
			desc: fmt.Sprintf("create_index %s %s on %s unique=%v", cn, name, field, unique),
			run:  func(ctx context.Context) error { return r.coll(cn).CreateIndex(ctx, name, field, Unique(unique)) },
			want: want,
			apply: func(m mState) {
				m[cn].indexes[name] = field
				m[cn].unique[name] = unique
			},
		}
	case 1: // drop_index
		name := modelIndex[r.pick(len(modelIndex))]
		want := "ok"
		if _, ok := mc.indexes[name]; !ok {
			want = "not found"
		}
		return writeOp{
			desc: fmt.Sprintf("drop_index %s %s", cn, name),
			run:  func(ctx context.Context) error { return r.coll(cn).DropIndex(ctx, name) },
			want: want,
			apply: func(m mState) {
				delete(m[cn].indexes, name)
				delete(m[cn].unique, name)
			},
		}
	default: // update_schema
		s := r.schema()
		want := "ok"
		for _, data := range mc.docs {
			if !mValid(s, data) {
				want = "schema"
				break
			}
		}
		return writeOp{
			desc:  fmt.Sprintf("update_schema %s %s", cn, js(s)),
			run:   func(ctx context.Context) error { return r.coll(cn).UpdateSchema(ctx, s) },
			want:  want,
			apply: func(m mState) { m[cn].schema = cloneSchema(s) },
		}
	}
}

// doWrite runs w and checks its outcome against the model.
func (r *modelRun) doWrite(w writeOp) {
	r.note("%s -> want %s", w.desc, w.want)
	err := w.run(context.Background())
	if got := errClass(err); got != w.want {
		r.fail("%s: got %s (%v), want %s", w.desc, got, err, w.want)
		return
	}
	if w.want == "ok" {
		w.apply(r.m)
	}
	r.stats["result "+w.want]++
	if kind := strings.Fields(w.desc)[0]; kind == "batch" || strings.HasSuffix(kind, "_many") {
		r.stats["result "+kind+" "+w.want]++
	}
}

func (r *modelRun) checkFind() {
	cn := modelColls[r.pick(len(modelColls))]
	q := r.query(cn)
	r.note("find %s %s", cn, js(q))
	got, err := r.coll(cn).Find(context.Background(), q)
	if err != nil {
		r.fail("find %s %s: %v", cn, js(q), err)
		return
	}
	if want := r.m[cn].find(q); js(got) != js(want) {
		r.fail("find %s %s:\n got %s\nwant %s", cn, js(q), js(got), js(want))
	}
}

func (r *modelRun) checkCount() {
	cn := modelColls[r.pick(len(modelColls))]
	q := r.query(cn)
	if r.chance(0.5) {
		// A single eq or has filter: answered from an index when the
		// field has one.
		// A single index-servable filter: counted from the index when the
		// field has one.
		f := r.filter(cn)
		f = r.filterOn(f.Field, []string{"eq", "has", "in", "gte", "lt", "starts_with", "exists"}[r.pick(7)])
		q = &Query{Filters: []QueryFilter{f}}
	}
	r.note("count %s %s", cn, js(q))
	got, err := r.coll(cn).CountDocuments(context.Background(), q)
	if err != nil {
		r.fail("count %s: %v", cn, err)
		return
	}
	if want := len(r.m[cn].find(&Query{Filters: q.Filters, Any: q.Any})); got != want {
		r.fail("count %s filters=%s any=%s: got %d, want %d", cn, js(q.Filters), js(q.Any), got, want)
	}
}

func (r *modelRun) checkDistinct() {
	cn := modelColls[r.pick(len(modelColls))]
	field := modelQueryFields[r.pick(len(modelQueryFields))]
	if f, ok := r.indexedField(cn); ok && r.chance(0.5) {
		field = f // answered from the index keys when there are no filters
	}
	q := &Query{}
	if r.chance(0.5) {
		q = r.query(cn)
	}
	r.note("distinct %s %s %s", cn, field, js(q))
	got, err := r.coll(cn).Distinct(context.Background(), field, q)
	if err != nil {
		r.fail("distinct %s %s: %v", cn, field, err)
		return
	}
	if want := r.m[cn].distinct(field, q); js(got) != js(want) {
		r.fail("distinct %s %s filters=%s any=%s:\n got %s\nwant %s", cn, field, js(q.Filters), js(q.Any), js(got), js(want))
	}
}

func (r *modelRun) checkGet() {
	cn := modelColls[r.pick(len(modelColls))]
	id := r.id()
	r.note("get %s/%s", cn, id)
	got, err := r.coll(cn).Get(context.Background(), id)
	data, ok := r.m[cn].docs[id]
	switch {
	case !ok && !errors.Is(err, ErrNotFound):
		r.fail("get %s/%s: got %v, %v; want not found", cn, id, got, err)
	case ok && (err != nil || js(got.Data) != js(data)):
		r.fail("get %s/%s: got %v, %v; want %s", cn, id, got, err, js(data))
	}
}

// engineState captures the engine's whole logical state as an mState.
func (r *modelRun) engineState() mState {
	out := mState{}
	for _, cn := range modelColls {
		c := r.coll(cn)
		mc := &mColl{schema: c.Schema(), indexes: map[string]string{}, unique: map[string]bool{}, docs: map[string]map[string]any{}}
		for _, ix := range c.Indexes() {
			mc.indexes[ix.Name] = ix.Field
			mc.unique[ix.Name] = ix.Unique
		}
		docs, err := c.Find(context.Background(), nil)
		if err != nil {
			r.t.Fatalf("seed %d, operation %d: scanning %s: %v", r.seed, r.step, cn, err)
		}
		for _, d := range docs {
			mc.docs[d.ID] = d.Data
		}
		out[cn] = mc
	}
	return out
}

func (r *modelRun) checkState(why string) {
	if got, want := r.engineState().dump(), r.m.dump(); got != want {
		r.fail("%s: state mismatch\n--- engine\n%s--- model\n%s--- differing lines\n%s", why, got, want, lineDiff(got, want))
	}
	r.checkIndexFiles(why)
}

// checkIndexFiles compares every collection's index file (key table and
// index entries, counters, the store's own check) with its documents.
func (r *modelRun) checkIndexFiles(why string) {
	for _, cn := range modelColls {
		c := r.coll(cn)
		docs, err := c.Find(context.Background(), nil)
		if err != nil {
			r.fail("%s: scanning %s: %v", why, cn, err)
			return
		}
		c.db.mu.RLock()
		checkIndexEntries(r.t, c, docs)
		c.db.mu.RUnlock()
		if r.t.Failed() {
			r.fail("%s: the index file of %s does not match its documents (see above)", why, cn)
			return
		}
	}
}

func (r *modelRun) reopen(why string) {
	r.note("%s", why)
	if err := r.e.Close(context.Background()); err != nil {
		r.fail("%s: Close: %v", why, err)
		return
	}
	r.e = openEngine(r.t, r.opts)
	r.checkState(why)
}

// crash reopens the engine with a crash armed at a hook point, runs an
// operation that reaches it, abandons the engine as a dead process would,
// and reopens it. An operation in flight at the crash may or may not have
// landed; everything acknowledged before it must be there.
func (r *modelRun) crash() {
	points := []hookPoint{hookAfterWALSync, hookMetaWrite, hookAfterSegWrite, hookAfterSegSync, hookAfterIndexCommit, hookCompactBeforeSwap}
	p := points[r.pick(len(points))]
	if err := r.e.Close(context.Background()); err != nil {
		r.fail("crash: Close: %v", err)
		return
	}
	opts := r.opts
	crashAt(r.t, &opts, p)
	r.e = openEngine(r.t, opts)

	before := r.m.clone()
	after := r.m.clone()
	desc := ""
	ctx := context.Background()
	switch p {
	case hookAfterWALSync, hookMetaWrite:
		w := r.anyDocWrite()
		if p == hookMetaWrite {
			w = r.metaWrite()
		}
		desc = w.desc
		err := w.run(ctx)
		switch {
		case w.want == "ok":
			w.apply(after)
			if err != nil && !errors.Is(err, errSimulatedCrash) {
				r.fail("crash at %s during %s: %v", p, w.desc, err)
			}
		case errClass(err) != w.want:
			r.fail("crash at %s during %s: got %v, want %s", p, w.desc, err, w.want)
		}
	case hookAfterSegWrite, hookAfterSegSync, hookAfterIndexCommit:
		cn := modelColls[r.pick(len(modelColls))]
		desc = "flush " + cn
		if err := r.coll(cn).flush(); err != nil && !errors.Is(err, errSimulatedCrash) {
			r.fail("crash at %s during flush: %v", p, err)
		}
	case hookCompactBeforeSwap:
		cn := modelColls[r.pick(len(modelColls))]
		desc = "flush and compact " + cn
		c := r.coll(cn)
		if err := c.flush(); err != nil && !errors.Is(err, errSimulatedCrash) {
			r.fail("crash: flush: %v", err)
		}
		if _, err := c.compact(ctx); err != nil && !errors.Is(err, errSimulatedCrash) {
			r.fail("crash at %s during compaction: %v", p, err)
		}
	}
	r.note("crash at %s during %s", p, desc)
	r.e.abandon()
	r.e = openEngine(r.t, r.opts)

	got := r.engineState().dump()
	switch got {
	case after.dump():
		r.m = after
	case before.dump():
		r.m = before
	default:
		want := after.dump()
		r.fail("after a crash at %s during %s the state is neither before nor after it\n--- engine\n%s--- model after\n%s--- differing lines\n%s",
			p, desc, got, want, lineDiff(got, want))
	}
}

func (r *modelRun) randomOp() {
	switch n := r.pick(100); {
	case n < 40:
		r.doWrite(r.anyDocWrite())
	case n < 47:
		r.doWrite(r.metaWrite())
	case n < 67:
		r.checkFind()
	case n < 74:
		r.checkCount()
	case n < 81:
		r.checkDistinct()
	case n < 86:
		r.checkGet()
	case n < 91:
		cn := modelColls[r.pick(len(modelColls))]
		r.note("flush %s", cn)
		if err := r.coll(cn).flush(); err != nil {
			r.fail("flush %s: %v", cn, err)
		}
	case n < 94:
		cn := modelColls[r.pick(len(modelColls))]
		r.note("compact %s", cn)
		if _, err := r.coll(cn).compact(context.Background()); err != nil {
			r.fail("compact %s: %v", cn, err)
		}
	case n < 97:
		r.reopen("reopen")
	default:
		r.crash()
	}
}
