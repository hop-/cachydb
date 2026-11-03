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
// semantics (if, unset, upsert, replace, delete, schemas, index metadata).
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
	modelFields = []string{"a", "b", "t", "o"}
	modelIndex  = []string{"i1", "i2", "i3"}
	modelOps    = []string{"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "has", "has_any", "has_all"}
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
		out[name] = &mColl{schema: cloneSchema(c.schema), indexes: maps.Clone(c.indexes), docs: docs}
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
			fmt.Fprintf(&b, "%s index %s=%s\n", name, ix, c.indexes[ix])
		}
		for _, id := range slices.Sorted(maps.Keys(c.docs)) {
			data, _ := json.Marshal(c.docs[id]) // plain data: cannot fail
			fmt.Fprintf(&b, "%s doc %s %s\n", name, id, data)
		}
	}
	return b.String()
}

// mValue returns the value of field in a document (id, data).
func mValue(id string, data map[string]any, field string) (any, bool) {
	if field == "_id" {
		return id, true
	}
	v, ok := data[field]
	return v, ok
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
			p := map[string]any{}
			for _, f := range q.Fields {
				if v, ok := data[f]; ok {
					p[f] = v
				}
			}
			data = p
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
		r.m[name] = &mColl{indexes: map[string]string{}, docs: map[string]map[string]any{}}
		for _, ix := range modelIndex[:r.pick(3)] {
			field := modelFields[r.pick(len(modelFields))]
			if err := mustColl(t, d, name).CreateIndex(context.Background(), ix, field); err != nil {
				t.Fatal(err)
			}
			r.m[name].indexes[ix] = field
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
	if field == "t" {
		p = 1 - p // "t" holds arrays most of the time
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
		if r.chance(0.6) {
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
	fields := []string{"a", "b", "t", "o", "_id", "missing"}
	f := QueryFilter{Field: fields[r.pick(len(fields))], Operator: modelOps[r.pick(len(modelOps))]}
	if field, ok := r.indexedField(cn); ok && r.chance(0.4) {
		f.Field = field // index-backed candidates
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
	q := &Query{Filters: r.filters(cn, 2)}
	if r.chance(0.3) {
		for range 1 + r.pick(2) {
			q.Any = append(q.Any, r.filters(cn, 2))
		}
	}
	if r.chance(0.5) {
		sortFields := []string{"a", "b", "t", "o", "_id", "missing"}
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
			q.Fields = append(q.Fields, []string{"a", "b", "t", "o", "missing"}[r.pick(5)])
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

func (r *modelRun) schema() *Schema {
	if r.chance(0.4) {
		return nil
	}
	types := []FieldType{TypeString, TypeNumber, TypeBoolean, TypeArray, TypeObject, TypeDate}
	s := &Schema{Fields: map[string]Field{}}
	for range 1 + r.pick(2) {
		s.Fields[modelFields[r.pick(len(modelFields))]] = Field{Type: types[r.pick(len(types))], Required: r.chance(0.5)}
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

func (r *modelRun) docWrite() writeOp {
	cn := modelColls[r.pick(len(modelColls))]
	mc := r.m[cn]
	id := r.id()
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
		return writeOp{
			desc: fmt.Sprintf("insert %s/%s %s", cn, id, js(data)),
			run: func(ctx context.Context) error {
				_, err := r.coll(cn).Insert(ctx, &Document{ID: id, Data: data})
				return err
			},
			want:  want,
			apply: func(m mState) { m[cn].docs[id] = data },
		}
	case 1: // update
		u := UpdateSpec{Set: map[string]any{}, If: cond, Upsert: r.chance(0.3)}
		for _, f := range modelFields {
			switch {
			case r.chance(0.35):
				u.Set[f] = r.value(f)
			case r.chance(0.2):
				u.Unset = append(u.Unset, f)
			}
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
		return writeOp{
			desc: fmt.Sprintf("update %s/%s set=%s unset=%v if=%s upsert=%v", cn, id, js(u.Set), u.Unset, js(cond), u.Upsert),
			run: func(ctx context.Context) error {
				_, _, err := r.coll(cn).UpdateWith(ctx, id, u)
				return err
			},
			want:  want,
			apply: func(m mState) { m[cn].docs[id] = next },
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
		return writeOp{
			desc: fmt.Sprintf("replace %s/%s %s if=%s", cn, id, js(data), js(cond)),
			run: func(ctx context.Context) error {
				_, err := r.coll(cn).Replace(ctx, id, &Document{Data: data}, cond)
				return err
			},
			want:  want,
			apply: func(m mState) { m[cn].docs[id] = data },
		}
	default: // delete
		want := "ok"
		switch {
		case !condOK:
			want = "condition failed"
		case !exists:
			want = "not found"
		}
		return writeOp{
			desc:  fmt.Sprintf("delete %s/%s if=%s", cn, id, js(cond)),
			run:   func(ctx context.Context) error { return r.coll(cn).DeleteWith(ctx, id, cond) },
			want:  want,
			apply: func(m mState) { delete(m[cn].docs, id) },
		}
	}
}

func (r *modelRun) metaWrite() writeOp {
	cn := modelColls[r.pick(len(modelColls))]
	mc := r.m[cn]
	switch r.pick(3) {
	case 0: // create_index
		name := modelIndex[r.pick(len(modelIndex))]
		field := []string{"a", "b", "t", "o"}[r.pick(4)]
		want := "ok"
		if _, ok := mc.indexes[name]; ok {
			want = "exists"
		}
		return writeOp{
			desc:  fmt.Sprintf("create_index %s %s on %s", cn, name, field),
			run:   func(ctx context.Context) error { return r.coll(cn).CreateIndex(ctx, name, field) },
			want:  want,
			apply: func(m mState) { m[cn].indexes[name] = field },
		}
	case 1: // drop_index
		name := modelIndex[r.pick(len(modelIndex))]
		want := "ok"
		if _, ok := mc.indexes[name]; !ok {
			want = "not found"
		}
		return writeOp{
			desc:  fmt.Sprintf("drop_index %s %s", cn, name),
			run:   func(ctx context.Context) error { return r.coll(cn).DropIndex(ctx, name) },
			want:  want,
			apply: func(m mState) { delete(m[cn].indexes, name) },
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
		f := r.filter(cn)
		f.Operator = []string{"eq", "has"}[r.pick(2)]
		if v, ok := r.storedValue(f.Field); ok && r.chance(0.7) {
			f.Value = v
		}
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
	field := []string{"a", "b", "t", "o", "_id", "missing"}[r.pick(6)]
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
		mc := &mColl{schema: c.Schema(), indexes: map[string]string{}, docs: map[string]map[string]any{}}
		for _, ix := range c.Indexes() {
			mc.indexes[ix.Name] = ix.Field
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
	points := []hookPoint{hookAfterWALSync, hookMetaWrite, hookAfterSegWrite, hookAfterSegSync, hookCompactBeforeSwap}
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
		w := r.docWrite()
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
	case hookAfterSegWrite, hookAfterSegSync:
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
		r.doWrite(r.docWrite())
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
