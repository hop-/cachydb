package db

import (
	"cmp"
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Query execution (plan 004 §2): candidates from the primary key or a
// secondary index, re-checked against every filter and OR group, then
// ordered (by _id with early exit, or by sort keys with a bounded heap),
// paged, and projected.

// Sort orders.
const (
	orderAsc  = "asc"
	orderDesc = "desc"
)

// Find returns copies of the documents matching q, after skipping q.Skip
// matches and returning at most q.Limit (0 or negative = no limit). A nil
// q matches everything. A document matches when it matches every filter
// of q.Filters and, if q.Any is not empty, every filter of at least one of
// its groups.
//
// Results are ordered by q.Sort, then by ascending _id; without q.Sort, by
// ascending _id. Candidates come from the smallest index-backed set (eq on
// _id or on an indexed field, has, has_any, has_all on an indexed field);
// without one, every live document is scanned. Each candidate is checked
// against all filters. In _id order the scan stops as soon as skip+limit
// matches are found; any other sort reads every match and keeps the best
// skip+limit in a bounded heap. q.Fields, if set, limits the returned
// fields (plus _id). $now filter values are resolved once.
func (c *Collection) Find(ctx context.Context, q *Query) ([]*Document, error) {
	docs, _, err := c.find(ctx, q)
	return docs, err
}

// preparedQuery is a query with its $now values resolved and its sort
// keys normalized.
type preparedQuery struct {
	filters     []QueryFilter
	anyOf       [][]QueryFilter
	sort        []SortKey // normalized orders; nil = _id ascending
	idDesc      bool      // sort is exactly _id descending
	fields      []string
	skip, limit int
}

func prepareQuery(q *Query) (*preparedQuery, error) {
	if q == nil {
		q = &Query{}
	}
	ts := nowString(time.Now())
	p := &preparedQuery{
		filters: resolveNowFilters(q.Filters, ts),
		anyOf:   resolveNowGroups(q.Any, ts),
		fields:  q.Fields,
		skip:    max(q.Skip, 0),
		limit:   max(q.Limit, 0),
	}
	sort, err := normalizeSort(q.Sort)
	if err != nil {
		return nil, err
	}
	switch {
	case len(sort) == 1 && sort[0].Field == "_id" && sort[0].Order == orderAsc:
		sort = nil // today's path
	case len(sort) == 1 && sort[0].Field == "_id":
		p.idDesc, sort = true, nil
	}
	p.sort = sort
	return p, nil
}

// normalizeSort checks the sort keys and fills in the default order.
func normalizeSort(keys []SortKey) ([]SortKey, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	out := make([]SortKey, len(keys))
	for i, k := range keys {
		if k.Field == "" {
			return nil, fmt.Errorf("sort key %d: field must not be empty", i)
		}
		switch strings.ToLower(k.Order) {
		case "", orderAsc:
			k.Order = orderAsc
		case orderDesc:
			k.Order = orderDesc
		default:
			return nil, fmt.Errorf("sort key %q: order %q must be asc or desc", k.Field, k.Order)
		}
		out[i] = k
	}
	return out, nil
}

// matches reports whether doc matches the query's filters and groups.
func (p *preparedQuery) matches(doc *Document) bool {
	return matchesQuery(doc, p.filters, p.anyOf)
}

// find implements Find and also reports how many candidates it examined.
func (c *Collection) find(ctx context.Context, q *Query) ([]*Document, int, error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return nil, 0, err
	}
	defer e.exit()
	p, err := prepareQuery(q)
	if err != nil {
		return nil, 0, err
	}
	ids, err := c.candidates(p.filters)
	if err != nil {
		return nil, 0, err
	}
	if p.sort != nil {
		return c.findSorted(ctx, p, ids)
	}
	if p.idDesc {
		slices.Reverse(ids)
	}

	out := make([]*Document, 0)
	matched := 0
	examined, err := c.scan(ctx, ids, p, func(doc *Document, owned bool) bool {
		matched++
		if matched <= p.skip {
			return true
		}
		out = append(out, project(doc, owned, p.fields))
		return p.limit == 0 || len(out) < p.limit
	})
	if err != nil {
		return nil, examined, err
	}
	return out, examined, nil
}

// scan reads the documents of ids in order and calls fn for each one that
// matches p, until fn returns false. doc is shared with the memtable or
// cache unless owned: fn must neither modify nor keep it without a Clone.
// It returns the number of candidates examined.
func (c *Collection) scan(ctx context.Context, ids []string, p *preparedQuery, fn func(doc *Document, owned bool) bool) (int, error) {
	examined := 0
	for _, id := range ids {
		if examined%findCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return examined, err
			}
		}
		examined++
		doc, owned, err := c.current(id)
		if err != nil {
			return examined, err
		}
		if doc == nil || !p.matches(doc) {
			continue // deleted since the candidates were taken, or no match
		}
		if !fn(doc, owned) {
			break
		}
	}
	return examined, nil
}

// findSorted is find for a sort other than _id: it reads every match,
// keeps the best skip+limit (all of them without a limit), sorts them, and
// pages.
func (c *Collection) findSorted(ctx context.Context, p *preparedQuery, ids []string) ([]*Document, int, error) {
	keep := 0
	if p.limit > 0 {
		keep = p.skip + p.limit
	}
	h := &sortHeap{keys: p.sort}
	examined, err := c.scan(ctx, ids, p, func(doc *Document, owned bool) bool {
		it := newSortItem(doc, owned, p.sort)
		switch {
		case keep == 0 || h.Len() < keep:
			heap.Push(h, it)
		case h.cmp(it, h.items[0]) < 0:
			h.items[0] = it // better than the worst kept
			heap.Fix(h, 0)
		}
		return true
	})
	if err != nil {
		return nil, examined, err
	}
	items := h.items
	slices.SortFunc(items, h.cmp)
	out := make([]*Document, 0)
	for i := p.skip; i < len(items); i++ {
		out = append(out, project(items[i].doc, items[i].owned, p.fields))
	}
	return out, examined, nil
}

// sortItem is a matching document with its sort values extracted.
type sortItem struct {
	doc   *Document
	owned bool
	vals  []sortValue
}

func newSortItem(doc *Document, owned bool, keys []SortKey) *sortItem {
	it := &sortItem{doc: doc, owned: owned, vals: make([]sortValue, len(keys))}
	for i, k := range keys {
		v, ok := doc.GetValue(k.Field)
		it.vals[i] = newSortValue(v, ok)
	}
	return it
}

// sortHeap keeps the best items seen so far with the worst on top.
type sortHeap struct {
	keys  []SortKey
	items []*sortItem
}

// cmp orders a before b by the sort keys, then by ascending _id.
func (h *sortHeap) cmp(a, b *sortItem) int {
	for i, k := range h.keys {
		c := compareSortValues(a.vals[i], b.vals[i])
		if k.Order == orderDesc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return strings.Compare(a.doc.ID, b.doc.ID)
}

func (h *sortHeap) Len() int           { return len(h.items) }
func (h *sortHeap) Less(i, j int) bool { return h.cmp(h.items[i], h.items[j]) > 0 } // worst first
func (h *sortHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *sortHeap) Push(x any)         { h.items = append(h.items, x.(*sortItem)) }
func (h *sortHeap) Pop() any {
	n := len(h.items) - 1
	it := h.items[n]
	h.items = h.items[:n]
	return it
}

// Sort ranks (plan 004 §2.4): missing < null < false < true < numbers <
// strings < arrays < objects.
const (
	rankMissing = iota
	rankNull
	rankBool
	rankNumber
	rankString
	rankArray
	rankObject
)

// sortValue is a field value prepared for sorting.
type sortValue struct {
	rank int
	b    bool
	n    float64
	s    string // strings; canonical JSON of arrays and objects
}

func newSortValue(v any, exists bool) sortValue {
	if !exists {
		return sortValue{rank: rankMissing}
	}
	if v == nil {
		return sortValue{rank: rankNull}
	}
	if f, ok := toFloat(v); ok {
		return sortValue{rank: rankNumber, n: f}
	}
	switch t := v.(type) {
	case bool:
		return sortValue{rank: rankBool, b: t}
	case string:
		return sortValue{rank: rankString, s: t}
	}
	rank := rankObject
	if _, ok := listValues(v); ok {
		rank = rankArray
	}
	return sortValue{rank: rank, s: canonicalJSON(v)}
}

// compareSortValues orders a and b by the total sort order.
func compareSortValues(a, b sortValue) int {
	if c := cmp.Compare(a.rank, b.rank); c != 0 {
		return c
	}
	switch a.rank {
	case rankBool:
		switch {
		case a.b == b.b:
			return 0
		case !a.b:
			return -1
		default:
			return 1
		}
	case rankNumber:
		return cmp.Compare(a.n, b.n) // NaN sorts first, consistently
	case rankString, rankArray, rankObject:
		return strings.Compare(a.s, b.s)
	}
	return 0
}

// canonicalJSON returns the JSON encoding of v with numbers normalized and
// object keys sorted (encoding/json sorts map keys).
func canonicalJSON(v any) string {
	data, err := json.Marshal(normalize(v))
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(data)
}

// project returns a copy of doc limited to fields (plus _id), or a full
// copy when fields is empty. Unknown fields are ignored. A doc that is
// owned is returned as is when no projection applies.
func project(doc *Document, owned bool, fields []string) *Document {
	if len(fields) == 0 {
		if owned {
			return doc
		}
		return doc.Clone()
	}
	out := &Document{ID: doc.ID, Data: make(map[string]any, len(fields))}
	for _, f := range fields {
		if v, ok := doc.Data[f]; ok {
			out.Data[f] = deepCopy(v)
		}
	}
	return out
}

// CountDocuments returns the number of documents matching q's filters and
// OR groups (plan 004 §2.6; sort, fields, skip, and limit are ignored). A
// nil or empty query returns Count(). A single eq or has filter on _id or
// an indexed field, with nothing else, is answered from the index without
// reading documents; anything else reads the candidates like Find.
func (c *Collection) CountDocuments(ctx context.Context, q *Query) (int, error) {
	n, _, err := c.countDocuments(ctx, q)
	return n, err
}

// countDocuments implements CountDocuments and also reports how many
// candidates it read (0 for the fast paths).
func (c *Collection) countDocuments(ctx context.Context, q *Query) (int, int, error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return 0, 0, err
	}
	defer e.exit()
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	var filters []QueryFilter
	var anyOf [][]QueryFilter
	if q != nil {
		filters, anyOf = q.Filters, q.Any
	}
	p, err := prepareQuery(&Query{Filters: filters, Any: anyOf})
	if err != nil {
		return 0, 0, err
	}
	if n, ok, err := c.countFast(p); err != nil || ok {
		return n, 0, err
	}
	ids, err := c.candidates(p.filters)
	if err != nil {
		return 0, 0, err
	}
	n := 0
	examined, err := c.scan(ctx, ids, p, func(*Document, bool) bool {
		n++
		return true
	})
	return n, examined, err
}

// countFast answers a count without reading documents when it can.
func (c *Collection) countFast(p *preparedQuery) (int, bool, error) {
	if len(p.anyOf) > 0 || len(p.filters) > 1 {
		return 0, false, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dropped {
		return 0, false, c.errDropped()
	}
	if len(p.filters) == 0 {
		return c.Count(), true, nil
	}
	f := p.filters[0]
	if f.Field == "_id" {
		if f.Operator != "eq" {
			return 0, false, nil
		}
		id, ok := f.Value.(string)
		if !ok || !c.liveLocked(id) {
			return 0, true, nil
		}
		return 1, true, nil
	}
	if f.Operator != "eq" && f.Operator != "has" {
		return 0, false, nil
	}
	if _, ok := indexKey(f.Value); !ok {
		return 0, false, nil
	}
	ix := c.indexOnLocked(f.Field)
	if ix == nil {
		return 0, false, nil
	}
	if f.Operator == "eq" {
		return ix.countEq(f.Value), true, nil
	}
	return ix.countHas(f.Value), true, nil
}

// liveLocked reports whether the document id exists. Callers hold mu.
func (c *Collection) liveLocked(id string) bool {
	for _, layer := range []map[string]memEntry{c.memtable, c.flushing} {
		if me, ok := layer[id]; ok {
			return me.doc != nil
		}
	}
	_, ok := c.keydir[id]
	return ok
}

// Distinct returns the distinct values of field among the documents
// matching q's filters and OR groups (plan 004 §2.6; a nil q matches
// every document). An array value contributes each of its elements.
// Values are deduplicated with the type-aware equality of eq (objects and
// arrays by their canonical JSON) and sorted in the sort order of Find.
// Documents without the field contribute nothing. Without filters, an
// indexed field is answered from the index keys without reading
// documents.
func (c *Collection) Distinct(ctx context.Context, field string, q *Query) ([]any, error) {
	vals, _, err := c.distinct(ctx, field, q)
	return vals, err
}

// distinct implements Distinct and also reports how many candidates it
// read (0 for the index path).
func (c *Collection) distinct(ctx context.Context, field string, q *Query) ([]any, int, error) {
	if field == "" {
		return nil, 0, fmt.Errorf("distinct: field must not be empty")
	}
	e := c.db.e
	if err := e.enter(); err != nil {
		return nil, 0, err
	}
	defer e.exit()
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	var filters []QueryFilter
	var anyOf [][]QueryFilter
	if q != nil {
		filters, anyOf = q.Filters, q.Any
	}
	p, err := prepareQuery(&Query{Filters: filters, Any: anyOf})
	if err != nil {
		return nil, 0, err
	}

	if len(p.filters) == 0 && len(p.anyOf) == 0 {
		vals, ok, err := c.distinctFast(field)
		if err != nil {
			return nil, 0, err
		}
		if ok {
			return sortDistinct(vals), 0, nil
		}
	}

	ids, err := c.candidates(p.filters)
	if err != nil {
		return nil, 0, err
	}
	seen := make(map[string]any)
	add := func(v any) {
		key, ok := indexKey(v)
		if !ok {
			key = "j:" + canonicalJSON(v)
		}
		if _, dup := seen[key]; !dup {
			seen[key] = deepCopy(v)
		}
	}
	examined, err := c.scan(ctx, ids, p, func(doc *Document, _ bool) bool {
		v, ok := doc.GetValue(field)
		if !ok {
			return true
		}
		if elems, isList := listValues(v); isList {
			for _, el := range elems {
				add(el)
			}
		} else {
			add(v)
		}
		return true
	})
	if err != nil {
		return nil, examined, err
	}
	vals := make([]any, 0, len(seen))
	for _, v := range seen {
		vals = append(vals, v)
	}
	return sortDistinct(vals), examined, nil
}

// distinctFast returns the distinct values of field without reading
// documents (every live id for _id, or the keys of an index on field),
// or false.
func (c *Collection) distinctFast(field string) ([]any, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dropped {
		return nil, false, c.errDropped()
	}
	if field == "_id" {
		ids := c.liveIDsLocked()
		out := make([]any, len(ids))
		for i, id := range ids {
			out[i] = id
		}
		return out, true, nil
	}
	ix := c.indexOnLocked(field)
	if ix == nil {
		return nil, false, nil
	}
	vals, ok := ix.distinctValues()
	return vals, ok, nil
}

// sortDistinct sorts vals in the sort order of Find (ascending) and
// returns it.
func sortDistinct(vals []any) []any {
	sv := make([]sortValue, len(vals))
	for i, v := range vals {
		sv[i] = newSortValue(v, true)
	}
	idx := make([]int, len(vals))
	for i := range idx {
		idx[i] = i
	}
	slices.SortFunc(idx, func(a, b int) int { return compareSortValues(sv[a], sv[b]) })
	out := make([]any, len(vals))
	for i, j := range idx {
		out[i] = vals[j]
	}
	return out
}
