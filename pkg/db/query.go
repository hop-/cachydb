package db

import (
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// Query execution (approach-index-and-query §5): a query is compiled once
// per call (compileQuery), a driver is chosen in one snapshot (plan.go),
// and the candidates it yields are re-checked against every filter and OR
// group, then ordered, paged, and projected. When the driver yields the
// requested order (_id, or the single sort key through its index), the
// query streams and stops at skip+limit; otherwise _id order sorts the
// candidate ids before reading, and a sort reads every candidate into a
// bounded heap.

// Sort orders.
const (
	orderAsc  = "asc"
	orderDesc = "desc"
)

// Find returns copies of the documents matching q, after skipping q.Skip
// matches and returning at most q.Limit (0 or negative = no limit). A nil
// q matches everything. A document matches when it matches every filter
// of q.Filters and, if q.Any is not empty, every filter of at least one of
// its groups. An unknown operator, exists with a value that is not a
// boolean, or starts_with with a value that is not a string is an error.
//
// Results are ordered by q.Sort, then by ascending _id; without q.Sort, by
// ascending _id. The candidates come from one driver (see QueryPlan): an
// id lookup, one or several values or a range of an index, the union of
// the OR groups' indexes, a walk of the sort field's index, or else every
// document. Each candidate is checked against all filters. When the driver
// yields the requested order, Find stops as soon as skip+limit matches are
// found; any other sort reads every candidate and keeps the best
// skip+limit in a bounded heap. q.Fields, if set, limits the returned
// fields (plus _id). $now filter values are resolved once.
func (c *Collection) Find(ctx context.Context, q *Query) ([]*Document, error) {
	docs, _, err := c.FindWithPlan(ctx, q)
	return docs, err
}

// FindWithPlan is Find that also describes how the query ran: the driver
// it chose, whether the driver provided the order, and how many documents
// it examined and returned.
func (c *Collection) FindWithPlan(ctx context.Context, q *Query) ([]*Document, QueryPlan, error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return nil, QueryPlan{}, err
	}
	defer e.exit()
	cq, err := compileQuery(q)
	if err != nil {
		return nil, QueryPlan{}, err
	}
	return c.runQuery(ctx, cq)
}

// Iterate runs q as Find does and calls fn with each result in order (a
// copy fn may keep), until fn returns false or the results end. Unlike
// Find, it never holds more than the results fn has not seen yet (a sort
// the driver does not provide still keeps the best q.Skip+q.Limit matches,
// or every match without a limit), so a caller can stop on something other
// than a count, such as the size of what it has taken.
//
// fn runs while the query holds its read snapshot: it must not write to
// the engine or block for long. When the query has to start over (a
// compaction or an index change interrupted it), Iterate continues after
// the results fn already received, by position, as paging with q.Skip
// does; once fn has returned false it does not start over.
func (c *Collection) Iterate(ctx context.Context, q *Query, fn func(*Document) bool) error {
	_, err := c.iterate(ctx, q, fn)
	return err
}

// iterate is Iterate that also describes how the query ran (see
// FindWithPlan); Returned counts the results fn received.
func (c *Collection) iterate(ctx context.Context, q *Query, fn func(*Document) bool) (QueryPlan, error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return QueryPlan{}, err
	}
	defer e.exit()
	cq, err := compileQuery(q)
	if err != nil {
		return QueryPlan{}, err
	}
	return c.streamQuery(ctx, cq, fn, nil)
}

// runQuery runs a compiled query and collects its results. Callers are
// inside an enter/exit pair.
func (c *Collection) runQuery(ctx context.Context, cq *compiledQuery) ([]*Document, QueryPlan, error) {
	out := make([]*Document, 0)
	plan, err := c.streamQuery(ctx, cq,
		func(doc *Document) bool { out = append(out, doc); return true },
		func() { out = out[:0] })
	if err != nil {
		return nil, plan, err
	}
	return out, plan, nil
}

// streamQuery runs a compiled query and calls emit with each result in
// order until emit returns false. When a compaction or an index change
// interrupts it, it plans the query again (up to maxReadRetries times):
// with a restart function it calls it and starts from the first result;
// without one it continues after the results emit already received, and
// once emit returned false it stops. Callers are inside an enter/exit
// pair.
func (c *Collection) streamQuery(ctx context.Context, cq *compiledQuery, emit func(*Document) bool, restart func()) (QueryPlan, error) {
	delivered, stopped := 0, false
	count := func(doc *Document) bool {
		delivered++
		if !emit(doc) {
			stopped = true
			return false
		}
		return true
	}
	run := cq
	for attempt := 0; ; attempt++ {
		plan, err := c.findOnce(ctx, run, attempt < maxReadRetries/2, count)
		if errors.Is(err, errQueryRetry) {
			switch {
			case restart == nil && stopped:
				err = nil // the consumer has what it wanted
			case attempt >= maxReadRetries:
				err = c.retryErr()
			case restart != nil:
				restart()
				delivered = 0
				continue
			default:
				if run = cq.resumeAfter(delivered); run != nil {
					continue
				}
				err = nil // the limit was reached
			}
		}
		plan.Returned = delivered
		return plan, err
	}
}

// resumeAfter returns cq with its first n results skipped, or nil when its
// limit leaves nothing after them.
func (cq *compiledQuery) resumeAfter(n int) *compiledQuery {
	next := *cq
	next.skip += n
	if cq.limit > 0 {
		next.limit -= n
		if next.limit <= 0 {
			return nil
		}
	}
	return &next
}

// find is FindWithPlan that reports only the number of candidates
// examined.
func (c *Collection) find(ctx context.Context, q *Query) ([]*Document, int, error) {
	docs, plan, err := c.FindWithPlan(ctx, q)
	return docs, plan.Examined, err
}

// retryErr is the error of a query that had to start over too often.
func (c *Collection) retryErr() error {
	return fmt.Errorf("collection %q: the query was interrupted by compactions or index changes %d times; retry it", c.name, maxReadRetries+1)
}

// compiledQuery is a query compiled once per call: $now values resolved,
// operators and values compiled, sort keys normalized.
type compiledQuery struct {
	filters     []compiledFilter
	anyOf       [][]compiledFilter // groups that can match something
	never       bool               // nothing can match (see compiledFilter.never)
	sort        []SortKey          // normalized orders; nil = _id ascending
	sortPaths   []fieldPath        // the fields of sort, split once
	idDesc      bool               // sort is exactly _id descending
	fields      []string
	fieldPaths  []fieldPath // the fields of the projection, split once
	skip, limit int
	now         string // the time $now values resolved to (RFC 3339)
}

// compileQuery compiles q (nil matches everything).
func compileQuery(q *Query) (*compiledQuery, error) {
	if q == nil {
		q = &Query{}
	}
	ts := nowString(time.Now())
	filters, err := compileFilters(resolveNowFilters(q.Filters, ts))
	if err != nil {
		return nil, err
	}
	cq := &compiledQuery{
		filters:    filters,
		fields:     q.Fields,
		fieldPaths: compilePaths(q.Fields),
		skip:       max(q.Skip, 0),
		limit:      max(q.Limit, 0),
		now:        ts,
	}
	for _, g := range resolveNowGroups(q.Any, ts) {
		cg, err := compileFilters(g)
		if err != nil {
			return nil, err
		}
		cq.anyOf = append(cq.anyOf, cg)
	}
	sort, err := normalizeSort(q.Sort)
	if err != nil {
		return nil, err
	}
	switch {
	case len(sort) == 1 && sort[0].Field == "_id" && sort[0].Order == orderAsc:
		sort = nil // the default order
	case len(sort) == 1 && sort[0].Field == "_id":
		cq.idDesc, sort = true, nil
	}
	cq.sort = sort
	for _, k := range sort {
		cq.sortPaths = append(cq.sortPaths, compilePath(k.Field))
	}
	cq.finish()
	return cq, nil
}

// finish drops the OR groups that can match nothing and sets never.
func (cq *compiledQuery) finish() {
	for i := range cq.filters {
		if cq.filters[i].never() {
			cq.never = true
		}
	}
	if len(cq.anyOf) == 0 {
		return
	}
	groups := cq.anyOf[:0:0]
	for _, g := range cq.anyOf {
		if !slices.ContainsFunc(g, func(f compiledFilter) bool { return f.never() }) {
			groups = append(groups, g)
		}
	}
	if len(groups) == 0 {
		cq.never = true
	}
	cq.anyOf = groups
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
func (cq *compiledQuery) matches(doc *Document) bool {
	return matchQuery(doc, cq.filters, cq.anyOf)
}

// findOnce runs a compiled query in one snapshot, calling emit with each
// result in order until emit returns false. It returns errQueryRetry when
// the query has to start over (see plan.go), possibly after results were
// emitted. Returned in the plan is the number of results emitted.
func (c *Collection) findOnce(ctx context.Context, cq *compiledQuery, useIndexes bool, emit func(*Document) bool) (QueryPlan, error) {
	s, err := c.openSnap(useIndexes)
	if err != nil {
		return QueryPlan{}, err
	}
	defer s.close()
	opt, err := c.choosePlan(s, cq, true)
	if err != nil {
		return QueryPlan{}, err
	}
	plan := QueryPlan{Driver: opt.driver, Index: strings.Join(opt.index, ",")}
	counted := func(doc *Document) bool {
		plan.Returned++
		return emit(doc)
	}
	switch {
	case opt.never:
		plan.OrderProvided = true
	case opt.sortWalk:
		plan.OrderProvided = true
		plan.Examined, err = c.findBySortWalk(ctx, s, cq, opt, counted)
	case cq.sort != nil:
		plan.Examined, err = c.findByHeap(ctx, s, cq, opt, counted)
	default:
		plan.Examined, plan.OrderProvided, err = c.findByID(ctx, s, cq, opt, counted)
	}
	if err != nil {
		return plan, err
	}
	s.close()
	if !c.indexesUnchanged(opt.used) {
		return plan, errQueryRetry // an index changed meanwhile: plan again
	}
	return plan, nil
}

// queryRun passes the results of a query on in order: it examines each
// candidate it is given, skips matches, emits the rest, and says when it
// has enough.
type queryRun struct {
	ctx                         context.Context
	cq                          *compiledQuery
	emit                        func(*Document) bool
	matched, examined, returned int
	err                         error
}

// take examines doc (shared: it is copied when emitted). It returns false
// when the query has all it needs, the consumer stopped, or the context
// ended (r.err).
func (r *queryRun) take(doc *Document) bool {
	if r.examined%findCtxCheckEvery == 0 {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			return false
		}
	}
	r.examined++
	if !r.cq.matches(doc) {
		return true
	}
	r.matched++
	if r.matched <= r.cq.skip {
		return true
	}
	r.returned++
	if !r.emit(project(doc, false, r.cq.fieldPaths)) {
		return false
	}
	return r.cq.limit == 0 || r.returned < r.cq.limit
}

// findByID runs a query in _id order (ascending, or descending for a sort
// on _id desc). A driver that yields _id order streams; any other has its
// ids collected and sorted first, without reading documents. The overlay's
// candidates are merged in by id.
func (c *Collection) findByID(ctx context.Context, s *querySnap, cq *compiledQuery, opt *planOption, emit func(*Document) bool) (int, bool, error) {
	desc := cq.idDesc
	src, err := opt.open(ctx, s, desc)
	if err != nil {
		return 0, false, err
	}
	ordered := !opt.byValue && (!desc || opt.driver == driverKeydir || opt.driver == driverIDs)
	if !ordered {
		ids, err := collectIDs(ctx, src)
		if err != nil {
			return 0, false, c.storeErr(err)
		}
		src = &listSource{ids: ids, desc: desc}
	}
	ov := s.overlayIDs(opt.cand)
	if desc {
		slices.Reverse(ov)
	}
	before := func(a, b string) bool { return a < b != desc }

	r := &queryRun{ctx: ctx, cq: cq, emit: emit}
	oi := 0
	for {
		id, ke, haveEntry, ok := src.next()
		if !ok {
			if err := src.err(); err != nil {
				return r.examined, ordered, c.storeErr(err)
			}
			break
		}
		for ; oi < len(ov) && before(ov[oi], id); oi++ {
			if !r.take(s.overlay[ov[oi]]) {
				return r.examined, ordered, r.err
			}
		}
		doc, err := c.snapDoc(s, id, ke, haveEntry)
		if err != nil {
			return r.examined, ordered, err
		}
		if doc != nil && !r.take(doc) {
			return r.examined, ordered, r.err
		}
	}
	for ; oi < len(ov); oi++ {
		if !r.take(s.overlay[ov[oi]]) {
			break
		}
	}
	return r.examined, ordered, r.err
}

// findBySortWalk runs a query whose single sort key is driven by its
// index: the walk yields the stored documents in the sort order, and the
// overlay's candidates, sorted the same way, are merged in.
func (c *Collection) findBySortWalk(ctx context.Context, s *querySnap, cq *compiledQuery, opt *planOption, emit func(*Document) bool) (int, error) {
	h := &sortHeap{keys: cq.sort}
	var ov []*sortItem
	for _, id := range s.overlayIDs(opt.cand) {
		ov = append(ov, newSortItem(s.overlay[id], false, cq.sortPaths))
	}
	slices.SortFunc(ov, h.cmp)
	src, err := opt.open(ctx, s, cq.sort[0].Order == orderDesc)
	if err != nil {
		return 0, err
	}
	r := &queryRun{ctx: ctx, cq: cq, emit: emit}
	oi := 0
	for {
		id, ke, haveEntry, ok := src.next()
		if !ok {
			if err := src.err(); err != nil {
				return r.examined, c.storeErr(err)
			}
			break
		}
		doc, err := c.snapDoc(s, id, ke, haveEntry)
		if err != nil {
			return r.examined, err
		}
		if doc == nil {
			continue
		}
		it := newSortItem(doc, false, cq.sortPaths)
		for ; oi < len(ov) && h.cmp(ov[oi], it) < 0; oi++ {
			if !r.take(ov[oi].doc) {
				return r.examined, r.err
			}
		}
		if !r.take(doc) {
			return r.examined, r.err
		}
	}
	for ; oi < len(ov); oi++ {
		if !r.take(ov[oi].doc) {
			break
		}
	}
	return r.examined, r.err
}

// findByHeap runs a query with a sort the driver does not provide: it
// reads every candidate, keeps the best skip+limit matches (all of them
// without a limit) in a bounded heap, sorts them, pages, and emits.
func (c *Collection) findByHeap(ctx context.Context, s *querySnap, cq *compiledQuery, opt *planOption, emit func(*Document) bool) (int, error) {
	keep := 0
	if cq.limit > 0 {
		keep = cq.skip + cq.limit
	}
	h := &sortHeap{keys: cq.sort}
	examined, err := c.eachCandidate(ctx, s, opt, func(doc *Document) bool {
		if !cq.matches(doc) {
			return true
		}
		it := newSortItem(doc, false, cq.sortPaths)
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
		return examined, err
	}
	items := h.items
	slices.SortFunc(items, h.cmp)
	for i := cq.skip; i < len(items); i++ {
		if !emit(project(items[i].doc, items[i].owned, cq.fieldPaths)) {
			break
		}
	}
	return examined, nil
}

// eachCandidate calls fn with every candidate of opt in s, in no
// particular order: the stored ones, then the overlay's, until fn returns
// false. The documents are shared: fn must neither modify nor keep them
// without a Clone. It returns the number of candidates read.
func (c *Collection) eachCandidate(ctx context.Context, s *querySnap, opt *planOption, fn func(*Document) bool) (int, error) {
	if opt.never {
		return 0, nil
	}
	src, err := opt.open(ctx, s, false)
	if err != nil {
		return 0, err
	}
	examined := 0
	for {
		if examined%findCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return examined, err
			}
		}
		id, ke, haveEntry, ok := src.next()
		if !ok {
			if err := src.err(); err != nil {
				return examined, c.storeErr(err)
			}
			break
		}
		doc, err := c.snapDoc(s, id, ke, haveEntry)
		if err != nil {
			return examined, err
		}
		if doc == nil {
			continue
		}
		examined++
		if !fn(doc) {
			return examined, nil
		}
	}
	for _, id := range s.overlayIDs(opt.cand) {
		examined++
		if !fn(s.overlay[id]) {
			break
		}
	}
	return examined, nil
}

// sortItem is a matching document with its sort values extracted.
type sortItem struct {
	doc   *Document
	owned bool
	vals  []sortValue
}

func newSortItem(doc *Document, owned bool, paths []fieldPath) *sortItem {
	it := &sortItem{doc: doc, owned: owned, vals: make([]sortValue, len(paths))}
	for i := range paths {
		v, ok := paths[i].get(doc)
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
// copy when fields is empty. Unknown fields are ignored. A field resolved
// through a nested path (GetPath) is returned nested under that path:
// "meta.author" gives {"meta": {"author": ...}}; a top-level key equal to
// the whole name is returned under that key. A doc that is owned is
// returned as is when no projection applies.
func project(doc *Document, owned bool, fields []fieldPath) *Document {
	if len(fields) == 0 {
		if owned {
			return doc
		}
		return doc.Clone()
	}
	out := &Document{ID: doc.ID, Data: make(map[string]any, len(fields))}
	for i := range fields {
		f := &fields[i]
		if f.name == "_id" {
			continue // always returned as the id
		}
		if v, ok := doc.Data[f.name]; ok {
			out.Data[f.name] = deepCopy(v)
			continue
		}
		if v, ok := f.get(doc); ok {
			setPath(out.Data, f.parts, deepCopy(v))
		}
	}
	return out
}

// setPath stores v in m under the nested path parts, creating the objects
// on the way. An object already there (from another projected path, or a
// deep copy of a whole projected object) is extended; a value that is not
// an object is left as it is.
func setPath(m map[string]any, parts []string, v any) {
	last := len(parts) - 1
	for _, p := range parts[:last] {
		next, ok := m[p]
		if !ok {
			child := make(map[string]any, 1)
			m[p] = child
			m = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			return
		}
		m = child
	}
	m[parts[last]] = v
}

// CountDocuments returns the number of documents matching q's filters and
// OR groups (plan 004 §2.6; sort, fields, skip, and limit are ignored). A
// nil or empty query returns Count(). A single filter that an index can
// answer exactly (eq, has, in, exists false, gt, gte, lt, lte, starts_with
// on an indexed field, or eq or in on _id), with nothing else, is counted
// without reading documents; anything else reads the candidates of the
// plan Find would choose.
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
	cq, err := compileQuery(&Query{Filters: filters, Any: anyOf})
	if err != nil {
		return 0, 0, err
	}
	if cq.never {
		return 0, 0, nil
	}
	if len(cq.filters) == 0 && len(cq.anyOf) == 0 {
		c.mu.RLock()
		dropped := c.dropped
		c.mu.RUnlock()
		if dropped {
			return 0, 0, c.errDropped()
		}
		return c.Count(), 0, nil
	}
	for attempt := 0; ; attempt++ {
		n, examined, err := c.countOnce(ctx, cq, attempt < maxReadRetries/2)
		if errors.Is(err, errQueryRetry) {
			if attempt < maxReadRetries {
				continue
			}
			err = c.retryErr()
		}
		return n, examined, err
	}
}

// countOnce counts in one snapshot.
func (c *Collection) countOnce(ctx context.Context, cq *compiledQuery, useIndexes bool) (int, int, error) {
	s, err := c.openSnap(useIndexes)
	if err != nil {
		return 0, 0, err
	}
	defer s.close()
	if len(cq.anyOf) == 0 && len(cq.filters) == 1 {
		n, def, ok, err := c.countExact(s, &cq.filters[0])
		if err != nil {
			return 0, 0, err
		}
		if ok {
			s.close()
			if def != nil && !c.indexesUnchanged([]indexDef{*def}) {
				return 0, 0, errQueryRetry
			}
			return n, 0, nil
		}
	}
	opt, err := c.choosePlan(s, cq, false)
	if err != nil {
		return 0, 0, err
	}
	n := 0
	examined, err := c.eachCandidate(ctx, s, opt, func(doc *Document) bool {
		if cq.matches(doc) {
			n++
		}
		return true
	})
	if err != nil {
		return 0, examined, err
	}
	s.close()
	if !c.indexesUnchanged(opt.used) {
		return 0, examined, errQueryRetry
	}
	return n, examined, nil
}

// countExact counts the documents matching the single filter f without
// reading documents, when the key table or an index answers exactly: eq or
// in on _id; eq, has, in (on values that are not cut, see shortValue),
// exists false, and the ranges and starts_with of rangeOf on an indexed
// field. The entries of the overlay's ids are left out and the overlay's
// documents matched directly (the snapshot rule). def is the index used,
// if any.
func (c *Collection) countExact(s *querySnap, f *compiledFilter) (int, *indexDef, bool, error) {
	if f.field == "_id" {
		var ids []string
		switch f.op {
		case opEq:
			if f.val.kind == kindString {
				ids = []string{f.val.s}
			}
		case opIn:
			for _, v := range f.list {
				if v.kind == kindString {
					ids = append(ids, v.s)
				}
			}
		default:
			return 0, nil, false, nil
		}
		n := 0
		for _, id := range slices.Compact(slices.Sorted(slices.Values(ids))) {
			if doc, in := s.overlay[id]; in {
				if doc != nil {
					n++
				}
				continue
			}
			_, ok, err := s.tx.Key(id)
			if err != nil {
				return 0, nil, false, c.storeErr(err)
			}
			if ok {
				n++
			}
		}
		return n, nil, true, nil
	}
	def, ok := s.indexes[f.field]
	if !ok {
		return 0, nil, false, nil
	}
	var (
		ranges []valueRange
		flags  = flagsScalar
	)
	switch f.op {
	case opEq, opHas:
		enc, ok := shortValue(f.value)
		if !ok {
			return 0, nil, false, nil
		}
		ranges = []valueRange{valueRangeOf(enc)}
		if f.op == opHas {
			flags = flagsElement
		}
	case opIn:
		vals, _ := listValues(f.value)
		var encs [][]byte
		for _, v := range vals {
			enc, ok := shortValue(v)
			if !ok {
				return 0, nil, false, nil
			}
			encs = append(encs, enc)
		}
		slices.SortFunc(encs, bytes.Compare)
		for _, enc := range slices.CompactFunc(encs, bytes.Equal) {
			ranges = append(ranges, valueRangeOf(enc))
		}
	case opExists:
		if f.exists {
			return 0, nil, false, nil
		}
		ranges = []valueRange{valueRangeOf(missingValue)}
	case opGt, opGte, opLt, opLte, opStartsWith:
		r, ok := rangeOf(f)
		if !ok {
			return 0, nil, false, nil
		}
		ranges = []valueRange{r}
	default:
		return 0, nil, false, nil
	}
	n := 0
	for _, r := range ranges {
		cur, err := s.tx.IndexCursor(def.Name)
		if err != nil {
			return 0, nil, false, indexErr(err)
		}
		src := &rangeSource{cur: cur, r: r, flags: flags, skip: s.overlay}
		for {
			if _, _, _, ok := src.next(); !ok {
				break
			}
			n++
		}
		if err := src.err(); err != nil {
			return 0, nil, false, c.storeErr(err)
		}
	}
	n += s.overlayCount(f.match)
	return n, &def, true, nil
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
	cq, err := compileQuery(&Query{Filters: filters, Any: anyOf})
	if err != nil {
		return nil, 0, err
	}
	if len(cq.filters) == 0 && len(cq.anyOf) == 0 && !cq.never {
		vals, ok, err := c.distinctFast(field)
		if err != nil {
			return nil, 0, err
		}
		if ok {
			return sortDistinct(vals), 0, nil
		}
	}
	for attempt := 0; ; attempt++ {
		vals, examined, err := c.distinctOnce(ctx, field, cq, attempt < maxReadRetries/2)
		if errors.Is(err, errQueryRetry) {
			if attempt < maxReadRetries {
				continue
			}
			err = c.retryErr()
		}
		if err != nil {
			return nil, examined, err
		}
		return sortDistinct(vals), examined, nil
	}
}

// distinctOnce collects the distinct values in one snapshot, unsorted.
func (c *Collection) distinctOnce(ctx context.Context, field string, cq *compiledQuery, useIndexes bool) ([]any, int, error) {
	s, err := c.openSnap(useIndexes)
	if err != nil {
		return nil, 0, err
	}
	defer s.close()
	opt, err := c.choosePlan(s, cq, false)
	if err != nil {
		return nil, 0, err
	}
	seen := make(map[string]any)
	examined, err := c.eachCandidate(ctx, s, opt, func(doc *Document) bool {
		if cq.matches(doc) {
			addDistinctValues(seen, doc, field)
		}
		return true
	})
	if err != nil {
		return nil, examined, err
	}
	s.close()
	if !c.indexesUnchanged(opt.used) {
		return nil, examined, errQueryRetry
	}
	vals := make([]any, 0, len(seen))
	for _, v := range seen {
		vals = append(vals, v)
	}
	return vals, examined, nil
}

// distinctFast returns the distinct values of field without reading
// documents (every live id for _id, or the keys of an index on field),
// or false.
func (c *Collection) distinctFast(field string) ([]any, bool, error) {
	if field == "_id" {
		ids, err := c.liveIDs()
		if err != nil {
			return nil, false, err
		}
		out := make([]any, len(ids))
		for i, id := range ids {
			out[i] = id
		}
		return out, true, nil
	}
	c.mu.RLock()
	if c.dropped {
		c.mu.RUnlock()
		return nil, false, c.errDropped()
	}
	def, indexed := c.indexOnLocked(field)
	c.mu.RUnlock()
	if !indexed {
		return nil, false, nil
	}

	// The snapshot rule: the overlay first, then the read transaction. The
	// entries hold every value of the documents outside the overlay when
	// the index has no complex document and no truncated entry; the
	// overlay's documents contribute their own values.
	overlay, st, err := c.captureOverlay()
	if err != nil {
		return nil, false, err
	}
	for _, doc := range overlay {
		if indexKeysOf(doc, field).complex {
			// A value without an entry, as on disk: read the documents,
			// so the answer does not depend on what is flushed.
			return nil, false, nil
		}
	}
	seen := make(map[string]any)
	exact := false
	err = st.View(func(tx *idxstore.Tx) error {
		stats, ok, err := tx.IndexStats(def.Name)
		if err != nil || !ok || stats.Complex > 0 || stats.Truncated > 0 {
			return err
		}
		cur, err := tx.IndexCursor(def.Name)
		if err != nil {
			return err
		}
		var last []byte // the last value taken; valid within the transaction
		for e, ok := cur.First(); ok; e, ok = cur.Next() {
			if e.Value[0] == idxstore.TagMissing || (last != nil && bytes.Equal(e.Value, last)) {
				continue
			}
			if _, in := overlay[string(e.ID)]; in {
				continue
			}
			v, _, err := idxstore.DecodeValue(e.Value)
			if err != nil {
				return err
			}
			seen["v"+string(e.Value)] = v
			last = e.Value
		}
		if err := cur.Err(); err != nil {
			return err
		}
		exact = true
		return nil
	})
	switch {
	case errors.Is(err, idxstore.ErrNoIndex), err == nil && !c.indexesUnchanged([]indexDef{def}):
		return nil, false, nil // the index was dropped meanwhile: read the documents
	case err != nil:
		return nil, false, c.storeErr(err)
	case !exact:
		return nil, false, nil
	}
	for _, doc := range overlay {
		if doc != nil {
			addDistinctValues(seen, doc, field)
		}
	}
	vals := make([]any, 0, len(seen))
	for _, v := range seen {
		vals = append(vals, v)
	}
	return vals, true, nil
}

// addDistinctValues adds the values doc holds in field (each element of an
// array) to seen, keyed by distinctKey, copying the ones it adds.
func addDistinctValues(seen map[string]any, doc *Document, field string) {
	v, ok := doc.GetPath(field)
	if !ok {
		return
	}
	add := func(v any) {
		key := distinctKey(v)
		if _, dup := seen[key]; !dup {
			seen[key] = deepCopy(v)
		}
	}
	if elems, isList := listValues(v); isList {
		for _, el := range elems {
			add(el)
		}
		return
	}
	add(v)
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
