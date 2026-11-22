package db

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/hop-/cachydb/pkg/db/internal/idxstore"
)

// Query plans (approach-index-and-query §5). A query runs in one snapshot
// (querySnap): the overlay of unflushed changes captured under
// Collection.mu, then one read transaction on the index file. One driver
// produces the candidate ids of the documents in the index file, in _id
// order or in value order; the overlay's documents are candidates by the
// same rule (planOption.cand) and are merged in. Every candidate is
// re-checked against every filter, so a driver only has to return a
// superset of the matches.
//
// While a query holds its read transaction it opens no other transaction
// (bbolt would deadlock against a flush that has to grow the file) and
// holds no collection lock across it.

// Drivers, as QueryPlan.Driver reports them.
const (
	driverNone      = "none"
	driverIDs       = "id lookup"
	driverIndex     = "index"
	driverRange     = "index range"
	driverUnion     = "or union"
	driverIndexSort = "index sort"
	driverKeydir    = "keydir"
)

// QueryPlan describes how a query ran (Collection.FindWithPlan).
type QueryPlan struct {
	// Driver is where the candidates came from: "id lookup" (eq or in on
	// _id), "index" (the entries of one or several values of an index:
	// eq, has, in, has_any, has_all, exists false), "index range" (a range
	// of an index's values: gt, gte, lt, lte, starts_with, exists true),
	// "or union" (one index-served filter per Any group, united), "index
	// sort" (the sort field's index walked in the requested order),
	// "keydir" (every document, in _id order), or "none" (a filter that
	// can match nothing, so nothing was read).
	Driver string
	// Index names the index the driver read; several, comma-separated, for
	// an OR union. Empty for id lookup, keydir, and none.
	Index string
	// OrderProvided reports whether the driver produced the candidates in
	// the requested order, so that the query streamed and stopped once it
	// had skip+limit matches, without sorting.
	OrderProvided bool
	// Examined is the number of candidate documents read and checked
	// against the filters; Returned is the number of documents returned.
	Examined, Returned int
}

// planEstimateCap bounds the cursor count behind a size estimate: a
// filter whose entries reach it counts as "large".
const planEstimateCap = 1000

// errQueryRetry makes a query start over in a new snapshot: a segment it
// was about to read was retired by compaction, or an index it chose was
// dropped or re-created meanwhile.
var errQueryRetry = errors.New("query must be retried")

// querySnap is the snapshot a query runs in (see the top of this file).
type querySnap struct {
	overlay map[string]*Document // unflushed changes: id -> document, nil = deleted
	indexes map[string]indexDef  // field -> its index (the smallest name); nil without index use
	tx      *idxstore.Tx
}

// openSnap captures the overlay and the index definitions under mu, then
// starts the read transaction (the snapshot rule). Without useIndexes the
// query sees no index. The caller closes the snapshot.
func (c *Collection) openSnap(useIndexes bool) (*querySnap, error) {
	s := &querySnap{}
	c.mu.RLock()
	st, err := c.storeLocked()
	if err == nil {
		s.overlay = c.overlayLocked()
		if useIndexes && len(c.indexes) > 0 {
			s.indexes = make(map[string]indexDef, len(c.indexes))
			for _, def := range c.indexes {
				if cur, ok := s.indexes[def.Field]; !ok || def.Name < cur.Name {
					s.indexes[def.Field] = def
				}
			}
		}
	}
	c.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if st == nil {
		if st, err = c.openStoreOnUse(); err != nil {
			return nil, err
		}
	}
	if s.tx, err = st.Begin(); err != nil {
		return nil, c.storeErr(err)
	}
	return s, nil
}

// close ends the read transaction; it may be called more than once.
func (s *querySnap) close() {
	if s.tx != nil {
		_ = s.tx.Close() // a read transaction has nothing to lose
		s.tx = nil
	}
}

// overlayIDs returns, ascending, the ids of the overlay's live documents
// for which cand holds.
func (s *querySnap) overlayIDs(cand func(*Document) bool) []string {
	var ids []string
	for id, doc := range s.overlay {
		if doc != nil && cand(doc) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// overlayCount returns the number of the overlay's live documents for
// which cand holds.
func (s *querySnap) overlayCount(cand func(*Document) bool) int {
	n := 0
	for _, doc := range s.overlay {
		if doc != nil && cand(doc) {
			n++
		}
	}
	return n
}

// doc reads the version of id that the snapshot's key table holds (ke when
// the caller has it), or nil if it holds none.
func (c *Collection) snapDoc(s *querySnap, id string, ke idxstore.Entry, haveEntry bool) (*Document, error) {
	if !haveEntry {
		var (
			ok  bool
			err error
		)
		if ke, ok, err = s.tx.Key(id); err != nil {
			return nil, c.storeErr(err)
		}
		if !ok {
			return nil, nil
		}
	}
	doc, err := c.readStored(id, ke)
	if errors.Is(err, errSegmentRetired) {
		return nil, errQueryRetry // compaction moved it after the snapshot
	}
	return doc, err
}

// ---- id sources ----

// idSource yields the ids of documents in the index file, never one that
// is in the overlay.
type idSource interface {
	// next returns the next id, with its key-table entry when the source
	// has it (haveEntry); ok is false at the end.
	next() (id string, ke idxstore.Entry, haveEntry, ok bool)
	// err reports the damage that ended the source early, if any.
	err() error
}

// keySource walks the key table in _id order, ascending or descending.
type keySource struct {
	cur           *idxstore.KeyCursor
	skip          map[string]*Document
	desc          bool
	started, done bool
}

func (s *keySource) next() (string, idxstore.Entry, bool, bool) {
	for !s.done {
		var (
			k  []byte
			ke idxstore.Entry
			ok bool
		)
		switch {
		case !s.started && s.desc:
			k, ke, ok = s.cur.Last()
		case !s.started:
			k, ke, ok = s.cur.First()
		case s.desc:
			k, ke, ok = s.cur.Prev()
		default:
			k, ke, ok = s.cur.Next()
		}
		s.started = true
		if !ok {
			s.done = true
			break
		}
		if _, in := s.skip[string(k)]; in {
			continue
		}
		return string(k), ke, true, true
	}
	return "", idxstore.Entry{}, false, false
}

func (s *keySource) err() error { return s.cur.Err() }

// listSource yields ids from a sorted list, ascending or descending.
type listSource struct {
	ids  []string
	skip map[string]*Document
	desc bool
	i    int
}

func (s *listSource) next() (string, idxstore.Entry, bool, bool) {
	for s.i < len(s.ids) {
		id := s.ids[s.i]
		if s.desc {
			id = s.ids[len(s.ids)-1-s.i]
		}
		s.i++
		if _, in := s.skip[id]; in {
			continue
		}
		return id, idxstore.Entry{}, false, true
	}
	return "", idxstore.Entry{}, false, false
}

func (s *listSource) err() error { return nil }

// entryFlags selects index entries by FlagElement.
type entryFlags uint8

const (
	flagsAny     entryFlags = iota
	flagsScalar             // whole values (eq, in, ranges, starts_with)
	flagsElement            // array elements (has, has_any, has_all)
)

func (m entryFlags) accepts(f idxstore.Flags) bool {
	switch m {
	case flagsScalar:
		return f&idxstore.FlagElement == 0
	case flagsElement:
		return f&idxstore.FlagElement != 0
	}
	return true
}

// valueRange is a range of encoded values of an index. A single value is
// the range [v, v].
type valueRange struct {
	lo, hi         []byte // encoded bounds; nil = open
	loIncl, hiIncl bool
	tag            byte   // non-zero: values of this tag only (numbers or strings)
	prefix         []byte // non-nil: values that start with it (starts_with)
	notMissing     bool   // values other than "missing" (exists true)
}

// valueRangeOf returns the range [enc, enc].
func valueRangeOf(enc []byte) valueRange {
	return valueRange{lo: enc, hi: enc, loIncl: true, hiIncl: true}
}

// below reports whether the value v lies before the range.
func (r *valueRange) below(v []byte) bool {
	switch {
	case r.tag != 0 && v[0] < r.tag, r.notMissing && v[0] == idxstore.TagMissing:
		return true
	case r.prefix != nil && bytes.Compare(v, r.prefix) < 0:
		return true
	case r.lo != nil:
		c := bytes.Compare(v, r.lo)
		return c < 0 || c == 0 && !r.loIncl
	}
	return false
}

// above reports whether the value v lies after the range.
func (r *valueRange) above(v []byte) bool {
	switch {
	case r.tag != 0 && v[0] > r.tag:
		return true
	case r.prefix != nil && !bytes.HasPrefix(v, r.prefix) && bytes.Compare(v, r.prefix) > 0:
		return true
	case r.hi != nil:
		c := bytes.Compare(v, r.hi)
		return c > 0 || c == 0 && !r.hiIncl
	}
	return false
}

// keys returns the index keys that bound the range: from, the smallest key
// that can be in it (nil = the first), and to, an exclusive upper bound
// (nil = past the last).
func (r *valueRange) keys() (from, to []byte) {
	switch {
	case r.lo != nil && r.loIncl:
		from = idxstore.ValuePrefix(r.lo)
	case r.lo != nil:
		from = idxstore.PrefixEnd(idxstore.ValuePrefix(r.lo))
	case r.prefix != nil:
		from = r.prefix
	case r.tag != 0:
		from = []byte{r.tag}
	case r.notMissing:
		from = idxstore.PrefixEnd(idxstore.ValuePrefix(missingValue))
	}
	switch {
	case r.hi != nil && r.hiIncl:
		to = idxstore.PrefixEnd(idxstore.ValuePrefix(r.hi))
	case r.hi != nil:
		to = idxstore.ValuePrefix(r.hi)
	case r.prefix != nil:
		to = idxstore.PrefixEnd(r.prefix)
	case r.tag != 0:
		to = []byte{r.tag + 1}
	}
	return from, to
}

// first moves cur to the first entry in the range.
func (r *valueRange) first(cur *idxstore.IndexCursor) (idxstore.IndexEntry, bool) {
	from, _ := r.keys()
	var (
		e  idxstore.IndexEntry
		ok bool
	)
	if from == nil {
		e, ok = cur.First()
	} else {
		e, ok = cur.Seek(from)
	}
	for ok && r.below(e.Value) {
		e, ok = cur.Next()
	}
	if ok && r.above(e.Value) {
		return idxstore.IndexEntry{}, false
	}
	return e, ok
}

// last moves cur to the last entry in the range.
func (r *valueRange) last(cur *idxstore.IndexCursor) (idxstore.IndexEntry, bool) {
	_, to := r.keys()
	var (
		e  idxstore.IndexEntry
		ok bool
	)
	if to == nil {
		e, ok = cur.Last()
	} else {
		e, ok = cur.SeekBefore(to)
	}
	for ok && r.above(e.Value) {
		e, ok = cur.Prev()
	}
	if ok && r.below(e.Value) {
		return idxstore.IndexEntry{}, false
	}
	return e, ok
}

// estimate counts the range's entries up to planEstimateCap.
func (r *valueRange) estimate(cur *idxstore.IndexCursor) int {
	from, to := r.keys()
	return cur.Count(from, to, planEstimateCap)
}

// rangeSource walks the entries of a value range, ascending (by value,
// then _id) or descending (value groups from the last, each group in
// ascending _id order, so that ties keep the engine's tie-break).
type rangeSource struct {
	cur           *idxstore.IndexCursor
	r             valueRange
	flags         entryFlags
	skip          map[string]*Document
	desc          bool
	started, done bool
	group         []byte // descending: the value of the group being walked
}

func (s *rangeSource) next() (string, idxstore.Entry, bool, bool) {
	for !s.done {
		e, ok := s.step()
		if !ok {
			s.done = true
			break
		}
		if !s.flags.accepts(e.Flags) {
			continue
		}
		if _, in := s.skip[string(e.ID)]; in {
			continue
		}
		return string(e.ID), idxstore.Entry{}, false, true
	}
	return "", idxstore.Entry{}, false, false
}

// step moves to the next entry of the range in the walk's order.
func (s *rangeSource) step() (idxstore.IndexEntry, bool) {
	if !s.desc {
		if !s.started {
			s.started = true
			return s.r.first(s.cur)
		}
		e, ok := s.cur.Next()
		if !ok || s.r.above(e.Value) {
			return idxstore.IndexEntry{}, false
		}
		return e, true
	}
	if !s.started {
		s.started = true
		return s.enterGroup(s.r.last(s.cur))
	}
	if e, ok := s.cur.Next(); ok && bytes.Equal(e.Value, s.group) {
		return e, true
	}
	// The group is done: on to the last entry of the group before it.
	return s.enterGroup(s.cur.SeekBefore(idxstore.ValuePrefix(s.group)))
}

// enterGroup takes e, the last entry of a value group (ok false: there is
// none), and moves to the first entry of that group.
func (s *rangeSource) enterGroup(e idxstore.IndexEntry, ok bool) (idxstore.IndexEntry, bool) {
	if !ok || s.r.below(e.Value) {
		return idxstore.IndexEntry{}, false
	}
	s.group = e.Value // valid until the transaction ends
	return s.cur.Seek(idxstore.ValuePrefix(s.group))
}

func (s *rangeSource) err() error { return s.cur.Err() }

// mergeSource unites (or intersects) sources that yield ascending ids,
// keeping the order and yielding each id once.
type mergeSource struct {
	subs      []idSource
	intersect bool
	heads     []string
	alive     []bool
	started   bool
}

func (m *mergeSource) advance(i int) { m.heads[i], _, _, m.alive[i] = m.subs[i].next() }

func (m *mergeSource) next() (string, idxstore.Entry, bool, bool) {
	if !m.started {
		m.started = true
		m.heads = make([]string, len(m.subs))
		m.alive = make([]bool, len(m.subs))
		for i := range m.subs {
			m.advance(i)
		}
	}
	if m.intersect {
		return m.nextCommon()
	}
	least := -1
	for i, ok := range m.alive {
		if ok && (least < 0 || m.heads[i] < m.heads[least]) {
			least = i
		}
	}
	if least < 0 {
		return "", idxstore.Entry{}, false, false
	}
	id := m.heads[least]
	for i, ok := range m.alive {
		if ok && m.heads[i] == id {
			m.advance(i)
		}
	}
	return id, idxstore.Entry{}, false, true
}

// nextCommon returns the next id that every source yields.
func (m *mergeSource) nextCommon() (string, idxstore.Entry, bool, bool) {
	for {
		greatest := ""
		for i, ok := range m.alive {
			if !ok {
				return "", idxstore.Entry{}, false, false
			}
			greatest = max(greatest, m.heads[i])
		}
		same := true
		for i := range m.subs {
			for m.alive[i] && m.heads[i] < greatest {
				m.advance(i)
			}
			if !m.alive[i] {
				return "", idxstore.Entry{}, false, false
			}
			same = same && m.heads[i] == greatest
		}
		if same {
			for i := range m.subs {
				m.advance(i)
			}
			return greatest, idxstore.Entry{}, false, true
		}
	}
}

func (m *mergeSource) err() error {
	for _, s := range m.subs {
		if err := s.err(); err != nil {
			return err
		}
	}
	return nil
}

// collectIDs drains src and returns its ids sorted and without duplicates.
func collectIDs(ctx context.Context, src idSource) ([]string, error) {
	var ids []string
	for {
		if len(ids)%(16*findCtxCheckEvery) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		id, _, _, ok := src.next()
		if !ok {
			break
		}
		ids = append(ids, id)
	}
	if err := src.err(); err != nil {
		return nil, err
	}
	slices.Sort(ids)
	return slices.Compact(ids), nil
}

// ---- plan options ----

// planOption is one way to produce a query's candidates.
type planOption struct {
	driver   string
	index    []string   // index names, for QueryPlan
	used     []indexDef // indexes that must still exist on the same fields afterwards
	est      int        // estimated number of candidates (stored entries capped, plus the overlay's)
	byValue  bool       // the source yields value order, not _id order
	never    bool       // nothing can match: no source
	sortWalk bool       // the source yields the query's sort order (driverIndexSort)
	// ranged is the range or starts_with filter whose value range of one
	// index this option walks (driverRange), or nil.
	ranged *compiledFilter
	// open builds the source on the snapshot's transaction; desc asks a
	// key-table walk, an id list, or an index sort walk for the reverse
	// order.
	open func(ctx context.Context, s *querySnap, desc bool) (idSource, error)
	// cand is the rule for the overlay's documents: the documents that
	// would be candidates were they in the index file.
	cand func(doc *Document) bool
}

func allDocs(*Document) bool { return true }

// keydirOption walks every document.
func keydirOption() *planOption {
	return &planOption{
		driver: driverKeydir,
		est:    -1, // not compared
		open: func(_ context.Context, s *querySnap, desc bool) (idSource, error) {
			return &keySource{cur: s.tx.KeyCursor(), skip: s.overlay, desc: desc}, nil
		},
		cand: allDocs,
	}
}

// idsOption looks the ids up directly.
func idsOption(ids []string) *planOption {
	ids = slices.Compact(slices.Sorted(slices.Values(ids)))
	return &planOption{
		driver: driverIDs,
		est:    len(ids),
		open: func(_ context.Context, s *querySnap, desc bool) (idSource, error) {
			return &listSource{ids: ids, skip: s.overlay, desc: desc}, nil
		},
		cand: func(doc *Document) bool {
			_, found := slices.BinarySearch(ids, doc.ID)
			return found
		},
	}
}

// indexProbe is one value group of an index: the entries of enc with the
// given element flag.
type indexProbe struct {
	enc     []byte
	element bool
}

// probeOption takes the candidates from the value groups probes of the
// index def: their union, or their intersection when all is set.
func probeOption(s *querySnap, def indexDef, probes []indexProbe, all bool) (*planOption, error) {
	cur, err := s.tx.IndexCursor(def.Name)
	if err != nil {
		return nil, indexErr(err)
	}
	est := 0
	for i, p := range probes {
		r := valueRangeOf(p.enc)
		n := r.estimate(cur)
		switch {
		case !all:
			est = min(est+n, planEstimateCap)
		case i == 0 || n < est:
			est = n
		}
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	field := def.Field
	cand := func(doc *Document) bool {
		keys := indexKeysOf(doc, field)
		for _, p := range probes {
			found := slices.ContainsFunc(keys.entries, func(e indexEntry) bool {
				return (e.flags&idxstore.FlagElement != 0) == p.element && bytes.Equal(e.enc, p.enc)
			})
			if found != all {
				return found
			}
		}
		return all
	}
	return &planOption{
		driver: driverIndex,
		index:  []string{def.Name},
		used:   []indexDef{def},
		est:    est + s.overlayCount(cand),
		open: func(_ context.Context, s *querySnap, _ bool) (idSource, error) {
			subs := make([]idSource, len(probes))
			for i, p := range probes {
				cur, err := s.tx.IndexCursor(def.Name)
				if err != nil {
					return nil, indexErr(err)
				}
				flags := flagsScalar
				if p.element {
					flags = flagsElement
				}
				subs[i] = &rangeSource{cur: cur, r: valueRangeOf(p.enc), flags: flags, skip: s.overlay}
			}
			if len(subs) == 1 {
				return subs[0], nil
			}
			return &mergeSource{subs: subs, intersect: all}, nil
		},
		cand: cand,
	}, nil
}

// rangeOption takes the candidates from a value range of the index def, in
// value order. The overlay's documents are candidates when they match f.
func rangeOption(s *querySnap, def indexDef, r valueRange, flags entryFlags, f *compiledFilter) (*planOption, error) {
	cur, err := s.tx.IndexCursor(def.Name)
	if err != nil {
		return nil, indexErr(err)
	}
	est := r.estimate(cur)
	if err := cur.Err(); err != nil {
		return nil, err
	}
	cand := f.match
	return &planOption{
		driver:  driverRange,
		index:   []string{def.Name},
		used:    []indexDef{def},
		est:     est + s.overlayCount(cand),
		byValue: true,
		open: func(_ context.Context, s *querySnap, _ bool) (idSource, error) {
			cur, err := s.tx.IndexCursor(def.Name)
			if err != nil {
				return nil, indexErr(err)
			}
			return &rangeSource{cur: cur, r: r, flags: flags, skip: s.overlay}, nil
		},
		cand: cand,
	}, nil
}

// indexErr maps a missing index bucket (dropped since the plan chose the
// index) to a retry.
func indexErr(err error) error {
	if errors.Is(err, idxstore.ErrNoIndex) {
		return errQueryRetry
	}
	return err
}

// shortValue encodes v for a range bound or a prefix when every truncated
// entry compares with it exactly as its whole string would: v is indexable,
// not cut, and shorter than any cut value (MaxValueLen-1 bytes).
func shortValue(v any) ([]byte, bool) {
	enc, truncated, ok := idxstore.AppendValue(nil, v)
	if !ok || truncated || len(enc) >= idxstore.MaxValueLen-1 {
		return nil, false
	}
	return enc, true
}

// rangeOf returns the value range of the whole values a range filter (gt,
// gte, lt, lte) or starts_with can match, and false when the index cannot
// serve it exactly enough (see shortValue).
func rangeOf(f *compiledFilter) (valueRange, bool) {
	if f.op == opStartsWith {
		enc, ok := shortValue(f.s)
		if !ok {
			return valueRange{}, false
		}
		return valueRange{prefix: enc}, true
	}
	if !f.ordered {
		return valueRange{}, false
	}
	var (
		enc []byte
		ok  bool
		r   valueRange
	)
	if f.num {
		enc, ok = shortValue(f.n)
		r.tag = idxstore.TagNumber
	} else {
		enc, ok = shortValue(f.s)
		r.tag = idxstore.TagString
	}
	if !ok {
		return valueRange{}, false
	}
	switch f.op {
	case opGt, opGte:
		r.lo, r.loIncl = enc, f.op == opGte
	case opLt, opLte:
		r.hi, r.hiIncl = enc, f.op == opLte
	default:
		return valueRange{}, false
	}
	return r, true
}

// filterOption returns the option of serving f from the primary key or an
// index, or nil when neither can.
func (c *Collection) filterOption(s *querySnap, f *compiledFilter) (*planOption, error) {
	if f.field == "_id" {
		switch f.op {
		case opEq:
			if f.val.kind == kindString {
				return idsOption([]string{f.val.s}), nil
			}
		case opIn:
			var ids []string
			for _, v := range f.list {
				if v.kind == kindString {
					ids = append(ids, v.s)
				}
			}
			return idsOption(ids), nil
		}
		return nil, nil
	}
	def, ok := s.indexes[f.field]
	if !ok {
		return nil, nil
	}
	switch f.op {
	case opEq, opHas:
		enc, _, ok := idxstore.AppendValue(nil, f.value)
		if !ok {
			return nil, nil // such a value can only equal one without an entry
		}
		return probeOption(s, def, []indexProbe{{enc: enc, element: f.op == opHas}}, false)
	case opIn, opHasAny, opHasAll:
		vals, _ := listValues(f.value)
		var probes []indexProbe
		for _, v := range vals {
			enc, _, ok := idxstore.AppendValue(nil, v)
			if !ok {
				if f.op == opHasAll {
					continue // a superset without it
				}
				return nil, nil // it could match a value without an entry
			}
			if !slices.ContainsFunc(probes, func(p indexProbe) bool { return bytes.Equal(p.enc, enc) }) {
				probes = append(probes, indexProbe{enc: enc, element: f.op != opIn})
			}
		}
		if len(probes) == 0 {
			return nil, nil
		}
		return probeOption(s, def, probes, f.op == opHasAll)
	case opExists:
		if !f.exists {
			return probeOption(s, def, []indexProbe{{enc: missingValue}}, false)
		}
		// Every document with the field has a non-missing entry only when
		// no value is complex or an array (an empty array has no entry).
		stats, ok, err := s.tx.IndexStats(def.Name)
		if err != nil || !ok || stats.Complex > 0 || stats.Arrays > 0 {
			return nil, err
		}
		return rangeOption(s, def, valueRange{notMissing: true}, flagsAny, f)
	case opGt, opGte, opLt, opLte, opStartsWith:
		r, ok := rangeOf(f)
		if !ok {
			return nil, nil
		}
		o, err := rangeOption(s, def, r, flagsScalar, f)
		if o != nil {
			o.ranged = f
		}
		return o, err
	}
	return nil, nil
}

// unionOption serves the OR groups when every group has a filter that can
// be served: the union of each group's best option. It returns nil when
// some group has none.
func (c *Collection) unionOption(s *querySnap, groups [][]compiledFilter) (*planOption, error) {
	var subs []*planOption
	for _, g := range groups {
		var best *planOption
		for i := range g {
			o, err := c.filterOption(s, &g[i])
			if err != nil {
				return nil, err
			}
			if better(o, best) {
				best = o
			}
		}
		if best == nil {
			return nil, nil
		}
		subs = append(subs, best)
	}
	u := &planOption{driver: driverUnion}
	for _, o := range subs {
		u.est = min(u.est+o.est, planEstimateCap)
		u.used = append(u.used, o.used...)
		for _, name := range o.index {
			if !slices.Contains(u.index, name) {
				u.index = append(u.index, name)
			}
		}
	}
	u.cand = func(doc *Document) bool {
		return slices.ContainsFunc(subs, func(o *planOption) bool { return o.cand(doc) })
	}
	u.open = func(ctx context.Context, s *querySnap, _ bool) (idSource, error) {
		srcs := make([]idSource, len(subs))
		for i, o := range subs {
			src, err := o.open(ctx, s, false)
			if err != nil {
				return nil, err
			}
			if o.byValue {
				ids, err := collectIDs(ctx, src)
				if err != nil {
					return nil, err
				}
				src = &listSource{ids: ids}
			}
			srcs[i] = src
		}
		return &mergeSource{subs: srcs}, nil
	}
	return u, nil
}

// better reports whether option o beats best: an id lookup beats any
// other driver, otherwise the smaller estimate wins.
func better(o, best *planOption) bool {
	switch {
	case o == nil:
		return false
	case best == nil:
		return true
	case (o.driver == driverIDs) != (best.driver == driverIDs):
		return o.driver == driverIDs
	}
	return o.est < best.est
}

// sortOption returns the option of walking the index of the sort key's
// field in the sort order, or nil. The index must give every document
// exactly one entry, ordered as Find sorts: no complex value, no array,
// and no truncated entry (approach-index-and-query §3, task 05 Amendment
// 1). Range and starts_with filters on the same field bound the walk.
func (c *Collection) sortOption(s *querySnap, key SortKey, filters []compiledFilter) (*planOption, error) {
	def, ok := s.indexes[key.Field]
	if !ok || key.Field == "_id" {
		return nil, nil
	}
	stats, ok, err := s.tx.IndexStats(def.Name)
	if err != nil || !ok || stats.Complex > 0 || stats.Truncated > 0 || stats.Arrays > 0 {
		return nil, err
	}
	var (
		r        valueRange
		bounding []compiledFilter
	)
	for i := range filters {
		f := &filters[i]
		if f.field != key.Field {
			continue
		}
		fr, ok := rangeOf(f)
		if !ok {
			continue
		}
		switch {
		case len(bounding) == 0:
			r = fr
		case fr.prefix == nil && r.prefix == nil && fr.tag == r.tag && fr.lo != nil && r.lo == nil:
			r.lo, r.loIncl = fr.lo, fr.loIncl
		case fr.prefix == nil && r.prefix == nil && fr.tag == r.tag && fr.hi != nil && r.hi == nil:
			r.hi, r.hiIncl = fr.hi, fr.hiIncl
		default:
			continue
		}
		bounding = append(bounding, *f)
	}
	cand := allDocs
	if len(bounding) > 0 {
		cand = func(doc *Document) bool { return matchAll(doc, bounding) }
	}
	return &planOption{
		driver:   driverIndexSort,
		index:    []string{def.Name},
		used:     []indexDef{def},
		est:      -1, // not compared
		byValue:  true,
		sortWalk: true,
		open: func(_ context.Context, s *querySnap, desc bool) (idSource, error) {
			cur, err := s.tx.IndexCursor(def.Name)
			if err != nil {
				return nil, indexErr(err)
			}
			return &rangeSource{cur: cur, r: r, flags: flagsAny, skip: s.overlay, desc: desc}, nil
		},
		cand: cand,
	}, nil
}

// choosePlan picks the driver of cq in snapshot s. Among the filters (and
// the OR groups as a whole) that the primary key or an index can serve,
// the smallest estimate wins, an id lookup always. For Find with a single
// sort key whose index can drive the sort, the index sort walk wins when
// no filter is served, when the best filter is a range or starts_with on
// the sort key's field (the walk is then bounded by it, so it reads the
// same entries in the requested order and stops at skip + limit), or when
// the query has a limit and the best filter is large. Without any of
// these, the key table is walked.
func (c *Collection) choosePlan(s *querySnap, cq *compiledQuery, forFind bool) (*planOption, error) {
	if cq.never {
		return &planOption{driver: driverNone, never: true}, nil
	}
	var best *planOption
	for i := range cq.filters {
		o, err := c.filterOption(s, &cq.filters[i])
		if err != nil {
			return nil, err
		}
		if better(o, best) {
			best = o
		}
	}
	if len(cq.anyOf) > 0 && (best == nil || best.driver != driverIDs) {
		o, err := c.unionOption(s, cq.anyOf)
		if err != nil {
			return nil, err
		}
		if better(o, best) {
			best = o
		}
	}
	if forFind && len(cq.sort) == 1 {
		walk, bounds := false, cq.filters
		switch {
		case best != nil && best.ranged != nil && best.ranged.field == cq.sort[0].Field:
			// The best filter goes first, so that it bounds the walk
			// whatever the other filters on the field are.
			walk = true
			bounds = make([]compiledFilter, 0, len(cq.filters))
			bounds = append(bounds, *best.ranged)
			for i := range cq.filters {
				if &cq.filters[i] != best.ranged {
					bounds = append(bounds, cq.filters[i])
				}
			}
		case best == nil, cq.limit > 0 && best.driver != driverIDs && best.est >= planEstimateCap:
			walk = true
		}
		if walk {
			o, err := c.sortOption(s, cq.sort[0], bounds)
			if err != nil {
				return nil, err
			}
			if o != nil {
				return o, nil
			}
		}
	}
	if best == nil {
		return keydirOption(), nil
	}
	return best, nil
}

// candidates returns, sorted, the ids a query with filters would examine:
// the ids of the chosen driver, plus the overlay's documents that are
// candidates by the same rule (every live id when no filter is served).
func (c *Collection) candidates(filters []QueryFilter) ([]string, error) {
	cfs, err := compileFilters(filters)
	if err != nil {
		return nil, err
	}
	cq := &compiledQuery{filters: cfs}
	cq.finish()
	for attempt := 0; ; attempt++ {
		ids, err := c.candidatesOnce(cq, attempt < maxReadRetries/2)
		if errors.Is(err, errQueryRetry) && attempt < maxReadRetries {
			continue
		}
		return ids, err
	}
}

func (c *Collection) candidatesOnce(cq *compiledQuery, useIndexes bool) ([]string, error) {
	s, err := c.openSnap(useIndexes)
	if err != nil {
		return nil, err
	}
	defer s.close()
	opt, err := c.choosePlan(s, cq, false)
	if err != nil || opt.never {
		return nil, err
	}
	ctx := context.Background()
	src, err := opt.open(ctx, s, false)
	if err != nil {
		return nil, err
	}
	ids, err := collectIDs(ctx, src)
	if err != nil {
		return nil, c.storeErr(err)
	}
	s.close()
	if !c.indexesUnchanged(opt.used) {
		return nil, errQueryRetry
	}
	return mergeSorted(ids, s.overlayIDs(opt.cand)), nil
}
