package db

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hop-/cachydb/pkg/db/internal/wal"
)

// Atomic batches (plan 013, approach-toolchain-and-storage §3.1).
//
// Collection.Batch writes many document changes of one collection as one
// WAL record (op 11, batch): one fsync, and all or nothing, also across a
// crash, since the record has one CRC and a torn record is removed whole by
// WAL recovery. Every item gets the record's offset as its version, so an
// id may appear only once per batch ("last writer wins by WAL offset" still
// orders every pair of versions). The items are flushed as ordinary puts
// and tombstones; replay applies each item whose id has an older version.
//
// Write: build every operation like a single write (docWrite) →
// backpressure → Database.mu (R) → the document stripe locks of all ids,
// ascending, each once → read the current versions, build and validate the
// after-images → unique values: checked against each other, then their
// stripes locked (ascending) and checked against the overlay and the index
// file, leaving out the batch's own documents → register inflight once →
// one WAL append (the commit point) → hookAfterWALSync → apply every item
// under one Collection.mu section. A single write holds one stripe lock, so
// taking several in ascending order cannot deadlock with it.

// OpKind is the kind of a batch operation.
type OpKind uint8

// Batch operation kinds. The zero value is not a valid kind.
const (
	OpInsert  OpKind = iota + 1 // insert Document (Insert)
	OpUpdate                    // apply Update to the document ID (UpdateWith)
	OpReplace                   // replace the document ID by Document (Replace)
	OpDelete                    // delete the document ID (DeleteWith)
)

// String returns the name of the kind ("insert", "update", ...).
func (k OpKind) String() string {
	switch k {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpReplace:
		return "replace"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("OpKind(%d)", uint8(k))
	}
}

// WriteOp is one operation of a batch. Each kind behaves as its single
// write: OpInsert as Insert, OpUpdate as UpdateWith, OpReplace as Replace,
// OpDelete as DeleteWith; $now values of the whole batch resolve to one
// time.
//
//   - OpInsert: Document is required. The id is ID, else Document.ID, else
//     a generated UUID v7; ID and Document.ID must agree when both are set.
//   - OpUpdate: ID and Update. The stored version must match both If and
//     Update.If.
//   - OpReplace: ID (else Document.ID) and Document.
//   - OpDelete: ID.
//
// If applies to OpUpdate, OpReplace, and OpDelete. A field the kind does
// not use must be left empty.
type WriteOp struct {
	Kind     OpKind
	ID       string
	Document *Document  // OpInsert, OpReplace
	Update   UpdateSpec // OpUpdate
	If       Condition  // OpUpdate, OpReplace, OpDelete
}

// BatchResult is the outcome of a successful Batch: per operation, in
// order, the document id (generated ones included) and whether an OpUpdate
// with Update.Upsert created the document.
type BatchResult struct {
	IDs      []string
	Upserted []bool
}

// BatchItemError reports the operation of a batch that failed; nothing of
// the batch was written. Err is that operation's error and wraps its
// sentinel (ErrExists, ErrNotFound, ErrConditionFailed,
// ErrSchemaValidation, ErrUniqueViolation, ...). An id that appears twice
// in a batch fails the second operation with ErrExists.
type BatchItemError struct {
	Index int // the operation's index in the batch
	Err   error
}

func (e *BatchItemError) Error() string { return fmt.Sprintf("batch operation %d: %v", e.Index, e.Err) }

// Unwrap returns the operation's error.
func (e *BatchItemError) Unwrap() error { return e.Err }

// batchItem is one operation of a batch being written.
type batchItem struct {
	op      int // the operation's index in the batch
	w       *docWrite
	cur     *Document // the version it replaces (nil if none); shared, read only
	curOff  uint64    // cur's WAL offset (0 if none)
	next    *Document // the stored after-image (nil for a delete)
	payload []byte    // next encoded (nil for a delete)
}

// Batch applies ops to the collection atomically, as one WAL record with
// one fsync: either every operation is applied and durable, or none is.
// An operation that fails (the document exists or is missing, a condition
// fails, the schema or a unique index rejects the result, or its id is
// also another operation's) fails the whole call with a *BatchItemError
// that names the operation and wraps its error. More operations than
// Options.MaxBatchDocs, or a payload larger than Options.MaxBatchBytes,
// fail with ErrBatchTooLarge. Inserts, updates, replaces, and deletes may
// be mixed. An empty batch writes nothing. ops and their documents are not
// modified or retained.
func (c *Collection) Batch(ctx context.Context, ops []WriteOp) (BatchResult, error) {
	opts := &c.db.e.opts
	if len(ops) > opts.MaxBatchDocs {
		return BatchResult{}, fmt.Errorf("%w: %d operations, the limit is %d (MaxBatchDocs); split the batch",
			ErrBatchTooLarge, len(ops), opts.MaxBatchDocs)
	}
	ts := nowString(time.Now())
	writes := make([]*docWrite, len(ops))
	index := make(map[string]int, len(ops)) // id → operation
	for i := range ops {
		w, err := c.prepareOp(&ops[i], ts)
		if err != nil {
			return BatchResult{}, &BatchItemError{Index: i, Err: err}
		}
		if j, dup := index[w.id]; dup {
			return BatchResult{}, &BatchItemError{Index: i, Err: fmt.Errorf(
				"document %q in collection %q is also operation %d of the batch; an id may appear once per batch: %w",
				w.id, c.name, j, ErrExists)}
		}
		index[w.id] = i
		writes[i] = w
	}
	if err := c.writeBatch(ctx, writes); err != nil {
		return BatchResult{}, err
	}
	res := BatchResult{IDs: make([]string, len(writes)), Upserted: make([]bool, len(writes))}
	for i, w := range writes {
		res.IDs[i], res.Upserted[i] = w.id, w.upserted
	}
	return res, nil
}

// UpdateMany applies the update u to every document that matches q, as
// one atomic batch (Batch), and returns how many documents matched and how
// many it changed (approach-toolchain-and-storage §3.1, "Writes by
// filter").
//
// Only q.Filters and q.Any apply; a query with Sort, Fields, Skip, or
// Limit is an error. A nil or empty query matches every document. u.Set
// and u.Unset work as in UpdateWith; u.If adds filters to the query (a
// document that does not match them is not matched); u.Upsert is an
// error. $now values resolve to one time.
//
// The matching ids are found on a snapshot, in ascending _id order. Each
// is then written only if, under its write lock, it still exists and
// matches the query; one that stopped matching (or was deleted) is
// skipped, not an error, so modified may be lower than matched. Every
// change is applied by one WAL record, so all of them are durable or none
// is.
//
// limit > 0 changes at most the first limit matching documents in _id
// order (matched then counts at most limit); a caller repeats the call to
// work through a larger set in atomic chunks. Without a limit (limit <= 0), or with a limit above
// Options.MaxBatchDocs, more than MaxBatchDocs matches fail with
// ErrBatchTooLarge, stating the match count (also returned as matched),
// and nothing is written. A batch whose payload exceeds MaxBatchBytes
// fails with ErrBatchTooLarge as well. A document the update would make
// invalid (schema, unique index) fails the whole call with that
// document's error.
func (c *Collection) UpdateMany(ctx context.Context, q *Query, u UpdateSpec, limit int) (matched, modified int, err error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return 0, 0, err
	}
	defer e.exit()
	if u.Upsert {
		return 0, 0, errors.New("updating documents: upsert is not supported by UpdateMany; use UpdateWith")
	}
	if p := updateFieldsProblem(u); p != "" {
		return 0, 0, fmt.Errorf("%w: updating documents: %s", ErrSchemaValidation, p)
	}
	if err := checkUpdateOps(&u, "updating documents"); err != nil {
		return 0, 0, err
	}
	wq, cq, err := compileWriteQuery(q, u.If, "updating documents")
	if err != nil {
		return 0, 0, err
	}
	ids, matched, err := c.matchingIDs(ctx, wq, cq, limit)
	if err != nil {
		return matched, 0, fmt.Errorf("updating documents: %w", err)
	}
	if err := e.hook(hookAfterMatch); err != nil {
		return matched, 0, err
	}
	ru := resolveNowUpdate(u, cq.now)
	writes := make([]*docWrite, len(ids))
	for i, id := range ids {
		writes[i] = &docWrite{id: id, build: func(cur *Document) (*Document, error) {
			if cur == nil || !cq.matches(cur) {
				return nil, errSkipWrite
			}
			data, err := applyUpdate(id, cur, &ru)
			if err != nil {
				return nil, err
			}
			return &Document{ID: id, Data: data}, nil
		}}
	}
	n, err := c.writeMatched(ctx, writes)
	return matched, n, err
}

// DeleteMany deletes every document that matches q, as one atomic batch
// (Batch), and returns how many documents matched and how many it deleted.
// The query, the snapshot, skipping documents that stopped matching, limit,
// and the limits work as in UpdateMany.
func (c *Collection) DeleteMany(ctx context.Context, q *Query, limit int) (matched, deleted int, err error) {
	e := c.db.e
	if err := e.enter(); err != nil {
		return 0, 0, err
	}
	defer e.exit()
	wq, cq, err := compileWriteQuery(q, nil, "deleting documents")
	if err != nil {
		return 0, 0, err
	}
	ids, matched, err := c.matchingIDs(ctx, wq, cq, limit)
	if err != nil {
		return matched, 0, fmt.Errorf("deleting documents: %w", err)
	}
	if err := e.hook(hookAfterMatch); err != nil {
		return matched, 0, err
	}
	writes := make([]*docWrite, len(ids))
	for i, id := range ids {
		writes[i] = &docWrite{id: id, build: func(cur *Document) (*Document, error) {
			if cur == nil || !cq.matches(cur) {
				return nil, errSkipWrite
			}
			return nil, nil
		}}
	}
	n, err := c.writeMatched(ctx, writes)
	return matched, n, err
}

// compileWriteQuery checks the query of UpdateMany or DeleteMany (what
// names the call in errors), adds the filters extra, and compiles it. It
// returns the query that was compiled and the compiled query.
func compileWriteQuery(q *Query, extra Condition, what string) (*Query, *compiledQuery, error) {
	if q == nil {
		q = &Query{}
	}
	if len(q.Sort) > 0 || len(q.Fields) > 0 || q.Skip != 0 || q.Limit != 0 {
		return nil, nil, fmt.Errorf("%s: only the query's filters and any apply; sort, fields, skip, and limit are not allowed (pass the limit to the call)", what)
	}
	wq := &Query{Filters: slices.Concat(q.Filters, extra), Any: q.Any}
	cq, err := compileQuery(wq)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", what, err)
	}
	return wq, cq, nil
}

// matchingIDs returns the ids of the documents matching cq (compiled from
// q), in ascending order, and their number: the first limit of them when
// 0 < limit <= MaxBatchDocs. Otherwise, when more than MaxBatchDocs
// documents match, it returns no ids, the match count, and an error
// wrapping ErrBatchTooLarge.
func (c *Collection) matchingIDs(ctx context.Context, q *Query, cq *compiledQuery, limit int) ([]string, int, error) {
	maxDocs := c.db.e.opts.MaxBatchDocs
	bounded := limit > 0 && limit <= maxDocs
	cq.limit = maxDocs + 1 // one more than allowed, to detect too many
	if bounded {
		cq.limit = limit
	}
	cq.fieldPaths = []fieldPath{compilePath("_id")} // read the ids only
	docs, _, err := c.runQuery(ctx, cq)
	if err != nil {
		return nil, 0, err
	}
	if !bounded && len(docs) > maxDocs {
		n, _, err := c.countDocuments(ctx, q)
		if err != nil {
			return nil, 0, err
		}
		return nil, n, fmt.Errorf("%w: %d documents match, more than %d (MaxBatchDocs); pass a limit of at most %d and repeat the call to change them in chunks",
			ErrBatchTooLarge, n, maxDocs, maxDocs)
	}
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	return ids, len(ids), nil
}

// writeMatched writes the batch of UpdateMany or DeleteMany and returns
// how many writes it applied (those not skipped). The error of a failing
// write is returned without its *BatchItemError wrapper: it names the
// document, and the write's position means nothing to the caller.
func (c *Collection) writeMatched(ctx context.Context, writes []*docWrite) (int, error) {
	if err := c.writeBatch(ctx, writes); err != nil {
		if bie, ok := errors.AsType[*BatchItemError](err); ok {
			return 0, bie.Err
		}
		return 0, err
	}
	n := 0
	for _, w := range writes {
		if !w.skipped {
			n++
		}
	}
	return n, nil
}

// prepareOp checks op and returns its write; $now values resolve to ts.
func (c *Collection) prepareOp(op *WriteOp, ts string) (*docWrite, error) {
	hasUpdate := len(op.Update.Set) > 0 || len(op.Update.Unset) > 0 || len(op.Update.If) > 0 || op.Update.Upsert || op.Update.hasOps()
	switch op.Kind {
	case OpInsert:
		if hasUpdate || len(op.If) > 0 {
			return nil, fmt.Errorf("%s: Update and If must be empty", op.Kind)
		}
		return c.prepareInsert(op.Document, op.ID, ts)
	case OpUpdate:
		if op.Document != nil {
			return nil, fmt.Errorf("%s: Document must be nil; use Update", op.Kind)
		}
		return c.prepareUpdate(op.ID, op.Update, op.If, ts)
	case OpReplace:
		if hasUpdate {
			return nil, fmt.Errorf("%s: Update must be empty; use Document and If", op.Kind)
		}
		id := op.ID
		if id == "" && op.Document != nil {
			id = op.Document.ID
		}
		return c.prepareReplace(id, op.Document, op.If, ts)
	case OpDelete:
		if op.Document != nil || hasUpdate {
			return nil, fmt.Errorf("%s: Document and Update must be empty", op.Kind)
		}
		return c.prepareDelete(op.ID, op.If, ts)
	default:
		return nil, fmt.Errorf("unknown operation kind %d", uint8(op.Kind))
	}
}

// errSkipWrite is returned by the build callback of a batch write that is
// to be left out of the batch instead of failing it: UpdateMany and
// DeleteMany return it for a document that no longer matches the query.
// No other build callback returns it.
var errSkipWrite = errors.New("document skipped")

// writeBatch runs the prepared writes as one batch (see the top of this
// file). Their ids are distinct. A write whose build returns errSkipWrite
// is left out and marked skipped; when every write is skipped, nothing is
// written. Once the WAL append has started, it waits for it even if ctx
// is cancelled, as write does.
func (c *Collection) writeBatch(ctx context.Context, writes []*docWrite) error {
	d, e := c.db, c.db.e
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(writes) == 0 {
		return nil
	}
	if err := e.waitPending(ctx); err != nil {
		return err
	}

	// Lock order (STORAGE.md): Database.mu (R) → docLocks (ascending) →
	// uniqueLocks (ascending) → Collection.mu.
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.dropped || c.dropped {
		return c.errDropped()
	}
	unlockDocs := c.lockDocs(writes)
	defer unlockDocs()

	items := make([]batchItem, 0, len(writes))
	size := wal.BatchPayloadSize(nil)
	for i, w := range writes {
		cur, curOff, err := c.currentVersion(w.id)
		if err != nil {
			return &BatchItemError{Index: i, Err: err}
		}
		next, err := w.build(cur)
		if errors.Is(err, errSkipWrite) {
			w.skipped = true
			continue
		}
		if err != nil {
			return &BatchItemError{Index: i, Err: err}
		}
		it := batchItem{op: i, w: w, cur: cur, curOff: curOff}
		if next != nil {
			if it.payload, it.next, err = c.encodeAfterImage(w.id, next); err != nil {
				return &BatchItemError{Index: i, Err: err}
			}
		}
		items = append(items, it)
		wi := batchWALItem(&it)
		size += wi.EncodedSize()
		if size > e.opts.MaxBatchBytes {
			return fmt.Errorf("%w: the payload exceeds %d bytes (MaxBatchBytes) at operation %d of %d; split the batch",
				ErrBatchTooLarge, e.opts.MaxBatchBytes, i, len(writes))
		}
	}
	if len(items) == 0 {
		return nil // every write was skipped
	}

	if unlock, err := c.checkBatchUnique(items); err != nil {
		return err
	} else if unlock != nil {
		defer unlock()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	walItems := make([]wal.BatchItem, len(items))
	for i := range items {
		walItems[i] = batchWALItem(&items[i])
	}
	payload, err := wal.AppendBatch(make([]byte, 0, size), walItems)
	if err != nil {
		return fmt.Errorf("encoding batch of %d documents: %w", len(items), err)
	}
	rec := wal.Record{Op: wal.OpBatch, DB: d.name, Coll: c.name, Payload: payload}

	lb := c.registerInflight()
	off, err := e.appendWAL(context.WithoutCancel(ctx), rec) // commit point
	if err != nil {
		c.unregisterInflight(lb)
		return fmt.Errorf("writing batch of %d documents: %w", len(items), err)
	}
	if err := e.hook(hookAfterWALSync); err != nil {
		return err // simulated crash: no cleanup
	}
	// The replaced versions' index keys (memEntry.base), outside mu; the
	// index set cannot change while Database.mu is held.
	bases := make([]indexBase, len(items))
	for i := range items {
		bases[i] = c.indexKeysAll(items[i].cur)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeInflightLocked(lb)
	for i := range items {
		it := &items[i]
		c.applyLocked(it.w.id, it.cur != nil, bases[i], it.curOff, it.next, it.payload, off)
	}
	return nil
}

// batchWALItem returns the WAL item of it.
func batchWALItem(it *batchItem) wal.BatchItem {
	if it.next == nil {
		return wal.BatchItem{Op: wal.OpDeleteDoc, ID: it.w.id}
	}
	return wal.BatchItem{Op: wal.OpPutDoc, ID: it.w.id, Doc: it.payload}
}

// lockDocs locks the document stripes of every write in ascending order,
// each once, and returns the function that unlocks them.
func (c *Collection) lockDocs(writes []*docWrite) func() {
	stripes := make([]int, 0, len(writes))
	for _, w := range writes {
		stripes = append(stripes, docStripe(w.id))
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	locks := make([]*sync.Mutex, len(stripes))
	for i, s := range stripes {
		locks[i] = &c.docLocks[s]
		locks[i].Lock()
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}
}

// checkBatchUnique checks the batch's after-images against the
// collection's unique indexes. First against each other: no two documents
// of the batch may hold the same value. Then the values an operation adds
// (that its document did not hold already) against every other live
// document outside the batch, with those values' stripes locked; the
// returned function unlocks them (nil when nothing was locked) and the
// caller holds the stripes until the batch is applied. A failure is a
// *BatchItemError wrapping ErrUniqueViolation, and nothing is left locked.
// Callers hold Database.mu and the document locks of the batch (skipped
// writes included).
func (c *Collection) checkBatchUnique(items []batchItem) (func(), error) {
	var defs []indexDef
	for _, def := range c.indexes {
		if def.Unique {
			defs = append(defs, def)
		}
	}
	if len(defs) == 0 {
		return nil, nil
	}
	holders := make(map[string]int) // index name · 0x00 · value identity → operation
	for i := range items {
		it := &items[i]
		for _, def := range defs {
			uv, ok := uniqueValueOf(it.next, def)
			if !ok {
				continue
			}
			k := def.Name + "\x00" + uv.dkey
			if j, dup := holders[k]; dup {
				return nil, &BatchItemError{Index: it.op, Err: fmt.Errorf("document %q in collection %q: %w",
					it.w.id, c.name, uniqueErr(uv, items[j].w.id))}
			}
			holders[k] = i
		}
	}

	added := make([][]uniqueValue, len(items))
	var all []uniqueValue
	for i := range items {
		added[i] = c.changedUniqueValues(items[i].cur, items[i].next)
		all = append(all, added[i]...)
	}
	if len(all) == 0 {
		return nil, nil
	}
	unlock := c.lockUnique(all)
	// Only the documents the batch writes are left out: a skipped write
	// keeps its stored values.
	ids := make(map[string]struct{}, len(items))
	for i := range items {
		ids[items[i].w.id] = struct{}{}
	}
	inBatch := func(id string) bool { _, ok := ids[id]; return ok }
	for i, vals := range added {
		if len(vals) == 0 {
			continue
		}
		if err := c.checkUniqueExcept(inBatch, vals); err != nil {
			unlock()
			return nil, &BatchItemError{Index: items[i].op, Err: fmt.Errorf("document %q in collection %q: %w", items[i].w.id, c.name, err)}
		}
	}
	return unlock, nil
}

// repeatedBatchID returns the index of the first item of a decoded batch
// record whose id an earlier item has, and that earlier item's index, or
// -1 and -1 when every id appears once. The batch format allows an id once
// per batch, and the write path never writes a repeat. Replay does not
// refuse one: every item gets the record's offset, so it applies the first
// item of the id and skips the rest. Verify reports it.
func repeatedBatchID(items []wal.BatchItem) (i, first int) {
	seen := make(map[string]int, len(items))
	for i, it := range items {
		if j, ok := seen[it.ID]; ok {
			return i, j
		}
		seen[it.ID] = i
	}
	return -1, -1
}

// replayBatch applies a batch record (op 11) item by item, each iff the
// record's offset is newer than the item's id's current version (as
// replayDoc does for single records), so replaying it again or after some
// of its items were flushed changes nothing more. It reports whether any
// item was applied. Callers have checked the offset against the
// collection's replay floor.
func (c *Collection) replayBatch(r wal.Record) (bool, error) {
	items, err := wal.DecodeBatch(r.Payload)
	if err != nil {
		return false, fmt.Errorf("%w: decoding batch payload: %w", ErrWALCorrupt, err)
	}
	ids := make(map[string]struct{}, len(items))
	for _, it := range items {
		ids[it.ID] = struct{}{}
	}
	inBatch := func(id string) bool { _, ok := ids[id]; return ok }
	applied := false
	for i, it := range items {
		ok, err := c.replayItem(it.Op, it.ID, it.Doc, r.Offset, inBatch)
		if err != nil {
			return applied, fmt.Errorf("batch item %d (document %q): %w", i, it.ID, err)
		}
		applied = applied || ok
	}
	return applied, nil
}
