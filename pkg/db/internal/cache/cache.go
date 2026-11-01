// Package cache provides the byte-bounded, concurrency-safe LRU that holds
// encoded documents for the storage engine's read path.
//
// Entries are keyed by (database, collection, id) and tagged with the WAL
// offset of the document version they hold. A lookup hits only when the
// caller's WAL offset matches the cached one, so a stale entry can never be
// returned for a newer document version.
package cache

import (
	"container/list"
	"sync"
)

// entryOverhead is the fixed number of bytes charged per entry on top of
// the value and key string lengths, approximating list element, map slot
// and struct bookkeeping.
const entryOverhead = 64

// Key identifies a cached document.
type Key struct{ DB, Coll, ID string }

type entry struct {
	key       Key
	walOffset uint64
	val       []byte
	size      int64
}

// LRU is a concurrency-safe least-recently-used cache bounded by a byte
// budget. Create one with New.
type LRU struct {
	mu       sync.Mutex
	maxBytes int64
	used     int64
	ll       *list.List // front = most recently used; elements hold *entry
	items    map[Key]*list.Element
}

// New returns a concurrency-safe LRU bounded by maxBytes. Each entry is
// charged len(value) + len(DB) + len(Coll) + len(ID) + a small fixed
// overhead; the total charge never exceeds maxBytes. maxBytes <= 0 disables
// the cache: Get always misses and Put is a no-op.
func New(maxBytes int64) *LRU {
	if maxBytes < 0 {
		maxBytes = 0
	}
	return &LRU{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[Key]*list.Element),
	}
}

func entrySize(k Key, v []byte) int64 {
	return int64(len(v)+len(k.DB)+len(k.Coll)+len(k.ID)) + entryOverhead
}

// Get returns the cached value for k. It hits only if the cached entry was
// stored with the same walOffset; a hit marks the entry most recently used.
// The returned slice is shared with the cache and must not be mutated.
func (c *LRU) Get(k Key, walOffset uint64) ([]byte, bool) {
	if c.maxBytes <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[k]
	if !ok {
		return nil, false
	}
	e := el.Value.(*entry)
	if e.walOffset != walOffset {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return e.val, true
}

// Put stores v for k at walOffset, replacing any existing entry for k, and
// evicts least-recently-used entries until the byte budget is respected.
// v is retained; the caller must not mutate it afterwards. A value whose
// charged size exceeds maxBytes is not cached (and any existing entry for k
// is dropped).
func (c *LRU) Put(k Key, walOffset uint64, v []byte) {
	if c.maxBytes <= 0 {
		return
	}
	size := entrySize(k, v)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		c.removeElement(el)
	}
	if size > c.maxBytes {
		return
	}
	el := c.ll.PushFront(&entry{key: k, walOffset: walOffset, val: v, size: size})
	c.items[k] = el
	c.used += size
	for c.used > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.removeElement(back)
	}
}

// Invalidate removes the entry for k, if any.
func (c *LRU) Invalidate(k Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		c.removeElement(el)
	}
}

// InvalidateCollection removes every entry belonging to collection coll of
// database db.
func (c *LRU) InvalidateCollection(db, coll string) {
	c.removeMatching(func(k Key) bool { return k.DB == db && k.Coll == coll })
}

// InvalidateDatabase removes every entry belonging to database db.
func (c *LRU) InvalidateDatabase(db string) {
	c.removeMatching(func(k Key) bool { return k.DB == db })
}

func (c *LRU) removeMatching(match func(Key) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, el := range c.items {
		if match(k) {
			c.removeElement(el)
		}
	}
}

// removeElement unlinks el from the list and map. c.mu must be held.
func (c *LRU) removeElement(el *list.Element) {
	e := c.ll.Remove(el).(*entry)
	delete(c.items, e.key)
	c.used -= e.size
}
