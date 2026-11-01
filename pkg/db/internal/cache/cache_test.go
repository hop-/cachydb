package cache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// checkInvariants verifies internal bookkeeping: map and list agree, the
// accounted size equals the sum of entry sizes, and the budget holds.
func checkInvariants(t *testing.T, c *LRU[[]byte]) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ll.Len() != len(c.items) {
		t.Fatalf("list len %d != map len %d", c.ll.Len(), len(c.items))
	}
	var sum, valueBytes int64
	for el := c.ll.Front(); el != nil; el = el.Next() {
		e := el.Value.(*entry[[]byte])
		if c.items[e.key] != el {
			t.Fatalf("map entry for %v does not point at its list element", e.key)
		}
		if want := entrySize(e.key, int64(len(e.val))); e.size != want {
			t.Fatalf("entry %v size %d, want %d", e.key, e.size, want)
		}
		sum += e.size
		valueBytes += int64(len(e.val))
	}
	if sum != c.used {
		t.Fatalf("used %d != sum of entry sizes %d", c.used, sum)
	}
	if c.maxBytes > 0 && c.used > c.maxBytes {
		t.Fatalf("used %d exceeds maxBytes %d", c.used, c.maxBytes)
	}
	if c.maxBytes > 0 && valueBytes > c.maxBytes {
		t.Fatalf("value bytes %d exceed maxBytes %d", valueBytes, c.maxBytes)
	}
}

func key(id string) Key { return Key{DB: "d", Coll: "c", ID: id} }

func TestEntrySize(t *testing.T) {
	k := Key{DB: "db", Coll: "coll", ID: "id1"}
	got := entrySize(k, int64(len(make([]byte, 10))))
	if want := int64(10 + 2 + 4 + 3 + 64); got != want {
		t.Fatalf("entrySize = %d, want %d", got, want)
	}
}

func TestGetPut(t *testing.T) {
	tests := []struct {
		name      string
		maxBytes  int64
		putOffset uint64
		getOffset uint64
		val       []byte
		wantHit   bool
	}{
		{name: "hit same offset", maxBytes: 1 << 20, putOffset: 7, getOffset: 7, val: []byte(`{"a":1}`), wantHit: true},
		{name: "miss different offset", maxBytes: 1 << 20, putOffset: 7, getOffset: 8, val: []byte(`{"a":1}`)},
		{name: "miss older offset", maxBytes: 1 << 20, putOffset: 7, getOffset: 6, val: []byte(`{"a":1}`)},
		{name: "empty value hits", maxBytes: 1 << 20, putOffset: 1, getOffset: 1, val: []byte{}, wantHit: true},
		{name: "disabled zero", maxBytes: 0, putOffset: 1, getOffset: 1, val: []byte("x")},
		{name: "disabled negative", maxBytes: -5, putOffset: 1, getOffset: 1, val: []byte("x")},
		{name: "value larger than maxBytes not cached", maxBytes: 100, putOffset: 1, getOffset: 1, val: make([]byte, 101)},
		{name: "value plus overhead larger than maxBytes not cached", maxBytes: 100, putOffset: 1, getOffset: 1, val: make([]byte, 50)},
		{name: "value exactly filling budget cached", maxBytes: entrySize(key("k"), int64(len(make([]byte, 30)))), putOffset: 1, getOffset: 1, val: make([]byte, 30), wantHit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newBytes(tt.maxBytes)
			c.Put(key("k"), tt.putOffset, tt.val)
			got, ok := c.Get(key("k"), tt.getOffset)
			if ok != tt.wantHit {
				t.Fatalf("Get hit = %v, want %v", ok, tt.wantHit)
			}
			if ok && !bytes.Equal(got, tt.val) {
				t.Fatalf("Get = %q, want %q", got, tt.val)
			}
			if !ok && got != nil {
				t.Fatalf("miss returned non-nil value %q", got)
			}
			checkInvariants(t, c)
		})
	}
}

func TestPutReplaces(t *testing.T) {
	c := newBytes(1 << 20)
	c.Put(key("k"), 1, []byte("old value"))
	c.Put(key("k"), 2, []byte("new"))
	if _, ok := c.Get(key("k"), 1); ok {
		t.Fatal("old offset still hits after replacement")
	}
	got, ok := c.Get(key("k"), 2)
	if !ok || string(got) != "new" {
		t.Fatalf("Get(2) = %q, %v; want \"new\", true", got, ok)
	}
	checkInvariants(t, c)
	if c.ll.Len() != 1 {
		t.Fatalf("len = %d, want 1", c.ll.Len())
	}

	// Replacing with an oversized value drops the existing entry.
	small := newBytes(entrySize(key("k"), int64(len(make([]byte, 10)))))
	small.Put(key("k"), 1, make([]byte, 10))
	small.Put(key("k"), 2, make([]byte, 11))
	if _, ok := small.Get(key("k"), 1); ok {
		t.Fatal("stale entry kept after oversized replacement")
	}
	checkInvariants(t, small)
	if small.used != 0 {
		t.Fatalf("used = %d, want 0", small.used)
	}
}

func TestEvictionLRUOrder(t *testing.T) {
	val := make([]byte, 36) // entry size = 36 + 1 + 1 + 2 + 64 = 104 for ids "kN"
	per := entrySize(key("k0"), int64(len(val)))
	tests := []struct {
		name     string
		ops      func(c *LRU[[]byte])
		wantKeep []string
		wantGone []string
	}{
		{
			name: "oldest evicted first",
			ops: func(c *LRU[[]byte]) {
				for i := range 4 {
					c.Put(key(fmt.Sprintf("k%d", i)), 1, val)
				}
			},
			wantKeep: []string{"k1", "k2", "k3"},
			wantGone: []string{"k0"},
		},
		{
			name: "get refreshes recency",
			ops: func(c *LRU[[]byte]) {
				c.Put(key("k0"), 1, val)
				c.Put(key("k1"), 1, val)
				c.Put(key("k2"), 1, val)
				c.Get(key("k0"), 1)
				c.Put(key("k3"), 1, val)
			},
			wantKeep: []string{"k0", "k2", "k3"},
			wantGone: []string{"k1"},
		},
		{
			name: "miss on wrong offset does not refresh recency",
			ops: func(c *LRU[[]byte]) {
				c.Put(key("k0"), 1, val)
				c.Put(key("k1"), 1, val)
				c.Put(key("k2"), 1, val)
				c.Get(key("k0"), 99)
				c.Put(key("k3"), 1, val)
			},
			wantKeep: []string{"k1", "k2", "k3"},
			wantGone: []string{"k0"},
		},
		{
			name: "put refreshes recency",
			ops: func(c *LRU[[]byte]) {
				c.Put(key("k0"), 1, val)
				c.Put(key("k1"), 1, val)
				c.Put(key("k2"), 1, val)
				c.Put(key("k0"), 1, val)
				c.Put(key("k3"), 1, val)
			},
			wantKeep: []string{"k0", "k2", "k3"},
			wantGone: []string{"k1"},
		},
		{
			name: "large insert evicts several",
			ops: func(c *LRU[[]byte]) {
				c.Put(key("k0"), 1, val)
				c.Put(key("k1"), 1, val)
				c.Put(key("k2"), 1, val)
				// Needs room of two regular entries.
				c.Put(key("k3"), 1, make([]byte, int(2*per)-68))
			},
			wantKeep: []string{"k2", "k3"},
			wantGone: []string{"k0", "k1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newBytes(3 * per) // room for exactly three regular entries
			tt.ops(c)
			checkInvariants(t, c)
			for _, id := range tt.wantKeep {
				if _, ok := c.Get(key(id), 1); !ok {
					t.Errorf("%s evicted, want kept", id)
				}
			}
			for _, id := range tt.wantGone {
				if _, ok := c.Get(key(id), 1); ok {
					t.Errorf("%s kept, want evicted", id)
				}
			}
		})
	}
}

func TestBudgetNeverExceeded(t *testing.T) {
	const maxBytes = 4096
	c := newBytes(maxBytes)
	for i := range 2000 {
		size := (i * 37) % 700
		c.Put(Key{DB: "d", Coll: fmt.Sprintf("c%d", i%3), ID: fmt.Sprintf("id%d", i%250)}, uint64(i), make([]byte, size))
		if i%50 == 0 {
			checkInvariants(t, c)
		}
	}
	checkInvariants(t, c)
}

func TestInvalidate(t *testing.T) {
	keys := []Key{
		{DB: "a", Coll: "x", ID: "1"},
		{DB: "a", Coll: "x", ID: "2"},
		{DB: "a", Coll: "y", ID: "1"},
		{DB: "b", Coll: "x", ID: "1"},
		{DB: "ab", Coll: "x", ID: "1"},
	}
	tests := []struct {
		name     string
		op       func(c *LRU[[]byte])
		wantGone []int // indexes into keys
	}{
		{name: "single key", op: func(c *LRU[[]byte]) { c.Invalidate(keys[1]) }, wantGone: []int{1}},
		{name: "missing key is no-op", op: func(c *LRU[[]byte]) { c.Invalidate(Key{DB: "z", Coll: "z", ID: "z"}) }},
		{name: "collection", op: func(c *LRU[[]byte]) { c.InvalidateCollection("a", "x") }, wantGone: []int{0, 1}},
		{name: "collection same name other db untouched", op: func(c *LRU[[]byte]) { c.InvalidateCollection("b", "x") }, wantGone: []int{3}},
		{name: "missing collection is no-op", op: func(c *LRU[[]byte]) { c.InvalidateCollection("a", "zz") }},
		{name: "database", op: func(c *LRU[[]byte]) { c.InvalidateDatabase("a") }, wantGone: []int{0, 1, 2}},
		{name: "database prefix not matched", op: func(c *LRU[[]byte]) { c.InvalidateDatabase("ab") }, wantGone: []int{4}},
		{name: "missing database is no-op", op: func(c *LRU[[]byte]) { c.InvalidateDatabase("nope") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newBytes(1 << 20)
			for i, k := range keys {
				c.Put(k, uint64(i), []byte(fmt.Sprintf("v%d", i)))
			}
			tt.op(c)
			checkInvariants(t, c)
			gone := make(map[int]bool, len(tt.wantGone))
			for _, i := range tt.wantGone {
				gone[i] = true
			}
			for i, k := range keys {
				_, ok := c.Get(k, uint64(i))
				if gone[i] && ok {
					t.Errorf("%v still cached, want invalidated", k)
				}
				if !gone[i] && !ok {
					t.Errorf("%v invalidated, want kept", k)
				}
			}
		})
	}
}

func TestDisabledInvalidateIsSafe(t *testing.T) {
	c := newBytes(0)
	c.Invalidate(key("k"))
	c.InvalidateCollection("d", "c")
	c.InvalidateDatabase("d")
	checkInvariants(t, c)
}

func TestConcurrentAccess(t *testing.T) {
	c := newBytes(16 * 1024)
	const (
		workers = 16
		iters   = 2000
	)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iters {
				k := Key{DB: fmt.Sprintf("d%d", i%2), Coll: fmt.Sprintf("c%d", i%3), ID: fmt.Sprintf("id%d", (i*7+w)%64)}
				off := uint64(i % 5)
				switch (i + w) % 7 {
				case 0, 1, 2:
					c.Put(k, off, make([]byte, (i*13)%300))
				case 3, 4:
					if v, ok := c.Get(k, off); ok && v == nil {
						t.Error("hit returned nil value")
					}
				case 5:
					c.Invalidate(k)
				case 6:
					if i%50 == 0 {
						c.InvalidateCollection(k.DB, k.Coll)
					} else if i%97 == 0 {
						c.InvalidateDatabase(k.DB)
					} else {
						c.Get(k, off)
					}
				}
			}
		}()
	}
	wg.Wait()
	checkInvariants(t, c)
}

// newBytes returns an LRU of byte slices charged by their length.
func newBytes(maxBytes int64) *LRU[[]byte] {
	return New(maxBytes, func(b []byte) int64 { return int64(len(b)) })
}
