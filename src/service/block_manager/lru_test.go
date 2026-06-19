package block_manager

import (
	"sync"
	"testing"

	doublyll "nosqlEngine/src/models/doubly_ll"
)

func lruWithCapacity(cap int) *LRUCache {
	return &LRUCache{
		capacity: cap,
		cache:    make(map[doublyll.BlockKey]*doublyll.Block),
		lruList:  doublyll.NewDoublyLinkedList(),
	}
}

func TestLRUPutAndGet(t *testing.T) {
	c := lruWithCapacity(3)

	c.Put("file.db", 0, []byte("block0"))
	c.Put("file.db", 1, []byte("block1"))

	data, ok := c.Get("file.db", 0)
	if !ok {
		t.Fatal("expected cache hit for block 0")
	}
	if string(data) != "block0" {
		t.Fatalf("got %q, want %q", data, "block0")
	}
}

func TestLRUEviction(t *testing.T) {
	c := lruWithCapacity(2)

	c.Put("f", 0, []byte("a"))
	c.Put("f", 1, []byte("b"))
	c.Put("f", 2, []byte("c")) // evicts block 0 (LRU)

	if _, ok := c.Get("f", 0); ok {
		t.Error("block 0 should have been evicted")
	}
	if _, ok := c.Get("f", 1); !ok {
		t.Error("block 1 should still be present")
	}
	if _, ok := c.Get("f", 2); !ok {
		t.Error("block 2 should be present")
	}
}

func TestLRUGetUpdatesRecency(t *testing.T) {
	c := lruWithCapacity(2)

	c.Put("f", 0, []byte("a"))
	c.Put("f", 1, []byte("b"))
	c.Get("f", 0)              // touch block 0 — block 1 becomes LRU
	c.Put("f", 2, []byte("c")) // should evict block 1

	if _, ok := c.Get("f", 0); !ok {
		t.Error("block 0 should survive — it was recently accessed")
	}
	if _, ok := c.Get("f", 1); ok {
		t.Error("block 1 should have been evicted as LRU")
	}
}

func TestLRUPutUpdateExisting(t *testing.T) {
	c := lruWithCapacity(3)

	c.Put("f", 0, []byte("old"))
	c.Put("f", 0, []byte("new"))

	data, ok := c.Get("f", 0)
	if !ok || string(data) != "new" {
		t.Fatalf("expected updated value %q, got %q (ok=%v)", "new", data, ok)
	}
	if c.Len() != 1 {
		t.Errorf("update should not add a duplicate entry; len=%d", c.Len())
	}
}

func TestLRUZeroCapacity(t *testing.T) {
	c := lruWithCapacity(0)
	c.Put("f", 0, []byte("x"))
	if _, ok := c.Get("f", 0); ok {
		t.Error("zero-capacity cache should never store entries")
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	c := lruWithCapacity(64)

	var wg sync.WaitGroup
	const goroutines = 16
	const ops = 100

	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			for i := range ops {
				block := (g*ops + i) % 32
				c.Put("f", block, []byte("data"))
				c.Get("f", block)
			}
		}()
	}
	wg.Wait()

	if c.Len() > 64 {
		t.Errorf("cache grew beyond capacity: len=%d", c.Len())
	}
}
