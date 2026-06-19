package block_manager

import (
	"fmt"
	"sync"

	cfg "nosqlEngine/src/config"
	doublyll "nosqlEngine/src/models/doubly_ll"
)

type LRUCache struct {
	mu       sync.Mutex
	capacity int
	cache    map[doublyll.BlockKey]*doublyll.Block
	lruList  *doublyll.DoublyLinkedList
}

func NewLRUCache() *LRUCache {
	return &LRUCache{
		capacity: cfg.GetConfig().CacheCapacity,
		cache:    make(map[doublyll.BlockKey]*doublyll.Block),
		lruList:  doublyll.NewDoublyLinkedList(),
	}
}

func (c *LRUCache) Put(filePath string, blockID int, data []byte) {
	if c.capacity <= 0 {
		return
	}
	key := doublyll.NewBlockKey(blockID, filePath)

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.cache[key]; found {
		elem.Set(data)
		c.lruList.MoveToFront(elem)
		return
	}

	node := doublyll.NewNode(data, key)
	c.lruList.InsertBeginning(node)
	c.cache[key] = node

	if c.lruList.ListLength() > c.capacity {
		c.evictLocked()
	}
}

func (c *LRUCache) Get(filePath string, blockID int) ([]byte, bool) {
	if c.capacity <= 0 {
		return nil, false
	}
	key := doublyll.NewBlockKey(blockID, filePath)

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.cache[key]; found {
		c.lruList.MoveToFront(elem)
		return elem.Get(), true
	}
	return nil, false
}

func (c *LRUCache) evictLocked() {
	tail := c.lruList.Back()
	if tail == nil {
		return
	}
	delete(c.cache, tail.BlockKey)
	c.lruList.DeleteEnd()
}

func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lruList.ListLength()
}

// String for debugging only.
func (c *LRUCache) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("LRUCache{capacity:%d, len:%d}", c.capacity, c.lruList.ListLength())
}
