package sstable

import (
	"container/list"
	"nosqlEngine/src/service/block_manager"
	"sync"
)

type tableEntry struct {
	path   string
	reader *SSTableReader
}

type TableCache struct {
	mu       sync.Mutex
	capacity int
	cache    map[string]*list.Element
	lru      *list.List
	bm       *block_manager.BlockManager
}

func NewTableCache(capacity int, bm *block_manager.BlockManager) *TableCache {
	if capacity <= 0 {
		capacity = 32
	}
	return &TableCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lru:      list.New(),
		bm:       bm,
	}
}

func (tc *TableCache) GetOrOpen(path string) (*SSTableReader, error) {
	tc.mu.Lock()
	if elem, ok := tc.cache[path]; ok {
		tc.lru.MoveToFront(elem)
		r := elem.Value.(*tableEntry).reader
		tc.mu.Unlock()
		return r, nil
	}
	tc.mu.Unlock()

	reader, err := Open(path, tc.bm)
	if err != nil {
		return nil, err
	}

	tc.mu.Lock()
	if elem, ok := tc.cache[path]; ok {
		tc.lru.MoveToFront(elem)
		r := elem.Value.(*tableEntry).reader
		tc.mu.Unlock()
		return r, nil
	}
	elem := tc.lru.PushFront(&tableEntry{path: path, reader: reader})
	tc.cache[path] = elem
	if tc.lru.Len() > tc.capacity {
		tc.evictLocked()
	}
	tc.mu.Unlock()
	return reader, nil
}

func (tc *TableCache) Evict(path string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if elem, ok := tc.cache[path]; ok {
		tc.lru.Remove(elem)
		delete(tc.cache, path)
	}
	tc.bm.CloseFile(path)
}

func (tc *TableCache) evictLocked() {
	back := tc.lru.Back()
	if back == nil {
		return
	}
	entry := tc.lru.Remove(back).(*tableEntry)
	delete(tc.cache, entry.path)
	tc.bm.CloseFile(entry.path)
}
