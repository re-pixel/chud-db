package engine

import (
	"nosqlEngine/src/memtable"
	"sync"
)

type immutableQueue struct {
	mu     sync.RWMutex
	cond   *sync.Cond
	items  []*memtable.ImmutableMemtable
	max    int
	closed bool
}

func newImmutableQueue(max int) *immutableQueue {
	if max < 1 {
		max = 1
	}
	q := &immutableQueue{max: max}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *immutableQueue) Push(im *memtable.ImmutableMemtable) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) >= q.max && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return
	}
	q.items = append(q.items, im)
	q.cond.Broadcast()
}

func (q *immutableQueue) Peek() *memtable.ImmutableMemtable {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}

func (q *immutableQueue) PopFront() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) > 0 {
		q.items = q.items[1:]
		q.cond.Broadcast()
	}
}

func (q *immutableQueue) Snapshot() []*memtable.ImmutableMemtable {
	q.mu.RLock()
	cp := make([]*memtable.ImmutableMemtable, len(q.items))
	copy(cp, q.items)
	q.mu.RUnlock()

	for i, j := 0, len(cp)-1; i < j; i, j = i+1, j-1 {
		cp[i], cp[j] = cp[j], cp[i]
	}
	return cp
}

func (q *immutableQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

func (q *immutableQueue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.items)
}
