package memtable

import (
	"nosqlEngine/src/models/key_value"
	"sync"
)

// syncMemtable wraps a Memtable with an RWMutex so callers can read and write
// concurrently. Writes take an exclusive lock; reads share the lock.
type syncMemtable struct {
	inner Memtable
	mu    sync.RWMutex
}

// NewSyncMemtable returns a thread-safe Memtable around inner.
func NewSyncMemtable(inner Memtable) Memtable {
	if inner == nil {
		return nil
	}
	return &syncMemtable{inner: inner}
}

func (s *syncMemtable) Add(key, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.Add(key, value)
}

func (s *syncMemtable) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.Get(key)
}

func (s *syncMemtable) Scan(pred func(key string) bool, fn func(key, value string)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.inner.Scan(pred, fn)
}

func (s *syncMemtable) ToRaw() []key_value.KeyValue {
	s.mu.RLock()
	raw := s.inner.ToRaw()
	s.mu.RUnlock()

	out := make([]key_value.KeyValue, len(raw))
	copy(out, raw)
	return out
}

func (s *syncMemtable) GetSize() int {
	return s.inner.GetSize()
}

func (s *syncMemtable) Clear() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.Clear()
}

func (s *syncMemtable) TakeSnapshot() []key_value.KeyValue {
	s.mu.Lock()
	raw := s.inner.TakeSnapshot() // ToRaw + reset under a single lock acquisition
	s.mu.Unlock()
	return raw
}
