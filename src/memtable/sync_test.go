package memtable

import (
	"fmt"
	"sync"
	"testing"
)

func TestSyncMemtableConcurrentReadWrite(t *testing.T) {
	backends := []struct {
		name string
		new  func() Memtable
	}{
		{"HashMap", func() Memtable { return NewSyncMemtable(NewHashMap()) }},
		{"SkipList", func() Memtable { return NewSyncMemtable(NewSkipList(4)) }},
		{"BTree", func() Memtable { return NewSyncMemtable(NewBTree(3)) }},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			mt := backend.new()
			const writers = 8
			const readers = 8
			const perWriter = 50

			var wg sync.WaitGroup
			wg.Add(writers + readers)

			for w := 0; w < writers; w++ {
				w := w
				go func() {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						key := fmt.Sprintf("w%d-k%d", w, i)
						mt.Add(key, fmt.Sprintf("v%d", i))
					}
				}()
			}

			for r := 0; r < readers; r++ {
				go func() {
					defer wg.Done()
					for i := 0; i < perWriter*writers; i++ {
						_, _ = mt.Get(fmt.Sprintf("w%d-k%d", i%writers, i%perWriter))
						_ = mt.ToRaw()
						_ = mt.GetSize()
					}
				}()
			}

			wg.Wait()

			if mt.GetSize() == 0 {
				t.Fatal("expected non-zero size after concurrent writes")
			}
		})
	}
}

func TestNewMemtableIsSynchronized(t *testing.T) {
	mt := NewMemtable()
	if _, ok := mt.(*syncMemtable); !ok {
		t.Fatalf("NewMemtable() = %T, want *syncMemtable", mt)
	}
}
