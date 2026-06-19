package memtable

import (
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"sync"
	"testing"
)

var tombstone = config.GetConfig().Tombstone

func implementations(t *testing.T) []struct {
	name string
	new  func() Memtable
} {
	t.Helper()
	return []struct {
		name string
		new  func() Memtable
	}{
		{"HashMap", func() Memtable { return NewHashMap() }},
		{"SkipList", func() Memtable { return NewSkipList(4) }},
		{"BTree", func() Memtable { return NewBTree(3) }},
	}
}

func TestAddGetAndSize(t *testing.T) {
	for _, impl := range implementations(t) {
		t.Run(impl.name, func(t *testing.T) {
			mt := impl.new()

			mt.Add("a", "1")
			mt.Add("b", "22")
			if got := mt.GetSize(); got != len("a")+len("1")+len("b")+len("22") {
				t.Fatalf("GetSize() = %d, want %d", got, len("a")+len("1")+len("b")+len("22"))
			}

			val, ok := mt.Get("a")
			if !ok || val != "1" {
				t.Fatalf("Get(a) = (%q, %v)", val, ok)
			}

			mt.Add("a", "longer")
			if got := mt.GetSize(); got != len("a")+len("longer")+len("b")+len("22") {
				t.Fatalf("GetSize after update = %d", got)
			}
			val, ok = mt.Get("a")
			if !ok || val != "longer" {
				t.Fatalf("Get(a) after update = (%q, %v)", val, ok)
			}
		})
	}
}

func TestTombstoneHiddenFromGet(t *testing.T) {
	for _, impl := range implementations(t) {
		t.Run(impl.name, func(t *testing.T) {
			mt := impl.new()
			mt.Add("gone", tombstone)

			if _, ok := mt.Get("gone"); ok {
				t.Fatal("Get should treat tombstone as missing")
			}

			raw := mt.ToRaw()
			if len(raw) != 1 || raw[0].GetValue() != tombstone {
				t.Fatalf("ToRaw should still expose tombstone entry, got %#v", raw)
			}
		})
	}
}

func TestClear(t *testing.T) {
	for _, impl := range implementations(t) {
		t.Run(impl.name, func(t *testing.T) {
			mt := impl.new()
			mt.Add("k", "v")
			if !mt.Clear() {
				t.Fatal("Clear returned false")
			}
			if mt.GetSize() != 0 {
				t.Fatalf("GetSize after Clear = %d", mt.GetSize())
			}
			if _, ok := mt.Get("k"); ok {
				t.Fatal("key still present after Clear")
			}
		})
	}
}

func TestToRawSortedForOrderedBackends(t *testing.T) {
	cases := []struct {
		name string
		new  func() Memtable
	}{
		{"SkipList", func() Memtable { return NewSkipList(4) }},
		{"BTree", func() Memtable { return NewBTree(3) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mt := tc.new()
			for _, kv := range []struct{ k, v string }{
				{"c", "3"}, {"a", "1"}, {"b", "2"},
			} {
				mt.Add(kv.k, kv.v)
			}

			raw := mt.ToRaw()
			if len(raw) != 3 {
				t.Fatalf("ToRaw len = %d", len(raw))
			}
			for i := 1; i < len(raw); i++ {
				if raw[i-1].GetKey() >= raw[i].GetKey() {
					t.Fatalf("ToRaw not sorted: %v", key_value.GetKeys(raw))
				}
			}
		})
	}
}

func TestSkipListGetMissingKeyDoesNotLoop(t *testing.T) {
	mt := NewSkipList(4)
	mt.Add("only", "value")

	if _, ok := mt.Get("missing"); ok {
		t.Fatal("expected missing key")
	}
}

func TestSkipListUpdateIsConsistentAcrossLevels(t *testing.T) {
	mt := NewSkipList(8)

	for i := range 200 {
		mt.Add(fmt.Sprintf("key-%04d", i), fmt.Sprintf("v%d", i))
	}

	const updated = "UPDATED"
	for i := range 200 {
		if i%3 == 0 {
			mt.Add(fmt.Sprintf("key-%04d", i), updated)
		}
	}

	for i := range 200 {
		key := fmt.Sprintf("key-%04d", i)
		want := fmt.Sprintf("v%d", i)
		if i%3 == 0 {
			want = updated
		}
		got, ok := mt.Get(key)
		if !ok || got != want {
			t.Fatalf("Get(%q) = (%q, %v), want (%q, true)", key, got, ok, want)
		}
	}

	for _, kv := range mt.ToRaw() {
		if kv.GetValue() != updated && kv.GetValue()[:1] == "v" {
			continue
		}
	}
}

func TestTakeSnapshot(t *testing.T) {
	for _, impl := range implementations(t) {
		t.Run(impl.name, func(t *testing.T) {
			mt := impl.new()
			mt.Add("a", "1")
			mt.Add("b", "2")

			snap := mt.TakeSnapshot()

			if mt.GetSize() != 0 {
				t.Fatalf("memtable not empty after TakeSnapshot; GetSize = %d", mt.GetSize())
			}
			if _, ok := mt.Get("a"); ok {
				t.Fatal("key still present in memtable after TakeSnapshot")
			}
			if len(snap) != 2 {
				t.Fatalf("snapshot len = %d, want 2", len(snap))
			}
		})
	}
}

func TestTakeSnapshotIsAtomicUnderConcurrency(t *testing.T) {
	for _, impl := range implementations(t) {
		t.Run(impl.name, func(t *testing.T) {
			mt := NewSyncMemtable(impl.new())
			for i := 0; i < 20; i++ {
				mt.Add(fmt.Sprintf("k%d", i), "v")
			}

			var wg sync.WaitGroup
			wg.Add(2)

			var snapLen int
			go func() {
				defer wg.Done()
				snap := mt.TakeSnapshot()
				snapLen = len(snap)
			}()
			go func() {
				defer wg.Done()
				mt.Get("k0")
				mt.ToRaw()
			}()

			wg.Wait()
			// After TakeSnapshot the memtable must be empty
			if mt.GetSize() != 0 {
				t.Fatalf("memtable not empty after concurrent TakeSnapshot; GetSize = %d", mt.GetSize())
			}
			if snapLen != 20 {
				t.Fatalf("snapshot captured %d entries, want 20", snapLen)
			}
		})
	}
}

func TestNewMemtableFactory(t *testing.T) {
	mt := NewMemtable()
	if mt == nil {
		t.Fatal("NewMemtable returned nil")
	}
	mt.Add("k", "v")
	if _, ok := mt.Get("k"); !ok {
		t.Fatal("factory memtable could not store value")
	}
}
