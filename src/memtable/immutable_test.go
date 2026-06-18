package memtable

import (
	"fmt"
	"nosqlEngine/src/models/key_value"
	"testing"
)

func makeSnapshot(pairs ...string) []key_value.KeyValue {
	kvs := make([]key_value.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		kvs = append(kvs, key_value.NewKeyValue(pairs[i], pairs[i+1]))
	}
	return kvs
}

func TestImmutableGet(t *testing.T) {
	im := NewImmutableMemtable(makeSnapshot("b", "2", "a", "1", "c", "3"))

	for _, tc := range []struct{ key, want string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"},
	} {
		got, ok := im.Get(tc.key)
		if !ok || got != tc.want {
			t.Errorf("Get(%q) = (%q, %v), want (%q, true)", tc.key, got, ok, tc.want)
		}
	}

	if _, ok := im.Get("missing"); ok {
		t.Error("expected miss for absent key")
	}
}

func TestImmutableTombstoneHiddenFromGet(t *testing.T) {
	ts := tombstone
	im := NewImmutableMemtable(makeSnapshot("gone", ts, "alive", "yes"))

	if _, ok := im.Get("gone"); ok {
		t.Error("Get should return false for tombstone")
	}

	// ToRaw must still expose the tombstone for the flush path
	raw := im.ToRaw()
	found := false
	for _, kv := range raw {
		if kv.GetKey() == "gone" && kv.GetValue() == ts {
			found = true
		}
	}
	if !found {
		t.Error("ToRaw should include tombstone entries")
	}
}

func TestImmutableToRawIsCopy(t *testing.T) {
	im := NewImmutableMemtable(makeSnapshot("k", "v"))
	r1 := im.ToRaw()
	r2 := im.ToRaw()
	if &r1[0] == &r2[0] {
		t.Error("ToRaw should return independent copies")
	}
}

func TestImmutableSortedAfterConstruction(t *testing.T) {
	im := NewImmutableMemtable(makeSnapshot("c", "3", "a", "1", "b", "2"))
	raw := im.ToRaw()
	for i := 1; i < len(raw); i++ {
		if raw[i-1].GetKey() >= raw[i].GetKey() {
			t.Errorf("not sorted at index %d: %q >= %q", i, raw[i-1].GetKey(), raw[i].GetKey())
		}
	}
}

func TestImmutableMarkAndWaitFlushed(t *testing.T) {
	im := NewImmutableMemtable(makeSnapshot("k", "v"))

	done := make(chan struct{})
	go func() {
		im.WaitFlushed()
		close(done)
	}()

	im.MarkFlushed()
	<-done
}

func TestImmutableFromAllBackends(t *testing.T) {
	for _, impl := range implementations(t) {
		t.Run(impl.name, func(t *testing.T) {
			mt := impl.new()
			for i := 0; i < 10; i++ {
				mt.Add(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
			}
			snap := mt.TakeSnapshot()
			im := NewImmutableMemtable(snap)

			if im.Len() != 10 {
				t.Fatalf("Len = %d, want 10", im.Len())
			}
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("k%02d", i)
				if _, ok := im.Get(key); !ok {
					t.Errorf("key %q not found in immutable", key)
				}
			}
		})
	}
}
