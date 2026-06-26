package versioning

import (
	"testing"
	"time"
)

func TestReconcileChoosesDominatingWinner(t *testing.T) {
	oldValue := NewPut(VectorClock{"node-a": 1}, "old", time.Unix(0, 1))
	newValue := NewPut(VectorClock{"node-a": 2}, "new", time.Unix(0, 2))

	result := Reconcile([]Envelope{oldValue, newValue})

	if result.Winner == nil {
		t.Fatalf("expected winner, got siblings %#v", result.Siblings)
	}
	if result.Winner.Value != "new" {
		t.Fatalf("winner value = %q", result.Winner.Value)
	}
	if len(result.Siblings) != 0 {
		t.Fatalf("unexpected siblings: %#v", result.Siblings)
	}
}

func TestReconcileReturnsConcurrentSiblings(t *testing.T) {
	left := NewPut(VectorClock{"node-a": 1}, "left", time.Unix(0, 1))
	right := NewPut(VectorClock{"node-b": 1}, "right", time.Unix(0, 2))

	result := Reconcile([]Envelope{left, right})

	if result.Winner != nil {
		t.Fatalf("unexpected winner: %#v", result.Winner)
	}
	if len(result.Siblings) != 2 {
		t.Fatalf("siblings = %#v", result.Siblings)
	}
}

func TestReconcileDeleteDominatesPut(t *testing.T) {
	put := NewPut(VectorClock{"node-a": 1}, "value", time.Unix(0, 1))
	delete := NewDelete(VectorClock{"node-a": 2}, time.Unix(0, 2))

	result := Reconcile([]Envelope{put, delete})

	if result.Winner == nil {
		t.Fatalf("expected delete winner")
	}
	if !result.Winner.Deleted {
		t.Fatalf("winner should be delete: %#v", result.Winner)
	}
}

func TestReconcilePutAfterDeleteRestoresKey(t *testing.T) {
	delete := NewDelete(VectorClock{"node-a": 2}, time.Unix(0, 2))
	put := NewPut(VectorClock{"node-a": 3}, "restored", time.Unix(0, 3))

	result := Reconcile([]Envelope{delete, put})

	if result.Winner == nil {
		t.Fatalf("expected put winner")
	}
	if result.Winner.Deleted {
		t.Fatalf("winner should be live value: %#v", result.Winner)
	}
	if result.Winner.Value != "restored" {
		t.Fatalf("winner value = %q", result.Winner.Value)
	}
}

func TestReconcileConcurrentDeleteAndPutAreSiblings(t *testing.T) {
	delete := NewDelete(VectorClock{"node-a": 1}, time.Unix(0, 1))
	put := NewPut(VectorClock{"node-b": 1}, "value", time.Unix(0, 2))

	result := Reconcile([]Envelope{delete, put})

	if result.Winner != nil {
		t.Fatalf("unexpected winner: %#v", result.Winner)
	}
	if len(result.Siblings) != 2 {
		t.Fatalf("siblings = %#v", result.Siblings)
	}
}

func TestPickByTimestampChoosesNewest(t *testing.T) {
	oldValue := NewPut(VectorClock{"node-a": 1}, "old", time.Unix(0, 1))
	newValue := NewPut(VectorClock{"node-b": 1}, "new", time.Unix(0, 2))

	picked := PickByTimestamp([]Envelope{oldValue, newValue})

	if picked.Value != "new" {
		t.Fatalf("picked value = %q", picked.Value)
	}
}

func TestPickByTimestampTieBreakIsDeterministic(t *testing.T) {
	a := NewPut(VectorClock{"node-a": 1}, "a", time.Unix(0, 1))
	b := NewPut(VectorClock{"node-b": 1}, "b", time.Unix(0, 1))

	first := PickByTimestamp([]Envelope{a, b})
	second := PickByTimestamp([]Envelope{b, a})

	if first.Value != second.Value {
		t.Fatalf("tie-break depends on input order: %q vs %q", first.Value, second.Value)
	}
	if first.Value != "b" {
		t.Fatalf("expected lexicographically greater value to win tie, got %q", first.Value)
	}
}

func TestReconcileClonesWinner(t *testing.T) {
	value := NewPut(VectorClock{"node-a": 1}, "value", time.Unix(0, 1))
	result := Reconcile([]Envelope{value})
	if result.Winner == nil {
		t.Fatalf("expected winner")
	}

	result.Winner.VectorClock["node-a"] = 99

	if value.VectorClock["node-a"] != 1 {
		t.Fatalf("input envelope mutated through winner: %#v", value.VectorClock)
	}
}
