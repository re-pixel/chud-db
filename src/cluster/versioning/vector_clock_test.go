package versioning

import "testing"

func TestCompareVectorClocks(t *testing.T) {
	tests := []struct {
		name string
		a    VectorClock
		b    VectorClock
		want Relation
	}{
		{
			name: "equal empty clocks",
			a:    VectorClock{},
			b:    VectorClock{},
			want: Equal,
		},
		{
			name: "equal same counters",
			a:    VectorClock{"node-a": 1, "node-b": 2},
			b:    VectorClock{"node-a": 1, "node-b": 2},
			want: Equal,
		},
		{
			name: "before",
			a:    VectorClock{"node-a": 1, "node-b": 1},
			b:    VectorClock{"node-a": 2, "node-b": 1},
			want: Before,
		},
		{
			name: "after",
			a:    VectorClock{"node-a": 3, "node-b": 1},
			b:    VectorClock{"node-a": 2, "node-b": 1},
			want: After,
		},
		{
			name: "concurrent",
			a:    VectorClock{"node-a": 2, "node-b": 1},
			b:    VectorClock{"node-a": 1, "node-b": 2},
			want: Concurrent,
		},
		{
			name: "missing node treated as zero",
			a:    VectorClock{"node-a": 1},
			b:    VectorClock{"node-a": 1, "node-b": 1},
			want: Before,
		},
		{
			name: "explicit zero equals missing node",
			a:    VectorClock{"node-a": 1, "node-b": 0},
			b:    VectorClock{"node-a": 1},
			want: Equal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Compare(tt.a, tt.b); got != tt.want {
				t.Fatalf("Compare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIncrementDoesNotMutateOriginal(t *testing.T) {
	original := VectorClock{"node-a": 1}
	next := original.Increment("node-a")

	if original["node-a"] != 1 {
		t.Fatalf("original mutated: %#v", original)
	}
	if next["node-a"] != 2 {
		t.Fatalf("incremented clock = %#v", next)
	}
}

func TestIncrementAddsMissingNode(t *testing.T) {
	next := VectorClock{"node-a": 1}.Increment("node-b")

	if next["node-a"] != 1 || next["node-b"] != 1 {
		t.Fatalf("incremented clock = %#v", next)
	}
}

func TestMergeTakesMaximumCounters(t *testing.T) {
	a := VectorClock{"node-a": 1, "node-b": 4}
	b := VectorClock{"node-a": 3, "node-c": 2}

	merged := Merge(a, b)

	if merged["node-a"] != 3 || merged["node-b"] != 4 || merged["node-c"] != 2 {
		t.Fatalf("merged clock = %#v", merged)
	}
}

func TestMergeDoesNotMutateInputs(t *testing.T) {
	a := VectorClock{"node-a": 1}
	b := VectorClock{"node-a": 2, "node-b": 1}

	merged := Merge(a, b)
	merged["node-a"] = 99

	if a["node-a"] != 1 {
		t.Fatalf("left input mutated: %#v", a)
	}
	if b["node-a"] != 2 || b["node-b"] != 1 {
		t.Fatalf("right input mutated: %#v", b)
	}
}

func TestCloneDoesNotShareMap(t *testing.T) {
	original := VectorClock{"node-a": 1}
	clone := original.Clone()
	clone["node-a"] = 2

	if original["node-a"] != 1 {
		t.Fatalf("original mutated through clone: %#v", original)
	}
}
