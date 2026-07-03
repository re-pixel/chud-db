package ring

import "testing"

func TestRangeContainsUnboundedOnBothSides(t *testing.T) {
	r := Range{Start: "", End: ""}
	for _, key := range []string{"", "a", "zzz", "\x00"} {
		if !r.Contains(key) {
			t.Fatalf("expected unbounded range to contain %q", key)
		}
	}
}

func TestRangeContainsHalfOpenBoundaries(t *testing.T) {
	r := Range{Start: "m", End: "z"}

	if r.Contains("a") {
		t.Fatalf("key below start should not be contained")
	}
	if !r.Contains("m") {
		t.Fatalf("start boundary should be inclusive")
	}
	if !r.Contains("n") {
		t.Fatalf("key inside range should be contained")
	}
	if r.Contains("z") {
		t.Fatalf("end boundary should be exclusive")
	}
	if r.Contains("zz") {
		t.Fatalf("key above end should not be contained")
	}
}

func TestRangeContainsUnboundedStart(t *testing.T) {
	r := Range{Start: "", End: "m"}
	if !r.Contains("") {
		t.Fatalf("empty key should be contained when start is unbounded")
	}
	if !r.Contains("a") {
		t.Fatalf("expected key below end to be contained")
	}
	if r.Contains("m") {
		t.Fatalf("end boundary should be exclusive")
	}
}

func TestRangeContainsUnboundedEnd(t *testing.T) {
	r := Range{Start: "m", End: ""}
	if r.Contains("a") {
		t.Fatalf("key below start should not be contained")
	}
	if !r.Contains("zzzzzzzz") {
		t.Fatalf("expected arbitrarily large key to be contained when end is unbounded")
	}
}

func TestRangeCloneIsIndependent(t *testing.T) {
	r := Range{Start: "a", End: "z", Replicas: []string{"node-1"}}
	clone := r.Clone()
	clone.Replicas[0] = "node-2"

	if r.Replicas[0] != "node-1" {
		t.Fatalf("mutating clone affected original: %#v", r)
	}
}
