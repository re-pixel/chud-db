package ring

import (
	"strings"
	"testing"
)

func singleRange(replicas ...string) RangeMap {
	return RangeMap{
		Generation: 1,
		Ranges:     []Range{{Start: "", End: "", Replicas: replicas}},
	}
}

func TestRangeMapValidateAcceptsSingleGlobalRange(t *testing.T) {
	m := singleRange("node-1", "node-2", "node-3")
	if err := m.Validate(); err != nil {
		t.Fatalf("expected valid single-range map, got %v", err)
	}
}

func TestRangeMapValidateAcceptsContiguousMultiRange(t *testing.T) {
	m := RangeMap{
		Generation: 2,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "m", End: "", Replicas: []string{"node-2"}},
		},
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("expected valid contiguous multi-range map, got %v", err)
	}
}

func TestRangeMapValidateRejectsEmpty(t *testing.T) {
	m := RangeMap{Generation: 1}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "at least one range") {
		t.Fatalf("expected empty-ranges error, got %v", err)
	}
}

func TestRangeMapValidateRejectsBoundedFirstRange(t *testing.T) {
	m := RangeMap{
		Generation: 1,
		Ranges: []Range{
			{Start: "a", End: "", Replicas: []string{"node-1"}},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "unbounded") {
		t.Fatalf("expected unbounded-start error, got %v", err)
	}
}

func TestRangeMapValidateRejectsBoundedLastRange(t *testing.T) {
	m := RangeMap{
		Generation: 1,
		Ranges: []Range{
			{Start: "", End: "z", Replicas: []string{"node-1"}},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "unbounded") {
		t.Fatalf("expected unbounded-end error, got %v", err)
	}
}

func TestRangeMapValidateRejectsGap(t *testing.T) {
	m := RangeMap{
		Generation: 1,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "n", End: "", Replicas: []string{"node-2"}},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "contiguous") {
		t.Fatalf("expected contiguity error for gap, got %v", err)
	}
}

func TestRangeMapValidateRejectsOverlap(t *testing.T) {
	m := RangeMap{
		Generation: 1,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "k", End: "", Replicas: []string{"node-2"}},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "contiguous") {
		t.Fatalf("expected contiguity error for overlap, got %v", err)
	}
}

func TestRangeMapValidateRejectsEmptyReplicas(t *testing.T) {
	m := singleRange()
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "no replicas") {
		t.Fatalf("expected no-replicas error, got %v", err)
	}
}

func TestRangeMapValidateRejectsDegenerateRange(t *testing.T) {
	m := RangeMap{
		Generation: 1,
		Ranges: []Range{
			{Start: "", End: "k", Replicas: []string{"node-1"}},
			{Start: "k", End: "k", Replicas: []string{"node-2"}},
			{Start: "k", End: "", Replicas: []string{"node-3"}},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "empty or inverted") {
		t.Fatalf("expected degenerate-range error, got %v", err)
	}
}

func TestRangeMapOwnersFindsContainingRange(t *testing.T) {
	m := RangeMap{
		Generation: 1,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "m", End: "", Replicas: []string{"node-2", "node-3"}},
		},
	}

	owners, ok := m.Owners("a")
	if !ok || len(owners) != 1 || owners[0] != "node-1" {
		t.Fatalf("owners(a) = %#v, ok=%v", owners, ok)
	}

	owners, ok = m.Owners("z")
	if !ok || len(owners) != 2 || owners[0] != "node-2" {
		t.Fatalf("owners(z) = %#v, ok=%v", owners, ok)
	}
}

func TestRangeMapCloneIsIndependent(t *testing.T) {
	m := singleRange("node-1")
	clone := m.Clone()
	clone.Ranges[0].Replicas[0] = "node-2"

	if m.Ranges[0].Replicas[0] != "node-1" {
		t.Fatalf("mutating clone affected original: %#v", m)
	}
}
