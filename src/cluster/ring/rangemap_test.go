package ring

import (
	"strings"
	"testing"
)

func singleRange(replicas ...string) RangeMap {
	return RangeMap{
		Ranges: []Range{{Start: "", End: "", Replicas: replicas, Generation: 1, ProposalID: "bootstrap"}},
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
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 2, ProposalID: "test"},
			{Start: "m", End: "", Replicas: []string{"node-2"}, Generation: 2, ProposalID: "test"},
		},
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("expected valid contiguous multi-range map, got %v", err)
	}
}

func TestRangeMapValidateRejectsEmpty(t *testing.T) {
	m := RangeMap{}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "at least one range") {
		t.Fatalf("expected empty-ranges error, got %v", err)
	}
}

func TestRangeMapValidateRejectsBoundedFirstRange(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "a", End: "", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "unbounded") {
		t.Fatalf("expected unbounded-start error, got %v", err)
	}
}

func TestRangeMapValidateRejectsBoundedLastRange(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "z", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "unbounded") {
		t.Fatalf("expected unbounded-end error, got %v", err)
	}
}

func TestRangeMapValidateRejectsGap(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "n", End: "", Replicas: []string{"node-2"}, Generation: 1, ProposalID: "test"},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "contiguous") {
		t.Fatalf("expected contiguity error for gap, got %v", err)
	}
}

func TestRangeMapValidateRejectsOverlap(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "k", End: "", Replicas: []string{"node-2"}, Generation: 1, ProposalID: "test"},
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

func TestRangeMapValidateRejectsMissingGeneration(t *testing.T) {
	m := singleRange("node-1")
	m.Ranges[0].Generation = 0

	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "generation 0") {
		t.Fatalf("expected generation error, got %v", err)
	}
}

func TestRangeMapValidateRejectsMissingProposalID(t *testing.T) {
	m := singleRange("node-1")
	m.Ranges[0].ProposalID = ""

	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "proposal ID") {
		t.Fatalf("expected proposal ID error, got %v", err)
	}
}

func TestRangeMapValidateRejectsDegenerateRange(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "k", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "k", End: "k", Replicas: []string{"node-2"}, Generation: 1, ProposalID: "test"},
			{Start: "k", End: "", Replicas: []string{"node-3"}, Generation: 1, ProposalID: "test"},
		},
	}
	err := m.Validate()
	if err == nil || !strings.Contains(err.Error(), "empty or inverted") {
		t.Fatalf("expected degenerate-range error, got %v", err)
	}
}

func TestRangeMapOwnersFindsContainingRange(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "m", End: "", Replicas: []string{"node-2", "node-3"}, Generation: 1, ProposalID: "test"},
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

func TestRangeMapOwnersForKeyRangeWithinSingleRange(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "m", End: "", Replicas: []string{"node-2", "node-3"}, Generation: 1, ProposalID: "test"},
		},
	}

	owners, ok := m.OwnersForKeyRange("a", "b")
	if !ok || len(owners) != 1 || owners[0] != "node-1" {
		t.Fatalf("owners = %#v, ok=%v", owners, ok)
	}
}

func TestRangeMapOwnersForKeyRangeRejectsBoundaryStraddling(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "m", End: "", Replicas: []string{"node-2"}, Generation: 1, ProposalID: "test"},
		},
	}

	if _, ok := m.OwnersForKeyRange("a", "z"); ok {
		t.Fatalf("expected scan straddling the range boundary to be rejected")
	}
	// end == the exclusive boundary of the containing range: the
	// boundary key itself belongs to the next range, so this still
	// straddles.
	if _, ok := m.OwnersForKeyRange("a", "m"); ok {
		t.Fatalf("expected scan ending exactly at the range boundary to be rejected")
	}
}

func TestRangeMapOwnersForKeyRangeUnboundedRangeAcceptsAnyEnd(t *testing.T) {
	m := singleRange("node-1")

	if _, ok := m.OwnersForKeyRange("a", "zzzzzzzz"); !ok {
		t.Fatalf("expected a fully unbounded range to own any end key")
	}
}

func TestRangeMapOwnersForRangeAcceptsExactBoundedRange(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "m", End: "", Replicas: []string{"node-2"}, Generation: 1, ProposalID: "test"},
		},
	}

	owners, ok := m.OwnersForRange("", "m")
	if !ok || len(owners) != 1 || owners[0] != "node-1" {
		t.Fatalf("owners = %#v, ok=%v", owners, ok)
	}
}

func TestRangeMapOwnersForRangeRejectsBoundaryStraddling(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "m", End: "", Replicas: []string{"node-2"}, Generation: 1, ProposalID: "test"},
		},
	}

	if _, ok := m.OwnersForRange("a", "z"); ok {
		t.Fatalf("expected half-open range straddling the boundary to be rejected")
	}
	if _, ok := m.OwnersForRange("a", ""); ok {
		t.Fatalf("expected bounded owner range to reject an unbounded requested end")
	}
}

func TestRangeMapOwnersForRangeRejectsDegenerateRange(t *testing.T) {
	m := singleRange("node-1")

	if _, ok := m.OwnersForRange("m", "m"); ok {
		t.Fatalf("expected degenerate range to be rejected")
	}
	if _, ok := m.OwnersForRange("z", "a"); ok {
		t.Fatalf("expected inverted range to be rejected")
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
