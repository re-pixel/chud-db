package ring

import (
	"math"
	"testing"
)

func versionedRange(start, end string, generation uint64, proposalID string, replicas ...string) Range {
	return Range{
		Start:      start,
		End:        end,
		Replicas:   replicas,
		Generation: generation,
		ProposalID: proposalID,
	}
}

func TestRangeMapMergeAppliesSplitWithoutCoalescingItsHalves(t *testing.T) {
	current := RangeMap{Ranges: []Range{
		versionedRange("", "", 1, "base", "node-1", "node-2"),
	}}
	incoming := RangeMap{Ranges: []Range{
		versionedRange("", "m", 2, "split", "node-1", "node-2"),
		versionedRange("m", "", 2, "split", "node-1", "node-2"),
	}}

	got, changed, err := current.Merge(incoming)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !changed {
		t.Fatalf("expected split to change the map")
	}
	if !rangeMapsEqual(got, incoming) {
		t.Fatalf("merged map = %#v, want %#v", got, incoming)
	}
}

func TestRangeMapMergeCoalescesFragmentsFromSameWinningRange(t *testing.T) {
	current := RangeMap{Ranges: []Range{
		versionedRange("", "", 3, "merged", "node-1"),
	}}
	incoming := RangeMap{Ranges: []Range{
		versionedRange("", "m", 1, "old-split", "node-1"),
		versionedRange("m", "", 2, "old-split", "node-1"),
	}}

	got, changed, err := current.Merge(incoming)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if changed {
		t.Fatalf("stale split should not change the current map")
	}
	if !rangeMapsEqual(got, current) {
		t.Fatalf("merged map = %#v, want %#v", got, current)
	}
}

func TestRangeMapMergeAppliesMerge(t *testing.T) {
	current := RangeMap{Ranges: []Range{
		versionedRange("", "m", 1, "left", "node-1"),
		versionedRange("m", "", 1, "right", "node-2"),
	}}
	incoming := RangeMap{Ranges: []Range{
		versionedRange("", "", 2, "merge", "node-1", "node-2"),
	}}

	got, changed, err := current.Merge(incoming)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !changed || !rangeMapsEqual(got, incoming) {
		t.Fatalf("merged map = %#v, changed=%v, want %#v", got, changed, incoming)
	}
}

func TestRangeMapMergeCombinesUnrelatedConcurrentChanges(t *testing.T) {
	local := RangeMap{Ranges: []Range{
		versionedRange("", "m", 2, "left-new", "node-2"),
		versionedRange("m", "", 1, "right-base", "node-1"),
	}}
	incoming := RangeMap{Ranges: []Range{
		versionedRange("", "m", 1, "left-base", "node-1"),
		versionedRange("m", "", 2, "right-new", "node-3"),
	}}
	want := RangeMap{Ranges: []Range{
		versionedRange("", "m", 2, "left-new", "node-2"),
		versionedRange("m", "", 2, "right-new", "node-3"),
	}}

	got, changed, err := local.Merge(incoming)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !changed || !rangeMapsEqual(got, want) {
		t.Fatalf("merged map = %#v, changed=%v, want %#v", got, changed, want)
	}
}

func TestRangeMapMergeResolvesCompetingProposalsDeterministically(t *testing.T) {
	proposalA := RangeMap{Ranges: []Range{
		versionedRange("", "m", 2, "a", "node-1"),
		versionedRange("m", "", 2, "a", "node-2"),
	}}
	proposalB := RangeMap{Ranges: []Range{
		versionedRange("", "n", 2, "b", "node-2"),
		versionedRange("n", "", 2, "b", "node-1"),
	}}
	want := RangeMap{Ranges: []Range{
		versionedRange("", "m", 3, "a", "node-1"),
		versionedRange("m", "", 3, "a", "node-2"),
	}}

	fromA, changedA, err := proposalA.Merge(proposalB)
	if err != nil {
		t.Fatalf("merge A with B: %v", err)
	}
	fromB, changedB, err := proposalB.Merge(proposalA)
	if err != nil {
		t.Fatalf("merge B with A: %v", err)
	}
	if !changedA || !changedB {
		t.Fatalf("both conflicting views should change: A=%v B=%v", changedA, changedB)
	}
	if !rangeMapsEqual(fromA, want) || !rangeMapsEqual(fromB, want) {
		t.Fatalf("results did not converge: fromA=%#v fromB=%#v want=%#v", fromA, fromB, want)
	}

	propagated, changed, err := proposalB.Merge(fromA)
	if err != nil {
		t.Fatalf("propagate resolution: %v", err)
	}
	if !changed || !rangeMapsEqual(propagated, want) {
		t.Fatalf("resolved generation did not propagate: %#v, changed=%v", propagated, changed)
	}
}

func TestRangeMapMergeIdenticalMapIsNoOp(t *testing.T) {
	current := RangeMap{Ranges: []Range{
		versionedRange("", "", 1, "base", "node-1"),
	}}

	got, changed, err := current.Merge(current.Clone())
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if changed || !rangeMapsEqual(got, current) {
		t.Fatalf("identical merge = %#v, changed=%v", got, changed)
	}
}

func TestRangeMapMergeRejectsTieAtMaximumGeneration(t *testing.T) {
	left := RangeMap{Ranges: []Range{
		versionedRange("", "", math.MaxUint64, "a", "node-1"),
	}}
	right := RangeMap{Ranges: []Range{
		versionedRange("", "", math.MaxUint64, "b", "node-2"),
	}}

	if _, _, err := left.Merge(right); err == nil {
		t.Fatalf("expected maximum-generation tie to fail")
	}
}
