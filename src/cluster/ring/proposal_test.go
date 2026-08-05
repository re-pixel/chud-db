package ring

import "testing"

func TestProposalIDIsCanonical(t *testing.T) {
	left := []Range{
		{Start: "m", End: "", Replicas: []string{"node-2", "node-1"}},
		{Start: "", End: "m", Replicas: []string{"node-3"}},
	}
	right := []Range{
		{Start: "", End: "m", Replicas: []string{"node-3"}},
		{Start: "m", End: "", Replicas: []string{"node-1", "node-2"}},
	}

	if ProposalID(left) != ProposalID(right) {
		t.Fatalf("proposal ID should ignore range and replica ordering")
	}
}

func TestProposalIDChangesWithRoutingContent(t *testing.T) {
	left := []Range{{Start: "", End: "", Replicas: []string{"node-1"}}}
	right := []Range{{Start: "", End: "", Replicas: []string{"node-2"}}}

	if ProposalID(left) == ProposalID(right) {
		t.Fatalf("different replica assignments should have different proposal IDs")
	}
}
