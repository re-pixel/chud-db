package ring

import (
	"testing"

	"nosqlEngine/src/cluster/transport/pb"
)

func TestRangeRoundTrip(t *testing.T) {
	r := Range{
		Start:      "a",
		End:        "m",
		Replicas:   []string{"node-1", "node-2"},
		Generation: 4,
		ProposalID: "proposal-1",
	}

	got := RangeFromProto(RangeToProto(r))
	if !rangesEqual(got, r) {
		t.Fatalf("round trip = %#v, want %#v", got, r)
	}
}

func TestRangeRoundTripPreservesUnboundedEnds(t *testing.T) {
	r := Range{Start: "", End: "", Replicas: []string{"node-1"}, Generation: 1}

	got := RangeFromProto(RangeToProto(r))
	if got.Start != "" || got.End != "" {
		t.Fatalf("expected unbounded ends to round trip as empty, got (%q, %q)", got.Start, got.End)
	}
}

func TestRangeFromProtoTreatsNilAsZeroValue(t *testing.T) {
	got := RangeFromProto(nil)
	if got.Start != "" || got.End != "" || got.Replicas != nil {
		t.Fatalf("expected zero value range for nil, got %+v", got)
	}
}

func TestRangeMapRoundTrip(t *testing.T) {
	m := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 2, ProposalID: "left"},
			{Start: "m", End: "", Replicas: []string{"node-2", "node-3"}, Generation: 3, ProposalID: "right"},
		},
	}

	wire := RangeMapToProto(m)
	if wire.GetGeneration() != 3 {
		t.Fatalf("wire epoch = %d, want 3", wire.GetGeneration())
	}
	got := RangeMapFromProto(wire)
	if !rangeMapsEqual(got, m) {
		t.Fatalf("round trip = %#v, want %#v", got, m)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("round-tripped map should still validate: %v", err)
	}
}

func TestRangeMapFromProtoTreatsNilAsZeroValue(t *testing.T) {
	got := RangeMapFromProto(nil)
	if got.Ranges != nil {
		t.Fatalf("expected zero value range map for nil, got %+v", got)
	}
}

func TestRangeMapToProtoAndFromProtoHandleEmptyRanges(t *testing.T) {
	m := RangeMap{}

	pm := RangeMapToProto(m)
	if len(pm.GetRanges()) != 0 {
		t.Fatalf("expected empty ranges, got %#v", pm.GetRanges())
	}

	got := RangeMapFromProto(&pb.RangeMap{Generation: 1})
	if len(got.Ranges) != 0 {
		t.Fatalf("expected empty ranges, got %#v", got.Ranges)
	}
}
