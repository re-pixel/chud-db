package ring

import (
	"testing"

	"nosqlEngine/src/cluster/transport/pb"
)

func TestRangeRoundTrip(t *testing.T) {
	r := Range{Start: "a", End: "m", Replicas: []string{"node-1", "node-2"}}

	got := RangeFromProto(RangeToProto(r))
	if got.Start != r.Start || got.End != r.End {
		t.Fatalf("round trip bounds = (%q, %q), want (%q, %q)", got.Start, got.End, r.Start, r.End)
	}
	if len(got.Replicas) != 2 || got.Replicas[0] != "node-1" || got.Replicas[1] != "node-2" {
		t.Fatalf("round trip replicas = %#v", got.Replicas)
	}
}

func TestRangeRoundTripPreservesUnboundedEnds(t *testing.T) {
	r := Range{Start: "", End: "", Replicas: []string{"node-1"}}

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
		Generation: 3,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "m", End: "", Replicas: []string{"node-2", "node-3"}},
		},
	}

	got := RangeMapFromProto(RangeMapToProto(m))
	if got.Generation != m.Generation {
		t.Fatalf("generation = %d, want %d", got.Generation, m.Generation)
	}
	if len(got.Ranges) != 2 {
		t.Fatalf("ranges length = %d, want 2", len(got.Ranges))
	}
	if got.Ranges[1].Start != "m" || len(got.Ranges[1].Replicas) != 2 {
		t.Fatalf("second range = %+v", got.Ranges[1])
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("round-tripped map should still validate: %v", err)
	}
}

func TestRangeMapFromProtoTreatsNilAsZeroValue(t *testing.T) {
	got := RangeMapFromProto(nil)
	if got.Generation != 0 || got.Ranges != nil {
		t.Fatalf("expected zero value range map for nil, got %+v", got)
	}
}

func TestRangeMapToProtoAndFromProtoHandleEmptyRanges(t *testing.T) {
	m := RangeMap{Generation: 1}

	pm := RangeMapToProto(m)
	if len(pm.GetRanges()) != 0 {
		t.Fatalf("expected empty ranges, got %#v", pm.GetRanges())
	}

	got := RangeMapFromProto(&pb.RangeMap{Generation: 1})
	if len(got.Ranges) != 0 {
		t.Fatalf("expected empty ranges, got %#v", got.Ranges)
	}
}
