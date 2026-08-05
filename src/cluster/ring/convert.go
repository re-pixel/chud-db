package ring

import "nosqlEngine/src/cluster/transport/pb"

func RangeToProto(r Range) *pb.RangeSpec {
	return &pb.RangeSpec{
		Start:          r.Start,
		End:            r.End,
		ReplicaNodeIds: append([]string(nil), r.Replicas...),
		Generation:     r.Generation,
		ProposalId:     r.ProposalID,
	}
}

func RangeFromProto(spec *pb.RangeSpec) Range {
	if spec == nil {
		return Range{}
	}
	return Range{
		Start:      spec.GetStart(),
		End:        spec.GetEnd(),
		Replicas:   append([]string(nil), spec.GetReplicaNodeIds()...),
		Generation: spec.GetGeneration(),
		ProposalID: spec.GetProposalId(),
	}
}

func RangeMapToProto(m RangeMap) *pb.RangeMap {
	ranges := make([]*pb.RangeSpec, 0, len(m.Ranges))
	for _, r := range m.Ranges {
		ranges = append(ranges, RangeToProto(r))
	}
	return &pb.RangeMap{
		Generation: m.Epoch(),
		Ranges:     ranges,
	}
}

func RangeMapFromProto(pm *pb.RangeMap) RangeMap {
	if pm == nil {
		return RangeMap{}
	}
	specs := pm.GetRanges()
	ranges := make([]Range, 0, len(specs))
	for _, spec := range specs {
		ranges = append(ranges, RangeFromProto(spec))
	}
	return RangeMap{Ranges: ranges}
}
