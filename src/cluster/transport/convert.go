package transport

import (
	"fmt"
	"sort"

	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"
)

func VectorClockToProto(clock versioning.VectorClock) *pb.VectorClock {
	entries := make([]*pb.VectorClockEntry, 0, len(clock))
	nodeIDs := make([]string, 0, len(clock))
	for nodeID := range clock {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	for _, nodeID := range nodeIDs {
		entries = append(entries, &pb.VectorClockEntry{
			NodeId:  nodeID,
			Counter: clock[nodeID],
		})
	}
	return &pb.VectorClock{Entries: entries}
}

func VectorClockFromProto(clock *pb.VectorClock) versioning.VectorClock {
	if clock == nil {
		return versioning.VectorClock{}
	}
	out := make(versioning.VectorClock, len(clock.GetEntries()))
	for _, entry := range clock.GetEntries() {
		if entry.GetNodeId() == "" {
			continue
		}
		out[entry.GetNodeId()] = entry.GetCounter()
	}
	return out
}

func EnvelopeToProto(envelope versioning.Envelope) *pb.Envelope {
	return &pb.Envelope{
		Version:           int32(envelope.Version),
		VectorClock:       VectorClockToProto(envelope.VectorClock),
		UpdatedAtUnixNano: envelope.UpdatedAtUnixNano,
		Deleted:           envelope.Deleted,
		Value:             envelope.Value,
	}
}

func EnvelopeFromProto(envelope *pb.Envelope) (versioning.Envelope, error) {
	if envelope == nil {
		return versioning.Envelope{}, fmt.Errorf("envelope is nil")
	}
	out := versioning.Envelope{
		Version:           int(envelope.GetVersion()),
		VectorClock:       VectorClockFromProto(envelope.GetVectorClock()),
		UpdatedAtUnixNano: envelope.GetUpdatedAtUnixNano(),
		Deleted:           envelope.GetDeleted(),
		Value:             envelope.GetValue(),
	}
	encoded, err := versioning.Encode(out)
	if err != nil {
		return versioning.Envelope{}, err
	}
	return versioning.Decode(encoded)
}

func KeyEnvelopeToProto(row versioning.KeyEnvelope) *pb.KeyEnvelope {
	return &pb.KeyEnvelope{
		Key:      row.Key,
		Envelope: EnvelopeToProto(row.Envelope),
	}
}

func KeyEnvelopeFromProto(row *pb.KeyEnvelope) (versioning.KeyEnvelope, error) {
	if row == nil {
		return versioning.KeyEnvelope{}, fmt.Errorf("key envelope is nil")
	}
	envelope, err := EnvelopeFromProto(row.GetEnvelope())
	if err != nil {
		return versioning.KeyEnvelope{}, fmt.Errorf("key %q: %w", row.GetKey(), err)
	}
	return versioning.KeyEnvelope{Key: row.GetKey(), Envelope: envelope}, nil
}

func OKStatus() *pb.Status {
	return &pb.Status{Ok: true}
}

func ErrorStatus(err error) *pb.Status {
	if err == nil {
		return OKStatus()
	}
	return &pb.Status{Ok: false, Error: err.Error()}
}
