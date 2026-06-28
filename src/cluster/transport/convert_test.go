package transport

import (
	"errors"
	"strings"
	"testing"
	"time"

	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"
)

func TestVectorClockToProtoSortsEntries(t *testing.T) {
	clock := versioning.VectorClock{"node-c": 3, "node-a": 1, "node-b": 2}

	got := VectorClockToProto(clock)

	if len(got.GetEntries()) != 3 {
		t.Fatalf("entries = %#v", got.GetEntries())
	}
	wantOrder := []string{"node-a", "node-b", "node-c"}
	for i, want := range wantOrder {
		if got.GetEntries()[i].GetNodeId() != want {
			t.Fatalf("entry %d node id = %q, want %q", i, got.GetEntries()[i].GetNodeId(), want)
		}
	}
}

func TestVectorClockFromProtoTreatsNilAsEmpty(t *testing.T) {
	got := VectorClockFromProto(nil)

	if len(got) != 0 {
		t.Fatalf("clock = %#v", got)
	}
}

func TestVectorClockRoundTrip(t *testing.T) {
	clock := versioning.VectorClock{"node-a": 1, "node-b": 2}

	got := VectorClockFromProto(VectorClockToProto(clock))

	if versioning.Compare(clock, got) != versioning.Equal {
		t.Fatalf("round trip clock = %#v", got)
	}
}

func TestVectorClockFromProtoSkipsEmptyNodeIDs(t *testing.T) {
	clock := &pb.VectorClock{Entries: []*pb.VectorClockEntry{
		{NodeId: "", Counter: 10},
		{NodeId: "node-a", Counter: 1},
	}}

	got := VectorClockFromProto(clock)

	if len(got) != 1 || got["node-a"] != 1 {
		t.Fatalf("clock = %#v", got)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	envelope := versioning.NewPut(versioning.VectorClock{"node-a": 2}, "value", time.Unix(0, 123))

	got, err := EnvelopeFromProto(EnvelopeToProto(envelope))
	if err != nil {
		t.Fatalf("from proto: %v", err)
	}

	if got.Version != envelope.Version || got.Value != envelope.Value || got.UpdatedAtUnixNano != envelope.UpdatedAtUnixNano {
		t.Fatalf("envelope = %#v", got)
	}
	if versioning.Compare(got.VectorClock, envelope.VectorClock) != versioning.Equal {
		t.Fatalf("clock = %#v", got.VectorClock)
	}
}

func TestEnvelopeFromProtoNormalizesDeletedValue(t *testing.T) {
	protoEnvelope := &pb.Envelope{
		Version:           versioning.EnvelopeVersion,
		VectorClock:       VectorClockToProto(versioning.VectorClock{"node-a": 1}),
		UpdatedAtUnixNano: 123,
		Deleted:           true,
		Value:             "stale-value",
	}

	got, err := EnvelopeFromProto(protoEnvelope)
	if err != nil {
		t.Fatalf("from proto: %v", err)
	}
	if !got.Deleted {
		t.Fatalf("expected deleted envelope")
	}
	if got.Value != "" {
		t.Fatalf("deleted value = %q", got.Value)
	}
}

func TestEnvelopeFromProtoRejectsNil(t *testing.T) {
	_, err := EnvelopeFromProto(nil)
	if err == nil {
		t.Fatalf("expected nil envelope error")
	}
}

func TestEnvelopeFromProtoRejectsInvalidEnvelope(t *testing.T) {
	_, err := EnvelopeFromProto(&pb.Envelope{Version: 99, UpdatedAtUnixNano: 1})
	if err == nil {
		t.Fatalf("expected invalid envelope error")
	}
}

func TestKeyEnvelopeRoundTrip(t *testing.T) {
	row := versioning.KeyEnvelope{
		Key:      "key",
		Envelope: versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1)),
	}

	got, err := KeyEnvelopeFromProto(KeyEnvelopeToProto(row))
	if err != nil {
		t.Fatalf("from proto: %v", err)
	}

	if got.Key != row.Key || got.Envelope.Value != row.Envelope.Value {
		t.Fatalf("row = %#v", got)
	}
}

func TestKeyEnvelopeFromProtoRejectsNil(t *testing.T) {
	_, err := KeyEnvelopeFromProto(nil)
	if err == nil {
		t.Fatalf("expected nil key envelope error")
	}
}

func TestKeyEnvelopeFromProtoWrapsEnvelopeError(t *testing.T) {
	_, err := KeyEnvelopeFromProto(&pb.KeyEnvelope{Key: "bad-key"})
	if err == nil {
		t.Fatalf("expected envelope error")
	}
	if got := err.Error(); !strings.HasPrefix(got, "key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStatusHelpers(t *testing.T) {
	ok := OKStatus()
	if !ok.GetOk() || ok.GetError() != "" {
		t.Fatalf("ok status = %#v", ok)
	}

	errStatus := ErrorStatus(errors.New("failed"))
	if errStatus.GetOk() || errStatus.GetError() != "failed" {
		t.Fatalf("error status = %#v", errStatus)
	}

	nilStatus := ErrorStatus(nil)
	if !nilStatus.GetOk() {
		t.Fatalf("nil error status = %#v", nilStatus)
	}
}
