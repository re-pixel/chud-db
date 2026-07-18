package coordination

import (
	"context"
	"testing"
	"time"

	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServerPutReturnsStampedClockAndAckCounts(t *testing.T) {
	owners := fakeOwners{owners: []string{"coordinator", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	local := &fakeLocalStore{}
	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())
	server := NewServer(coord)

	resp, err := server.Put(context.Background(), &pb.CoordinatedPutRequest{Key: "key", Value: "value"})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if !resp.GetStatus().GetOk() {
		t.Fatalf("expected ok status, got %+v", resp.GetStatus())
	}
	if resp.GetAcks() != 2 || resp.GetRequired() != 2 {
		t.Fatalf("unexpected acks/required: %+v", resp)
	}
	clock := transport.VectorClockFromProto(resp.GetVectorClock())
	if clock["coordinator"] != 1 {
		t.Fatalf("expected stamped clock from coordinator, got %+v", clock)
	}
}

func TestServerPutMapsEmptyKeyToInvalidArgument(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "a"}}
	coord := NewCoordinator("coordinator", owners, addresses, newFakeReplicaClient(), &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	_, err := server.Put(context.Background(), &pb.CoordinatedPutRequest{Key: "", Value: "value"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestServerPutMapsQuorumNotMetToUnavailable(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	replicas.errByAddr["10.0.0.1:7000"] = context.DeadlineExceeded
	replicas.errByAddr["10.0.0.2:7000"] = context.DeadlineExceeded
	coord := NewCoordinator("coordinator", owners, addresses, replicas, &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	_, err := server.Put(context.Background(), &pb.CoordinatedPutRequest{Key: "key", Value: "value"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

func TestServerPutMapsQuorumAboveOwnerCountToInvalidArgument(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "a"}}
	coord := NewCoordinator("coordinator", owners, addresses, newFakeReplicaClient(), &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	_, err := server.Put(context.Background(), &pb.CoordinatedPutRequest{Key: "key", Value: "value", WriteQuorum: 5})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestServerDeletePassesThroughContextAndQuorum(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	coord := NewCoordinator("coordinator", owners, addresses, replicas, &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	req := &pb.CoordinatedDeleteRequest{
		Key:         "key",
		Context:     transport.VectorClockToProto(versioning.VectorClock{"node-1": 3}),
		WriteQuorum: 2,
	}
	resp, err := server.Delete(context.Background(), req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	clock := transport.VectorClockFromProto(resp.GetVectorClock())
	if clock["node-1"] != 3 || clock["coordinator"] != 1 {
		t.Fatalf("expected merged clock carrying forward client context, got %+v", clock)
	}
	if len(replicas.deleteCalls) != 2 {
		t.Fatalf("expected delete fanned out to both replicas, got %#v", replicas.deleteCalls)
	}
}

func TestServerGetReturnsFoundValueAndConflictFlag(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	older := versioning.NewPut(versioning.VectorClock{"node-1": 1}, "older", time.Now().Add(-time.Minute))
	newer := versioning.NewPut(versioning.VectorClock{"node-2": 1}, "newer", time.Now())
	replicas.getByAddr["10.0.0.1:7000"] = storedEnvelope{envelope: older, found: true}
	replicas.getByAddr["10.0.0.2:7000"] = storedEnvelope{envelope: newer, found: true}
	coord := NewCoordinator("coordinator", owners, addresses, replicas, &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	resp, err := server.Get(context.Background(), &pb.CoordinatedGetRequest{Key: "key"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !resp.GetFound() || resp.GetValue() != "newer" || !resp.GetHadConflict() {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestServerGetReturnsNotFoundWithoutValue(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000"}}
	coord := NewCoordinator("coordinator", owners, addresses, newFakeReplicaClient(), &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	resp, err := server.Get(context.Background(), &pb.CoordinatedGetRequest{Key: "key", ReadQuorum: 1})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.GetFound() || resp.GetValue() != "" {
		t.Fatalf("expected not found with empty value, got %+v", resp)
	}
}

func TestServerGetMapsQuorumNotMetToUnavailable(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	replicas.errByAddr["10.0.0.1:7000"] = context.DeadlineExceeded
	replicas.errByAddr["10.0.0.2:7000"] = context.DeadlineExceeded
	coord := NewCoordinator("coordinator", owners, addresses, replicas, &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	_, err := server.Get(context.Background(), &pb.CoordinatedGetRequest{Key: "key"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

func TestServerRejectsCanceledContext(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1"}}
	coord := NewCoordinator("coordinator", owners, fakeAddresses{addrs: map[string]string{}}, newFakeReplicaClient(), &fakeLocalStore{}, testConfig())
	server := NewServer(coord)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := server.Get(ctx, &pb.CoordinatedGetRequest{Key: "key"})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("expected Canceled, got %v", err)
	}
}
