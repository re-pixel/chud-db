package node

import (
	"context"
	"errors"
	"testing"
	"time"

	clusterconfig "nosqlEngine/src/cluster/config"
	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServerHealth(t *testing.T) {
	cfg := clusterconfig.DefaultConfig()
	cfg.NodeID = "node-a"
	cfg.ClusterID = "cluster-a"
	cfg.AdvertiseAddr = "127.0.0.1:7000"
	server := NewServer(cfg, newFakeStore())

	resp, err := server.Health(context.Background(), &pb.HealthRequest{})
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !resp.GetReady() {
		t.Fatalf("expected ready health response")
	}
	if resp.GetNode().GetNodeId() != "node-a" || resp.GetNode().GetClusterId() != "cluster-a" {
		t.Fatalf("node info = %#v", resp.GetNode())
	}
}

func TestServerPut(t *testing.T) {
	store := newFakeStore()
	server := NewServer(clusterconfig.DefaultConfig(), store)
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))

	resp, err := server.Put(context.Background(), &pb.PutRequest{
		Key:      "key",
		Envelope: transport.EnvelopeToProto(env),
		Sync:     true,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !resp.GetStatus().GetOk() {
		t.Fatalf("status = %#v", resp.GetStatus())
	}
	if store.put.key != "key" || !store.put.sync || store.put.envelope.Value != "value" {
		t.Fatalf("put call = %#v", store.put)
	}
}

func TestServerPutRejectsInvalidRequest(t *testing.T) {
	server := NewServer(clusterconfig.DefaultConfig(), newFakeStore())

	_, err := server.Put(context.Background(), &pb.PutRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerDelete(t *testing.T) {
	store := newFakeStore()
	server := NewServer(clusterconfig.DefaultConfig(), store)
	env := versioning.NewDelete(versioning.VectorClock{"node-a": 2}, time.Unix(0, 2))

	resp, err := server.Delete(context.Background(), &pb.DeleteRequest{
		Key:      "key",
		Envelope: transport.EnvelopeToProto(env),
		Sync:     true,
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !resp.GetStatus().GetOk() {
		t.Fatalf("status = %#v", resp.GetStatus())
	}
	if store.delete.key != "key" || !store.delete.sync || !store.delete.envelope.Deleted {
		t.Fatalf("delete call = %#v", store.delete)
	}
}

func TestServerGetFound(t *testing.T) {
	store := newFakeStore()
	store.getEnvelope = versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))
	store.getFound = true
	server := NewServer(clusterconfig.DefaultConfig(), store)

	resp, err := server.Get(context.Background(), &pb.GetRequest{Key: "key"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() {
		t.Fatalf("expected found response")
	}
	env, err := transport.EnvelopeFromProto(resp.GetEnvelope())
	if err != nil {
		t.Fatalf("decode response envelope: %v", err)
	}
	if env.Value != "value" {
		t.Fatalf("value = %q", env.Value)
	}
}

func TestServerGetMissing(t *testing.T) {
	server := NewServer(clusterconfig.DefaultConfig(), newFakeStore())

	resp, err := server.Get(context.Background(), &pb.GetRequest{Key: "missing"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetFound() {
		t.Fatalf("missing key returned found")
	}
	if resp.GetEnvelope() != nil {
		t.Fatalf("missing key returned envelope")
	}
}

func TestServerRangeScan(t *testing.T) {
	store := newFakeStore()
	store.rangeRows = []versioning.KeyEnvelope{
		{Key: "a", Envelope: versioning.NewPut(versioning.VectorClock{"node-a": 1}, "a-value", time.Unix(0, 1))},
		{Key: "b", Envelope: versioning.NewDelete(versioning.VectorClock{"node-a": 2}, time.Unix(0, 2))},
	}
	server := NewServer(clusterconfig.DefaultConfig(), store)

	resp, err := server.RangeScan(context.Background(), &pb.RangeScanRequest{
		Start:    "a",
		End:      "z",
		PageNum:  1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(resp.GetRows()) != 2 {
		t.Fatalf("rows = %#v", resp.GetRows())
	}
	if store.rangeCall.start != "a" || store.rangeCall.end != "z" || store.rangeCall.pageNum != 1 || store.rangeCall.pageSize != 10 {
		t.Fatalf("range call = %#v", store.rangeCall)
	}
}

func TestServerRangeScanRejectsBadPagination(t *testing.T) {
	server := NewServer(clusterconfig.DefaultConfig(), newFakeStore())

	_, err := server.RangeScan(context.Background(), &pb.RangeScanRequest{PageNum: 0, PageSize: 10})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerMapsStoreErrorsToInternal(t *testing.T) {
	store := newFakeStore()
	store.getErr = errors.New("store failed")
	server := NewServer(clusterconfig.DefaultConfig(), store)

	_, err := server.Get(context.Background(), &pb.GetRequest{Key: "key"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerContextCancellation(t *testing.T) {
	server := NewServer(clusterconfig.DefaultConfig(), newFakeStore())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := server.Get(ctx, &pb.GetRequest{Key: "key"})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

type fakeStore struct {
	put         storeWriteCall
	delete      storeWriteCall
	getEnvelope versioning.Envelope
	getFound    bool
	rangeRows   []versioning.KeyEnvelope
	rangeCall   storeRangeCall
	putErr      error
	deleteErr   error
	getErr      error
	rangeErr    error
}

type storeWriteCall struct {
	key      string
	envelope versioning.Envelope
	sync     bool
}

type storeRangeCall struct {
	start    string
	end      string
	pageNum  int
	pageSize int
}

func newFakeStore() *fakeStore {
	return &fakeStore{}
}

func (s *fakeStore) Put(key string, envelope versioning.Envelope, sync bool) error {
	s.put = storeWriteCall{key: key, envelope: envelope, sync: sync}
	return s.putErr
}

func (s *fakeStore) Delete(key string, envelope versioning.Envelope, sync bool) error {
	s.delete = storeWriteCall{key: key, envelope: envelope, sync: sync}
	return s.deleteErr
}

func (s *fakeStore) Get(key string) (versioning.Envelope, bool, error) {
	return s.getEnvelope, s.getFound, s.getErr
}

func (s *fakeStore) ScanRange(start, end string, pageNum, pageSize int) ([]versioning.KeyEnvelope, error) {
	s.rangeCall = storeRangeCall{start: start, end: end, pageNum: pageNum, pageSize: pageSize}
	return s.rangeRows, s.rangeErr
}
