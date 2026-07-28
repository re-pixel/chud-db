package antientropy

import (
	"context"
	"net"
	"testing"
	"time"

	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startTestServer runs a real gRPC server backed by srv on an
// OS-assigned loopback port, returning the dial address. The server and
// listener are stopped when the test completes.
func startTestServer(t *testing.T, srv *Server) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterAntiEntropyServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
	})
	return lis.Addr().String()
}

func TestClientGetMerkleRoot(t *testing.T) {
	store := newFakeStore()
	store.scanEntries = []RawEntry{
		{Key: "a", Value: mustEncode(t, versioning.NewPut(versioning.VectorClock{"node-a": 1}, "va", time.Unix(0, 1)))},
		{Key: "b", Value: mustEncode(t, versioning.NewPut(versioning.VectorClock{"node-a": 1}, "vb", time.Unix(0, 2)))},
	}
	addr := startTestServer(t, NewServer(store, newFakeOwnership()))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	rootHash, itemCount, err := client.GetMerkleRoot(context.Background(), addr, "a", "z")
	if err != nil {
		t.Fatalf("GetMerkleRoot: %v", err)
	}
	if itemCount != 2 {
		t.Fatalf("item count = %d, want 2", itemCount)
	}

	wantHash, wantCount, err := ComputeMerkleRoot(store, "a", "z")
	if err != nil {
		t.Fatalf("ComputeMerkleRoot: %v", err)
	}
	if itemCount != wantCount || string(rootHash) != string(wantHash) {
		t.Fatalf("root hash/count mismatch: got (%x, %d), want (%x, %d)", rootHash, itemCount, wantHash, wantCount)
	}
}

func TestClientGetMerkleRootPropagatesServerRejection(t *testing.T) {
	ownership := newFakeOwnership()
	ownership.ownsRange = false
	addr := startTestServer(t, NewServer(newFakeStore(), ownership))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	_, _, err := client.GetMerkleRoot(context.Background(), addr, "a", "z")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestClientStreamRange(t *testing.T) {
	store := newFakeStore()
	store.scanEntries = []RawEntry{
		{Key: "a", Value: mustEncode(t, versioning.NewPut(versioning.VectorClock{"node-a": 1}, "va", time.Unix(0, 1)))},
		{Key: "b", Value: mustEncode(t, versioning.NewDelete(versioning.VectorClock{"node-a": 2}, time.Unix(0, 2)))},
	}
	addr := startTestServer(t, NewServer(store, newFakeOwnership()))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	rows, err := client.StreamRange(context.Background(), addr, "a", "z")
	if err != nil {
		t.Fatalf("StreamRange: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %#v", rows)
	}
	byKey := make(map[string]versioning.KeyEnvelope, len(rows))
	for _, row := range rows {
		byKey[row.Key] = row
	}
	if byKey["a"].Envelope.Value != "va" || byKey["a"].Envelope.Deleted {
		t.Fatalf("row a = %#v", byKey["a"])
	}
	if !byKey["b"].Envelope.Deleted {
		t.Fatalf("row b = %#v", byKey["b"])
	}
}

func TestClientStreamRangeEmpty(t *testing.T) {
	addr := startTestServer(t, NewServer(newFakeStore(), newFakeOwnership()))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	rows, err := client.StreamRange(context.Background(), addr, "a", "z")
	if err != nil {
		t.Fatalf("StreamRange: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %#v, want none", rows)
	}
}

func TestClientStreamRangePropagatesServerRejection(t *testing.T) {
	ownership := newFakeOwnership()
	ownership.ownsRange = false
	addr := startTestServer(t, NewServer(newFakeStore(), ownership))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	_, err := client.StreamRange(context.Background(), addr, "a", "z")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestClientRepairKeysAppliesPutsAndDeletes(t *testing.T) {
	store := newFakeStore()
	addr := startTestServer(t, NewServer(store, newFakeOwnership()))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	putEnv := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))
	deleteEnv := versioning.NewDelete(versioning.VectorClock{"node-a": 2}, time.Unix(0, 2))

	err := client.RepairKeys(context.Background(), addr, []versioning.KeyEnvelope{
		{Key: "put-key", Envelope: putEnv},
		{Key: "delete-key", Envelope: deleteEnv},
	})
	if err != nil {
		t.Fatalf("RepairKeys: %v", err)
	}
	if len(store.puts) != 1 || store.puts[0].key != "put-key" {
		t.Fatalf("puts = %#v", store.puts)
	}
	if len(store.deletes) != 1 || store.deletes[0].key != "delete-key" {
		t.Fatalf("deletes = %#v", store.deletes)
	}
}

func TestClientRepairKeysPropagatesServerRejection(t *testing.T) {
	ownership := newFakeOwnership()
	ownership.ownsKey = false
	addr := startTestServer(t, NewServer(newFakeStore(), ownership))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))
	err := client.RepairKeys(context.Background(), addr, []versioning.KeyEnvelope{{Key: "key", Envelope: env}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestClientReusesCachedConnectionPerAddress(t *testing.T) {
	addr := startTestServer(t, NewServer(newFakeStore(), newFakeOwnership()))
	client := NewClient()
	t.Cleanup(func() { _ = client.Close() })

	conn1, err := client.conn(addr)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	conn2, err := client.conn(addr)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	if conn1 != conn2 {
		t.Fatalf("expected cached connection to be reused for the same address")
	}
}

func TestClientCloseClearsConnections(t *testing.T) {
	addr := startTestServer(t, NewServer(newFakeStore(), newFakeOwnership()))
	client := NewClient()

	if _, err := client.conn(addr); err != nil {
		t.Fatalf("conn: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(client.conns) != 0 {
		t.Fatalf("expected no cached connections after Close, got %d", len(client.conns))
	}
}
