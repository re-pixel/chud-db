package coordination

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"nosqlEngine/src/cluster/versioning"
)

type fakeOwners struct {
	owners []string
}

func (f fakeOwners) Owners(key string) []string {
	return append([]string(nil), f.owners...)
}

type fakeAddresses struct {
	addrs map[string]string
}

func (f fakeAddresses) Address(nodeID string) (string, bool) {
	addr, ok := f.addrs[nodeID]
	return addr, ok
}

type storedEnvelope struct {
	envelope versioning.Envelope
	found    bool
}

type fakeReplicaClient struct {
	mu sync.Mutex

	// byAddr overrides the response/error for a given address, keyed
	// by address; missing entries default to a stored-envelope lookup.
	errByAddr map[string]error
	getByAddr map[string]storedEnvelope

	putCalls    []string
	deleteCalls []string
	getCalls    []string
}

func newFakeReplicaClient() *fakeReplicaClient {
	return &fakeReplicaClient{
		errByAddr: make(map[string]error),
		getByAddr: make(map[string]storedEnvelope),
	}
}

func (f *fakeReplicaClient) Put(ctx context.Context, addr, key string, envelope versioning.Envelope, sync bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls = append(f.putCalls, addr)
	return f.errByAddr[addr]
}

func (f *fakeReplicaClient) Delete(ctx context.Context, addr, key string, envelope versioning.Envelope, sync bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, addr)
	return f.errByAddr[addr]
}

func (f *fakeReplicaClient) Get(ctx context.Context, addr, key string) (versioning.Envelope, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls = append(f.getCalls, addr)
	if err, ok := f.errByAddr[addr]; ok {
		return versioning.Envelope{}, false, err
	}
	stored := f.getByAddr[addr]
	return stored.envelope, stored.found, nil
}

type fakeLocalStore struct {
	mu sync.Mutex

	putCalls    int
	deleteCalls int
	getCalls    int

	err error
	get storedEnvelope
}

func (f *fakeLocalStore) Put(key string, envelope versioning.Envelope, sync bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls++
	return f.err
}

func (f *fakeLocalStore) Delete(key string, envelope versioning.Envelope, sync bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	return f.err
}

func (f *fakeLocalStore) Get(key string) (versioning.Envelope, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return f.get.envelope, f.get.found, f.err
}

func testConfig() Config {
	return Config{ReplicationTimeout: time.Second, ReadQuorum: 2, WriteQuorum: 2}
}

func TestCoordinatorPutSucceedsWhenWriteQuorumMet(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2", "node-3"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000", "node-3": "10.0.0.3:7000"}}
	replicas := newFakeReplicaClient()
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Put(context.Background(), "key", "value", versioning.VectorClock{}, 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if result.Acks != 3 || result.Required != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.VectorClock["coordinator"] != 1 {
		t.Fatalf("expected clock stamped by coordinator, got %+v", result.VectorClock)
	}
}

func TestCoordinatorPutFailsWhenWriteQuorumNotMet(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2", "node-3"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000", "node-3": "10.0.0.3:7000"}}
	replicas := newFakeReplicaClient()
	replicas.errByAddr["10.0.0.2:7000"] = fmt.Errorf("unreachable")
	replicas.errByAddr["10.0.0.3:7000"] = fmt.Errorf("unreachable")
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Put(context.Background(), "key", "value", versioning.VectorClock{}, 0)
	if err == nil {
		t.Fatalf("expected write quorum error, got nil (result=%+v)", result)
	}
	if result.Acks != 1 || result.Required != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestCoordinatorPutUsesLocalStoreShortcutForSelfOwnedReplica(t *testing.T) {
	owners := fakeOwners{owners: []string{"coordinator", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Put(context.Background(), "key", "value", versioning.VectorClock{}, 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if result.Acks != 2 {
		t.Fatalf("unexpected acks: %+v", result)
	}
	if local.putCalls != 1 {
		t.Fatalf("expected local store to be used once for self-owned replica, got %d", local.putCalls)
	}
	if len(replicas.putCalls) != 1 || replicas.putCalls[0] != "10.0.0.2:7000" {
		t.Fatalf("expected exactly one gRPC put to node-2, got %#v (no loopback expected for self)", replicas.putCalls)
	}
}

func TestCoordinatorDeleteWritesTombstoneAndCountsAcks(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Delete(context.Background(), "key", versioning.VectorClock{}, 0)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.Acks != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(replicas.deleteCalls) != 2 {
		t.Fatalf("expected delete fanned out to both replicas, got %#v", replicas.deleteCalls)
	}
}

func TestCoordinatorPutRejectsRequestedQuorumAboveOwnerCount(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "a", "node-2": "b"}}
	replicas := newFakeReplicaClient()
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	_, err := coord.Put(context.Background(), "key", "value", versioning.VectorClock{}, 5)
	if err == nil || !strings.Contains(err.Error(), "exceeds owner count") {
		t.Fatalf("expected quorum-exceeds-owner-count error, got %v", err)
	}
	if len(replicas.putCalls) != 0 {
		t.Fatalf("expected no fan-out when request is rejected outright, got %#v", replicas.putCalls)
	}
}

func TestCoordinatorGetReturnsCleanWinnerWhenAllReplicasAgree(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	clock := versioning.VectorClock{"node-1": 1}
	envelope := versioning.NewPut(clock, "value", time.Now())
	replicas.getByAddr["10.0.0.1:7000"] = storedEnvelope{envelope: envelope, found: true}
	replicas.getByAddr["10.0.0.2:7000"] = storedEnvelope{envelope: envelope, found: true}
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Get(context.Background(), "key", 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(result.Versions) != 1 || result.Versions[0].Value != "value" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestCoordinatorGetReturnsConcurrentSiblingsToCaller(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()

	left := versioning.NewPut(versioning.VectorClock{"node-1": 1}, "left", time.Now().Add(-time.Minute))
	right := versioning.NewPut(versioning.VectorClock{"node-2": 1}, "right", time.Now())
	replicas.getByAddr["10.0.0.1:7000"] = storedEnvelope{envelope: left, found: true}
	replicas.getByAddr["10.0.0.2:7000"] = storedEnvelope{envelope: right, found: true}
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Get(context.Background(), "key", 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(result.Versions) != 2 {
		t.Fatalf("expected both concurrent siblings surfaced, got %+v", result)
	}
	values := map[string]bool{result.Versions[0].Value: true, result.Versions[1].Value: true}
	if !values["left"] || !values["right"] {
		t.Fatalf("expected both sibling values present, got %+v", result.Versions)
	}
	for _, version := range result.Versions {
		if versioning.Compare(version.VectorClock, result.VectorClock) != versioning.Before {
			t.Fatalf("merged context %+v does not dominate sibling clock %+v", result.VectorClock, version.VectorClock)
		}
	}
}

func TestCoordinatorGetSurfacesConcurrentPutAndDeleteAsBothVersions(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()

	tombstone := versioning.NewDelete(versioning.VectorClock{"node-1": 1}, time.Now())
	put := versioning.NewPut(versioning.VectorClock{"node-2": 1}, "value", time.Now())
	replicas.getByAddr["10.0.0.1:7000"] = storedEnvelope{envelope: tombstone, found: true}
	replicas.getByAddr["10.0.0.2:7000"] = storedEnvelope{envelope: put, found: true}
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Get(context.Background(), "key", 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(result.Versions) != 2 {
		t.Fatalf("expected both the tombstone and the concurrent put surfaced, got %+v", result)
	}
	sawDeleted, sawLive := false, false
	for _, version := range result.Versions {
		if version.Deleted {
			sawDeleted = true
		} else {
			sawLive = true
		}
	}
	if !sawDeleted || !sawLive {
		t.Fatalf("expected one deleted and one live version, got %+v", result.Versions)
	}
}

func TestCoordinatorGetTreatsReplicaRejectionAsFailedAckNotCrash(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	replicas.errByAddr["10.0.0.1:7000"] = fmt.Errorf("rpc error: code = FailedPrecondition desc = key not owned by this node")
	envelope := versioning.NewPut(versioning.VectorClock{"node-2": 1}, "value", time.Now())
	replicas.getByAddr["10.0.0.2:7000"] = storedEnvelope{envelope: envelope, found: true}
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Get(context.Background(), "key", 1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(result.Versions) != 1 || result.Versions[0].Value != "value" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestCoordinatorGetRejectsRequestedQuorumAboveOwnerCount(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "a"}}
	replicas := newFakeReplicaClient()
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	_, err := coord.Get(context.Background(), "key", 3)
	if err == nil || !strings.Contains(err.Error(), "exceeds owner count") {
		t.Fatalf("expected quorum-exceeds-owner-count error, got %v", err)
	}
	if len(replicas.getCalls) != 0 {
		t.Fatalf("expected no fan-out when request is rejected outright, got %#v", replicas.getCalls)
	}
}

func TestCoordinatorGetReturnsNotFoundWhenNoReplicaHasKey(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Get(context.Background(), "key", 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(result.Versions) != 0 {
		t.Fatalf("expected not found, got %+v", result)
	}
}

func TestCoordinatorGetSurfacesDeletedWinnerAsDeletedVersion(t *testing.T) {
	owners := fakeOwners{owners: []string{"node-1", "node-2"}}
	addresses := fakeAddresses{addrs: map[string]string{"node-1": "10.0.0.1:7000", "node-2": "10.0.0.2:7000"}}
	replicas := newFakeReplicaClient()
	tombstone := versioning.NewDelete(versioning.VectorClock{"node-1": 2}, time.Now())
	replicas.getByAddr["10.0.0.1:7000"] = storedEnvelope{envelope: tombstone, found: true}
	replicas.getByAddr["10.0.0.2:7000"] = storedEnvelope{envelope: tombstone, found: true}
	local := &fakeLocalStore{}

	coord := NewCoordinator("coordinator", owners, addresses, replicas, local, testConfig())

	result, err := coord.Get(context.Background(), "key", 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(result.Versions) != 1 || !result.Versions[0].Deleted {
		t.Fatalf("expected deleted key to surface as a single deleted version, got %+v", result)
	}
	if result.VectorClock["node-1"] != 2 {
		t.Fatalf("expected tombstone's vector clock preserved as next context, got %+v", result.VectorClock)
	}
}
