package antientropy

import (
	"context"
	"sync"
	"testing"
	"time"

	"nosqlEngine/src/cluster/versioning"
)

func testSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		NodeID:            "node-a",
		Interval:          10 * time.Millisecond,
		Timeout:           time.Second,
		Fanout:            4,
		LeafItemThreshold: 100,
		MaxDepth:          6,
	}
}

func TestSchedulerRepairRangeNoOpWhenInSync(t *testing.T) {
	local := newMemStore()
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "v", time.Unix(0, 1))
	local.set("k", env)

	client := newFakeReplicaClient()
	client.peerStore("addr-b").set("k", env)

	sched := NewScheduler(testSchedulerConfig(), local, fakeAddressResolver{}, client, fakeOwnedRangesSource{})

	if err := sched.RepairRange(context.Background(), "addr-b", "", ""); err != nil {
		t.Fatalf("RepairRange: %v", err)
	}
	if got := client.rootCallsSnapshot(); len(got) != 1 {
		t.Fatalf("root calls = %d, want 1: %#v", len(got), got)
	}
	if got := client.streamCallsSnapshot(); len(got) != 0 {
		t.Fatalf("stream calls = %d, want 0: %#v", len(got), got)
	}
	if got := client.repairCallsSnapshot("addr-b"); len(got) != 0 {
		t.Fatalf("expected no RepairKeys calls, got %#v", got)
	}
}

func TestSchedulerRepairsOneSidedMissingKeysBothDirections(t *testing.T) {
	local := newMemStore()
	local.set("local-only", versioning.NewPut(versioning.VectorClock{"node-a": 1}, "local-value", time.Unix(0, 1)))

	client := newFakeReplicaClient()
	peer := client.peerStore("addr-b")
	peer.set("peer-only", versioning.NewPut(versioning.VectorClock{"node-b": 1}, "peer-value", time.Unix(0, 2)))

	sched := NewScheduler(testSchedulerConfig(), local, fakeAddressResolver{}, client, fakeOwnedRangesSource{})
	if err := sched.RepairRange(context.Background(), "addr-b", "", ""); err != nil {
		t.Fatalf("RepairRange: %v", err)
	}

	if got, ok := local.get("peer-only"); !ok || got.Value != "peer-value" {
		t.Fatalf("expected peer-only key pulled locally, got %#v (ok=%v)", got, ok)
	}
	if got, ok := peer.get("local-only"); !ok || got.Value != "local-value" {
		t.Fatalf("expected local-only key pushed to peer, got %#v (ok=%v)", got, ok)
	}
}

func TestSchedulerRepairsDominatedStaleKeyBothDirections(t *testing.T) {
	local := newMemStore()
	local.set("stale-on-peer", versioning.NewPut(versioning.VectorClock{"x": 2}, "local-newer", time.Unix(0, 2)))
	local.set("stale-on-local", versioning.NewPut(versioning.VectorClock{"x": 1}, "local-older", time.Unix(0, 1)))

	client := newFakeReplicaClient()
	peer := client.peerStore("addr-b")
	peer.set("stale-on-peer", versioning.NewPut(versioning.VectorClock{"x": 1}, "peer-older", time.Unix(0, 1)))
	peer.set("stale-on-local", versioning.NewPut(versioning.VectorClock{"x": 2}, "peer-newer", time.Unix(0, 2)))

	sched := NewScheduler(testSchedulerConfig(), local, fakeAddressResolver{}, client, fakeOwnedRangesSource{})
	if err := sched.RepairRange(context.Background(), "addr-b", "", ""); err != nil {
		t.Fatalf("RepairRange: %v", err)
	}

	if got, _ := peer.get("stale-on-peer"); got.Value != "local-newer" {
		t.Fatalf("expected peer's stale-on-peer overwritten with local's newer version, got %#v", got)
	}
	if got, _ := local.get("stale-on-local"); got.Value != "peer-newer" {
		t.Fatalf("expected local's stale-on-local overwritten with peer's newer version, got %#v", got)
	}
}

func TestSchedulerLeavesConcurrentConflictUntouched(t *testing.T) {
	local := newMemStore()
	local.set("conflict", versioning.NewPut(versioning.VectorClock{"node-a": 1}, "local-value", time.Unix(0, 1)))

	client := newFakeReplicaClient()
	peer := client.peerStore("addr-b")
	peer.set("conflict", versioning.NewPut(versioning.VectorClock{"node-b": 1}, "peer-value", time.Unix(0, 2)))

	sched := NewScheduler(testSchedulerConfig(), local, fakeAddressResolver{}, client, fakeOwnedRangesSource{})
	if err := sched.RepairRange(context.Background(), "addr-b", "", ""); err != nil {
		t.Fatalf("RepairRange: %v", err)
	}

	if got, _ := local.get("conflict"); got.Value != "local-value" {
		t.Fatalf("expected local's conflicting version untouched, got %#v", got)
	}
	if got, _ := peer.get("conflict"); got.Value != "peer-value" {
		t.Fatalf("expected peer's conflicting version untouched, got %#v", got)
	}
	if got := client.repairCallsSnapshot("addr-b"); len(got) != 0 {
		t.Fatalf("expected no RepairKeys call for a concurrent conflict, got %#v", got)
	}
}

// eightKeySplitTable is shared by the recursion-shape tests below: a
// hand-picked, fully deterministic 3-level binary split of an 8-key
// universe ("a".."h"), standing in for SplitRange's real byte-
// interpolation math so recursion depth/shape can be asserted exactly
// regardless of it.
func eightKeySplitTable() map[splitKey][]Bucket {
	return map[splitKey][]Bucket{
		{"", ""}:  {{Start: "", End: "d"}, {Start: "d", End: ""}},
		{"", "d"}: {{Start: "", End: "b"}, {Start: "b", End: "d"}},
		{"d", ""}: {{Start: "d", End: "f"}, {Start: "f", End: ""}},
	}
}

// seedEightKeyMismatch populates local and peer identically for keys
// "b".."g", but only the peer holds "a" (in the left subtree) and "h"
// (in the right subtree) - so both halves of the tree, and exactly one
// leaf on each side, must actually be visited to fully converge.
func seedEightKeyMismatch(local *memStore, peer *memStore) {
	for _, k := range []string{"b", "c", "d", "e", "f", "g"} {
		env := versioning.NewPut(versioning.VectorClock{"node-b": 1}, k+"-value", time.Unix(0, 1))
		local.set(k, env)
		peer.set(k, env)
	}
	peer.set("a", versioning.NewPut(versioning.VectorClock{"node-b": 1}, "a-value", time.Unix(0, 1)))
	peer.set("h", versioning.NewPut(versioning.VectorClock{"node-b": 1}, "h-value", time.Unix(0, 1)))
}

func TestSchedulerRecursesMultipleLevelsWhenItemCountAboveThreshold(t *testing.T) {
	local := newMemStore()
	client := newFakeReplicaClient()
	peer := client.peerStore("addr-b")
	seedEightKeyMismatch(local, peer)

	cfg := testSchedulerConfig()
	cfg.Fanout = 2
	cfg.LeafItemThreshold = 0
	cfg.MaxDepth = 10
	sched := NewScheduler(cfg, local, fakeAddressResolver{}, client, fakeOwnedRangesSource{})
	sched.split = splitTable(eightKeySplitTable())

	if err := sched.RepairRange(context.Background(), "addr-b", "", ""); err != nil {
		t.Fatalf("RepairRange: %v", err)
	}

	if got := client.rootCallsSnapshot(); len(got) != 7 {
		t.Fatalf("root calls = %d, want 7 (1 root + 2 children + 4 grandchildren): %#v", len(got), got)
	}
	if got := client.streamCallsSnapshot(); len(got) != 2 {
		t.Fatalf("stream calls = %d, want 2 (the two mismatching leaves): %#v", len(got), got)
	}
	if got, ok := local.get("a"); !ok || got.Value != "a-value" {
		t.Fatalf("expected key 'a' pulled from peer, got %#v (ok=%v)", got, ok)
	}
	if got, ok := local.get("h"); !ok || got.Value != "h-value" {
		t.Fatalf("expected key 'h' pulled from peer, got %#v (ok=%v)", got, ok)
	}
}

func TestSchedulerStopsAtMaxDepthEvenIfItemCountStaysAboveThreshold(t *testing.T) {
	local := newMemStore()
	client := newFakeReplicaClient()
	peer := client.peerStore("addr-b")
	seedEightKeyMismatch(local, peer)

	cfg := testSchedulerConfig()
	cfg.Fanout = 2
	cfg.LeafItemThreshold = 0 // would otherwise never stop on item count alone
	cfg.MaxDepth = 1
	sched := NewScheduler(cfg, local, fakeAddressResolver{}, client, fakeOwnedRangesSource{})
	sched.split = splitTable(eightKeySplitTable())

	if err := sched.RepairRange(context.Background(), "addr-b", "", ""); err != nil {
		t.Fatalf("RepairRange: %v", err)
	}

	if got := client.rootCallsSnapshot(); len(got) != 3 {
		t.Fatalf("root calls = %d, want 3 (1 root + 2 children; grandchildren never visited): %#v", len(got), got)
	}
	if got := client.streamCallsSnapshot(); len(got) != 2 {
		t.Fatalf("stream calls = %d, want 2 (both children forced to leaf by MaxDepth): %#v", len(got), got)
	}
	// MaxDepth stopped recursion before splitting further, but the
	// coarser leaf diff at depth 1 still covers and repairs both keys.
	if got, ok := local.get("a"); !ok || got.Value != "a-value" {
		t.Fatalf("expected key 'a' still repaired via the depth-1 leaf, got %#v (ok=%v)", got, ok)
	}
	if got, ok := local.get("h"); !ok || got.Value != "h-value" {
		t.Fatalf("expected key 'h' still repaired via the depth-1 leaf, got %#v (ok=%v)", got, ok)
	}
}

func TestSchedulerStopsImmediatelyOnDegenerateSplit(t *testing.T) {
	local := newMemStore()
	client := newFakeReplicaClient()
	peer := client.peerStore("addr-b")
	peer.set("only-on-peer", versioning.NewPut(versioning.VectorClock{"node-b": 1}, "v", time.Unix(0, 1)))

	cfg := testSchedulerConfig()
	cfg.LeafItemThreshold = 0 // would otherwise never stop on item count alone
	cfg.MaxDepth = 10         // would otherwise allow plenty of recursion
	sched := NewScheduler(cfg, local, fakeAddressResolver{}, client, fakeOwnedRangesSource{})
	sched.split = splitTable(map[splitKey][]Bucket{}) // every call degenerates

	if err := sched.RepairRange(context.Background(), "addr-b", "", ""); err != nil {
		t.Fatalf("RepairRange: %v", err)
	}

	if got := client.rootCallsSnapshot(); len(got) != 1 {
		t.Fatalf("root calls = %d, want exactly 1 (no recursion past the degenerate root): %#v", len(got), got)
	}
	if got := client.streamCallsSnapshot(); len(got) != 1 {
		t.Fatalf("stream calls = %d, want exactly 1 (root itself became the leaf): %#v", len(got), got)
	}
	if got, ok := local.get("only-on-peer"); !ok || got.Value != "v" {
		t.Fatalf("expected key repaired via the degenerate root-as-leaf, got %#v (ok=%v)", got, ok)
	}
}

func TestSchedulerTickRunsRepairForEachOwnedRangeAgainstAnotherReplica(t *testing.T) {
	local := newMemStore()
	client := newFakeReplicaClient()

	peerB := client.peerStore("addr-b")
	// "b-only" sorts below "m", so it falls within range A's [ "", "m" )
	// boundary; "only-on-c" sorts at/above "m", within range B's.
	peerB.set("b-only", versioning.NewPut(versioning.VectorClock{"node-b": 1}, "b-value", time.Unix(0, 1)))
	peerC := client.peerStore("addr-c")
	peerC.set("only-on-c", versioning.NewPut(versioning.VectorClock{"node-c": 1}, "c-value", time.Unix(0, 1)))

	addresses := fakeAddressResolver{"node-b": "addr-b", "node-c": "addr-c"}
	ranges := fakeOwnedRangesSource{ranges: []OwnedRange{
		{Start: "", End: "m", Replicas: []string{"node-a", "node-b"}},
		{Start: "m", End: "", Replicas: []string{"node-a", "node-c"}},
	}}

	sched := NewScheduler(testSchedulerConfig(), local, addresses, client, ranges)
	sched.tick(context.Background())

	if got, ok := local.get("b-only"); !ok || got.Value != "b-value" {
		t.Fatalf("expected b-only repaired via range A's peer, got %#v (ok=%v)", got, ok)
	}
	if got, ok := local.get("only-on-c"); !ok || got.Value != "c-value" {
		t.Fatalf("expected only-on-c repaired via range B's peer, got %#v (ok=%v)", got, ok)
	}
	if len(client.rootCallsForAddr("addr-b")) == 0 {
		t.Fatalf("expected at least one GetMerkleRoot call against addr-b")
	}
	if len(client.rootCallsForAddr("addr-c")) == 0 {
		t.Fatalf("expected at least one GetMerkleRoot call against addr-c")
	}
}

func TestSchedulerTickSkipsRangeWithNoOtherReplicas(t *testing.T) {
	local := newMemStore()
	client := newFakeReplicaClient()
	ranges := fakeOwnedRangesSource{ranges: []OwnedRange{
		{Start: "", End: "", Replicas: []string{"node-a"}},
	}}

	sched := NewScheduler(testSchedulerConfig(), local, fakeAddressResolver{}, client, ranges)
	sched.tick(context.Background())

	if got := client.rootCallsSnapshot(); len(got) != 0 {
		t.Fatalf("expected no RPCs for a range with no other replicas, got %#v", got)
	}
}

func TestSchedulerTickSkipsRangeWhenAddressUnknown(t *testing.T) {
	local := newMemStore()
	client := newFakeReplicaClient()
	ranges := fakeOwnedRangesSource{ranges: []OwnedRange{
		{Start: "", End: "", Replicas: []string{"node-a", "node-b"}},
	}}

	sched := NewScheduler(testSchedulerConfig(), local, fakeAddressResolver{}, client, ranges)
	sched.tick(context.Background())

	if got := client.rootCallsSnapshot(); len(got) != 0 {
		t.Fatalf("expected no RPCs when the peer's address cannot be resolved, got %#v", got)
	}
}

func TestSchedulerStartStopRunsLoopAndStopsCleanly(t *testing.T) {
	local := newMemStore()
	client := newFakeReplicaClient()
	peer := client.peerStore("addr-b")
	peer.set("only-on-peer", versioning.NewPut(versioning.VectorClock{"node-b": 1}, "v", time.Unix(0, 1)))

	cfg := testSchedulerConfig()
	cfg.Interval = 5 * time.Millisecond
	addresses := fakeAddressResolver{"node-b": "addr-b"}
	ranges := fakeOwnedRangesSource{ranges: []OwnedRange{
		{Start: "", End: "", Replicas: []string{"node-a", "node-b"}},
	}}
	sched := NewScheduler(cfg, local, addresses, client, ranges)

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := local.get("only-on-peer"); ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	sched.Stop()

	if _, ok := local.get("only-on-peer"); !ok {
		t.Fatalf("expected background loop to converge before stopping")
	}
}

// memStore is a simple in-memory Store used to drive Scheduler tests
// with data that actually respects range boundaries, unlike fakeStore
// in server_test.go (which returns a fixed row set regardless of the
// requested range - fine there since each test issues one call with a
// fixed range, but not for Scheduler's recursive multi-range walk).
type memStore struct {
	mu   sync.Mutex
	data map[string]versioning.Envelope
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string]versioning.Envelope)}
}

func (m *memStore) set(key string, env versioning.Envelope) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = env
}

func (m *memStore) get(key string) (versioning.Envelope, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	env, ok := m.data[key]
	return env, ok
}

func (m *memStore) ScanRawRange(start, end string) ([]RawEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := make([]RawEntry, 0)
	for k, v := range m.data {
		if k < start {
			continue
		}
		if end != "" && k >= end {
			continue
		}
		raw, err := versioning.Encode(v)
		if err != nil {
			return nil, err
		}
		entries = append(entries, RawEntry{Key: k, Value: raw})
	}
	return entries, nil
}

func (m *memStore) Put(key string, envelope versioning.Envelope, sync bool) error {
	m.set(key, envelope)
	return nil
}

func (m *memStore) Delete(key string, envelope versioning.Envelope, sync bool) error {
	m.set(key, envelope)
	return nil
}

// rangeCall records the arguments of one recorded ReplicaClient call.
type rangeCall struct {
	addr, start, end string
}

// fakeReplicaClient is a ReplicaClient backed by one in-memory memStore
// per dialed address, so Scheduler's recursive tree walk can be driven
// against realistic, range-aware peer data while every call is
// recorded for assertions.
type fakeReplicaClient struct {
	mu    sync.Mutex
	peers map[string]*memStore

	rootCalls   []rangeCall
	streamCalls []rangeCall
	repairCalls map[string][][]versioning.KeyEnvelope

	getRootErr error
	streamErr  error
	repairErr  error
}

func newFakeReplicaClient() *fakeReplicaClient {
	return &fakeReplicaClient{
		peers:       make(map[string]*memStore),
		repairCalls: make(map[string][][]versioning.KeyEnvelope),
	}
}

func (c *fakeReplicaClient) peerStore(addr string) *memStore {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.peers[addr]; ok {
		return s
	}
	s := newMemStore()
	c.peers[addr] = s
	return s
}

func (c *fakeReplicaClient) GetMerkleRoot(ctx context.Context, addr, start, end string) ([]byte, uint64, error) {
	c.mu.Lock()
	c.rootCalls = append(c.rootCalls, rangeCall{addr, start, end})
	c.mu.Unlock()
	if c.getRootErr != nil {
		return nil, 0, c.getRootErr
	}
	return ComputeMerkleRoot(c.peerStore(addr), start, end)
}

func (c *fakeReplicaClient) StreamRange(ctx context.Context, addr, start, end string) ([]versioning.KeyEnvelope, error) {
	c.mu.Lock()
	c.streamCalls = append(c.streamCalls, rangeCall{addr, start, end})
	c.mu.Unlock()
	if c.streamErr != nil {
		return nil, c.streamErr
	}
	entries, err := c.peerStore(addr).ScanRawRange(start, end)
	if err != nil {
		return nil, err
	}
	rows := make([]versioning.KeyEnvelope, 0, len(entries))
	for _, e := range entries {
		env, err := versioning.Decode(e.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, versioning.KeyEnvelope{Key: e.Key, Envelope: env})
	}
	return rows, nil
}

func (c *fakeReplicaClient) RepairKeys(ctx context.Context, addr string, rows []versioning.KeyEnvelope) error {
	c.mu.Lock()
	c.repairCalls[addr] = append(c.repairCalls[addr], rows)
	c.mu.Unlock()
	if c.repairErr != nil {
		return c.repairErr
	}
	store := c.peerStore(addr)
	for _, row := range rows {
		if row.Envelope.Deleted {
			if err := store.Delete(row.Key, row.Envelope, true); err != nil {
				return err
			}
			continue
		}
		if err := store.Put(row.Key, row.Envelope, true); err != nil {
			return err
		}
	}
	return nil
}

func (c *fakeReplicaClient) rootCallsSnapshot() []rangeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]rangeCall(nil), c.rootCalls...)
}

func (c *fakeReplicaClient) streamCallsSnapshot() []rangeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]rangeCall(nil), c.streamCalls...)
}

func (c *fakeReplicaClient) repairCallsSnapshot(addr string) [][]versioning.KeyEnvelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]versioning.KeyEnvelope(nil), c.repairCalls[addr]...)
}

func (c *fakeReplicaClient) rootCallsForAddr(addr string) []rangeCall {
	var out []rangeCall
	for _, call := range c.rootCallsSnapshot() {
		if call.addr == addr {
			out = append(out, call)
		}
	}
	return out
}

// fakeAddressResolver is a static nodeID -> address lookup table.
type fakeAddressResolver map[string]string

func (r fakeAddressResolver) Address(nodeID string) (string, bool) {
	addr, ok := r[nodeID]
	return addr, ok
}

// fakeOwnedRangesSource returns a fixed set of owned ranges.
type fakeOwnedRangesSource struct {
	ranges []OwnedRange
}

func (f fakeOwnedRangesSource) OwnedRanges() []OwnedRange {
	return f.ranges
}

// splitKey identifies one diffNode call site for splitTable below.
type splitKey struct {
	start, end string
}

// splitTable builds a deterministic stand-in for SplitRange from an
// explicit (start, end) -> children lookup table, so recursion-shape
// tests can reason exactly about which keys land in which child
// without depending on SplitRange's internal byte-interpolation math.
// Any (start, end) pair not in table degenerates to a single bucket
// identical to its input, exactly like SplitRange's own documented
// degenerate case - so an untabled node is automatically forced into
// becoming a leaf.
func splitTable(table map[splitKey][]Bucket) func(start, end string, n int) ([]Bucket, error) {
	return func(start, end string, n int) ([]Bucket, error) {
		if buckets, ok := table[splitKey{start, end}]; ok {
			return buckets, nil
		}
		return []Bucket{{Start: start, End: end}}, nil
	}
}
