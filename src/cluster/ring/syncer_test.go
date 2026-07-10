package ring

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testSyncerConfig() SyncerConfig {
	return SyncerConfig{
		Interval:    10 * time.Millisecond,
		PullTimeout: 50 * time.Millisecond,
	}
}

func TestSyncerTickSkipsPeersAtOrBelowLocalGeneration(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	client := newFakeRangeMapClient()
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 1},
	}}

	s := NewSyncer(table, client, peers, testSyncerConfig())
	s.tick(context.Background())

	if len(client.callsSnapshot()) != 0 {
		t.Fatalf("expected no pull for a peer at or below local generation, got %v", client.callsSnapshot())
	}
}

func TestSyncerTickPullsAndReplacesFromNewerPeer(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	newer := RangeMap{
		Generation: 2,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "m", End: "", Replicas: []string{"node-2"}},
		},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
	}}

	s := NewSyncer(table, client, peers, testSyncerConfig())
	s.tick(context.Background())

	if table.Generation() != 2 {
		t.Fatalf("generation = %d, want 2", table.Generation())
	}
	if len(client.callsSnapshot()) != 1 || client.callsSnapshot()[0] != "10.0.0.1:7000" {
		t.Fatalf("calls = %v", client.callsSnapshot())
	}
}

func TestSyncerTickTriesNextPeerOnPullFailure(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	newer := RangeMap{
		Generation: 2,
		Ranges:     []Range{{Start: "", End: "", Replicas: []string{"node-1", "node-2"}}},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{err: errors.New("unreachable")}
	client.results["10.0.0.2:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
		{NodeID: "peer-2", AdvertiseAddr: "10.0.0.2:7000", RangeMapEpoch: 2},
	}}

	s := NewSyncer(table, client, peers, testSyncerConfig())
	s.tick(context.Background())

	if table.Generation() != 2 {
		t.Fatalf("generation = %d, want 2 after falling back to second peer", table.Generation())
	}
	calls := client.callsSnapshot()
	if len(calls) != 2 || calls[0] != "10.0.0.1:7000" || calls[1] != "10.0.0.2:7000" {
		t.Fatalf("calls = %v", calls)
	}
}

func TestSyncerTickTriesNextPeerWhenFetchedMapIsStale(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	// peer-1 claims epoch 2 but actually returns a map still at
	// generation 1 (e.g. it raced its own Replace) - should not stop
	// the search, since Table.Replace will reject it as unchanged.
	stale := singleRange("node-1")
	newer := RangeMap{
		Generation: 2,
		Ranges:     []Range{{Start: "", End: "", Replicas: []string{"node-1", "node-2"}}},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: stale}
	client.results["10.0.0.2:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
		{NodeID: "peer-2", AdvertiseAddr: "10.0.0.2:7000", RangeMapEpoch: 2},
	}}

	s := NewSyncer(table, client, peers, testSyncerConfig())
	s.tick(context.Background())

	if table.Generation() != 2 {
		t.Fatalf("generation = %d, want 2 after falling back past a stale response", table.Generation())
	}
}

func TestSyncerStartStopRunsLoopAndStopsCleanly(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	newer := RangeMap{
		Generation: 2,
		Ranges:     []Range{{Start: "", End: "", Replicas: []string{"node-1", "node-2"}}},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
	}}

	cfg := testSyncerConfig()
	cfg.Interval = 5 * time.Millisecond
	s := NewSyncer(table, client, peers, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if table.Generation() == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	s.Stop()

	if table.Generation() != 2 {
		t.Fatalf("expected table to converge to generation 2 before stopping, got %d", table.Generation())
	}
}

type rangeMapResult struct {
	rangeMap RangeMap
	err      error
}

type fakeRangeMapClient struct {
	mu      sync.Mutex
	results map[string]rangeMapResult
	calls   []string
}

func newFakeRangeMapClient() *fakeRangeMapClient {
	return &fakeRangeMapClient{results: make(map[string]rangeMapResult)}
}

func (c *fakeRangeMapClient) GetRangeMap(_ context.Context, addr string) (RangeMap, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, addr)
	r, ok := c.results[addr]
	if !ok {
		return RangeMap{}, errors.New("fake: no result configured for " + addr)
	}
	return r.rangeMap, r.err
}

func (c *fakeRangeMapClient) callsSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

type fakePeerEpochSource struct {
	peers []PeerEpoch
}

func (s *fakePeerEpochSource) PeerEpochs() []PeerEpoch {
	return s.peers
}
