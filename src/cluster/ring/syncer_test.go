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

func TestSyncerTickSamplesPeerAtLocalEpoch(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	client := newFakeRangeMapClient()
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 1},
	}}

	s := NewSyncer(table, client, peers, nil, testSyncerConfig())
	s.tick(context.Background())

	if len(client.callsSnapshot()) != 1 {
		t.Fatalf("expected an unconditional convergence pull, got %v", client.callsSnapshot())
	}
}

func TestSyncerTickMergesMissingUpdateFromEqualEpochPeer(t *testing.T) {
	local := RangeMap{Ranges: []Range{
		versionedRange("", "m", 2, "left-new", "node-1"),
		versionedRange("m", "", 1, "right-base", "node-1"),
	}}
	table, err := NewTable("node-1", local)
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	remote := RangeMap{Ranges: []Range{
		versionedRange("", "m", 1, "left-base", "node-1"),
		versionedRange("m", "", 2, "right-new", "node-2"),
	}}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: remote}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
	}}

	NewSyncer(table, client, peers, nil, testSyncerConfig()).tick(context.Background())

	got := table.Snapshot()
	if got.Ranges[0].ProposalID != "left-new" || got.Ranges[1].ProposalID != "right-new" {
		t.Fatalf("equal-epoch maps did not reconcile: %#v", got)
	}
}

func TestSyncerTickPullsAndMergesFromNewerPeer(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	newer := RangeMap{
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 2, ProposalID: "split"},
			{Start: "m", End: "", Replicas: []string{"node-2"}, Generation: 2, ProposalID: "split"},
		},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
	}}

	s := NewSyncer(table, client, peers, nil, testSyncerConfig())
	s.tick(context.Background())

	if table.Epoch() != 2 {
		t.Fatalf("epoch = %d, want 2", table.Epoch())
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
		Ranges: []Range{{Start: "", End: "", Replicas: []string{"node-1", "node-2"}, Generation: 2, ProposalID: "replicas"}},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{err: errors.New("unreachable")}
	client.results["10.0.0.2:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
		{NodeID: "peer-2", AdvertiseAddr: "10.0.0.2:7000", RangeMapEpoch: 2},
	}}

	s := NewSyncer(table, client, peers, nil, testSyncerConfig())
	s.tick(context.Background())

	if table.Epoch() != 2 {
		t.Fatalf("epoch = %d, want 2 after falling back to second peer", table.Epoch())
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
	stale := singleRange("node-1")
	newer := RangeMap{
		Ranges: []Range{{Start: "", End: "", Replicas: []string{"node-1", "node-2"}, Generation: 2, ProposalID: "replicas"}},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: stale}
	client.results["10.0.0.2:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
		{NodeID: "peer-2", AdvertiseAddr: "10.0.0.2:7000", RangeMapEpoch: 2},
	}}

	s := NewSyncer(table, client, peers, nil, testSyncerConfig())
	s.tick(context.Background())

	if table.Epoch() != 2 {
		t.Fatalf("epoch = %d, want 2 after falling back past a stale response", table.Epoch())
	}
}

func TestSyncerTickPublishesNewEpochOnSuccessfulMerge(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	newer := RangeMap{
		Ranges: []Range{{Start: "", End: "", Replicas: []string{"node-1", "node-2"}, Generation: 2, ProposalID: "replicas"}},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
	}}
	publisher := &fakeEpochPublisher{}

	s := NewSyncer(table, client, peers, publisher, testSyncerConfig())
	s.tick(context.Background())

	published := publisher.publishedSnapshot()
	if len(published) != 1 || published[0] != 2 {
		t.Fatalf("published = %v, want [2]", published)
	}
}

func TestSyncerTickDoesNotPublishWhenNoPeerHasNewerEpoch(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	client := newFakeRangeMapClient()
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 1},
	}}
	publisher := &fakeEpochPublisher{}

	s := NewSyncer(table, client, peers, publisher, testSyncerConfig())
	s.tick(context.Background())

	if published := publisher.publishedSnapshot(); len(published) != 0 {
		t.Fatalf("published = %v, want none", published)
	}
}

func TestSyncerStartStopRunsLoopAndStopsCleanly(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	newer := RangeMap{
		Ranges: []Range{{Start: "", End: "", Replicas: []string{"node-1", "node-2"}, Generation: 2, ProposalID: "replicas"}},
	}
	client := newFakeRangeMapClient()
	client.results["10.0.0.1:7000"] = rangeMapResult{rangeMap: newer}
	peers := &fakePeerEpochSource{peers: []PeerEpoch{
		{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", RangeMapEpoch: 2},
	}}

	cfg := testSyncerConfig()
	cfg.Interval = 5 * time.Millisecond
	s := NewSyncer(table, client, peers, nil, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if table.Epoch() == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	s.Stop()

	if table.Epoch() != 2 {
		t.Fatalf("expected table to converge to epoch 2 before stopping, got %d", table.Epoch())
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

type fakeEpochPublisher struct {
	mu        sync.Mutex
	published []uint64
}

func (p *fakeEpochPublisher) PublishRangeMapEpoch(epoch uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, epoch)
}

func (p *fakeEpochPublisher) publishedSnapshot() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint64(nil), p.published...)
}
