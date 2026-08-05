package tablet

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"nosqlEngine/src/cluster/ring"
)

func testSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		NodeID:            "node-1",
		Interval:          5 * time.Millisecond,
		SplitBytes:        100,
		MergeBytes:        10,
		ReplicationFactor: 2,
		SettlingTicks:     1,
	}
}

func TestSchedulerTickSplitsOversizedRange(t *testing.T) {
	table := newTestRangeTable(t, ring.RangeMap{Ranges: []ring.Range{
		versionedRingRange("", "", 1, "base", "node-1", "node-2"),
	}})
	store := &rangeStore{entries: map[string][]RawEntry{
		rangeKey("", ""): {
			{Key: "a", Value: "large-value"},
			{Key: "m", Value: "large-value"},
		},
	}}
	cfg := testSchedulerConfig()
	cfg.SplitBytes = 1
	publisher := &recordingPublisher{}
	scheduler := NewScheduler(cfg, store, table, staticCandidates{"node-1", "node-2"}, publisher)

	scheduler.tick()

	got := table.Snapshot()
	if len(got.Ranges) != 2 || got.Ranges[0].End != "m" || got.Ranges[1].Start != "m" {
		t.Fatalf("split map = %#v", got)
	}
	if got.Ranges[0].Generation != 2 || got.Ranges[1].Generation != 2 {
		t.Fatalf("split generations = %d, %d", got.Ranges[0].Generation, got.Ranges[1].Generation)
	}
	if got.Ranges[0].ProposalID == "" || got.Ranges[0].ProposalID != got.Ranges[1].ProposalID {
		t.Fatalf("split proposal IDs = %q, %q", got.Ranges[0].ProposalID, got.Ranges[1].ProposalID)
	}
	if !slices.Equal(publisher.epochs, []uint64{2}) {
		t.Fatalf("published epochs = %v, want [2]", publisher.epochs)
	}
}

func TestSchedulerTickMergesAdjacentSmallRanges(t *testing.T) {
	table := newTestRangeTable(t, ring.RangeMap{Ranges: []ring.Range{
		versionedRingRange("", "m", 2, "left", "node-1", "node-2"),
		versionedRingRange("m", "", 3, "right", "node-1", "node-3"),
	}})
	store := &rangeStore{entries: map[string][]RawEntry{
		rangeKey("", "m"): {{}},
		rangeKey("m", ""): {{}},
	}}
	cfg := testSchedulerConfig()
	cfg.ReplicationFactor = 1
	scheduler := NewScheduler(cfg, store, table, staticCandidates{"node-1", "node-2", "node-3"}, nil)

	scheduler.tick()

	got := table.Snapshot()
	if len(got.Ranges) != 1 {
		t.Fatalf("merged map = %#v", got)
	}
	merged := got.Ranges[0]
	if merged.Generation != 4 || !slices.Equal(merged.Replicas, []string{"node-1"}) {
		t.Fatalf("merged range = %#v", merged)
	}
}

func TestSchedulerTickDoesNotMergeRangeOwnedOnlyOnOneSide(t *testing.T) {
	initial := ring.RangeMap{Ranges: []ring.Range{
		versionedRingRange("", "m", 1, "left", "node-1"),
		versionedRingRange("m", "", 1, "right", "node-2"),
	}}
	table := newTestRangeTable(t, initial)
	store := &rangeStore{entries: map[string][]RawEntry{
		rangeKey("", "m"): {{}},
	}}
	cfg := testSchedulerConfig()
	cfg.ReplicationFactor = 1
	NewScheduler(cfg, store, table, staticCandidates{"node-1", "node-2"}, nil).tick()

	if got := table.Snapshot(); len(got.Ranges) != 2 {
		t.Fatalf("non-overlapping ownership should not merge: %#v", got)
	}
}

func TestSchedulerWaitsForRangeToSettle(t *testing.T) {
	table := newTestRangeTable(t, ring.RangeMap{Ranges: []ring.Range{
		versionedRingRange("", "", 1, "base", "node-1"),
	}})
	store := &rangeStore{entries: map[string][]RawEntry{
		rangeKey("", ""): {
			{Key: "a", Value: "value"},
			{Key: "m", Value: "value"},
		},
	}}
	cfg := testSchedulerConfig()
	cfg.SplitBytes = 1
	cfg.ReplicationFactor = 1
	cfg.SettlingTicks = 2
	scheduler := NewScheduler(cfg, store, table, staticCandidates{"node-1"}, nil)

	scheduler.tick()
	if got := table.Snapshot(); len(got.Ranges) != 1 {
		t.Fatalf("range changed before settling: %#v", got)
	}
	scheduler.tick()
	if got := table.Snapshot(); len(got.Ranges) != 2 {
		t.Fatalf("range did not split after settling: %#v", got)
	}
}

func TestSchedulerStartStopRunsLoop(t *testing.T) {
	table := newTestRangeTable(t, ring.RangeMap{Ranges: []ring.Range{
		versionedRingRange("", "", 1, "base", "node-1"),
	}})
	store := &rangeStore{entries: map[string][]RawEntry{rangeKey("", ""): nil}}
	cfg := testSchedulerConfig()
	cfg.SplitBytes = 100
	cfg.MergeBytes = 1
	cfg.ReplicationFactor = 1
	scheduler := NewScheduler(cfg, store, table, staticCandidates{"node-1"}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.calls() > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	scheduler.Stop()
	if store.calls() == 0 {
		t.Fatalf("scheduler loop did not scan any range")
	}
}

func newTestRangeTable(t *testing.T, initial ring.RangeMap) *ring.Table {
	t.Helper()
	table, err := ring.NewTable("node-1", initial)
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	return table
}

func versionedRingRange(start, end string, generation uint64, proposalID string, replicas ...string) ring.Range {
	return ring.Range{
		Start:      start,
		End:        end,
		Replicas:   replicas,
		Generation: generation,
		ProposalID: proposalID,
	}
}

func rangeKey(start, end string) string {
	return start + "\x00" + end
}

type rangeStore struct {
	mu      sync.Mutex
	entries map[string][]RawEntry
	count   int
}

func (s *rangeStore) ScanRawRange(start, end string) ([]RawEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	return append([]RawEntry(nil), s.entries[rangeKey(start, end)]...), nil
}

func (s *rangeStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

type staticCandidates []string

func (c staticCandidates) AliveNodeIDs() []string {
	return append([]string(nil), c...)
}

type recordingPublisher struct {
	epochs []uint64
}

func (p *recordingPublisher) PublishRangeMapEpoch(epoch uint64) {
	p.epochs = append(p.epochs, epoch)
}
