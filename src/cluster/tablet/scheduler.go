package tablet

import (
	"context"
	"math"
	"sync"
	"time"

	"nosqlEngine/src/cluster/ring"
)

type RangeTable interface {
	Snapshot() ring.RangeMap
	Merge(incoming ring.RangeMap) (bool, error)
	Epoch() uint64
}

type CandidateSource interface {
	AliveNodeIDs() []string
}

type EpochPublisher interface {
	PublishRangeMapEpoch(epoch uint64)
}

type SchedulerConfig struct {
	NodeID            string
	Interval          time.Duration
	SplitBytes        int64
	MergeBytes        int64
	ReplicationFactor int
	SettlingTicks     int
}

type Scheduler struct {
	cfg        SchedulerConfig
	store      Store
	table      RangeTable
	candidates CandidateSource
	publisher  EpochPublisher

	estimate func(RangeScanner, string, string) (int64, error)
	boundary func(RangeScanner, string, string) (string, error)
	assign   func(string, string, []string, []string, int) ([]string, error)
	settled  map[settledRange]int

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

type settledRange struct {
	start      string
	end        string
	generation uint64
	proposalID string
}

func NewScheduler(cfg SchedulerConfig, store Store, table RangeTable, candidates CandidateSource, publisher EpochPublisher) *Scheduler {
	return &Scheduler{
		cfg:        cfg,
		store:      store,
		table:      table,
		candidates: candidates,
		publisher:  publisher,
		estimate:   EstimateSize,
		boundary:   PickSplitBoundary,
		assign:     AssignReplicas,
		settled:    make(map[settledRange]int),
		stopCh:     make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.run(ctx)
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

func (s *Scheduler) tick() {
	snapshot := s.table.Snapshot()
	s.updateSettled(snapshot)
	candidates := s.candidates.AliveNodeIDs()

	for i, r := range snapshot.Ranges {
		if !containsReplica(r.Replicas, s.cfg.NodeID) || !s.isSettled(r) {
			continue
		}
		size, err := s.estimate(s.store, r.Start, r.End)
		if err != nil {
			continue
		}
		if size >= s.cfg.SplitBytes {
			if s.splitRange(snapshot, i, candidates) {
				return
			}
			continue
		}
		if size <= s.cfg.MergeBytes && i+1 < len(snapshot.Ranges) {
			next := snapshot.Ranges[i+1]
			if !containsReplica(next.Replicas, s.cfg.NodeID) || !s.isSettled(next) {
				continue
			}
			nextSize, err := s.estimate(s.store, next.Start, next.End)
			if err == nil && nextSize <= s.cfg.MergeBytes && s.mergeRanges(snapshot, i, candidates) {
				return
			}
		}
	}
}

func (s *Scheduler) updateSettled(snapshot ring.RangeMap) {
	next := make(map[settledRange]int)
	for _, r := range snapshot.Ranges {
		if !containsReplica(r.Replicas, s.cfg.NodeID) {
			continue
		}
		key := settledRange{start: r.Start, end: r.End, generation: r.Generation, proposalID: r.ProposalID}
		next[key] = s.settled[key] + 1
	}
	s.settled = next
}

func (s *Scheduler) isSettled(r ring.Range) bool {
	key := settledRange{start: r.Start, end: r.End, generation: r.Generation, proposalID: r.ProposalID}
	return s.settled[key] >= s.cfg.SettlingTicks
}

func (s *Scheduler) splitRange(snapshot ring.RangeMap, index int, candidates []string) bool {
	current := snapshot.Ranges[index]
	if current.Generation == math.MaxUint64 {
		return false
	}
	boundary, err := s.boundary(s.store, current.Start, current.End)
	if err != nil || boundary == "" || boundary == current.Start || boundary == current.End {
		return false
	}
	leftReplicas, err := s.assign(current.Start, boundary, candidates, current.Replicas, s.cfg.ReplicationFactor)
	if err != nil {
		return false
	}
	rightReplicas, err := s.assign(boundary, current.End, candidates, current.Replicas, s.cfg.ReplicationFactor)
	if err != nil {
		return false
	}

	generation := current.Generation + 1
	children := []ring.Range{
		{Start: current.Start, End: boundary, Replicas: leftReplicas, Generation: generation},
		{Start: boundary, End: current.End, Replicas: rightReplicas, Generation: generation},
	}
	proposalID := ring.ProposalID(children)
	children[0].ProposalID = proposalID
	children[1].ProposalID = proposalID

	ranges := make([]ring.Range, 0, len(snapshot.Ranges)+1)
	ranges = append(ranges, snapshot.Ranges[:index]...)
	ranges = append(ranges, children...)
	ranges = append(ranges, snapshot.Ranges[index+1:]...)
	return s.apply(ring.RangeMap{Ranges: ranges})
}

func (s *Scheduler) mergeRanges(snapshot ring.RangeMap, leftIndex int, candidates []string) bool {
	left := snapshot.Ranges[leftIndex]
	right := snapshot.Ranges[leftIndex+1]
	generation := max(left.Generation, right.Generation)
	if generation == math.MaxUint64 {
		return false
	}
	retained := intersectReplicas(left.Replicas, right.Replicas)
	replicas, err := s.assign(left.Start, right.End, candidates, retained, s.cfg.ReplicationFactor)
	if err != nil {
		return false
	}

	merged := ring.Range{
		Start:      left.Start,
		End:        right.End,
		Replicas:   replicas,
		Generation: generation + 1,
	}
	merged.ProposalID = ring.ProposalID([]ring.Range{merged})

	ranges := make([]ring.Range, 0, len(snapshot.Ranges)-1)
	ranges = append(ranges, snapshot.Ranges[:leftIndex]...)
	ranges = append(ranges, merged)
	ranges = append(ranges, snapshot.Ranges[leftIndex+2:]...)
	return s.apply(ring.RangeMap{Ranges: ranges})
}

func (s *Scheduler) apply(proposal ring.RangeMap) bool {
	changed, err := s.table.Merge(proposal)
	if err != nil || !changed {
		return false
	}
	if s.publisher != nil {
		s.publisher.PublishRangeMapEpoch(s.table.Epoch())
	}
	return true
}

func containsReplica(replicas []string, nodeID string) bool {
	for _, replica := range replicas {
		if replica == nodeID {
			return true
		}
	}
	return false
}

func intersectReplicas(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, replica := range right {
		rightSet[replica] = struct{}{}
	}
	intersection := make([]string, 0, min(len(left), len(right)))
	for _, replica := range left {
		if _, ok := rightSet[replica]; ok {
			intersection = append(intersection, replica)
		}
	}
	return intersection
}
