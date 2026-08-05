package ring

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// PeerEpoch is the minimal peer information Syncer needs: where to
// reach a peer, and the highest range generation it claims to know.
// Defined here rather than depending on the membership package, so ring
// stays a leaf package; cmd/node adapts membership.Table.Snapshot()
// into this shape.
type PeerEpoch struct {
	NodeID        string
	AdvertiseAddr string
	RangeMapEpoch uint64
}

// PeerEpochSource supplies the current set of known peers and their
// advertised range map epochs.
type PeerEpochSource interface {
	PeerEpochs() []PeerEpoch
}

// LocalEpochPublisher lets Syncer advertise the local node's current
// range map epoch (e.g. over membership gossip) immediately after
// a pull changes it, rather than leaving other nodes to notice only
// once something else happens to touch local membership state.
type LocalEpochPublisher interface {
	PublishRangeMapEpoch(epoch uint64)
}

// LocalEpochPublisherFunc adapts a plain function to LocalEpochPublisher.
type LocalEpochPublisherFunc func(epoch uint64)

func (f LocalEpochPublisherFunc) PublishRangeMapEpoch(epoch uint64) { f(epoch) }

// SyncerConfig carries the timing knobs the syncer loop needs.
type SyncerConfig struct {
	Interval    time.Duration
	PullTimeout time.Duration
}

// Syncer pulls newer advertised range maps and periodically samples an
// equal-epoch peer so independent per-range updates still converge.
type Syncer struct {
	table     *Table
	client    RangeMapClient
	peers     PeerEpochSource
	publisher LocalEpochPublisher
	cfg       SyncerConfig

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewSyncer builds a Syncer. publisher may be nil, in which case a
// successful pull updates the local Table but nothing announces the new
// epoch to peers (only useful in tests or single-node setups).
func NewSyncer(table *Table, client RangeMapClient, peers PeerEpochSource, publisher LocalEpochPublisher, cfg SyncerConfig) *Syncer {
	return &Syncer{
		table:     table,
		client:    client,
		peers:     peers,
		publisher: publisher,
		cfg:       cfg,
		stopCh:    make(chan struct{}),
	}
}

// Start runs the sync loop in a background goroutine until ctx is
// canceled or Stop is called.
func (s *Syncer) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.run(ctx)
}

// Stop signals the loop to exit and waits for it to finish. Safe to
// call multiple times.
func (s *Syncer) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

func (s *Syncer) run(ctx context.Context) {
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
			s.tick(ctx)
		}
	}
}

func (s *Syncer) tick(ctx context.Context) {
	peers := s.peers.PeerEpochs()
	localEpoch := s.table.Epoch()
	attempted := make(map[int]struct{}, len(peers))

	for i, peer := range peers {
		if peer.RangeMapEpoch <= localEpoch {
			continue
		}
		attempted[i] = struct{}{}
		if s.pull(ctx, peer) {
			return
		}
	}

	candidates := make([]int, 0, len(peers)-len(attempted))
	for i := range peers {
		if _, ok := attempted[i]; !ok {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) > 0 {
		s.pull(ctx, peers[candidates[rand.Intn(len(candidates))]])
	}
}

func (s *Syncer) pull(ctx context.Context, peer PeerEpoch) bool {
	pullCtx, cancel := context.WithTimeout(ctx, s.cfg.PullTimeout)
	fetched, err := s.client.GetRangeMap(pullCtx, peer.AdvertiseAddr)
	cancel()
	if err != nil {
		return false
	}
	changed, err := s.table.Merge(fetched)
	if err != nil || !changed {
		return false
	}
	if s.publisher != nil {
		s.publisher.PublishRangeMapEpoch(s.table.Epoch())
	}
	return true
}
