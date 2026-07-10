package ring

import (
	"context"
	"sync"
	"time"
)

// PeerEpoch is the minimal peer information Syncer needs: where to
// reach a peer, and what range map generation it claims to know about.
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
// range map generation (e.g. over membership gossip) immediately after
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

// Syncer periodically checks known peers' advertised range map epochs
// against the local Table's generation, and pulls the full map (via
// RangeMapClient.GetRangeMap) from the first peer it finds reporting a
// strictly newer epoch. This is the "pull on staleness" half of range
// map propagation: the epoch itself already rides along on membership
// gossip for free, so Syncer only pays for a full-map RPC when there is
// concrete evidence it is actually behind.
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
// generation to peers (only useful in tests or single-node setups).
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

// tick pulls the full range map from the first peer whose advertised
// epoch is strictly newer than the local generation, trying subsequent
// candidates if a pull fails or the fetched map turns out to be stale
// or invalid by the time it is applied. It stops at the first
// successful replace.
func (s *Syncer) tick(ctx context.Context) {
	localGen := s.table.Generation()

	for _, peer := range s.peers.PeerEpochs() {
		if peer.RangeMapEpoch <= localGen {
			continue
		}

		pullCtx, cancel := context.WithTimeout(ctx, s.cfg.PullTimeout)
		fetched, err := s.client.GetRangeMap(pullCtx, peer.AdvertiseAddr)
		cancel()
		if err != nil {
			continue
		}
		changed, err := s.table.Replace(fetched)
		if err != nil || !changed {
			continue
		}
		if s.publisher != nil {
			s.publisher.PublishRangeMapEpoch(fetched.Generation)
		}
		return
	}
}
