package antientropy

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"nosqlEngine/src/cluster/versioning"
)

// OwnedRange is the minimal information Scheduler needs about a locally
// owned range: its key boundaries and full replica set (including the
// local node itself). Defined here rather than depending on the ring
// package, so antientropy stays a leaf package; a cmd/node adapter
// wraps *ring.Table to supply this.
type OwnedRange struct {
	Start    string
	End      string
	Replicas []string
}

// OwnedRangesSource supplies current range and key ownership.
type OwnedRangesSource interface {
	OwnedRanges() []OwnedRange
	IsOwner(key string) bool
}

// AddressResolver resolves a node ID to the network address the
// scheduler should dial to run a repair round against it. Defined
// locally (mirroring coordination.AddressResolver) to keep this
// package decoupled from membership's concrete adapter.
type AddressResolver interface {
	Address(nodeID string) (string, bool)
}

// SchedulerConfig carries the timing/tree-shape knobs the repair loop
// needs, mirroring the relevant fields of cluster/config.Config so
// callers can pass that config through directly.
type SchedulerConfig struct {
	NodeID            string
	Interval          time.Duration
	Timeout           time.Duration
	Fanout            int
	LeafItemThreshold int
	MaxDepth          int
}

// Scheduler periodically runs one recursive Merkle-tree repair round
// per locally owned range against one other replica of that range,
// driving both sides towards convergence on causally-settled state.
// Genuine concurrent conflicts are left untouched on both sides - see
// package doc.
type Scheduler struct {
	cfg       SchedulerConfig
	local     Store
	addresses AddressResolver
	replicas  ReplicaClient
	ranges    OwnedRangesSource

	random func(n int) int
	split  func(start, end string, n int) ([]Bucket, error)

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(cfg SchedulerConfig, local Store, addresses AddressResolver, replicas ReplicaClient, ranges OwnedRangesSource) *Scheduler {
	return &Scheduler{
		cfg:       cfg,
		local:     local,
		addresses: addresses,
		replicas:  replicas,
		ranges:    ranges,
		random:    rand.Intn,
		split:     SplitRange,
		stopCh:    make(chan struct{}),
	}
}

// Start runs the repair loop in a background goroutine until ctx is
// canceled or Stop is called.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.run(ctx)
}

// Stop signals the loop to exit and waits for it to finish. Safe to
// call multiple times.
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
			s.tick(ctx)
		}
	}
}

// tick runs one repair round per locally owned range, each against one
// randomly picked other replica. Peer selection comes purely from the
// range's own replica list (same random-peer philosophy as
// membership.Gossiper); an unresolvable address or a failed round is
// just treated as a miss for that range this tick - not retried, not
// treated specially.
func (s *Scheduler) tick(ctx context.Context) {
	for _, r := range s.ranges.OwnedRanges() {
		peerID, ok := s.pickPeer(r.Replicas)
		if !ok {
			continue
		}
		addr, ok := s.addresses.Address(peerID)
		if !ok {
			continue
		}

		roundCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
		_ = s.RepairRange(roundCtx, addr, r.Start, r.End)
		cancel()
	}
}

func (s *Scheduler) pickPeer(replicas []string) (string, bool) {
	others := make([]string, 0, len(replicas))
	for _, id := range replicas {
		if id != s.cfg.NodeID {
			others = append(others, id)
		}
	}
	if len(others) == 0 {
		return "", false
	}
	return others[s.random(len(others))], true
}

// RepairRange runs one recursive Merkle-tree diff-and-repair round for
// [start, end) against the peer at addr, starting at the tree root.
// Exported so the background loop and callers/tests can drive a round
// directly without waiting for a tick.
func (s *Scheduler) RepairRange(ctx context.Context, addr, start, end string) error {
	return s.diffNode(ctx, addr, start, end, 0)
}

// diffNode is the recursive tree walk: compare this node's root against
// the peer's; if they already match, there is nothing to do. On a
// mismatch, either stop and diff the node's actual contents as a leaf,
// or split into Fanout children and recurse into each independently -
// a failed child does not abort its siblings.
func (s *Scheduler) diffNode(ctx context.Context, addr, start, end string, depth int) error {
	localHash, localCount, err := ComputeMerkleRoot(s.local, start, end)
	if err != nil {
		return fmt.Errorf("local root [%q,%q): %w", start, end, err)
	}
	peerHash, peerCount, err := s.replicas.GetMerkleRoot(ctx, addr, start, end)
	if err != nil {
		return fmt.Errorf("peer root [%q,%q): %w", start, end, err)
	}
	if localCount == peerCount && bytes.Equal(localHash, peerHash) {
		return nil
	}

	mismatchCount := localCount
	if peerCount > mismatchCount {
		mismatchCount = peerCount
	}
	if s.shouldStop(depth, mismatchCount) {
		return s.diffLeaf(ctx, addr, start, end)
	}

	buckets, err := s.split(start, end, s.cfg.Fanout)
	if err != nil {
		return fmt.Errorf("split [%q,%q): %w", start, end, err)
	}
	if len(buckets) == 1 && buckets[0].Start == start && buckets[0].End == end {
		// Degenerate: can't subdivide further (e.g. a shared byte
		// prefix collision - see SplitRange), so this node is a leaf
		// for this round regardless of depth or item count.
		return s.diffLeaf(ctx, addr, start, end)
	}

	var firstErr error
	for _, bucket := range buckets {
		if err := s.diffNode(ctx, addr, bucket.Start, bucket.End, depth+1); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Scheduler) shouldStop(depth int, itemCount uint64) bool {
	if depth >= s.cfg.MaxDepth {
		return true
	}
	return itemCount <= uint64(s.cfg.LeafItemThreshold)
}

// diffLeaf streams both sides' actual contents for [start, end),
// merge-diffs them by key, and applies whatever repairs the
// classification calls for. Local repairs and peer-pushed repairs are
// both attempted even if one direction fails.
func (s *Scheduler) diffLeaf(ctx context.Context, addr, start, end string) error {
	localRows, err := s.localRows(start, end)
	if err != nil {
		return fmt.Errorf("local leaf scan [%q,%q): %w", start, end, err)
	}
	peerRows, err := s.replicas.StreamRange(ctx, addr, start, end)
	if err != nil {
		return fmt.Errorf("peer leaf scan [%q,%q): %w", start, end, err)
	}
	sort.Slice(peerRows, func(i, j int) bool { return peerRows[i].Key < peerRows[j].Key })

	toPull, toPush := mergeDiff(localRows, peerRows)

	pullErr := s.applyPulls(toPull)
	pushErr := s.pushRepairs(ctx, addr, toPush)
	if pullErr != nil {
		return pullErr
	}
	return pushErr
}

// localRows scans and decodes every entry in [start, end) from local
// storage, sorted by key - the local-side mirror of what Server.
// StreamRange sends a peer for the same range.
func (s *Scheduler) localRows(start, end string) ([]versioning.KeyEnvelope, error) {
	entries, err := s.local.ScanRawRange(start, end)
	if err != nil {
		return nil, err
	}
	rows := make([]versioning.KeyEnvelope, 0, len(entries))
	for _, entry := range entries {
		envelope, err := versioning.Decode(entry.Value)
		if err != nil {
			return nil, fmt.Errorf("decode local %q: %w", entry.Key, err)
		}
		rows = append(rows, versioning.KeyEnvelope{Key: entry.Key, Envelope: envelope})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows, nil
}

// mergeDiff walks two key-sorted row lists with a two-pointer merge and
// classifies every key: present on only one side is a straggler catch-up
// (pull it locally, or queue it to push to the peer); present on both
// sides is resolved by classifyKey.
func mergeDiff(localRows, peerRows []versioning.KeyEnvelope) (toPull, toPush []versioning.KeyEnvelope) {
	i, j := 0, 0
	for i < len(localRows) && j < len(peerRows) {
		switch {
		case localRows[i].Key < peerRows[j].Key:
			toPush = append(toPush, localRows[i])
			i++
		case peerRows[j].Key < localRows[i].Key:
			toPull = append(toPull, peerRows[j])
			j++
		default:
			if pull, push := classifyKey(localRows[i], peerRows[j]); pull != nil {
				toPull = append(toPull, *pull)
			} else if push != nil {
				toPush = append(toPush, *push)
			}
			i++
			j++
		}
	}
	for ; i < len(localRows); i++ {
		toPush = append(toPush, localRows[i])
	}
	for ; j < len(peerRows); j++ {
		toPull = append(toPull, peerRows[j])
	}
	return toPull, toPush
}

// classifyKey decides what to do for a key present on both sides,
// purely by vector clock dominance: the causally older side is a
// straggler and gets the newer envelope propagated to it. Concurrent
// clocks are a genuine sibling conflict, left untouched on both sides -
// resolving those is the client's job via Coordinator.Get's sibling
// exposure. Equal clocks take no action either way, whether the two
// envelopes are byte-identical or (an anomaly that should not occur
// under correct operation) merely clock-equal with different content -
// neither side can be safely preferred over the other in that case.
func classifyKey(local, peer versioning.KeyEnvelope) (pull, push *versioning.KeyEnvelope) {
	switch versioning.Compare(local.Envelope.VectorClock, peer.Envelope.VectorClock) {
	case versioning.Before:
		return &peer, nil
	case versioning.After:
		return nil, &local
	default: // Equal, Concurrent
		return nil, nil
	}
}

func (s *Scheduler) applyPulls(rows []versioning.KeyEnvelope) error {
	var firstErr error
	for _, row := range rows {
		if !s.ranges.IsOwner(row.Key) {
			continue
		}
		var err error
		if row.Envelope.Deleted {
			err = s.local.Delete(row.Key, row.Envelope, true)
		} else {
			err = s.local.Put(row.Key, row.Envelope, true)
		}
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("apply local repair for %q: %w", row.Key, err)
		}
	}
	return firstErr
}

func (s *Scheduler) pushRepairs(ctx context.Context, addr string, rows []versioning.KeyEnvelope) error {
	if len(rows) == 0 {
		return nil
	}
	if err := s.replicas.RepairKeys(ctx, addr, rows); err != nil {
		return fmt.Errorf("push repair to %s: %w", addr, err)
	}
	return nil
}
