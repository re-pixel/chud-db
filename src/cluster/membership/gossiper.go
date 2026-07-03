package membership

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// GossiperConfig carries the timing/fanout knobs the gossiper loop needs.
// It mirrors the relevant fields of cluster/config.Config so callers can
// pass that config through directly.
type GossiperConfig struct {
	GossipInterval     time.Duration
	PingTimeout        time.Duration
	SuspectTimeout     time.Duration
	DeadTimeout        time.Duration
	IndirectPingFanout int

	// Seeds are bootstrap peer addresses from static config. Their
	// NodeID is not known in advance, so they are not added to the
	// Table as members; instead the gossiper keeps attempting direct
	// contact until each one responds, at which point its real,
	// self-reported Member state is merged in like any other peer.
	Seeds []string
}

// Gossiper periodically probes a random peer for liveness (with
// indirect-ping fallback) and exchanges membership state with a random
// peer, driving the local Table's convergence with the rest of the
// cluster.
type Gossiper struct {
	table  *Table
	client GossipClient
	cfg    GossiperConfig

	now    func() time.Time
	random func(n int) int

	mu             sync.Mutex
	suspectedSince map[string]time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewGossiper(table *Table, client GossipClient, cfg GossiperConfig) *Gossiper {
	return &Gossiper{
		table:          table,
		client:         client,
		cfg:            cfg,
		now:            time.Now,
		random:         rand.Intn,
		suspectedSince: make(map[string]time.Time),
		stopCh:         make(chan struct{}),
	}
}

// Start runs the gossip loop in a background goroutine until ctx is
// canceled or Stop is called.
func (g *Gossiper) Start(ctx context.Context) {
	g.wg.Add(1)
	go g.run(ctx)
}

// Stop signals the loop to exit and waits for it to finish. It is safe
// to call multiple times.
func (g *Gossiper) Stop() {
	g.stopOnce.Do(func() { close(g.stopCh) })
	g.wg.Wait()
}

func (g *Gossiper) run(ctx context.Context) {
	defer g.wg.Done()

	ticker := time.NewTicker(g.cfg.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.tick(ctx)
		}
	}
}

func (g *Gossiper) tick(ctx context.Context) {
	g.probeSeeds(ctx)
	g.probeRandomPeer(ctx)
	g.gossipRandomPeer(ctx)
}

// probeSeeds attempts direct contact with any configured seed address
// that isn't already known as a table member (matched by AdvertiseAddr,
// since a seed's NodeID is unknown until it responds). A successful
// contact's self-reported state is upserted into the table, after which
// normal peer selection takes over for that node; unresolved seeds are
// retried on every subsequent tick.
func (g *Gossiper) probeSeeds(ctx context.Context) {
	pending := g.pendingSeeds()
	if len(pending) == 0 {
		return
	}

	local := g.table.Local()
	for _, addr := range pending {
		pingCtx, cancel := context.WithTimeout(ctx, g.cfg.PingTimeout)
		responder, err := g.client.Ping(pingCtx, addr, "", local)
		cancel()
		if err != nil {
			continue
		}
		g.table.Upsert(responder)
	}
}

func (g *Gossiper) pendingSeeds() []string {
	if len(g.cfg.Seeds) == 0 {
		return nil
	}

	known := make(map[string]struct{})
	for _, m := range g.table.Snapshot() {
		known[m.AdvertiseAddr] = struct{}{}
	}

	pending := make([]string, 0, len(g.cfg.Seeds))
	for _, addr := range g.cfg.Seeds {
		if _, ok := known[addr]; !ok {
			pending = append(pending, addr)
		}
	}
	return pending
}

// probeRandomPeer runs one round of SWIM failure detection: direct
// ping, indirect ping on failure, and suspect/dead escalation if both
// fail.
func (g *Gossiper) probeRandomPeer(ctx context.Context) {
	peers := g.peers()
	if len(peers) == 0 {
		return
	}
	target := peers[g.random(len(peers))]

	if g.directPing(ctx, target) {
		g.clearSuspicion(target.NodeID)
		return
	}
	if g.indirectPing(ctx, target) {
		g.clearSuspicion(target.NodeID)
		return
	}
	g.escalateFailure(target)
}

func (g *Gossiper) directPing(ctx context.Context, target Member) bool {
	pingCtx, cancel := context.WithTimeout(ctx, g.cfg.PingTimeout)
	defer cancel()

	responder, err := g.client.Ping(pingCtx, target.AdvertiseAddr, target.NodeID, g.table.Local())
	if err != nil {
		return false
	}
	// A direct, successful probe is locally-authoritative evidence of
	// liveness: it must clear any locally-held suspicion regardless of
	// incarnation/status precedence, so Upsert (not Merge) is used.
	g.table.Upsert(responder)
	return true
}

func (g *Gossiper) indirectPing(ctx context.Context, target Member) bool {
	helpers := g.randomPeersExcluding(target.NodeID, g.cfg.IndirectPingFanout)
	if len(helpers) == 0 {
		return false
	}

	local := g.table.Local()
	type result struct {
		acked     bool
		responder Member
	}
	results := make(chan result, len(helpers))

	for _, helper := range helpers {
		helper := helper
		go func() {
			pingCtx, cancel := context.WithTimeout(ctx, g.cfg.PingTimeout)
			defer cancel()
			acked, responder, err := g.client.IndirectPing(pingCtx, helper.AdvertiseAddr, local, target)
			if err != nil {
				results <- result{}
				return
			}
			results <- result{acked: acked, responder: responder}
		}()
	}

	acked := false
	var confirmed Member
	for i := 0; i < len(helpers); i++ {
		if r := <-results; r.acked && !acked {
			acked = true
			confirmed = r.responder
		}
	}
	if acked {
		g.table.Upsert(confirmed)
	}
	return acked
}

// escalateFailure advances the failure detector state machine for a
// peer that failed both a direct and an indirect probe: alive members
// become suspect immediately, and suspect members become dead once
// they have been suspect for at least SuspectTimeout.
func (g *Gossiper) escalateFailure(target Member) {
	now := g.now()

	switch target.Status {
	case StatusAlive:
		if g.table.MarkSuspect(target.NodeID, target.Incarnation) {
			g.mu.Lock()
			g.suspectedSince[target.NodeID] = now
			g.mu.Unlock()
		}
	case StatusSuspect:
		g.mu.Lock()
		since, tracked := g.suspectedSince[target.NodeID]
		if !tracked {
			since = now
			g.suspectedSince[target.NodeID] = since
		}
		g.mu.Unlock()

		if now.Sub(since) >= g.cfg.SuspectTimeout {
			if g.table.MarkDead(target.NodeID, target.Incarnation) {
				g.clearSuspicion(target.NodeID)
			}
		}
	}
}

func (g *Gossiper) clearSuspicion(nodeID string) {
	g.mu.Lock()
	delete(g.suspectedSince, nodeID)
	g.mu.Unlock()
}

// gossipRandomPeer exchanges full membership snapshots with one random
// peer so that information learned indirectly (about nodes this node
// has never probed directly) still propagates through the cluster.
func (g *Gossiper) gossipRandomPeer(ctx context.Context) {
	peers := g.peers()
	if len(peers) == 0 {
		return
	}
	peer := peers[g.random(len(peers))]

	gossipCtx, cancel := context.WithTimeout(ctx, g.cfg.PingTimeout)
	defer cancel()

	membership, err := g.client.Gossip(gossipCtx, peer.AdvertiseAddr, g.table.Local(), g.table.Snapshot())
	if err != nil {
		return
	}
	for _, m := range membership {
		g.table.Merge(m)
	}
}

// peers returns known members other than the local node, excluding
// those already marked dead.
func (g *Gossiper) peers() []Member {
	snapshot := g.table.Snapshot()
	localID := g.table.LocalID()

	peers := make([]Member, 0, len(snapshot))
	for _, m := range snapshot {
		if m.NodeID == localID || m.Status == StatusDead {
			continue
		}
		peers = append(peers, m)
	}
	return peers
}

func (g *Gossiper) randomPeersExcluding(excludeNodeID string, n int) []Member {
	if n <= 0 {
		return nil
	}

	candidates := make([]Member, 0)
	for _, m := range g.peers() {
		if m.NodeID != excludeNodeID {
			candidates = append(candidates, m)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	for i := len(candidates) - 1; i > 0; i-- {
		j := g.random(i + 1)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}
	if len(candidates) > n {
		candidates = candidates[:n]
	}
	return candidates
}
