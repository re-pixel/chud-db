package membership

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testGossiperConfig() GossiperConfig {
	return GossiperConfig{
		GossipInterval:     10 * time.Millisecond,
		PingTimeout:        50 * time.Millisecond,
		SuspectTimeout:     100 * time.Millisecond,
		DeadTimeout:        time.Second,
		IndirectPingFanout: 2,
	}
}

func TestGossiperDirectPingSuccessUpsertsResponder(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
	table.Merge(Member{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", Status: StatusAlive, Incarnation: 1})

	client := newFakeGossipClient()
	client.pingResults["10.0.0.1:7000"] = pingResult{member: Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 5}}

	g := NewGossiper(table, client, testGossiperConfig())
	g.probeRandomPeer(context.Background())

	m, _ := table.Get("peer-1")
	if m.Incarnation != 5 {
		t.Fatalf("peer-1 = %+v, want incarnation 5 after successful ping", m)
	}
}

func TestGossiperDirectPingClearsSuspicion(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
	table.Merge(Member{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", Status: StatusAlive, Incarnation: 1})
	table.MarkSuspect("peer-1", 1)

	client := newFakeGossipClient()
	client.pingResults["10.0.0.1:7000"] = pingResult{member: Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 1}}

	g := NewGossiper(table, client, testGossiperConfig())
	g.suspectedSince["peer-1"] = time.Now()

	g.probeRandomPeer(context.Background())

	m, _ := table.Get("peer-1")
	if m.Status != StatusAlive {
		t.Fatalf("peer-1 status = %v, want alive after successful direct ping", m.Status)
	}
	if _, tracked := g.suspectedSince["peer-1"]; tracked {
		t.Fatalf("expected suspicion tracking to be cleared")
	}
}

func TestGossiperFallsBackToIndirectPingOnDirectFailure(t *testing.T) {
	// "zz-helper-1" is named to sort after "peer-1" so the fixed
	// always-pick-first random source deterministically selects peer-1
	// as the direct-ping target.
	table := NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
	table.Merge(Member{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", Status: StatusAlive, Incarnation: 1})
	table.Merge(Member{NodeID: "zz-helper-1", AdvertiseAddr: "10.0.0.2:7000", Status: StatusAlive, Incarnation: 1})

	client := newFakeGossipClient()
	client.pingResults["10.0.0.1:7000"] = pingResult{err: errors.New("unreachable")}
	client.indirectResults["10.0.0.2:7000"] = indirectResult{acked: true, responder: Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 9}}

	g := NewGossiper(table, client, testGossiperConfig())
	g.random = func(int) int { return 0 }
	g.probeRandomPeer(context.Background())

	m, _ := table.Get("peer-1")
	if m.Status != StatusAlive || m.Incarnation != 9 {
		t.Fatalf("peer-1 = %+v, want alive at incarnation 9 via indirect confirmation", m)
	}
	if len(client.indirectCallsFor("10.0.0.2:7000")) != 1 {
		t.Fatalf("expected exactly one indirect ping call to helper-1")
	}
}

func TestGossiperBothProbesFailingMarksSuspect(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
	table.Merge(Member{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", Status: StatusAlive, Incarnation: 1})
	table.Merge(Member{NodeID: "zz-helper-1", AdvertiseAddr: "10.0.0.2:7000", Status: StatusAlive, Incarnation: 1})

	client := newFakeGossipClient()
	client.pingResults["10.0.0.1:7000"] = pingResult{err: errors.New("unreachable")}
	client.indirectResults["10.0.0.2:7000"] = indirectResult{acked: false}

	g := NewGossiper(table, client, testGossiperConfig())
	g.random = func(int) int { return 0 }
	g.probeRandomPeer(context.Background())

	m, _ := table.Get("peer-1")
	if m.Status != StatusSuspect {
		t.Fatalf("peer-1 status = %v, want suspect after both probes fail", m.Status)
	}
	if _, tracked := g.suspectedSince["peer-1"]; !tracked {
		t.Fatalf("expected suspicion to be tracked")
	}
}

func TestGossiperEscalatesSuspectToDeadAfterTimeout(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
	table.Merge(Member{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", Status: StatusAlive, Incarnation: 1})
	table.Merge(Member{NodeID: "zz-helper-1", AdvertiseAddr: "10.0.0.2:7000", Status: StatusAlive, Incarnation: 1})

	client := newFakeGossipClient()
	client.pingResults["10.0.0.1:7000"] = pingResult{err: errors.New("unreachable")}
	client.indirectResults["10.0.0.2:7000"] = indirectResult{acked: false}

	cfg := testGossiperConfig()
	g := NewGossiper(table, client, cfg)
	g.random = func(int) int { return 0 }
	clock := time.Now()
	g.now = func() time.Time { return clock }

	g.probeRandomPeer(context.Background())
	m, _ := table.Get("peer-1")
	if m.Status != StatusSuspect {
		t.Fatalf("peer-1 status = %v, want suspect after first failed round", m.Status)
	}

	clock = clock.Add(cfg.SuspectTimeout)
	g.probeRandomPeer(context.Background())
	m, _ = table.Get("peer-1")
	if m.Status != StatusDead {
		t.Fatalf("peer-1 status = %v, want dead after suspect timeout elapses", m.Status)
	}
}

func TestGossiperPeersExcludesLocalAndDead(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local"})
	table.Merge(Member{NodeID: "alive-1", Status: StatusAlive})
	table.Merge(Member{NodeID: "suspect-1", Status: StatusSuspect})
	table.Merge(Member{NodeID: "dead-1", Status: StatusDead})

	g := NewGossiper(table, newFakeGossipClient(), testGossiperConfig())
	peers := g.peers()

	if len(peers) != 2 {
		t.Fatalf("peers = %+v, want alive-1 and suspect-1 only", peers)
	}
	for _, p := range peers {
		if p.NodeID == "local" || p.NodeID == "dead-1" {
			t.Fatalf("unexpected peer in list: %+v", p)
		}
	}
}

func TestGossiperRandomPeersExcludingRespectsExclusionAndFanout(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local"})
	table.Merge(Member{NodeID: "peer-a", Status: StatusAlive})
	table.Merge(Member{NodeID: "peer-b", Status: StatusAlive})
	table.Merge(Member{NodeID: "peer-c", Status: StatusAlive})

	g := NewGossiper(table, newFakeGossipClient(), testGossiperConfig())
	helpers := g.randomPeersExcluding("peer-a", 2)

	if len(helpers) != 2 {
		t.Fatalf("helpers = %+v, want length 2", helpers)
	}
	for _, h := range helpers {
		if h.NodeID == "peer-a" {
			t.Fatalf("excluded peer present in helpers: %+v", helpers)
		}
	}
}

func TestGossiperGossipRandomPeerMergesResponse(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local"})
	table.Merge(Member{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", Status: StatusAlive})

	client := newFakeGossipClient()
	client.gossipResult = []Member{{NodeID: "peer-2", Status: StatusSuspect, Incarnation: 2}}

	g := NewGossiper(table, client, testGossiperConfig())
	g.gossipRandomPeer(context.Background())

	if _, ok := table.Get("peer-2"); !ok {
		t.Fatalf("expected gossiped member to be merged")
	}
	if len(client.gossipCalls) != 1 || client.gossipCalls[0] != "10.0.0.1:7000" {
		t.Fatalf("gossip calls = %#v", client.gossipCalls)
	}
}

func TestGossiperStartStopRunsLoopAndStopsCleanly(t *testing.T) {
	table := NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
	table.Merge(Member{NodeID: "peer-1", AdvertiseAddr: "10.0.0.1:7000", Status: StatusAlive})

	client := newFakeGossipClient()
	client.pingResults["10.0.0.1:7000"] = pingResult{member: Member{NodeID: "peer-1", Status: StatusAlive}}
	client.gossipResult = nil

	cfg := testGossiperConfig()
	cfg.GossipInterval = 5 * time.Millisecond
	g := NewGossiper(table, client, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	g.Start(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(client.pingCallsSnapshot()) > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	g.Stop()

	if len(client.pingCallsSnapshot()) == 0 {
		t.Fatalf("expected at least one ping call before stopping")
	}
}

type pingResult struct {
	member Member
	err    error
}

type indirectResult struct {
	acked     bool
	responder Member
	err       error
}

type indirectCall struct {
	addr   string
	target string
}

type fakeGossipClient struct {
	mu sync.Mutex

	pingResults map[string]pingResult
	pingCalls   []string

	indirectResults map[string]indirectResult
	indirectCalls   []indirectCall

	gossipResult []Member
	gossipErr    error
	gossipCalls  []string
}

func newFakeGossipClient() *fakeGossipClient {
	return &fakeGossipClient{
		pingResults:     make(map[string]pingResult),
		indirectResults: make(map[string]indirectResult),
	}
}

func (c *fakeGossipClient) Ping(_ context.Context, addr, _ string, _ Member) (Member, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pingCalls = append(c.pingCalls, addr)
	r, ok := c.pingResults[addr]
	if !ok {
		return Member{}, errors.New("fake: no ping result configured for " + addr)
	}
	return r.member, r.err
}

func (c *fakeGossipClient) Gossip(_ context.Context, addr string, _ Member, _ []Member) ([]Member, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gossipCalls = append(c.gossipCalls, addr)
	return c.gossipResult, c.gossipErr
}

func (c *fakeGossipClient) IndirectPing(_ context.Context, addr string, _ Member, target Member) (bool, Member, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.indirectCalls = append(c.indirectCalls, indirectCall{addr: addr, target: target.NodeID})
	r, ok := c.indirectResults[addr]
	if !ok {
		return false, Member{}, errors.New("fake: no indirect ping result configured for " + addr)
	}
	return r.acked, r.responder, r.err
}

func (c *fakeGossipClient) indirectCallsFor(addr string) []indirectCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []indirectCall
	for _, call := range c.indirectCalls {
		if call.addr == addr {
			out = append(out, call)
		}
	}
	return out
}

func (c *fakeGossipClient) pingCallsSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.pingCalls...)
}
