package membership

import (
	"sort"
	"sync"
	"time"
)

// Table is a thread-safe SWIM membership table. It enforces cluster
// isolation and incarnation/status merge precedence, and protects the
// local node from wrongful suspicion or death by refuting with a higher
// incarnation.
type Table struct {
	mu        sync.RWMutex
	clusterID string
	localID   string
	members   map[string]Member
	epoch     uint64
	now       func() time.Time
}

// NewTable creates a membership table seeded with the local node's own
// record. local.ClusterID is overwritten to match clusterID.
func NewTable(clusterID string, local Member) *Table {
	if local.Status == StatusUnknown {
		local.Status = StatusAlive
	}
	local.ClusterID = clusterID

	t := &Table{
		clusterID: clusterID,
		localID:   local.NodeID,
		members:   make(map[string]Member),
		now:       time.Now,
	}
	t.setLocked(local)
	return t
}

// LocalID returns the node ID this table treats as "self".
func (t *Table) LocalID() string {
	return t.localID
}

// Local returns the current local node record.
func (t *Table) Local() Member {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.members[t.localID]
}

// Get returns the member record for nodeID, if known.
func (t *Table) Get(nodeID string) (Member, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	m, ok := t.members[nodeID]
	return m, ok
}

// Snapshot returns a copy of all known members sorted by NodeID.
func (t *Table) Snapshot() []Member {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]Member, 0, len(t.members))
	for _, m := range t.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Upsert unconditionally installs m in the table, bypassing merge
// precedence. It is intended for locally-authoritative updates such as
// seeding a newly-contacted peer's confirmed identity. Records from a
// different cluster are rejected.
func (t *Table) Upsert(m Member) bool {
	if m.ClusterID != "" && m.ClusterID != t.clusterID {
		return false
	}
	m.ClusterID = t.clusterID

	t.mu.Lock()
	defer t.mu.Unlock()
	t.setLocked(m)
	return true
}

// Merge applies incoming under SWIM precedence: a different ClusterID is
// ignored; higher incarnation always wins; at equal incarnation, dead
// beats suspect beats alive. Suspicion or death reported about the local
// node is refuted rather than accepted. Merge reports whether the table
// changed.
func (t *Table) Merge(incoming Member) bool {
	if incoming.ClusterID != "" && incoming.ClusterID != t.clusterID {
		return false
	}
	incoming.ClusterID = t.clusterID

	t.mu.Lock()
	defer t.mu.Unlock()

	if incoming.NodeID == t.localID {
		return t.refuteLocked(incoming)
	}

	current, exists := t.members[incoming.NodeID]
	if !exists {
		t.setLocked(incoming)
		return true
	}
	if !supersedes(incoming, current) {
		return false
	}
	t.setLocked(incoming)
	return true
}

// MarkSuspect locally transitions nodeID from alive to suspect, gated on
// the caller observing the same incarnation the table currently holds
// for that node. It never applies to the local node.
func (t *Table) MarkSuspect(nodeID string, incarnation uint64) bool {
	if nodeID == t.localID {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	current, exists := t.members[nodeID]
	if !exists || current.Incarnation != incarnation || current.Status != StatusAlive {
		return false
	}
	current.Status = StatusSuspect
	t.setLocked(current)
	return true
}

// MarkDead locally transitions a suspected nodeID to dead, gated on the
// caller observing the same incarnation the table currently holds for
// that node. It never applies to the local node.
func (t *Table) MarkDead(nodeID string, incarnation uint64) bool {
	if nodeID == t.localID {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	current, exists := t.members[nodeID]
	if !exists || current.Incarnation != incarnation || current.Status != StatusSuspect {
		return false
	}
	current.Status = StatusDead
	t.setLocked(current)
	return true
}

// RefuteLocalSuspect unconditionally increments the local node's
// incarnation and marks it alive. Callers use this to proactively clear
// suspicion about the local node observed outside of Merge.
func (t *Table) RefuteLocalSuspect() Member {
	t.mu.Lock()
	defer t.mu.Unlock()

	local := t.members[t.localID]
	local.Incarnation++
	local.Status = StatusAlive
	t.setLocked(local)
	return t.members[t.localID]
}

// refuteLocked reacts to an incoming record about the local node. Only
// suspicion or death accusations at an incarnation greater than or equal
// to our own trigger a refutation; anything else is ignored since the
// local node is authoritative about its own state. Must be called with
// mu held.
func (t *Table) refuteLocked(accusation Member) bool {
	if accusation.Status != StatusSuspect && accusation.Status != StatusDead {
		return false
	}
	local := t.members[t.localID]
	if accusation.Incarnation < local.Incarnation {
		return false
	}
	local.Incarnation = accusation.Incarnation + 1
	local.Status = StatusAlive
	t.setLocked(local)
	return true
}

// setLocked installs m, stamping local bookkeeping fields. Must be
// called with mu held.
func (t *Table) setLocked(m Member) {
	if m.LastSeen.IsZero() {
		m.LastSeen = t.now()
	}
	t.epoch++
	m.MembershipEpoch = t.epoch
	t.members[m.NodeID] = m
}
