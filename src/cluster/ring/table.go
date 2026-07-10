package ring

import "sync"

// Table is a thread-safe, generation-gated holder of the local node's
// current view of the cluster's RangeMap.
type Table struct {
	mu          sync.RWMutex
	localNodeID string
	rangeMap    RangeMap
}

// NewTable creates a Table seeded with initial. initial must be valid
// (see RangeMap.Validate); an invalid bootstrap map fails fast here
// rather than surfacing as confusing ownership errors later.
func NewTable(localNodeID string, initial RangeMap) (*Table, error) {
	if err := initial.Validate(); err != nil {
		return nil, err
	}
	return &Table{localNodeID: localNodeID, rangeMap: initial.Clone()}, nil
}

// Generation returns the currently held RangeMap's generation.
func (t *Table) Generation() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rangeMap.Generation
}

// Snapshot returns a deep copy of the currently held RangeMap.
func (t *Table) Snapshot() RangeMap {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rangeMap.Clone()
}

// Owners returns the replica node IDs for the range containing key.
func (t *Table) Owners(key string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	owners, _ := t.rangeMap.Owners(key)
	return owners
}

// IsOwner reports whether the local node is among the replicas for key.
func (t *Table) IsOwner(key string) bool {
	for _, id := range t.Owners(key) {
		if id == t.localNodeID {
			return true
		}
	}
	return false
}

// OwnsKeyRange reports whether the local node owns every key in the
// closed interval [start, end]. See RangeMap.OwnersForKeyRange.
func (t *Table) OwnsKeyRange(start, end string) bool {
	t.mu.RLock()
	owners, ok := t.rangeMap.OwnersForKeyRange(start, end)
	t.mu.RUnlock()
	if !ok {
		return false
	}
	for _, id := range owners {
		if id == t.localNodeID {
			return true
		}
	}
	return false
}

// Replace installs incoming as the current RangeMap if, and only if, it
// passes validation and its generation is strictly greater than the one
// currently held. It reports whether the table actually changed.
func (t *Table) Replace(incoming RangeMap) (bool, error) {
	if err := incoming.Validate(); err != nil {
		return false, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if incoming.Generation <= t.rangeMap.Generation {
		return false, nil
	}
	t.rangeMap = incoming.Clone()
	return true, nil
}
