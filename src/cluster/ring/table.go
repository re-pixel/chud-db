package ring

import "sync"

// Table is a thread-safe holder of the local node's current RangeMap.
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

// Epoch returns the highest range generation currently held.
func (t *Table) Epoch() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rangeMap.Epoch()
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

// OwnsRange reports whether the local node owns the half-open interval
// [start, end). See RangeMap.OwnersForRange.
func (t *Table) OwnsRange(start, end string) bool {
	t.mu.RLock()
	owners, ok := t.rangeMap.OwnersForRange(start, end)
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

// Merge reconciles incoming with the current RangeMap.
func (t *Table) Merge(incoming RangeMap) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	merged, changed, err := t.rangeMap.Merge(incoming)
	if err != nil || !changed {
		return false, err
	}
	t.rangeMap = merged
	return true, nil
}
