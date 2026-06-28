package versioning

type VectorClock map[string]uint64

type Relation int

const (
	Equal Relation = iota
	Before
	After
	Concurrent
)

func (vc VectorClock) Clone() VectorClock {
	if len(vc) == 0 {
		return VectorClock{}
	}
	clone := make(VectorClock, len(vc))
	for nodeID, counter := range vc {
		clone[nodeID] = counter
	}
	return clone
}

func (vc VectorClock) Increment(nodeID string) VectorClock {
	next := vc.Clone()
	next[nodeID]++
	return next
}

func Merge(a, b VectorClock) VectorClock {
	merged := a.Clone()
	for nodeID, counter := range b {
		if counter > merged[nodeID] {
			merged[nodeID] = counter
		}
	}
	return merged
}

func Compare(a, b VectorClock) Relation {
	aLess := false
	aGreater := false

	for nodeID, aCounter := range a {
		bCounter := b[nodeID]
		if aCounter < bCounter {
			aLess = true
		}
		if aCounter > bCounter {
			aGreater = true
		}
	}

	for nodeID, bCounter := range b {
		if _, seen := a[nodeID]; seen {
			continue
		}
		if bCounter > 0 {
			aLess = true
		}
	}

	switch {
	case !aLess && !aGreater:
		return Equal
	case aLess && !aGreater:
		return Before
	case !aLess && aGreater:
		return After
	default:
		return Concurrent
	}
}
