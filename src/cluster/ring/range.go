package ring

// Range is a half-open key interval [Start, End) owned by a set of
// replicas. This is a routing/ownership concept and deliberately uses a
// different boundary convention than engine.RangeScan
// (src/engine/range_scan.go), which is inclusive on both ends and has no
// "unbounded" sentinel: here, Start == "" means "no lower bound" and
// End == "" means "no upper bound", so Range{Start: "", End: ""} matches
// every key. This lets the initial bootstrap map be a single range
// covering the whole keyspace, and lets a future split pick a boundary
// key that belongs unambiguously to exactly one side.
type Range struct {
	Start      string
	End        string
	Replicas   []string
	Generation uint64
	ProposalID string
}

// Contains reports whether key falls within the half-open interval
// [Start, End).
func (r Range) Contains(key string) bool {
	if r.Start != "" && key < r.Start {
		return false
	}
	if r.End != "" && key >= r.End {
		return false
	}
	return true
}

// Clone returns a deep copy of r.
func (r Range) Clone() Range {
	return Range{
		Start:      r.Start,
		End:        r.End,
		Replicas:   append([]string(nil), r.Replicas...),
		Generation: r.Generation,
		ProposalID: r.ProposalID,
	}
}
