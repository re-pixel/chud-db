package ring

import "fmt"

// RangeMap is the full, versioned routing table: an ordered, contiguous,
// non-overlapping list of Ranges that together cover the entire
// keyspace. Generation increases monotonically whenever the map
// changes (split, merge, or replica reassignment); Table.Replace only
// ever accepts a RangeMap with a strictly higher generation than the
// one it currently holds.
type RangeMap struct {
	Generation uint64
	Ranges     []Range
}

// Clone returns a deep copy of m.
func (m RangeMap) Clone() RangeMap {
	ranges := make([]Range, len(m.Ranges))
	for i, r := range m.Ranges {
		ranges[i] = r.Clone()
	}
	return RangeMap{Generation: m.Generation, Ranges: ranges}
}

// Validate checks that Ranges is non-empty, starts and ends unbounded
// (the first range's Start and the last range's End must be ""),
// contiguous with no gaps or overlaps, individually non-degenerate, and
// that every range has at least one replica.
func (m RangeMap) Validate() error {
	if len(m.Ranges) == 0 {
		return fmt.Errorf("range map must have at least one range")
	}
	if m.Ranges[0].Start != "" {
		return fmt.Errorf("first range must start unbounded (empty start), got %q", m.Ranges[0].Start)
	}
	if m.Ranges[len(m.Ranges)-1].End != "" {
		return fmt.Errorf("last range must end unbounded (empty end), got %q", m.Ranges[len(m.Ranges)-1].End)
	}

	for i, r := range m.Ranges {
		if len(r.Replicas) == 0 {
			return fmt.Errorf("range %d (%q, %q) has no replicas", i, r.Start, r.End)
		}
		if r.End != "" && r.Start >= r.End {
			return fmt.Errorf("range %d (%q, %q) is empty or inverted", i, r.Start, r.End)
		}
		if i == 0 {
			continue
		}
		prev := m.Ranges[i-1]
		if r.Start != prev.End {
			return fmt.Errorf("range %d starts at %q but previous range ends at %q: ranges must be contiguous", i, r.Start, prev.End)
		}
	}
	return nil
}

// Owners returns the replica node IDs for the range containing key, and
// whether a matching range was found. It only returns false for a map
// that fails Validate (a valid map covers every key by construction).
func (m RangeMap) Owners(key string) ([]string, bool) {
	for _, r := range m.Ranges {
		if r.Contains(key) {
			return append([]string(nil), r.Replicas...), true
		}
	}
	return nil, false
}

// OwnersForKeyRange returns the replica node IDs owning every key in
// the closed interval [start, end] - matching engine.RangeScan's
// inclusive-on-both-ends convention, unlike Range's own half-open
// [Start, End) convention - and whether the whole interval falls
// within a single Range. A scan straddling a range boundary has no
// single owner set and reports false, even if every individual range
// it touches happens to share the same replicas.
func (m RangeMap) OwnersForKeyRange(start, end string) ([]string, bool) {
	for _, r := range m.Ranges {
		if !r.Contains(start) {
			continue
		}
		if r.End != "" && end >= r.End {
			return nil, false
		}
		return append([]string(nil), r.Replicas...), true
	}
	return nil, false
}

// OwnersForRange returns the replica node IDs owning the half-open
// interval [start, end), and whether it falls within a single Range.
func (m RangeMap) OwnersForRange(start, end string) ([]string, bool) {
	if end != "" && start >= end {
		return nil, false
	}
	for _, r := range m.Ranges {
		if !r.Contains(start) {
			continue
		}
		if end == "" {
			if r.End != "" {
				return nil, false
			}
		} else if r.End != "" && end > r.End {
			return nil, false
		}
		return append([]string(nil), r.Replicas...), true
	}
	return nil, false
}
