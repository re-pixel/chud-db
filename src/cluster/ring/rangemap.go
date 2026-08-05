package ring

import (
	"fmt"
	"math"
	"slices"
	"sort"
)

// RangeMap is the full routing table: an ordered, contiguous,
// non-overlapping list of versioned Ranges covering the keyspace.
type RangeMap struct {
	Ranges []Range
}

// Clone returns a deep copy of m.
func (m RangeMap) Clone() RangeMap {
	ranges := make([]Range, len(m.Ranges))
	for i, r := range m.Ranges {
		ranges[i] = r.Clone()
	}
	return RangeMap{Ranges: ranges}
}

func (m RangeMap) Epoch() uint64 {
	var epoch uint64
	for _, r := range m.Ranges {
		if r.Generation > epoch {
			epoch = r.Generation
		}
	}
	return epoch
}

// Validate checks that Ranges is non-empty, starts and ends unbounded
// (the first range's Start and the last range's End must be ""),
// contiguous with no gaps or overlaps, individually non-degenerate, and
// that every range has a generation, proposal ID, and at least one replica.
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
		if r.Generation == 0 {
			return fmt.Errorf("range %d (%q, %q) has generation 0", i, r.Start, r.End)
		}
		if r.ProposalID == "" {
			return fmt.Errorf("range %d (%q, %q) has no proposal ID", i, r.Start, r.End)
		}
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

type rangeSource struct {
	side  uint8
	index int
}

type mergedSegment struct {
	start  string
	end    string
	range_ Range
	source rangeSource
}

type proposalRef struct {
	side       uint8
	proposalID string
	generation uint64
	index      int
}

func (m RangeMap) Merge(incoming RangeMap) (RangeMap, bool, error) {
	if err := m.Validate(); err != nil {
		return RangeMap{}, false, fmt.Errorf("validate current range map: %w", err)
	}
	if err := incoming.Validate(); err != nil {
		return RangeMap{}, false, fmt.Errorf("validate incoming range map: %w", err)
	}

	boundaries := mergeBoundaries(m, incoming)
	segments := make([]mergedSegment, 0, len(boundaries)+1)
	bumps := make(map[proposalRef]struct{})
	start := ""
	for _, end := range append(boundaries, "") {
		localIndex := rangeIndexAt(m.Ranges, start)
		incomingIndex := rangeIndexAt(incoming.Ranges, start)
		local := m.Ranges[localIndex]
		remote := incoming.Ranges[incomingIndex]

		winner, source, tied := pickRange(local, remote, localIndex, incomingIndex)
		if tied {
			if winner.Generation == math.MaxUint64 {
				return RangeMap{}, false, fmt.Errorf("cannot resolve generation tie at maximum generation")
			}
			bumps[proposalReference(source, winner)] = struct{}{}
		}
		segments = append(segments, mergedSegment{
			start:  start,
			end:    end,
			range_: winner,
			source: source,
		})
		start = end
	}

	for i := range segments {
		if _, ok := bumps[proposalReference(segments[i].source, segments[i].range_)]; ok {
			segments[i].range_.Generation++
		}
	}

	result := RangeMap{Ranges: coalesceSegments(segments)}
	if err := result.Validate(); err != nil {
		return RangeMap{}, false, fmt.Errorf("validate merged range map: %w", err)
	}
	return result, !rangeMapsEqual(m, result), nil
}

func mergeBoundaries(left, right RangeMap) []string {
	unique := make(map[string]struct{}, len(left.Ranges)+len(right.Ranges))
	for _, ranges := range [][]Range{left.Ranges, right.Ranges} {
		for _, r := range ranges {
			if r.End != "" {
				unique[r.End] = struct{}{}
			}
		}
	}
	boundaries := make([]string, 0, len(unique))
	for boundary := range unique {
		boundaries = append(boundaries, boundary)
	}
	sort.Strings(boundaries)
	return boundaries
}

func rangeIndexAt(ranges []Range, key string) int {
	for i, r := range ranges {
		if r.Contains(key) {
			return i
		}
	}
	panic("validated range map does not cover key")
}

func pickRange(local, incoming Range, localIndex, incomingIndex int) (Range, rangeSource, bool) {
	if local.Generation > incoming.Generation {
		return local.Clone(), rangeSource{index: localIndex}, false
	}
	if incoming.Generation > local.Generation {
		return incoming.Clone(), rangeSource{side: 1, index: incomingIndex}, false
	}
	if rangesEqual(local, incoming) {
		return local.Clone(), rangeSource{index: localIndex}, false
	}
	if compareTieCandidate(local, incoming) <= 0 {
		return local.Clone(), rangeSource{index: localIndex}, true
	}
	return incoming.Clone(), rangeSource{side: 1, index: incomingIndex}, true
}

func compareTieCandidate(left, right Range) int {
	if left.ProposalID < right.ProposalID {
		return -1
	}
	if left.ProposalID > right.ProposalID {
		return 1
	}
	if left.Start < right.Start {
		return -1
	}
	if left.Start > right.Start {
		return 1
	}
	if left.End < right.End {
		return -1
	}
	if left.End > right.End {
		return 1
	}
	for i := 0; i < min(len(left.Replicas), len(right.Replicas)); i++ {
		if left.Replicas[i] < right.Replicas[i] {
			return -1
		}
		if left.Replicas[i] > right.Replicas[i] {
			return 1
		}
	}
	if len(left.Replicas) < len(right.Replicas) {
		return -1
	}
	if len(left.Replicas) > len(right.Replicas) {
		return 1
	}
	return 0
}

func proposalReference(source rangeSource, r Range) proposalRef {
	index := -1
	if r.ProposalID == "" {
		index = source.index
	}
	return proposalRef{
		side:       source.side,
		proposalID: r.ProposalID,
		generation: r.Generation,
		index:      index,
	}
}

func coalesceSegments(segments []mergedSegment) []Range {
	ranges := make([]Range, 0, len(segments))
	sources := make([]rangeSource, 0, len(segments))
	for _, segment := range segments {
		r := segment.range_.Clone()
		r.Start = segment.start
		r.End = segment.end
		if len(ranges) > 0 && sources[len(sources)-1] == segment.source && sameRangeMetadata(ranges[len(ranges)-1], r) {
			ranges[len(ranges)-1].End = r.End
			continue
		}
		ranges = append(ranges, r)
		sources = append(sources, segment.source)
	}
	return ranges
}

func sameRangeMetadata(left, right Range) bool {
	return left.Generation == right.Generation &&
		left.ProposalID == right.ProposalID &&
		slices.Equal(left.Replicas, right.Replicas)
}

func rangesEqual(left, right Range) bool {
	return left.Start == right.Start &&
		left.End == right.End &&
		sameRangeMetadata(left, right)
}

func rangeMapsEqual(left, right RangeMap) bool {
	if len(left.Ranges) != len(right.Ranges) {
		return false
	}
	for i := range left.Ranges {
		if !rangesEqual(left.Ranges[i], right.Ranges[i]) {
			return false
		}
	}
	return true
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
