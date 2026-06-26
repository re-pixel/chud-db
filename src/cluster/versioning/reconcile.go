package versioning

import (
	"sort"
	"strconv"
)

type ReconcileResult struct {
	Winner   *Envelope
	Siblings []Envelope
}

func Reconcile(values []Envelope) ReconcileResult {
	if len(values) == 0 {
		return ReconcileResult{}
	}

	nonDominated := make([]Envelope, 0, len(values))
	for i, candidate := range values {
		dominated := false
		for j, other := range values {
			if i == j {
				continue
			}
			if Compare(candidate.VectorClock, other.VectorClock) == Before {
				dominated = true
				break
			}
		}
		if !dominated {
			nonDominated = append(nonDominated, cloneEnvelope(candidate))
		}
	}

	if len(nonDominated) == 1 {
		winner := nonDominated[0]
		return ReconcileResult{Winner: &winner}
	}
	return ReconcileResult{Siblings: nonDominated}
}

func PickByTimestamp(values []Envelope) Envelope {
	if len(values) == 0 {
		return Envelope{}
	}
	picked := cloneEnvelope(values[0])
	for _, value := range values[1:] {
		if value.UpdatedAtUnixNano > picked.UpdatedAtUnixNano {
			picked = cloneEnvelope(value)
			continue
		}
		if value.UpdatedAtUnixNano == picked.UpdatedAtUnixNano && stableEnvelopeLess(picked, value) {
			picked = cloneEnvelope(value)
		}
	}
	return picked
}

func cloneEnvelope(e Envelope) Envelope {
	e.VectorClock = e.VectorClock.Clone()
	return e
}

func stableEnvelopeLess(a, b Envelope) bool {
	if a.Deleted != b.Deleted {
		return !a.Deleted && b.Deleted
	}
	if a.Value != b.Value {
		return a.Value < b.Value
	}
	return stableClockString(a.VectorClock) < stableClockString(b.VectorClock)
}

func stableClockString(clock VectorClock) string {
	keys := make([]string, 0, len(clock))
	for nodeID := range clock {
		keys = append(keys, nodeID)
	}
	sort.Strings(keys)

	out := ""
	for _, nodeID := range keys {
		out += nodeID + "\x00" + strconv.FormatUint(clock[nodeID], 10) + "\x00"
	}
	return out
}
