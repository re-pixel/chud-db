package membership

import "time"

// Status is a node's liveness state as tracked by the local SWIM
// membership table.
type Status int

const (
	StatusUnknown Status = iota
	StatusAlive
	StatusSuspect
	StatusDead
)

func (s Status) String() string {
	switch s {
	case StatusAlive:
		return "alive"
	case StatusSuspect:
		return "suspect"
	case StatusDead:
		return "dead"
	default:
		return "unknown"
	}
}

// rank orders statuses for SWIM merge precedence at equal incarnation:
// dead beats suspect beats alive.
func (s Status) rank() int {
	switch s {
	case StatusDead:
		return 2
	case StatusSuspect:
		return 1
	case StatusAlive:
		return 0
	default:
		return -1
	}
}

// Member is the local view of one cluster node's membership state.
type Member struct {
	NodeID          string
	ClusterID       string
	AdvertiseAddr   string
	Status          Status
	Incarnation     uint64
	LastSeen        time.Time
	MembershipEpoch uint64
	RangeMapEpoch   uint64
}

// supersedes reports whether incoming should replace current under SWIM
// precedence: higher incarnation always wins; at equal incarnation,
// higher status rank (dead > suspect > alive) wins.
func supersedes(incoming, current Member) bool {
	if incoming.Incarnation != current.Incarnation {
		return incoming.Incarnation > current.Incarnation
	}
	return incoming.Status.rank() > current.Status.rank()
}
