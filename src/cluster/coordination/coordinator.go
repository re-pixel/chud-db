package coordination

import (
	"context"
	"fmt"
	"time"

	"nosqlEngine/src/cluster/versioning"
)

// OwnerLookup resolves the replica set responsible for a key. Satisfied
// as-is by *ring.Table.
type OwnerLookup interface {
	Owners(key string) []string
}

// AddressResolver resolves a node ID to the network address the
// coordinator should dial to reach it. A cmd/node adapter wraps
// *membership.Table to satisfy this without replication importing
// membership.
type AddressResolver interface {
	Address(nodeID string) (string, bool)
}

// ReplicaClient is the outbound RPC surface the coordinator needs to
// apply a write or perform a read against a specific, already-resolved
// peer address. Implemented by Client against the peer's NodeService.
type ReplicaClient interface {
	Put(ctx context.Context, addr, key string, envelope versioning.Envelope, sync bool) error
	Delete(ctx context.Context, addr, key string, envelope versioning.Envelope, sync bool) error
	Get(ctx context.Context, addr, key string) (versioning.Envelope, bool, error)
}

// LocalStore is the local, in-process apply path used when the
// coordinating node itself is one of the key's owners (no gRPC
// loopback). Satisfied as-is by *node.NodeStore (structural typing).
type LocalStore interface {
	Put(key string, envelope versioning.Envelope, sync bool) error
	Delete(key string, envelope versioning.Envelope, sync bool) error
	Get(key string) (versioning.Envelope, bool, error)
}

// Config carries the quorum defaults and per-replica RPC timeout the
// coordinator uses when a request doesn't specify its own quorum.
type Config struct {
	ReplicationTimeout time.Duration
	ReadQuorum         int
	WriteQuorum        int
}

// InvalidRequestError marks a failure as the caller's mistake (empty
// key, a requested quorum the range map can't possibly satisfy) rather
// than a transient/retryable failure to reach enough replicas. Server
// uses this to pick codes.InvalidArgument vs codes.Unavailable.
type InvalidRequestError struct {
	msg string
}

func (e *InvalidRequestError) Error() string { return e.msg }

func invalidRequestf(format string, args ...any) error {
	return &InvalidRequestError{msg: fmt.Sprintf(format, args...)}
}

// WriteResult reports the outcome of a coordinated Put/Delete.
type WriteResult struct {
	VectorClock versioning.VectorClock
	Acks        int
	Required    int
}

// ReadResult reports the outcome of a coordinated Get. VectorClock is
// populated even when Found is false (deleted key), so the client can
// hand it back as the causality context of a subsequent write. See
// Coordinator.Get for the Deleted->Found=false translation rationale.
type ReadResult struct {
	Found       bool
	Value       string
	VectorClock versioning.VectorClock
	HadConflict bool
}

// Coordinator implements the leaderless quorum write/read path: any
// node can run one for any key, since it resolves the actual owners of
// the key from OwnerLookup rather than requiring the coordinator itself
// to own it.
type Coordinator struct {
	nodeID    string
	owners    OwnerLookup
	addresses AddressResolver
	replicas  ReplicaClient
	local     LocalStore
	cfg       Config
}

func NewCoordinator(nodeID string, owners OwnerLookup, addresses AddressResolver, replicas ReplicaClient, local LocalStore, cfg Config) *Coordinator {
	return &Coordinator{
		nodeID:    nodeID,
		owners:    owners,
		addresses: addresses,
		replicas:  replicas,
		local:     local,
		cfg:       cfg,
	}
}

// Put increments context (the vector clock the client last observed,
// or empty for a blind/new-key write) with this coordinator's node ID,
// then fans the resulting envelope out to every owner of key, always
// with sync=true (no async-ack quorum counting - see TECH_DEBT.md).
func (c *Coordinator) Put(ctx context.Context, key, value string, clockContext versioning.VectorClock, requestedQuorum int) (WriteResult, error) {
	if key == "" {
		return WriteResult{}, invalidRequestf("key must not be empty")
	}
	owners, quorum, err := c.resolveWriteQuorum(key, requestedQuorum)
	if err != nil {
		return WriteResult{}, err
	}

	newClock := clockContext.Increment(c.nodeID)
	envelope := versioning.NewPut(newClock, value, time.Now())

	acks := c.fanOutWrite(ctx, owners, func(ctx context.Context, ownerID string) error {
		return c.writeToOwner(ctx, ownerID, key, envelope)
	})
	return c.writeResult(newClock, acks, quorum, key, "put")
}

// Delete is Put's mirror image, writing a tombstone envelope instead of
// a value. It never touches the underlying engine's own tombstone/GC
// mechanism (see TECH_DEBT.md).
func (c *Coordinator) Delete(ctx context.Context, key string, clockContext versioning.VectorClock, requestedQuorum int) (WriteResult, error) {
	if key == "" {
		return WriteResult{}, invalidRequestf("key must not be empty")
	}
	owners, quorum, err := c.resolveWriteQuorum(key, requestedQuorum)
	if err != nil {
		return WriteResult{}, err
	}

	newClock := clockContext.Increment(c.nodeID)
	envelope := versioning.NewDelete(newClock, time.Now())

	acks := c.fanOutWrite(ctx, owners, func(ctx context.Context, ownerID string) error {
		return c.writeToOwner(ctx, ownerID, key, envelope)
	})
	return c.writeResult(newClock, acks, quorum, key, "delete")
}

// Get fans a read out to every owner of key, reconciles the returned
// versions via versioning.Reconcile, and falls back to
// versioning.PickByTimestamp (setting HadConflict) when genuine
// concurrent siblings are found. Full sibling exposure to the client is
// deferred to Phase 5; this phase only surfaces that a conflict
// happened and which version won.
func (c *Coordinator) Get(ctx context.Context, key string, requestedQuorum int) (ReadResult, error) {
	if key == "" {
		return ReadResult{}, invalidRequestf("key must not be empty")
	}
	owners := c.owners.Owners(key)
	if len(owners) == 0 {
		return ReadResult{}, fmt.Errorf("no owners found for key %q", key)
	}
	quorum := requestedQuorum
	if quorum <= 0 {
		quorum = c.cfg.ReadQuorum
	}
	if quorum > len(owners) {
		return ReadResult{}, invalidRequestf("requested read quorum %d exceeds owner count %d for key %q", quorum, len(owners), key)
	}

	outcomes := c.fanOutRead(ctx, owners, key)

	acks := 0
	found := make([]versioning.Envelope, 0, len(outcomes))
	for _, outcome := range outcomes {
		if outcome.err != nil {
			continue
		}
		acks++
		if outcome.found {
			found = append(found, outcome.envelope)
		}
	}
	if acks < quorum {
		return ReadResult{}, fmt.Errorf("read quorum not met for key %q: got %d/%d acks", key, acks, quorum)
	}
	if len(found) == 0 {
		return ReadResult{}, nil
	}

	reconciled := versioning.Reconcile(found)
	final := versioning.Envelope{}
	hadConflict := false
	if reconciled.Winner != nil {
		final = *reconciled.Winner
	} else {
		final = versioning.PickByTimestamp(reconciled.Siblings)
		hadConflict = true
	}
	return ReadResult{
		Found:       !final.Deleted,
		Value:       final.Value,
		VectorClock: final.VectorClock,
		HadConflict: hadConflict,
	}, nil
}

func (c *Coordinator) resolveWriteQuorum(key string, requestedQuorum int) ([]string, int, error) {
	owners := c.owners.Owners(key)
	if len(owners) == 0 {
		return nil, 0, fmt.Errorf("no owners found for key %q", key)
	}
	quorum := requestedQuorum
	if quorum <= 0 {
		quorum = c.cfg.WriteQuorum
	}
	if quorum > len(owners) {
		return nil, 0, invalidRequestf("requested write quorum %d exceeds owner count %d for key %q", quorum, len(owners), key)
	}
	return owners, quorum, nil
}

func (c *Coordinator) writeResult(clock versioning.VectorClock, acks, quorum int, key, op string) (WriteResult, error) {
	result := WriteResult{VectorClock: clock, Acks: acks, Required: quorum}
	if acks < quorum {
		return result, fmt.Errorf("%s quorum not met for key %q: got %d/%d acks", op, key, acks, quorum)
	}
	return result, nil
}

func (c *Coordinator) writeToOwner(ctx context.Context, ownerID, key string, envelope versioning.Envelope) error {
	if ownerID == c.nodeID {
		if envelope.Deleted {
			return c.local.Delete(key, envelope, true)
		}
		return c.local.Put(key, envelope, true)
	}
	addr, ok := c.addresses.Address(ownerID)
	if !ok {
		return fmt.Errorf("no address known for node %q", ownerID)
	}
	if envelope.Deleted {
		return c.replicas.Delete(ctx, addr, key, envelope, true)
	}
	return c.replicas.Put(ctx, addr, key, envelope, true)
}

func (c *Coordinator) readFromOwner(ctx context.Context, ownerID, key string) (versioning.Envelope, bool, error) {
	if ownerID == c.nodeID {
		return c.local.Get(key)
	}
	addr, ok := c.addresses.Address(ownerID)
	if !ok {
		return versioning.Envelope{}, false, fmt.Errorf("no address known for node %q", ownerID)
	}
	return c.replicas.Get(ctx, addr, key)
}

// fanOutWrite launches one goroutine per owner and blocks until all
// have replied (or the shared ReplicationTimeout expires), mirroring
// Gossiper.indirectPing's fan-out-and-collect shape. It reports how
// many owners acked without error; a failed ack (network error or an
// ownership-rejection from a replica whose range map has drifted) is
// not retried or treated specially - a plain miss.
func (c *Coordinator) fanOutWrite(ctx context.Context, owners []string, apply func(ctx context.Context, ownerID string) error) int {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicationTimeout)
	defer cancel()

	results := make(chan error, len(owners))
	for _, ownerID := range owners {
		ownerID := ownerID
		go func() {
			results <- apply(ctx, ownerID)
		}()
	}

	acks := 0
	for range owners {
		if err := <-results; err == nil {
			acks++
		}
	}
	return acks
}

type readOutcome struct {
	envelope versioning.Envelope
	found    bool
	err      error
}

func (c *Coordinator) fanOutRead(ctx context.Context, owners []string, key string) []readOutcome {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicationTimeout)
	defer cancel()

	results := make(chan readOutcome, len(owners))
	for _, ownerID := range owners {
		ownerID := ownerID
		go func() {
			envelope, found, err := c.readFromOwner(ctx, ownerID, key)
			results <- readOutcome{envelope: envelope, found: found, err: err}
		}()
	}

	outcomes := make([]readOutcome, 0, len(owners))
	for range owners {
		outcomes = append(outcomes, <-results)
	}
	return outcomes
}
