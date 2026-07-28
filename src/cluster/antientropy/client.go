package antientropy

import (
	"context"
	"fmt"
	"io"
	"sync"

	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ReplicaClient is the outbound RPC surface the Scheduler needs against
// a specific, already-resolved peer address. Implemented by Client
// against the peer's AntiEntropyService.
type ReplicaClient interface {
	GetMerkleRoot(ctx context.Context, addr, start, end string) ([]byte, uint64, error)
	StreamRange(ctx context.Context, addr, start, end string) ([]versioning.KeyEnvelope, error)
	RepairKeys(ctx context.Context, addr string, rows []versioning.KeyEnvelope) error
}

// Client is a gRPC-backed ReplicaClient calling a peer's
// AntiEntropyService, with a lazily-established, cached connection per
// peer address. This is yet another independent per-peer connection
// cache alongside membership.Client / ring.Client / coordination.Client
// - a known, accepted duplication (see TECH_DEBT.md) rather than
// sharing connections across services.
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewClient() *Client {
	return &Client{conns: make(map[string]*grpc.ClientConn)}
}

// Close closes all cached peer connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for addr, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.conns, addr)
	}
	return firstErr
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

// GetMerkleRoot asks addr for its root hash and item count over
// [start, end) - a single call regardless of how deep the recursive
// tree walk currently is.
func (c *Client) GetMerkleRoot(ctx context.Context, addr, start, end string) ([]byte, uint64, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, 0, err
	}
	resp, err := pb.NewAntiEntropyServiceClient(conn).GetMerkleRoot(ctx, &pb.MerkleRootRequest{
		RangeStart: start,
		RangeEnd:   end,
	})
	if err != nil {
		return nil, 0, err
	}
	return resp.GetRootHash(), resp.GetItemCount(), nil
}

// StreamRange pulls every (key, envelope) row in [start, end) from addr,
// collecting the full stream client-side. A leaf bucket is bounded by
// LeafItemThreshold, so this is expected to stay small.
func (c *Client) StreamRange(ctx context.Context, addr, start, end string) ([]versioning.KeyEnvelope, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	stream, err := pb.NewAntiEntropyServiceClient(conn).StreamRange(ctx, &pb.StreamRangeRequest{
		RangeStart: start,
		RangeEnd:   end,
	})
	if err != nil {
		return nil, err
	}

	var rows []versioning.KeyEnvelope
	for {
		protoRow, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		row, err := transport.KeyEnvelopeFromProto(protoRow)
		if err != nil {
			return nil, fmt.Errorf("decode streamed row: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// RepairKeys pushes already-resolved, causally-dominant rows to addr to
// be applied verbatim (see Server.RepairKeys).
func (c *Client) RepairKeys(ctx context.Context, addr string, rows []versioning.KeyEnvelope) error {
	conn, err := c.conn(addr)
	if err != nil {
		return err
	}
	pbRows := make([]*pb.KeyEnvelope, 0, len(rows))
	for _, row := range rows {
		pbRows = append(pbRows, transport.KeyEnvelopeToProto(row))
	}
	_, err = pb.NewAntiEntropyServiceClient(conn).RepairKeys(ctx, &pb.RepairKeysRequest{Rows: pbRows})
	return err
}
