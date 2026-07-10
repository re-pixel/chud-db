package ring

import (
	"context"
	"sync"

	"nosqlEngine/src/cluster/transport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RangeMapClient is what Syncer needs to pull a peer's current range
// map.
type RangeMapClient interface {
	GetRangeMap(ctx context.Context, addr string) (RangeMap, error)
}

// Client is a gRPC-backed RangeMapClient with a lazily-established,
// cached connection per peer address. This cache is deliberately
// separate from membership.Client's - a known, accepted duplication
// (see TECH_DEBT.md) rather than sharing connections across services.
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

func (c *Client) GetRangeMap(ctx context.Context, addr string) (RangeMap, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return RangeMap{}, err
	}
	resp, err := pb.NewRangeMapServiceClient(conn).GetRangeMap(ctx, &pb.GetRangeMapRequest{})
	if err != nil {
		return RangeMap{}, err
	}
	return RangeMapFromProto(resp.GetMap()), nil
}
