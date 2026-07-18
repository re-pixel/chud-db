package coordination

import (
	"context"
	"sync"

	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a gRPC-backed ReplicaClient calling a peer's NodeService,
// with a lazily-established, cached connection per peer address. This
// is a third independent per-peer connection cache alongside
// membership.Client and ring.Client - a known, accepted duplication
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

func (c *Client) Put(ctx context.Context, addr, key string, envelope versioning.Envelope, sync bool) error {
	conn, err := c.conn(addr)
	if err != nil {
		return err
	}
	_, err = pb.NewNodeServiceClient(conn).Put(ctx, &pb.PutRequest{
		Key:      key,
		Envelope: transport.EnvelopeToProto(envelope),
		Sync:     sync,
	})
	return err
}

func (c *Client) Delete(ctx context.Context, addr, key string, envelope versioning.Envelope, sync bool) error {
	conn, err := c.conn(addr)
	if err != nil {
		return err
	}
	_, err = pb.NewNodeServiceClient(conn).Delete(ctx, &pb.DeleteRequest{
		Key:      key,
		Envelope: transport.EnvelopeToProto(envelope),
		Sync:     sync,
	})
	return err
}

func (c *Client) Get(ctx context.Context, addr, key string) (versioning.Envelope, bool, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return versioning.Envelope{}, false, err
	}
	resp, err := pb.NewNodeServiceClient(conn).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return versioning.Envelope{}, false, err
	}
	if !resp.GetFound() {
		return versioning.Envelope{}, false, nil
	}
	envelope, err := transport.EnvelopeFromProto(resp.GetEnvelope())
	if err != nil {
		return versioning.Envelope{}, false, err
	}
	return envelope, true, nil
}
