package membership

import (
	"context"
	"sync"

	"nosqlEngine/src/cluster/transport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GossipClient is what the gossiper loop needs to talk to peers: direct
// ping, full membership exchange, and asking a peer to ping a third
// node on the caller's behalf.
type GossipClient interface {
	PingClient
	Gossip(ctx context.Context, addr string, sender Member, membership []Member) ([]Member, error)
	IndirectPing(ctx context.Context, addr string, sender Member, target Member) (bool, Member, error)
}

// Client is a gRPC-backed GossipClient with a lazily-established,
// cached connection per peer address.
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

func (c *Client) Ping(ctx context.Context, addr, targetNodeID string, sender Member) (Member, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return Member{}, err
	}
	resp, err := pb.NewGossipServiceClient(conn).Ping(ctx, &pb.PingRequest{
		Sender:       MemberToProto(sender),
		TargetNodeId: targetNodeID,
	})
	if err != nil {
		return Member{}, err
	}
	return MemberFromProto(resp.GetResponder()), nil
}

func (c *Client) Gossip(ctx context.Context, addr string, sender Member, membership []Member) ([]Member, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	resp, err := pb.NewGossipServiceClient(conn).Gossip(ctx, &pb.GossipRequest{
		Sender:     MemberToProto(sender),
		Membership: MembersToProto(membership),
	})
	if err != nil {
		return nil, err
	}
	return MembersFromProto(resp.GetMembership()), nil
}

func (c *Client) IndirectPing(ctx context.Context, addr string, sender Member, target Member) (bool, Member, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return false, Member{}, err
	}
	resp, err := pb.NewGossipServiceClient(conn).IndirectPing(ctx, &pb.IndirectPingRequest{
		Sender: MemberToProto(sender),
		Target: NodeInfoToProto(target),
	})
	if err != nil {
		return false, Member{}, err
	}
	return resp.GetTargetAcknowledged(), MemberFromProto(resp.GetResponder()), nil
}
