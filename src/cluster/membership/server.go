package membership

import (
	"context"
	"time"

	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PingClient sends a direct Ping RPC to a peer on behalf of the local
// node. It is satisfied by the gRPC client wrapper added alongside the
// gossiper loop.
type PingClient interface {
	Ping(ctx context.Context, addr, targetNodeID string, sender Member) (Member, error)
}

// Server implements pb.GossipServiceServer against a local Table.
type Server struct {
	pb.UnimplementedGossipServiceServer

	table       *Table
	client      PingClient
	pingTimeout time.Duration
}

func NewServer(table *Table, client PingClient, pingTimeout time.Duration) *Server {
	return &Server{table: table, client: client, pingTimeout: pingTimeout}
}

// Ping handles a direct liveness check from a peer, merging its
// self-reported state and replying with this node's current state.
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if targetID := req.GetTargetNodeId(); targetID != "" && targetID != s.table.LocalID() {
		return nil, status.Errorf(codes.NotFound, "target node %q does not match local node %q", targetID, s.table.LocalID())
	}

	sender := MemberFromProto(req.GetSender())
	if sender.NodeID != "" {
		s.table.Merge(sender)
	}

	return &pb.PingResponse{
		Status:    transport.OKStatus(),
		Responder: MemberToProto(s.table.Local()),
	}, nil
}

// Gossip merges the sender's own state and its membership sample into
// the local table, then replies with a full snapshot of local knowledge.
func (s *Server) Gossip(ctx context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	sender := MemberFromProto(req.GetSender())
	if sender.NodeID != "" {
		s.table.Merge(sender)
	}
	for _, ms := range req.GetMembership() {
		s.table.Merge(MemberFromProto(ms))
	}

	return &pb.GossipResponse{
		Status:     transport.OKStatus(),
		Membership: MembersToProto(s.table.Snapshot()),
	}, nil
}

// IndirectPing pings target on behalf of the requester when the
// requester suspects it cannot reach target directly. A failure to
// reach target is reported as TargetAcknowledged=false, not as an RPC
// error, since it is an expected outcome of the failure detector.
func (s *Server) IndirectPing(ctx context.Context, req *pb.IndirectPingRequest) (*pb.IndirectPingResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	sender := MemberFromProto(req.GetSender())
	if sender.NodeID != "" {
		s.table.Merge(sender)
	}

	targetNodeID, _, targetAddr := NodeIdentityFromProto(req.GetTarget())
	if targetNodeID == "" || targetAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "target must include node_id and advertise_addr")
	}
	if s.client == nil {
		return nil, status.Error(codes.FailedPrecondition, "indirect ping client is not configured")
	}

	pingCtx, cancel := context.WithTimeout(ctx, s.pingTimeout)
	defer cancel()

	responder, err := s.client.Ping(pingCtx, targetAddr, targetNodeID, s.table.Local())
	if err != nil {
		return &pb.IndirectPingResponse{
			Status:             transport.OKStatus(),
			TargetAcknowledged: false,
		}, nil
	}

	s.table.Merge(responder)
	return &pb.IndirectPingResponse{
		Status:             transport.OKStatus(),
		TargetAcknowledged: true,
		Responder:          MemberToProto(responder),
	}, nil
}
