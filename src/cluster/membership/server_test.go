package membership

import (
	"context"
	"errors"
	"testing"
	"time"

	"nosqlEngine/src/cluster/transport/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestServer(client PingClient) (*Server, *Table) {
	table := NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
	return NewServer(table, client, time.Second), table
}

func TestServerPingMergesSenderAndReturnsLocal(t *testing.T) {
	server, table := newTestServer(nil)

	resp, err := server.Ping(context.Background(), &pb.PingRequest{
		Sender: MemberToProto(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 2}),
	})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp.GetResponder().GetNode().GetNodeId() != "local" {
		t.Fatalf("responder = %#v", resp.GetResponder())
	}
	if _, ok := table.Get("peer-1"); !ok {
		t.Fatalf("expected sender to be merged into table")
	}
}

func TestServerPingRejectsMismatchedTarget(t *testing.T) {
	server, _ := newTestServer(nil)

	_, err := server.Ping(context.Background(), &pb.PingRequest{TargetNodeId: "someone-else"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerPingAllowsEmptyOrMatchingTarget(t *testing.T) {
	server, _ := newTestServer(nil)

	if _, err := server.Ping(context.Background(), &pb.PingRequest{}); err != nil {
		t.Fatalf("empty target: %v", err)
	}
	if _, err := server.Ping(context.Background(), &pb.PingRequest{TargetNodeId: "local"}); err != nil {
		t.Fatalf("matching target: %v", err)
	}
}

func TestServerGossipMergesAndReturnsSnapshot(t *testing.T) {
	server, table := newTestServer(nil)
	table.Merge(Member{NodeID: "peer-existing", Status: StatusAlive})

	resp, err := server.Gossip(context.Background(), &pb.GossipRequest{
		Sender: MemberToProto(Member{NodeID: "peer-sender", Status: StatusAlive}),
		Membership: MembersToProto([]Member{
			{NodeID: "peer-gossiped", Status: StatusSuspect, Incarnation: 1},
		}),
	})
	if err != nil {
		t.Fatalf("Gossip: %v", err)
	}

	if _, ok := table.Get("peer-sender"); !ok {
		t.Fatalf("expected sender merged")
	}
	if _, ok := table.Get("peer-gossiped"); !ok {
		t.Fatalf("expected gossiped entry merged")
	}

	if len(resp.GetMembership()) != 4 { // local + peer-existing + peer-sender + peer-gossiped
		t.Fatalf("membership snapshot = %#v", resp.GetMembership())
	}
}

func TestServerIndirectPingSuccess(t *testing.T) {
	client := &fakePingClient{
		result: Member{NodeID: "target", Status: StatusAlive, Incarnation: 3},
	}
	server, table := newTestServer(client)

	resp, err := server.IndirectPing(context.Background(), &pb.IndirectPingRequest{
		Sender: MemberToProto(Member{NodeID: "requester", Status: StatusAlive}),
		Target: &pb.NodeInfo{NodeId: "target", AdvertiseAddr: "10.0.0.2:7000"},
	})
	if err != nil {
		t.Fatalf("IndirectPing: %v", err)
	}
	if !resp.GetTargetAcknowledged() {
		t.Fatalf("expected target acknowledged")
	}
	if resp.GetResponder().GetNode().GetNodeId() != "target" {
		t.Fatalf("responder = %#v", resp.GetResponder())
	}
	if client.calledAddr != "10.0.0.2:7000" || client.calledTargetID != "target" {
		t.Fatalf("client call = addr:%q target:%q", client.calledAddr, client.calledTargetID)
	}
	if _, ok := table.Get("target"); !ok {
		t.Fatalf("expected successful responder to be merged")
	}
	if _, ok := table.Get("requester"); !ok {
		t.Fatalf("expected sender to be merged")
	}
}

func TestServerIndirectPingFailureIsNotAnRPCError(t *testing.T) {
	client := &fakePingClient{err: errors.New("dial timeout")}
	server, table := newTestServer(client)

	resp, err := server.IndirectPing(context.Background(), &pb.IndirectPingRequest{
		Target: &pb.NodeInfo{NodeId: "target", AdvertiseAddr: "10.0.0.2:7000"},
	})
	if err != nil {
		t.Fatalf("IndirectPing should not return an error on unreachable target: %v", err)
	}
	if resp.GetTargetAcknowledged() {
		t.Fatalf("expected target not acknowledged")
	}
	if resp.GetResponder() != nil {
		t.Fatalf("expected no responder on failure, got %#v", resp.GetResponder())
	}
	if _, ok := table.Get("target"); ok {
		t.Fatalf("unreachable target should not be merged into table")
	}
}

func TestServerIndirectPingRejectsMissingTarget(t *testing.T) {
	server, _ := newTestServer(&fakePingClient{})

	_, err := server.IndirectPing(context.Background(), &pb.IndirectPingRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerIndirectPingRequiresConfiguredClient(t *testing.T) {
	server, _ := newTestServer(nil)

	_, err := server.IndirectPing(context.Background(), &pb.IndirectPingRequest{
		Target: &pb.NodeInfo{NodeId: "target", AdvertiseAddr: "10.0.0.2:7000"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerContextCancellation(t *testing.T) {
	server, _ := newTestServer(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := server.Ping(ctx, &pb.PingRequest{})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

type fakePingClient struct {
	result         Member
	err            error
	calledAddr     string
	calledTargetID string
}

func (c *fakePingClient) Ping(_ context.Context, addr, targetNodeID string, _ Member) (Member, error) {
	c.calledAddr = addr
	c.calledTargetID = targetNodeID
	return c.result, c.err
}
