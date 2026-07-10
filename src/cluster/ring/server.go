package ring

import (
	"context"

	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"

	"google.golang.org/grpc/status"
)

// Server implements pb.RangeMapServiceServer against a local Table.
type Server struct {
	pb.UnimplementedRangeMapServiceServer

	table *Table
}

func NewServer(table *Table) *Server {
	return &Server{table: table}
}

// GetRangeMap returns the local node's current view of the range map, so
// a peer that notices this node advertises a newer RangeMapEpoch can
// pull the full map on demand.
func (s *Server) GetRangeMap(ctx context.Context, _ *pb.GetRangeMapRequest) (*pb.GetRangeMapResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	return &pb.GetRangeMapResponse{
		Status: transport.OKStatus(),
		Map:    RangeMapToProto(s.table.Snapshot()),
	}, nil
}
