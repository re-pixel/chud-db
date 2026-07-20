package coordination

import (
	"context"
	"errors"

	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements pb.CoordinationServiceServer, translating between
// the wire protocol and Coordinator's domain-level calls.
type Server struct {
	pb.UnimplementedCoordinationServiceServer

	coordinator *Coordinator
}

func NewServer(coordinator *Coordinator) *Server {
	return &Server{coordinator: coordinator}
}

func (s *Server) Put(ctx context.Context, req *pb.CoordinatedPutRequest) (*pb.CoordinatedWriteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	clockContext := transport.VectorClockFromProto(req.GetContext())
	result, err := s.coordinator.Put(ctx, req.GetKey(), req.GetValue(), clockContext, int(req.GetWriteQuorum()))
	if err != nil {
		return nil, statusError(err)
	}
	return &pb.CoordinatedWriteResponse{
		Status:      transport.OKStatus(),
		VectorClock: transport.VectorClockToProto(result.VectorClock),
		Acks:        int32(result.Acks),
		Required:    int32(result.Required),
	}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.CoordinatedDeleteRequest) (*pb.CoordinatedWriteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	clockContext := transport.VectorClockFromProto(req.GetContext())
	result, err := s.coordinator.Delete(ctx, req.GetKey(), clockContext, int(req.GetWriteQuorum()))
	if err != nil {
		return nil, statusError(err)
	}
	return &pb.CoordinatedWriteResponse{
		Status:      transport.OKStatus(),
		VectorClock: transport.VectorClockToProto(result.VectorClock),
		Acks:        int32(result.Acks),
		Required:    int32(result.Required),
	}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.CoordinatedGetRequest) (*pb.CoordinatedGetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	result, err := s.coordinator.Get(ctx, req.GetKey(), int(req.GetReadQuorum()))
	if err != nil {
		return nil, statusError(err)
	}
	versions := make([]*pb.Envelope, 0, len(result.Versions))
	for _, version := range result.Versions {
		versions = append(versions, transport.EnvelopeToProto(version))
	}
	return &pb.CoordinatedGetResponse{
		Status:      transport.OKStatus(),
		Versions:    versions,
		VectorClock: transport.VectorClockToProto(result.VectorClock),
	}, nil
}

// statusError classifies InvalidRequestError (empty key, an impossible
// requested quorum) as codes.InvalidArgument; everything else - most
// commonly a not-enough-acks quorum failure - as codes.Unavailable,
// since it's the caller's cue to retry rather than a malformed request.
func statusError(err error) error {
	var invalid *InvalidRequestError
	if errors.As(err, &invalid) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}
