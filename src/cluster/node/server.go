package node

import (
	"context"

	clusterconfig "nosqlEngine/src/cluster/config"
	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Store interface {
	Put(key string, envelope versioning.Envelope, sync bool) error
	Delete(key string, envelope versioning.Envelope, sync bool) error
	Get(key string) (versioning.Envelope, bool, error)
	ScanRange(start, end string, pageNum, pageSize int) ([]versioning.KeyEnvelope, error)
}

// RangeOwnership reports whether the local node currently owns a given
// key or key range, per the cluster's range map. It is a hard,
// non-forwarding gate: a request for a key/range this node doesn't own
// is rejected outright rather than proxied to the right node (that is
// the caller's - e.g. the coordinator/proxy's - responsibility).
type RangeOwnership interface {
	IsOwner(key string) bool
	OwnsKeyRange(start, end string) bool
	Owners(key string) []string
}

type Server struct {
	pb.UnimplementedNodeServiceServer

	cfg       clusterconfig.Config
	store     Store
	ownership RangeOwnership
	ready     bool
}

func NewServer(cfg clusterconfig.Config, store Store, ownership RangeOwnership) *Server {
	return &Server{cfg: cfg, store: store, ownership: ownership, ready: true}
}

func (s *Server) notOwnerError(key string) error {
	return status.Errorf(codes.FailedPrecondition, "key %q is not owned by this node (current owners: %v)", key, s.ownership.Owners(key))
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.WriteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if !s.ownership.IsOwner(req.GetKey()) {
		return nil, s.notOwnerError(req.GetKey())
	}
	envelope, err := transport.EnvelopeFromProto(req.GetEnvelope())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid envelope: %v", err)
	}
	if err := s.store.Put(req.GetKey(), envelope, req.GetSync()); err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &pb.WriteResponse{Status: transport.OKStatus()}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.WriteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if !s.ownership.IsOwner(req.GetKey()) {
		return nil, s.notOwnerError(req.GetKey())
	}
	envelope, err := transport.EnvelopeFromProto(req.GetEnvelope())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid envelope: %v", err)
	}
	if err := s.store.Delete(req.GetKey(), envelope, req.GetSync()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &pb.WriteResponse{Status: transport.OKStatus()}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if !s.ownership.IsOwner(req.GetKey()) {
		return nil, s.notOwnerError(req.GetKey())
	}
	envelope, found, err := s.store.Get(req.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	resp := &pb.GetResponse{Status: transport.OKStatus(), Found: found}
	if found {
		resp.Envelope = transport.EnvelopeToProto(envelope)
	}
	return resp, nil
}

func (s *Server) RangeScan(ctx context.Context, req *pb.RangeScanRequest) (*pb.RangeScanResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req.GetPageNum() < 1 {
		return nil, status.Error(codes.InvalidArgument, "page_num must be >= 1")
	}
	if req.GetPageSize() < 1 {
		return nil, status.Error(codes.InvalidArgument, "page_size must be >= 1")
	}
	if !s.ownership.OwnsKeyRange(req.GetStart(), req.GetEnd()) {
		return nil, status.Errorf(codes.FailedPrecondition, "range [%q, %q] is not fully owned by this node", req.GetStart(), req.GetEnd())
	}

	rows, err := s.store.ScanRange(req.GetStart(), req.GetEnd(), int(req.GetPageNum()), int(req.GetPageSize()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "range scan: %v", err)
	}
	resp := &pb.RangeScanResponse{
		Status: transport.OKStatus(),
		Rows:   make([]*pb.KeyEnvelope, 0, len(rows)),
	}
	for _, row := range rows {
		resp.Rows = append(resp.Rows, transport.KeyEnvelopeToProto(row))
	}
	return resp, nil
}

func (s *Server) Health(ctx context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.cfg.NodeID == "" {
		return nil, status.Error(codes.FailedPrecondition, "node_id is not configured")
	}
	return &pb.HealthResponse{
		Status: transport.OKStatus(),
		Node: &pb.NodeInfo{
			NodeId:        s.cfg.NodeID,
			ClusterId:     s.cfg.ClusterID,
			AdvertiseAddr: s.cfg.AdvertiseAddr,
		},
		Ready: s.ready,
	}, nil
}
