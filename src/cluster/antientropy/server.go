package antientropy

import (
	"context"

	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Store is the local storage capability the anti-entropy server needs:
// an unpaginated raw range scan (reused for both root hashing and, once
// decoded, leaf streaming - see roothash.go) plus the same verbatim
// Put/Delete used by coordination.Coordinator to apply already-stamped
// envelopes without incrementing any clock.
type Store interface {
	RangeScanner
	Put(key string, envelope versioning.Envelope, sync bool) error
	Delete(key string, envelope versioning.Envelope, sync bool) error
}

// RangeOwnership reports whether the local node currently owns a given
// key or key range, mirroring node.RangeOwnership. It is defined locally
// to keep this package decoupled from node's concrete adapter.
type RangeOwnership interface {
	IsOwner(key string) bool
	OwnsRange(start, end string) bool
}

// Server implements pb.AntiEntropyServiceServer against local storage. It
// is the passive side of a repair round: the Scheduler on the initiating
// node calls GetMerkleRoot/StreamRange/RepairKeys against a peer's Server.
type Server struct {
	pb.UnimplementedAntiEntropyServiceServer

	store     Store
	ownership RangeOwnership
}

func NewServer(store Store, ownership RangeOwnership) *Server {
	return &Server{store: store, ownership: ownership}
}

func (s *Server) notOwnerRangeError(start, end string) error {
	return status.Errorf(codes.FailedPrecondition, "range [%q, %q] is not fully owned by this node", start, end)
}

func (s *Server) GetMerkleRoot(ctx context.Context, req *pb.MerkleRootRequest) (*pb.MerkleRootResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	start, end := req.GetRangeStart(), req.GetRangeEnd()
	if !s.ownership.OwnsRange(start, end) {
		return nil, s.notOwnerRangeError(start, end)
	}

	rootHash, itemCount, err := ComputeMerkleRoot(s.store, start, end)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "compute merkle root: %v", err)
	}
	return &pb.MerkleRootResponse{
		Status:    transport.OKStatus(),
		RootHash:  rootHash,
		ItemCount: itemCount,
	}, nil
}

func (s *Server) StreamRange(req *pb.StreamRangeRequest, stream pb.AntiEntropyService_StreamRangeServer) error {
	start, end := req.GetRangeStart(), req.GetRangeEnd()
	if !s.ownership.OwnsRange(start, end) {
		return s.notOwnerRangeError(start, end)
	}

	entries, err := s.store.ScanRawRange(start, end)
	if err != nil {
		return status.Errorf(codes.Internal, "scan range: %v", err)
	}
	for _, entry := range entries {
		if err := stream.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		envelope, err := versioning.Decode(entry.Value)
		if err != nil {
			return status.Errorf(codes.Internal, "decode %q: %v", entry.Key, err)
		}
		row := transport.KeyEnvelopeToProto(versioning.KeyEnvelope{Key: entry.Key, Envelope: envelope})
		if err := stream.Send(row); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) RepairKeys(ctx context.Context, req *pb.RepairKeysRequest) (*pb.WriteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	for _, row := range req.GetRows() {
		if !s.ownership.IsOwner(row.GetKey()) {
			return nil, status.Errorf(codes.FailedPrecondition, "key %q is not owned by this node", row.GetKey())
		}
	}

	for _, row := range req.GetRows() {
		envelope, err := transport.EnvelopeFromProto(row.GetEnvelope())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid envelope for key %q: %v", row.GetKey(), err)
		}
		var applyErr error
		if envelope.Deleted {
			applyErr = s.store.Delete(row.GetKey(), envelope, true)
		} else {
			applyErr = s.store.Put(row.GetKey(), envelope, true)
		}
		if applyErr != nil {
			return nil, status.Errorf(codes.Internal, "apply repair for key %q: %v", row.GetKey(), applyErr)
		}
	}
	return &pb.WriteResponse{Status: transport.OKStatus()}, nil
}
