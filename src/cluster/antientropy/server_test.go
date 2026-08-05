package antientropy

import (
	"context"
	"errors"
	"testing"
	"time"

	"nosqlEngine/src/cluster/ring"
	"nosqlEngine/src/cluster/transport"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/cluster/versioning"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestServerGetMerkleRootHappyPath(t *testing.T) {
	store := newFakeStore()
	store.scanEntries = []RawEntry{
		{Key: "a", Value: mustEncode(t, versioning.NewPut(versioning.VectorClock{"node-a": 1}, "va", time.Unix(0, 1)))},
		{Key: "b", Value: mustEncode(t, versioning.NewPut(versioning.VectorClock{"node-a": 1}, "vb", time.Unix(0, 2)))},
	}
	server := NewServer(store, newFakeOwnership())

	resp, err := server.GetMerkleRoot(context.Background(), &pb.MerkleRootRequest{RangeStart: "a", RangeEnd: "z"})
	if err != nil {
		t.Fatalf("GetMerkleRoot: %v", err)
	}
	if !resp.GetStatus().GetOk() {
		t.Fatalf("status = %#v", resp.GetStatus())
	}
	if resp.GetItemCount() != 2 {
		t.Fatalf("item count = %d, want 2", resp.GetItemCount())
	}
	if len(resp.GetRootHash()) == 0 {
		t.Fatalf("expected non-empty root hash")
	}
	if store.scanCall.start != "a" || store.scanCall.end != "z" {
		t.Fatalf("scan call = %#v", store.scanCall)
	}
}

func TestServerGetMerkleRootAcceptsExactBoundedOwnedRange(t *testing.T) {
	table, err := ring.NewTable("node-1", ring.RangeMap{
		Ranges: []ring.Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}, Generation: 1, ProposalID: "test"},
			{Start: "m", End: "", Replicas: []string{"node-2"}, Generation: 1, ProposalID: "test"},
		},
	})
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	server := NewServer(newFakeStore(), table)

	if _, err := server.GetMerkleRoot(context.Background(), &pb.MerkleRootRequest{RangeStart: "", RangeEnd: "m"}); err != nil {
		t.Fatalf("GetMerkleRoot: %v", err)
	}
}

func TestServerGetMerkleRootEmptyRange(t *testing.T) {
	server := NewServer(newFakeStore(), newFakeOwnership())

	resp, err := server.GetMerkleRoot(context.Background(), &pb.MerkleRootRequest{RangeStart: "a", RangeEnd: "z"})
	if err != nil {
		t.Fatalf("GetMerkleRoot: %v", err)
	}
	if resp.GetItemCount() != 0 {
		t.Fatalf("item count = %d, want 0", resp.GetItemCount())
	}
}

func TestServerGetMerkleRootRejectsNonOwnedRange(t *testing.T) {
	store := newFakeStore()
	ownership := newFakeOwnership()
	ownership.ownsRange = false
	server := NewServer(store, ownership)

	_, err := server.GetMerkleRoot(context.Background(), &pb.MerkleRootRequest{RangeStart: "a", RangeEnd: "z"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
	if store.scanCall.start != "" {
		t.Fatalf("expected store.ScanRawRange not to be called for a non-owned range")
	}
}

func TestServerGetMerkleRootPropagatesScanError(t *testing.T) {
	store := newFakeStore()
	store.scanErr = errors.New("scan failed")
	server := NewServer(store, newFakeOwnership())

	_, err := server.GetMerkleRoot(context.Background(), &pb.MerkleRootRequest{RangeStart: "a", RangeEnd: "z"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerGetMerkleRootContextCancellation(t *testing.T) {
	server := NewServer(newFakeStore(), newFakeOwnership())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := server.GetMerkleRoot(ctx, &pb.MerkleRootRequest{RangeStart: "a", RangeEnd: "z"})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerStreamRangeHappyPath(t *testing.T) {
	store := newFakeStore()
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "va", time.Unix(0, 1))
	store.scanEntries = []RawEntry{{Key: "a", Value: mustEncode(t, env)}}
	server := NewServer(store, newFakeOwnership())
	stream := newFakeStream()

	if err := server.StreamRange(&pb.StreamRangeRequest{RangeStart: "a", RangeEnd: "z"}, stream); err != nil {
		t.Fatalf("StreamRange: %v", err)
	}
	if len(stream.rows) != 1 {
		t.Fatalf("streamed rows = %#v", stream.rows)
	}
	row, err := transport.KeyEnvelopeFromProto(stream.rows[0])
	if err != nil {
		t.Fatalf("decode streamed row: %v", err)
	}
	if row.Key != "a" || row.Envelope.Value != "va" {
		t.Fatalf("streamed row = %#v", row)
	}
}

func TestServerStreamRangeEmptyRangeSendsNothing(t *testing.T) {
	server := NewServer(newFakeStore(), newFakeOwnership())
	stream := newFakeStream()

	if err := server.StreamRange(&pb.StreamRangeRequest{RangeStart: "a", RangeEnd: "z"}, stream); err != nil {
		t.Fatalf("StreamRange: %v", err)
	}
	if len(stream.rows) != 0 {
		t.Fatalf("expected no rows streamed, got %#v", stream.rows)
	}
}

func TestServerStreamRangeRejectsNonOwnedRange(t *testing.T) {
	store := newFakeStore()
	ownership := newFakeOwnership()
	ownership.ownsRange = false
	server := NewServer(store, ownership)
	stream := newFakeStream()

	err := server.StreamRange(&pb.StreamRangeRequest{RangeStart: "a", RangeEnd: "z"}, stream)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
	if len(stream.rows) != 0 {
		t.Fatalf("expected no rows streamed for non-owned range")
	}
}

func TestServerStreamRangePropagatesScanError(t *testing.T) {
	store := newFakeStore()
	store.scanErr = errors.New("scan failed")
	server := NewServer(store, newFakeOwnership())
	stream := newFakeStream()

	err := server.StreamRange(&pb.StreamRangeRequest{RangeStart: "a", RangeEnd: "z"}, stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerStreamRangePropagatesDecodeError(t *testing.T) {
	store := newFakeStore()
	store.scanEntries = []RawEntry{{Key: "a", Value: "not-a-valid-envelope"}}
	server := NewServer(store, newFakeOwnership())
	stream := newFakeStream()

	err := server.StreamRange(&pb.StreamRangeRequest{RangeStart: "a", RangeEnd: "z"}, stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerRepairKeysAppliesPutsAndDeletes(t *testing.T) {
	store := newFakeStore()
	server := NewServer(store, newFakeOwnership())
	putEnv := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))
	deleteEnv := versioning.NewDelete(versioning.VectorClock{"node-a": 2}, time.Unix(0, 2))

	resp, err := server.RepairKeys(context.Background(), &pb.RepairKeysRequest{Rows: []*pb.KeyEnvelope{
		transport.KeyEnvelopeToProto(versioning.KeyEnvelope{Key: "put-key", Envelope: putEnv}),
		transport.KeyEnvelopeToProto(versioning.KeyEnvelope{Key: "delete-key", Envelope: deleteEnv}),
	}})
	if err != nil {
		t.Fatalf("RepairKeys: %v", err)
	}
	if !resp.GetStatus().GetOk() {
		t.Fatalf("status = %#v", resp.GetStatus())
	}
	if len(store.puts) != 1 || store.puts[0].key != "put-key" || !store.puts[0].sync {
		t.Fatalf("puts = %#v", store.puts)
	}
	if len(store.deletes) != 1 || store.deletes[0].key != "delete-key" || !store.deletes[0].sync {
		t.Fatalf("deletes = %#v", store.deletes)
	}
}

func TestServerRepairKeysRejectsNonOwnedKey(t *testing.T) {
	store := newFakeStore()
	ownership := newFakeOwnership()
	ownership.ownsKey = false
	server := NewServer(store, ownership)
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))

	_, err := server.RepairKeys(context.Background(), &pb.RepairKeysRequest{Rows: []*pb.KeyEnvelope{
		transport.KeyEnvelopeToProto(versioning.KeyEnvelope{Key: "key", Envelope: env}),
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
	if len(store.puts) != 0 {
		t.Fatalf("expected no puts applied for a non-owned key")
	}
}

func TestServerRepairKeysRejectsInvalidEnvelope(t *testing.T) {
	store := newFakeStore()
	server := NewServer(store, newFakeOwnership())

	_, err := server.RepairKeys(context.Background(), &pb.RepairKeysRequest{Rows: []*pb.KeyEnvelope{
		{Key: "key", Envelope: nil},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerRepairKeysPropagatesStoreError(t *testing.T) {
	store := newFakeStore()
	store.putErr = errors.New("put failed")
	server := NewServer(store, newFakeOwnership())
	env := versioning.NewPut(versioning.VectorClock{"node-a": 1}, "value", time.Unix(0, 1))

	_, err := server.RepairKeys(context.Background(), &pb.RepairKeysRequest{Rows: []*pb.KeyEnvelope{
		transport.KeyEnvelopeToProto(versioning.KeyEnvelope{Key: "key", Envelope: env}),
	}})
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func TestServerRepairKeysContextCancellation(t *testing.T) {
	server := NewServer(newFakeStore(), newFakeOwnership())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := server.RepairKeys(ctx, &pb.RepairKeysRequest{})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}

func mustEncode(t *testing.T, envelope versioning.Envelope) string {
	t.Helper()
	raw, err := versioning.Encode(envelope)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	return raw
}

type storeCall struct {
	key      string
	envelope versioning.Envelope
	sync     bool
}

type fakeStore struct {
	scanEntries []RawEntry
	scanErr     error
	scanCall    struct{ start, end string }

	puts      []storeCall
	deletes   []storeCall
	putErr    error
	deleteErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{}
}

func (s *fakeStore) ScanRawRange(start, end string) ([]RawEntry, error) {
	s.scanCall.start = start
	s.scanCall.end = end
	if s.scanErr != nil {
		return nil, s.scanErr
	}
	return s.scanEntries, nil
}

func (s *fakeStore) Put(key string, envelope versioning.Envelope, sync bool) error {
	s.puts = append(s.puts, storeCall{key: key, envelope: envelope, sync: sync})
	return s.putErr
}

func (s *fakeStore) Delete(key string, envelope versioning.Envelope, sync bool) error {
	s.deletes = append(s.deletes, storeCall{key: key, envelope: envelope, sync: sync})
	return s.deleteErr
}

// fakeOwnership defaults to owning everything, so tests that don't care
// about ownership don't need to configure anything.
type fakeOwnership struct {
	ownsKey   bool
	ownsRange bool
}

func newFakeOwnership() *fakeOwnership {
	return &fakeOwnership{ownsKey: true, ownsRange: true}
}

func (o *fakeOwnership) IsOwner(string) bool           { return o.ownsKey }
func (o *fakeOwnership) OwnsRange(string, string) bool { return o.ownsRange }

// fakeStream is a minimal grpc.ServerStreamingServer[pb.KeyEnvelope] stand-in.
type fakeStream struct {
	ctx  context.Context
	rows []*pb.KeyEnvelope
}

func newFakeStream() *fakeStream {
	return &fakeStream{ctx: context.Background()}
}

func (f *fakeStream) Send(row *pb.KeyEnvelope) error {
	f.rows = append(f.rows, row)
	return nil
}

func (f *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeStream) SetTrailer(metadata.MD)       {}
func (f *fakeStream) Context() context.Context     { return f.ctx }
func (f *fakeStream) SendMsg(m any) error          { return nil }
func (f *fakeStream) RecvMsg(m any) error          { return nil }
