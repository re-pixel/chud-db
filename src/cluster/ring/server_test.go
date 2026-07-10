package ring

import (
	"context"
	"testing"

	"nosqlEngine/src/cluster/transport/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestServer(t *testing.T) (*Server, *Table) {
	t.Helper()
	table, err := NewTable("node-1", singleRange("node-1", "node-2"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	return NewServer(table), table
}

func TestServerGetRangeMapReturnsCurrentSnapshot(t *testing.T) {
	server, _ := newTestServer(t)

	resp, err := server.GetRangeMap(context.Background(), &pb.GetRangeMapRequest{})
	if err != nil {
		t.Fatalf("GetRangeMap: %v", err)
	}

	got := RangeMapFromProto(resp.GetMap())
	if got.Generation != 1 {
		t.Fatalf("generation = %d, want 1", got.Generation)
	}
	if len(got.Ranges) != 1 || got.Ranges[0].Start != "" || got.Ranges[0].End != "" {
		t.Fatalf("ranges = %#v", got.Ranges)
	}
	if len(got.Ranges[0].Replicas) != 2 {
		t.Fatalf("replicas = %#v", got.Ranges[0].Replicas)
	}
}

func TestServerGetRangeMapReflectsReplacedMap(t *testing.T) {
	server, table := newTestServer(t)

	newer := RangeMap{
		Generation: 2,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "m", End: "", Replicas: []string{"node-2"}},
		},
	}
	if changed, err := table.Replace(newer); err != nil || !changed {
		t.Fatalf("replace: changed=%v err=%v", changed, err)
	}

	resp, err := server.GetRangeMap(context.Background(), &pb.GetRangeMapRequest{})
	if err != nil {
		t.Fatalf("GetRangeMap: %v", err)
	}

	got := RangeMapFromProto(resp.GetMap())
	if got.Generation != 2 || len(got.Ranges) != 2 {
		t.Fatalf("expected updated map, got %#v", got)
	}
}

func TestServerGetRangeMapContextCancellation(t *testing.T) {
	server, _ := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := server.GetRangeMap(ctx, &pb.GetRangeMapRequest{})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("status = %v, err = %v", status.Code(err), err)
	}
}
