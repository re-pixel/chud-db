package membership

import (
	"testing"
	"time"

	"nosqlEngine/src/cluster/transport/pb"
)

func TestNodeInfoRoundTrip(t *testing.T) {
	m := Member{NodeID: "node-1", ClusterID: "cluster-a", AdvertiseAddr: "10.0.0.1:7000"}

	nodeID, clusterID, advertiseAddr := NodeIdentityFromProto(NodeInfoToProto(m))
	if nodeID != m.NodeID || clusterID != m.ClusterID || advertiseAddr != m.AdvertiseAddr {
		t.Fatalf("round trip = (%q, %q, %q), want (%q, %q, %q)",
			nodeID, clusterID, advertiseAddr, m.NodeID, m.ClusterID, m.AdvertiseAddr)
	}
}

func TestNodeIdentityFromProtoTreatsNilAsEmpty(t *testing.T) {
	nodeID, clusterID, advertiseAddr := NodeIdentityFromProto(nil)
	if nodeID != "" || clusterID != "" || advertiseAddr != "" {
		t.Fatalf("expected empty identity for nil, got (%q, %q, %q)", nodeID, clusterID, advertiseAddr)
	}
}

func TestMemberRoundTrip(t *testing.T) {
	m := Member{
		NodeID:          "node-1",
		ClusterID:       "cluster-a",
		AdvertiseAddr:   "10.0.0.1:7000",
		Status:          StatusSuspect,
		Incarnation:     4,
		LastSeen:        time.Unix(0, 123456789),
		MembershipEpoch: 7,
		RangeMapEpoch:   2,
	}

	got := MemberFromProto(MemberToProto(m))
	if got != m {
		t.Fatalf("round trip = %+v, want %+v", got, m)
	}
}

func TestMemberRoundTripPreservesZeroLastSeen(t *testing.T) {
	m := Member{NodeID: "node-1", Status: StatusAlive}

	got := MemberFromProto(MemberToProto(m))
	if !got.LastSeen.IsZero() {
		t.Fatalf("expected zero LastSeen to round trip as zero, got %v", got.LastSeen)
	}
}

func TestMemberFromProtoTreatsNilAsZeroValue(t *testing.T) {
	got := MemberFromProto(nil)
	if got != (Member{}) {
		t.Fatalf("expected zero value member for nil, got %+v", got)
	}
}

func TestMemberFromProtoTreatsNilNodeAsEmptyIdentity(t *testing.T) {
	got := MemberFromProto(&pb.MemberState{Status: pb.MemberStatus_MEMBER_STATUS_ALIVE, Incarnation: 1})
	if got.NodeID != "" || got.ClusterID != "" || got.AdvertiseAddr != "" {
		t.Fatalf("expected empty identity, got %+v", got)
	}
	if got.Status != StatusAlive || got.Incarnation != 1 {
		t.Fatalf("expected status/incarnation preserved, got %+v", got)
	}
}

func TestStatusMappingRoundTrip(t *testing.T) {
	statuses := []Status{StatusUnknown, StatusAlive, StatusSuspect, StatusDead}
	for _, s := range statuses {
		if got := statusFromProto(statusToProto(s)); got != s {
			t.Fatalf("status round trip = %v, want %v", got, s)
		}
	}
}

func TestStatusFromProtoDefaultsToUnknown(t *testing.T) {
	if got := statusFromProto(pb.MemberStatus(99)); got != StatusUnknown {
		t.Fatalf("status = %v, want unknown for unrecognized proto value", got)
	}
}

func TestMembersToProtoAndFromProtoPreserveOrder(t *testing.T) {
	members := []Member{
		{NodeID: "node-a", Status: StatusAlive},
		{NodeID: "node-b", Status: StatusSuspect},
		{NodeID: "node-c", Status: StatusDead},
	}

	got := MembersFromProto(MembersToProto(members))
	if len(got) != len(members) {
		t.Fatalf("length = %d, want %d", len(got), len(members))
	}
	for i, m := range members {
		if got[i].NodeID != m.NodeID || got[i].Status != m.Status {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], m)
		}
	}
}

func TestMembersToProtoAndFromProtoHandleEmpty(t *testing.T) {
	if got := MembersToProto(nil); len(got) != 0 {
		t.Fatalf("expected empty slice, got %#v", got)
	}
	if got := MembersFromProto(nil); len(got) != 0 {
		t.Fatalf("expected empty slice, got %#v", got)
	}
}
