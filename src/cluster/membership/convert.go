package membership

import (
	"time"

	"nosqlEngine/src/cluster/transport/pb"
)

func NodeInfoToProto(m Member) *pb.NodeInfo {
	return &pb.NodeInfo{
		NodeId:        m.NodeID,
		ClusterId:     m.ClusterID,
		AdvertiseAddr: m.AdvertiseAddr,
	}
}

func NodeIdentityFromProto(info *pb.NodeInfo) (nodeID, clusterID, advertiseAddr string) {
	if info == nil {
		return "", "", ""
	}
	return info.GetNodeId(), info.GetClusterId(), info.GetAdvertiseAddr()
}

func MemberToProto(m Member) *pb.MemberState {
	var lastSeen int64
	if !m.LastSeen.IsZero() {
		lastSeen = m.LastSeen.UnixNano()
	}
	return &pb.MemberState{
		Node:             NodeInfoToProto(m),
		Status:           statusToProto(m.Status),
		Incarnation:      m.Incarnation,
		LastSeenUnixNano: lastSeen,
		MembershipEpoch:  m.MembershipEpoch,
		RangeMapEpoch:    m.RangeMapEpoch,
	}
}

func MemberFromProto(ms *pb.MemberState) Member {
	if ms == nil {
		return Member{}
	}

	nodeID, clusterID, advertiseAddr := NodeIdentityFromProto(ms.GetNode())
	m := Member{
		NodeID:          nodeID,
		ClusterID:       clusterID,
		AdvertiseAddr:   advertiseAddr,
		Status:          statusFromProto(ms.GetStatus()),
		Incarnation:     ms.GetIncarnation(),
		MembershipEpoch: ms.GetMembershipEpoch(),
		RangeMapEpoch:   ms.GetRangeMapEpoch(),
	}
	if seen := ms.GetLastSeenUnixNano(); seen != 0 {
		m.LastSeen = time.Unix(0, seen)
	}
	return m
}

// MembersToProto and MembersFromProto preserve input order; callers that
// need deterministic wire order should pass an already-sorted slice,
// such as Table.Snapshot's output.
func MembersToProto(members []Member) []*pb.MemberState {
	out := make([]*pb.MemberState, 0, len(members))
	for _, m := range members {
		out = append(out, MemberToProto(m))
	}
	return out
}

func MembersFromProto(states []*pb.MemberState) []Member {
	out := make([]Member, 0, len(states))
	for _, ms := range states {
		out = append(out, MemberFromProto(ms))
	}
	return out
}

func statusToProto(s Status) pb.MemberStatus {
	switch s {
	case StatusAlive:
		return pb.MemberStatus_MEMBER_STATUS_ALIVE
	case StatusSuspect:
		return pb.MemberStatus_MEMBER_STATUS_SUSPECT
	case StatusDead:
		return pb.MemberStatus_MEMBER_STATUS_DEAD
	default:
		return pb.MemberStatus_MEMBER_STATUS_UNKNOWN
	}
}

func statusFromProto(v pb.MemberStatus) Status {
	switch v {
	case pb.MemberStatus_MEMBER_STATUS_ALIVE:
		return StatusAlive
	case pb.MemberStatus_MEMBER_STATUS_SUSPECT:
		return StatusSuspect
	case pb.MemberStatus_MEMBER_STATUS_DEAD:
		return StatusDead
	default:
		return StatusUnknown
	}
}
