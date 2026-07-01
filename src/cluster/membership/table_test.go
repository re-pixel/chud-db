package membership

import "testing"

func newTestTable() *Table {
	return NewTable("test-cluster", Member{NodeID: "local", AdvertiseAddr: "127.0.0.1:7000"})
}

func TestNewTableSeedsLocalAlive(t *testing.T) {
	table := newTestTable()

	local := table.Local()
	if local.Status != StatusAlive {
		t.Fatalf("local status = %v, want alive", local.Status)
	}
	if local.ClusterID != "test-cluster" {
		t.Fatalf("local cluster id = %q", local.ClusterID)
	}
	if local.Incarnation != 0 {
		t.Fatalf("local incarnation = %d, want 0", local.Incarnation)
	}
}

func TestMergeInsertsUnknownMember(t *testing.T) {
	table := newTestTable()

	changed := table.Merge(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 0})
	if !changed {
		t.Fatalf("expected merge to report change for new member")
	}

	m, ok := table.Get("peer-1")
	if !ok {
		t.Fatalf("expected peer-1 to be known")
	}
	if m.Status != StatusAlive {
		t.Fatalf("peer status = %v, want alive", m.Status)
	}
}

func TestMergeHigherIncarnationWins(t *testing.T) {
	table := newTestTable()
	table.Merge(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 1})

	changed := table.Merge(Member{NodeID: "peer-1", Status: StatusSuspect, Incarnation: 0})
	if changed {
		t.Fatalf("lower incarnation should not override higher incarnation")
	}

	changed = table.Merge(Member{NodeID: "peer-1", Status: StatusSuspect, Incarnation: 2})
	if !changed {
		t.Fatalf("higher incarnation should override regardless of status")
	}
	m, _ := table.Get("peer-1")
	if m.Status != StatusSuspect || m.Incarnation != 2 {
		t.Fatalf("peer-1 = %+v, want suspect at incarnation 2", m)
	}
}

func TestMergeStatusPrecedenceAtEqualIncarnation(t *testing.T) {
	table := newTestTable()
	table.Merge(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 1})

	if !table.Merge(Member{NodeID: "peer-1", Status: StatusSuspect, Incarnation: 1}) {
		t.Fatalf("suspect should beat alive at equal incarnation")
	}
	if !table.Merge(Member{NodeID: "peer-1", Status: StatusDead, Incarnation: 1}) {
		t.Fatalf("dead should beat suspect at equal incarnation")
	}
	if table.Merge(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 1}) {
		t.Fatalf("alive should not beat dead at equal incarnation")
	}
}

func TestMergeIgnoresDifferentClusterID(t *testing.T) {
	table := newTestTable()

	changed := table.Merge(Member{NodeID: "peer-1", ClusterID: "other-cluster", Status: StatusAlive})
	if changed {
		t.Fatalf("expected cross-cluster record to be ignored")
	}
	if _, ok := table.Get("peer-1"); ok {
		t.Fatalf("cross-cluster record should not be added")
	}
}

func TestMergeRefutesSuspicionAboutLocalNode(t *testing.T) {
	table := newTestTable()

	changed := table.Merge(Member{NodeID: "local", Status: StatusSuspect, Incarnation: 0})
	if !changed {
		t.Fatalf("expected local refutation to change table")
	}

	local := table.Local()
	if local.Status != StatusAlive {
		t.Fatalf("local status = %v, want alive after refutation", local.Status)
	}
	if local.Incarnation != 1 {
		t.Fatalf("local incarnation = %d, want 1 after refutation", local.Incarnation)
	}
}

func TestMergeIgnoresStaleAccusationAboutLocalNode(t *testing.T) {
	table := newTestTable()
	table.RefuteLocalSuspect() // bump local incarnation to 1

	changed := table.Merge(Member{NodeID: "local", Status: StatusDead, Incarnation: 0})
	if changed {
		t.Fatalf("stale accusation below current incarnation should be ignored")
	}
	local := table.Local()
	if local.Status != StatusAlive || local.Incarnation != 1 {
		t.Fatalf("local = %+v, want unchanged alive at incarnation 1", local)
	}
}

func TestMergeIgnoresNonAccusationAboutLocalNode(t *testing.T) {
	table := newTestTable()

	changed := table.Merge(Member{NodeID: "local", Status: StatusAlive, Incarnation: 5})
	if changed {
		t.Fatalf("plain alive gossip about self should not force a change")
	}
	local := table.Local()
	if local.Incarnation != 0 {
		t.Fatalf("local incarnation = %d, want unchanged 0", local.Incarnation)
	}
}

func TestMarkSuspectRequiresMatchingIncarnationAndAliveStatus(t *testing.T) {
	table := newTestTable()
	table.Merge(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 3})

	if table.MarkSuspect("peer-1", 2) {
		t.Fatalf("mismatched incarnation should not transition to suspect")
	}
	if !table.MarkSuspect("peer-1", 3) {
		t.Fatalf("expected transition to suspect")
	}
	m, _ := table.Get("peer-1")
	if m.Status != StatusSuspect {
		t.Fatalf("peer-1 status = %v, want suspect", m.Status)
	}
	if table.MarkSuspect("peer-1", 3) {
		t.Fatalf("already-suspect member should not transition again")
	}
}

func TestMarkSuspectNeverAppliesToLocalNode(t *testing.T) {
	table := newTestTable()
	if table.MarkSuspect("local", 0) {
		t.Fatalf("local node should never be locally marked suspect")
	}
}

func TestMarkDeadRequiresSuspectStatus(t *testing.T) {
	table := newTestTable()
	table.Merge(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 1})

	if table.MarkDead("peer-1", 1) {
		t.Fatalf("alive member should not transition directly to dead")
	}
	table.MarkSuspect("peer-1", 1)
	if !table.MarkDead("peer-1", 1) {
		t.Fatalf("expected suspect member to transition to dead")
	}
	m, _ := table.Get("peer-1")
	if m.Status != StatusDead {
		t.Fatalf("peer-1 status = %v, want dead", m.Status)
	}
}

func TestRefuteLocalSuspectAlwaysBumpsIncarnation(t *testing.T) {
	table := newTestTable()

	first := table.RefuteLocalSuspect()
	if first.Incarnation != 1 || first.Status != StatusAlive {
		t.Fatalf("first refutation = %+v", first)
	}
	second := table.RefuteLocalSuspect()
	if second.Incarnation != 2 {
		t.Fatalf("second refutation incarnation = %d, want 2", second.Incarnation)
	}
}

func TestUpsertOverwritesUnconditionally(t *testing.T) {
	table := newTestTable()
	table.Merge(Member{NodeID: "peer-1", Status: StatusDead, Incarnation: 5})

	if !table.Upsert(Member{NodeID: "peer-1", Status: StatusAlive, Incarnation: 0}) {
		t.Fatalf("expected upsert to succeed")
	}
	m, _ := table.Get("peer-1")
	if m.Status != StatusAlive || m.Incarnation != 0 {
		t.Fatalf("peer-1 = %+v, want alive at incarnation 0 after upsert", m)
	}
}

func TestUpsertRejectsDifferentClusterID(t *testing.T) {
	table := newTestTable()
	if table.Upsert(Member{NodeID: "peer-1", ClusterID: "other-cluster"}) {
		t.Fatalf("expected upsert to reject cross-cluster record")
	}
}

func TestSnapshotIsSortedAndIsolated(t *testing.T) {
	table := newTestTable()
	table.Merge(Member{NodeID: "peer-b", Status: StatusAlive})
	table.Merge(Member{NodeID: "peer-a", Status: StatusAlive})

	snap := table.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot length = %d, want 3", len(snap))
	}
	if snap[0].NodeID != "local" || snap[1].NodeID != "peer-a" || snap[2].NodeID != "peer-b" {
		t.Fatalf("snapshot not sorted by node id: %+v", snap)
	}

	snap[0].Status = StatusDead
	if local := table.Local(); local.Status != StatusAlive {
		t.Fatalf("mutating snapshot copy affected table: %v", local.Status)
	}
}

func TestMembershipEpochIncrementsOnChange(t *testing.T) {
	table := newTestTable()
	before := table.Local().MembershipEpoch

	table.Merge(Member{NodeID: "peer-1", Status: StatusAlive})
	after, _ := table.Get("peer-1")
	if after.MembershipEpoch <= before {
		t.Fatalf("expected membership epoch to advance, before=%d after=%d", before, after.MembershipEpoch)
	}
}
