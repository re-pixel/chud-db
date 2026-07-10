package ring

import "testing"

func TestNewTableRejectsInvalidRangeMap(t *testing.T) {
	_, err := NewTable("node-1", RangeMap{})
	if err == nil {
		t.Fatalf("expected error constructing table from invalid range map")
	}
}

func TestNewTableSeedsGenerationAndOwnership(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1", "node-2"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	if table.Generation() != 1 {
		t.Fatalf("generation = %d, want 1", table.Generation())
	}
	if !table.IsOwner("anykey") {
		t.Fatalf("expected node-1 to own everything in the single global range")
	}
}

func TestIsOwnerFalseWhenNotInReplicaSet(t *testing.T) {
	table, err := NewTable("node-3", singleRange("node-1", "node-2"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	if table.IsOwner("anykey") {
		t.Fatalf("node-3 should not own a range it's not a replica of")
	}
}

func TestOwnsKeyRangeWithinSingleOwnedRange(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1", "node-2"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	if !table.OwnsKeyRange("a", "z") {
		t.Fatalf("expected node-1 to own a scan fully within the single global range")
	}
}

func TestOwnsKeyRangeFalseWhenNotInReplicaSet(t *testing.T) {
	table, err := NewTable("node-3", singleRange("node-1", "node-2"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	if table.OwnsKeyRange("a", "z") {
		t.Fatalf("node-3 should not own a scan over a range it's not a replica of")
	}
}

func TestOwnsKeyRangeFalseWhenScanStraddlesBoundary(t *testing.T) {
	table, err := NewTable("node-1", RangeMap{
		Generation: 2,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "m", End: "", Replicas: []string{"node-1"}},
		},
	})
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	if table.OwnsKeyRange("a", "z") {
		t.Fatalf("expected scan straddling a range boundary to be rejected even when node-1 owns both sides")
	}
}

func TestReplaceAcceptsStrictlyNewerGeneration(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}

	changed, err := table.Replace(singleRange("node-1"))
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if changed {
		t.Fatalf("equal generation should not replace")
	}

	newer := RangeMap{
		Generation: 2,
		Ranges: []Range{
			{Start: "", End: "m", Replicas: []string{"node-1"}},
			{Start: "m", End: "", Replicas: []string{"node-2"}},
		},
	}
	changed, err = table.Replace(newer)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if !changed {
		t.Fatalf("strictly newer generation should replace")
	}
	if table.Generation() != 2 {
		t.Fatalf("generation = %d, want 2", table.Generation())
	}
	if table.IsOwner("z") {
		t.Fatalf("node-1 should no longer own the second range after split")
	}
}

func TestReplaceRejectsInvalidIncomingMap(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}

	_, err = table.Replace(RangeMap{Generation: 2})
	if err == nil {
		t.Fatalf("expected validation error for empty incoming map")
	}
	if table.Generation() != 1 {
		t.Fatalf("invalid replace should not mutate table")
	}
}

func TestSnapshotIsIsolatedFromTable(t *testing.T) {
	table, err := NewTable("node-1", singleRange("node-1"))
	if err != nil {
		t.Fatalf("new table: %v", err)
	}

	snap := table.Snapshot()
	snap.Ranges[0].Replicas[0] = "node-2"

	if !table.IsOwner("anykey") {
		t.Fatalf("mutating snapshot should not affect table's actual ownership")
	}
}
