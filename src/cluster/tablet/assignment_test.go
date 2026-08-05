package tablet

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestAssignReplicasIsDeterministic(t *testing.T) {
	candidates := []string{"node-4", "node-2", "node-1", "node-3"}
	previous := []string{"node-1", "node-2", "node-3"}

	first, err := AssignReplicas("a", "m", candidates, previous, 3)
	if err != nil {
		t.Fatalf("assign replicas: %v", err)
	}
	second, err := AssignReplicas("a", "m", slices.Clone(candidates), slices.Clone(previous), 3)
	if err != nil {
		t.Fatalf("assign replicas again: %v", err)
	}
	if !slices.Equal(first, second) {
		t.Fatalf("assignments differ: %v vs %v", first, second)
	}
}

func TestAssignReplicasIntroducesAtMostOneNewReplica(t *testing.T) {
	previous := []string{"node-1", "node-2", "node-3"}
	got, err := AssignReplicas("", "m", []string{"node-1", "node-2", "node-3", "node-4", "node-5"}, previous, 3)
	if err != nil {
		t.Fatalf("assign replicas: %v", err)
	}

	previousSet := map[string]bool{"node-1": true, "node-2": true, "node-3": true}
	newCount := 0
	for _, nodeID := range got {
		if !previousSet[nodeID] {
			newCount++
		}
	}
	if len(got) != 3 || newCount > 1 {
		t.Fatalf("assignment = %v, new replicas = %d", got, newCount)
	}
}

func TestAssignReplicasWithReplicationFactorOneRetainsSource(t *testing.T) {
	got, err := AssignReplicas("", "", []string{"node-1", "node-2"}, []string{"node-1"}, 1)
	if err != nil {
		t.Fatalf("assign replicas: %v", err)
	}
	if !slices.Equal(got, []string{"node-1"}) {
		t.Fatalf("assignment = %v, want retained node-1", got)
	}
}

func TestAssignReplicasRejectsUnsafeReplacement(t *testing.T) {
	_, err := AssignReplicas("", "", []string{"node-1", "node-4", "node-5"}, []string{"node-1", "node-2", "node-3"}, 3)
	if !errors.Is(err, ErrUnsafeReplicaAssignment) {
		t.Fatalf("error = %v, want ErrUnsafeReplicaAssignment", err)
	}
}

func TestAssignReplicasRejectsInsufficientCandidates(t *testing.T) {
	if _, err := AssignReplicas("", "", []string{"node-1"}, []string{"node-1"}, 2); err == nil {
		t.Fatalf("expected insufficient candidates error")
	}
}

func TestAssignReplicasDistributesRangesAcrossCandidates(t *testing.T) {
	candidates := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		start := fmt.Sprintf("%03d", i)
		end := fmt.Sprintf("%03d", i+1)
		got, err := AssignReplicas(start, end, candidates, candidates, 2)
		if err != nil {
			t.Fatalf("assign range %d: %v", i, err)
		}
		for _, nodeID := range got {
			seen[nodeID] = true
		}
	}
	if len(seen) < 4 {
		t.Fatalf("assignments used only %d candidates: %v", len(seen), seen)
	}
}
