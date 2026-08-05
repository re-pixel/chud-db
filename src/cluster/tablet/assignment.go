package tablet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
)

var ErrUnsafeReplicaAssignment = errors.New("cannot retain enough existing replicas")

type scoredCandidate struct {
	nodeID string
	score  uint64
}

func AssignReplicas(start, end string, candidates, previous []string, count int) ([]string, error) {
	ranked := rankCandidates(start, end, candidates)
	if count < 1 {
		return nil, fmt.Errorf("replica count must be >= 1")
	}
	if len(ranked) < count {
		return nil, fmt.Errorf("replica count %d exceeds candidate count %d", count, len(ranked))
	}

	previousSet := make(map[string]struct{}, len(previous))
	for _, nodeID := range previous {
		previousSet[nodeID] = struct{}{}
	}
	retainedNeeded := count
	if count > 1 {
		retainedNeeded--
	}

	selected := make(map[string]struct{}, count)
	for _, candidate := range ranked {
		if _, wasReplica := previousSet[candidate.nodeID]; !wasReplica {
			continue
		}
		selected[candidate.nodeID] = struct{}{}
		if len(selected) == retainedNeeded {
			break
		}
	}
	if len(selected) < retainedNeeded {
		return nil, ErrUnsafeReplicaAssignment
	}
	for _, candidate := range ranked {
		if len(selected) == count {
			break
		}
		selected[candidate.nodeID] = struct{}{}
	}

	result := make([]string, 0, count)
	for _, candidate := range ranked {
		if _, ok := selected[candidate.nodeID]; ok {
			result = append(result, candidate.nodeID)
		}
	}
	return result, nil
}

func rankCandidates(start, end string, candidates []string) []scoredCandidate {
	unique := make(map[string]struct{}, len(candidates))
	ranked := make([]scoredCandidate, 0, len(candidates))
	for _, nodeID := range candidates {
		if nodeID == "" {
			continue
		}
		if _, exists := unique[nodeID]; exists {
			continue
		}
		unique[nodeID] = struct{}{}
		ranked = append(ranked, scoredCandidate{
			nodeID: nodeID,
			score:  replicaScore(start, end, nodeID),
		})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].nodeID < ranked[j].nodeID
	})
	return ranked
}

func replicaScore(start, end, nodeID string) uint64 {
	digest := xxhash.New()
	for _, value := range []string{start, end, nodeID} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = digest.Write(size[:])
		_, _ = digest.WriteString(value)
	}
	return digest.Sum64()
}
