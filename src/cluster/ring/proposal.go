package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

func ProposalID(ranges []Range) string {
	canonical := make([]Range, len(ranges))
	for i, r := range ranges {
		canonical[i] = r.Clone()
		sort.Strings(canonical[i].Replicas)
	}
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].Start != canonical[j].Start {
			return canonical[i].Start < canonical[j].Start
		}
		return canonical[i].End < canonical[j].End
	})

	hash := sha256.New()
	writeUint64(hash.Write, uint64(len(canonical)))
	for _, r := range canonical {
		writeString(hash.Write, r.Start)
		writeString(hash.Write, r.End)
		writeUint64(hash.Write, uint64(len(r.Replicas)))
		for _, replica := range r.Replicas {
			writeString(hash.Write, replica)
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func writeString(write func([]byte) (int, error), value string) {
	writeUint64(write, uint64(len(value)))
	_, _ = write([]byte(value))
}

func writeUint64(write func([]byte) (int, error), value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	_, _ = write(buf[:])
}
