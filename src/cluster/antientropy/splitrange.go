package antientropy

import (
	"encoding/binary"
	"fmt"
	"math"
)

// boundaryPrefixBytes is how many leading bytes of a key are treated as
// a big-endian integer for interpolation purposes. Keys that agree on
// this many leading bytes fall into the same bucket regardless of what
// follows - a known simplification for skewed key distributions
const boundaryPrefixBytes = 8

// Bucket is a half-open key sub-interval [Start, End) of a range being
// split for anti-entropy comparison. It uses the same boundary
// convention as ring.Range (Start == "" means no lower bound, End == ""
// means no upper bound) but is defined locally to keep this package
// decoupled from ring, mirroring the coordination/versioning leaf-
// package convention elsewhere in the cluster layer.
type Bucket struct {
	Start string
	End   string
}

// SplitRange deterministically derives n contiguous buckets covering
// [start, end). Both replicas of a range compute identical boundaries
// from (start, end, n) alone, with no coordination required, by
// interpolating evenly between the two ends' leading bytes treated as
// big-endian integers. The first bucket's Start and the last bucket's
// End are always exactly start and end (unrounded); only the n-1
// internal boundaries are interpolated.
func SplitRange(start, end string, n int) ([]Bucket, error) {
	if n <= 0 {
		return nil, fmt.Errorf("bucket count must be positive, got %d", n)
	}
	if n == 1 {
		return []Bucket{{Start: start, End: end}}, nil
	}

	lo := prefixToUint64(start, 0x00)
	hi := uint64(math.MaxUint64)
	if end != "" {
		hi = prefixToUint64(end, 0xFF)
	}
	if hi <= lo {
		return []Bucket{{Start: start, End: end}}, nil
	}

	step := (hi - lo) / uint64(n)
	if step == 0 {
		step = 1
	}

	buckets := make([]Bucket, 0, n)
	prev := start
	for i := 1; i < n; i++ {
		boundaryValue := lo + step*uint64(i)
		if boundaryValue >= hi {
			break
		}
		boundary := uint64ToBytes(boundaryValue)
		buckets = append(buckets, Bucket{Start: prev, End: boundary})
		prev = boundary
	}
	buckets = append(buckets, Bucket{Start: prev, End: end})
	return buckets, nil
}

// prefixToUint64 interprets the leading boundaryPrefixBytes bytes of s
// as a big-endian integer, padding any missing bytes with pad.
func prefixToUint64(s string, pad byte) uint64 {
	var buf [boundaryPrefixBytes]byte
	for i := range buf {
		buf[i] = pad
	}
	copy(buf[:], s)
	return binary.BigEndian.Uint64(buf[:])
}

func uint64ToBytes(v uint64) string {
	var buf [boundaryPrefixBytes]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return string(buf[:])
}
