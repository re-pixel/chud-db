package antientropy

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// RawEntry is a single (key, raw stored value) pair as returned by a range
// scan. The value is kept exactly as stored - typically an encoded
// versioning.Envelope string - and is never decoded here, so computing a
// root hash never pays envelope decode cost unless a mismatch is later
// found and the caller drills down to individual keys.
type RawEntry struct {
	Key   string
	Value string
}

// RangeScanner is the minimal local-storage capability root-hash
// computation needs: return every (key, rawValue) pair in [start, end).
// It is defined here rather than depending on node.NodeStore directly, so
// this package stays decoupled from the concrete storage adapter (which is
// supplied at the call site, mirroring coordination.ReplicaClient and
// ring.PeerEpochSource elsewhere in the cluster layer).
type RangeScanner interface {
	ScanRawRange(start, end string) ([]RawEntry, error)
}

// ComputeMerkleRoot scans [start, end) via scanner, sorts the results by
// key defensively (a scanner's own iteration order is not assumed to be
// sorted), and folds every (key, value) pair into a single streaming
// xxhash digest. It returns the resulting root hash together with the
// number of entries scanned; two replicas are in sync over the same range
// only if both values match.
//
// Both the key and value are length-prefixed before being written to the
// digest so that no pair of distinct (key, value) sequences can hash to
// the same byte stream.
func ComputeMerkleRoot(scanner RangeScanner, start, end string) ([]byte, uint64, error) {
	entries, err := scanner.ScanRawRange(start, end)
	if err != nil {
		return nil, 0, fmt.Errorf("scan range [%q, %q): %w", start, end, err)
	}

	sorted := make([]RawEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	digest := xxhash.New()
	for _, entry := range sorted {
		writeLengthPrefixed(digest, entry.Key)
		writeLengthPrefixed(digest, entry.Value)
	}
	return digest.Sum(nil), uint64(len(sorted)), nil
}

func writeLengthPrefixed(digest *xxhash.Digest, s string) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(s)))
	_, _ = digest.Write(lenBuf[:])
	_, _ = digest.Write([]byte(s))
}
