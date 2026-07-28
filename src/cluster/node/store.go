package node

import (
	"fmt"
	"strings"

	"nosqlEngine/src/cluster/versioning"
)

const DefaultStoreUser = "cluster"

type LocalEngine interface {
	Write(user, key, value string, fromWal bool) error
	WriteAsync(user, key, value string) error
	Read(user, key string) (string, bool, error)
	RangeScan(user, start, end string, pageNum, pageSize int) ([][]string, error)
}

type NodeStore struct {
	engine LocalEngine
	user   string
}

func NewNodeStore(engine LocalEngine, user string) *NodeStore {
	if user == "" {
		user = DefaultStoreUser
	}
	return &NodeStore{engine: engine, user: user}
}

func (s *NodeStore) Put(key string, envelope versioning.Envelope, sync bool) error {
	if envelope.Deleted {
		return fmt.Errorf("put %q: envelope is marked deleted", key)
	}
	return s.writeEnvelope(key, envelope, sync)
}

func (s *NodeStore) Delete(key string, envelope versioning.Envelope, sync bool) error {
	if !envelope.Deleted {
		return fmt.Errorf("delete %q: envelope is not marked deleted", key)
	}
	return s.writeEnvelope(key, envelope, sync)
}

func (s *NodeStore) Get(key string) (versioning.Envelope, bool, error) {
	raw, ok, err := s.engine.Read(s.user, key)
	if err != nil {
		return versioning.Envelope{}, false, err
	}
	if !ok {
		return versioning.Envelope{}, false, nil
	}
	envelope, err := versioning.Decode(raw)
	if err != nil {
		return versioning.Envelope{}, false, fmt.Errorf("decode %q: %w", key, err)
	}
	return envelope, true, nil
}

func (s *NodeStore) ScanRange(start, end string, pageNum, pageSize int) ([]versioning.KeyEnvelope, error) {
	rows, err := s.engine.RangeScan(s.user, start, end, pageNum, pageSize)
	if err != nil {
		return nil, err
	}

	results := make([]versioning.KeyEnvelope, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("range scan returned malformed row: %#v", row)
		}
		envelope, err := versioning.Decode(row[1])
		if err != nil {
			return nil, fmt.Errorf("decode %q: %w", row[0], err)
		}
		results = append(results, versioning.KeyEnvelope{Key: row[0], Envelope: envelope})
	}
	return results, nil
}

// rawScanPageSize bounds how many rows ScanRawRange fetches per
// underlying RangeScan call. Anti-entropy buckets are already capped in
// size by the Merkle tree's leaf-item threshold, so this only matters
// for a caller requesting a very wide (e.g. whole-keyspace) range.
const rawScanPageSize = 1000

// unboundedRangeEnd stands in for ScanRawRange's "no upper bound" end
// ("") when calling into the underlying engine, which - unlike this
// method's own half-open convention - has no unbounded-end sentinel at
// all (see ring.Range's doc comment) and always compares end literally.
// A real key that itself begins with this many 0xFF bytes and is
// longer would incorrectly be excluded from an "unbounded" scan; this
// is accepted as vanishingly unlikely for this engine's key space in
// practice (see TECH_DEBT.md).
var unboundedRangeEnd = strings.Repeat("\xff", 64)

// RawKV is a single (key, raw stored value) pair as returned by
// ScanRawRange - the value is left exactly as stored (an encoded
// versioning.Envelope), never decoded here.
type RawKV struct {
	Key   string
	Value string
}

// ScanRawRange returns every (key, raw stored value) pair in the
// half-open interval [start, end) - the convention used throughout the
// cluster layer's ring/anti-entropy code (see ring.Range's doc
// comment) - unpaginated from the caller's perspective, without
// decoding envelopes. This is the local-storage capability anti-entropy
// needs for both Merkle root hashing and leaf streaming - see
// antientropy.Store.
func (s *NodeStore) ScanRawRange(start, end string) ([]RawKV, error) {
	scanEnd := end
	if scanEnd == "" {
		scanEnd = unboundedRangeEnd
	}

	rows := make([]RawKV, 0)
	for page := 1; ; page++ {
		// engine.RangeScan is inclusive of end; drop a trailing exact
		// match on the caller's own end to translate its convention
		// into this method's half-open one.
		batch, err := s.engine.RangeScan(s.user, start, scanEnd, page, rawScanPageSize)
		if err != nil {
			return nil, err
		}
		for _, row := range batch {
			if len(row) < 2 {
				return nil, fmt.Errorf("range scan returned malformed row: %#v", row)
			}
			if end != "" && row[0] == end {
				continue
			}
			rows = append(rows, RawKV{Key: row[0], Value: row[1]})
		}
		if len(batch) < rawScanPageSize {
			return rows, nil
		}
	}
}

func (s *NodeStore) writeEnvelope(key string, envelope versioning.Envelope, sync bool) error {
	raw, err := versioning.Encode(envelope)
	if err != nil {
		return fmt.Errorf("encode %q: %w", key, err)
	}
	if sync {
		return s.engine.Write(s.user, key, raw, false)
	}
	return s.engine.WriteAsync(s.user, key, raw)
}
