package node

import (
	"fmt"

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
