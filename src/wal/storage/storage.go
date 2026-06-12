package storage

import "nosqlEngine/src/wal"

type SegmentInfo struct {
	ID   uint64
	Path string
}

type AppendStorage interface {
	Append(op wal.Op, key, value []byte) (lsn uint64, err error)
	Sync() error
	RotateIfNeeded() error
	ActiveSegment() SegmentInfo
	Close() error

	ListSegments() ([]SegmentInfo, error)
	OpenSegmentReader(segmentID uint64) (SegmentReader, error)
}

type SegmentReader interface {
	Next() (wal.Record, error)
}
