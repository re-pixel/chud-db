package storage

import "nosqlEngine/src/wal/record"

type SegmentInfo struct {
	ID   uint64
	Path string
}

type AppendStorage interface {
	Append(op record.Op, key, value []byte) (lsn uint64, err error)
	Sync() error
	RotateIfNeeded() error
	ActiveSegment() SegmentInfo
	DurableLSN() uint64
	AppendedLSN() uint64
	Close() error
	Purge() error

	ListSegments() ([]SegmentInfo, error)
	OpenSegmentReader(segmentID uint64) (SegmentReader, error)
}

type SegmentReader interface {
	Next() (record.Record, error)
}
