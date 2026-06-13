package wal

import (
	"errors"
	"io"
	"nosqlEngine/src/wal/record"
	"nosqlEngine/src/wal/storage"
	"sync"
	"testing"
)

// countingStorage is an in-memory AppendStorage double that counts Sync calls
// and tracks appended/durable LSNs, so tests can observe group-commit batching
// without touching the filesystem.
type countingStorage struct {
	mu          sync.Mutex
	nextLSN     uint64
	appendedLSN uint64
	durableLSN  uint64
	syncCount   int
	failSync    error
	records     []record.Record
	durableLen  int
}

func newCountingStorage() *countingStorage {
	return &countingStorage{nextLSN: 1}
}

func (c *countingStorage) Append(op record.Op, key, value []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lsn := c.nextLSN
	c.nextLSN++
	c.appendedLSN = lsn
	c.records = append(c.records, record.Record{LSN: lsn, Op: op, Key: key, Value: value})
	return lsn, nil
}

func (c *countingStorage) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.syncCount++
	if c.failSync != nil {
		return c.failSync
	}
	c.durableLSN = c.appendedLSN
	c.durableLen = len(c.records)
	return nil
}

func (c *countingStorage) RotateIfNeeded() error { return nil }

func (c *countingStorage) ActiveSegment() storage.SegmentInfo { return storage.SegmentInfo{} }

func (c *countingStorage) DurableLSN() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.durableLSN
}

func (c *countingStorage) AppendedLSN() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appendedLSN
}

func (c *countingStorage) Close() error { return nil }

func (c *countingStorage) Purge() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextLSN = 1
	c.appendedLSN = 0
	c.durableLSN = 0
	c.records = nil
	c.durableLen = 0
	return nil
}

func (c *countingStorage) ListSegments() ([]storage.SegmentInfo, error) {
	return []storage.SegmentInfo{{ID: 1}}, nil
}

func (c *countingStorage) OpenSegmentReader(segmentID uint64) (storage.SegmentReader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	durable := make([]record.Record, c.durableLen)
	copy(durable, c.records[:c.durableLen])
	return &sliceReader{records: durable}, nil
}

func (c *countingStorage) syncCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncCount
}

type sliceReader struct {
	records []record.Record
	pos     int
}

func (r *sliceReader) Next() (record.Record, error) {
	if r.pos >= len(r.records) {
		return record.Record{}, io.EOF
	}
	rec := r.records[r.pos]
	r.pos++
	return rec, nil
}

// TestGroupCommitBatchesConcurrentWrites verifies that when many records are
// appended before the leader fsyncs, a single Sync makes them all durable.
func TestGroupCommitBatchesConcurrentWrites(t *testing.T) {
	const n = 8
	store := newCountingStorage()
	w := NewWALWithStorage(store, "group")

	// Phase 1: every writer appends, so all records exist before any fsync.
	lsns := make([]uint64, n)
	var appendWG sync.WaitGroup
	appendWG.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer appendWG.Done()
			lsn, err := w.AppendPut("k", "v")
			if err != nil {
				t.Errorf("AppendPut failed: %v", err)
			}
			lsns[i] = lsn
		}(i)
	}
	appendWG.Wait()

	// Phase 2: everyone waits for durability concurrently.
	var waitWG sync.WaitGroup
	waitWG.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer waitWG.Done()
			if err := w.WaitDurable(lsns[i]); err != nil {
				t.Errorf("WaitDurable failed: %v", err)
			}
		}(i)
	}
	waitWG.Wait()

	if calls := store.syncCalls(); calls < 1 || calls >= n {
		t.Fatalf("expected group commit to use fewer than %d syncs, got %d", n, calls)
	}
	if store.DurableLSN() < uint64(n) {
		t.Fatalf("expected all %d records durable, durableLSN=%d", n, store.DurableLSN())
	}
}

// TestWaitDurablePropagatesSyncError ensures a failing fsync surfaces to the caller.
func TestWaitDurablePropagatesSyncError(t *testing.T) {
	store := newCountingStorage()
	store.failSync = errors.New("disk on fire")
	w := NewWALWithStorage(store, "group")

	lsn, err := w.AppendPut("k", "v")
	if err != nil {
		t.Fatalf("AppendPut failed: %v", err)
	}
	if err := w.WaitDurable(lsn); err == nil {
		t.Fatal("expected WaitDurable to return the sync error, got nil")
	}
}

// TestSyncModeFsyncsEveryWrite verifies "sync" mode does one fsync per write
// (no batching), as a baseline contrast to group commit.
func TestSyncModeFsyncsEveryWrite(t *testing.T) {
	const n = 5
	store := newCountingStorage()
	w := NewWALWithStorage(store, "sync")

	for i := 0; i < n; i++ {
		if err := w.WritePut("k", "v"); err != nil {
			t.Fatalf("WritePut failed: %v", err)
		}
	}

	if calls := store.syncCalls(); calls != n {
		t.Fatalf("expected %d syncs in sync mode, got %d", n, calls)
	}
}
