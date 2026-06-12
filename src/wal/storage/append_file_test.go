package storage

import (
	"bytes"
	"io"
	"nosqlEngine/src/wal"
	"os"
	"testing"
)

func newTestStorage(t *testing.T, segmentSize int64) AppendStorage {
	t.Helper()
	store, err := NewAppendStorageInDir(t.TempDir(), segmentSize, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
	return store
}

func readAllRecords(t *testing.T, store AppendStorage) []wal.Record {
	t.Helper()
	segments, err := store.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments failed: %v", err)
	}

	var records []wal.Record
	for _, segment := range segments {
		reader, err := store.OpenSegmentReader(segment.ID)
		if err != nil {
			t.Fatalf("OpenSegmentReader failed: %v", err)
		}
		for {
			record, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next failed: %v", err)
			}
			records = append(records, record)
		}
	}
	return records
}

func TestAppendAndSync(t *testing.T) {
	dir := t.TempDir()
	store, err := NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	if _, err := store.Append(wal.OpPut, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	records := readAllRecords(t, reopened)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].LSN != 1 {
		t.Fatalf("LSN mismatch: got %d want 1", records[0].LSN)
	}
	if !bytes.Equal(records[0].Key, []byte("key1")) {
		t.Fatalf("key mismatch: got %q", records[0].Key)
	}
	if !bytes.Equal(records[0].Value, []byte("value1")) {
		t.Fatalf("value mismatch: got %q", records[0].Value)
	}
}

func TestSegmentRotation(t *testing.T) {
	store := newTestStorage(t, 256)

	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		value := []byte("value-with-enough-bytes-to-grow-segment")
		if _, err := store.Append(wal.OpPut, key, value); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	segments, err := store.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments failed: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("expected at least 2 segments, got %d", len(segments))
	}
}

func TestReplayOrdering(t *testing.T) {
	store := newTestStorage(t, 256)

	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		value := []byte("value-with-enough-bytes-to-grow-segment")
		if _, err := store.Append(wal.OpPut, key, value); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	records := readAllRecords(t, store)
	if len(records) != 20 {
		t.Fatalf("expected 20 records, got %d", len(records))
	}
	for i := 1; i < len(records); i++ {
		if records[i].LSN <= records[i-1].LSN {
			t.Fatalf("LSN not increasing at index %d: %d then %d", i, records[i-1].LSN, records[i].LSN)
		}
	}
}

func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	if _, err := store.Append(wal.OpPut, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("first Append failed: %v", err)
	}
	if _, err := store.Append(wal.OpPut, []byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("second Append failed: %v", err)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	segment := store.ActiveSegment()
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	info, err := os.Stat(segment.Path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if err := os.Truncate(segment.Path, info.Size()-5); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	reader, err := openSegmentReader(segment.Path)
	if err != nil {
		t.Fatalf("openSegmentReader failed: %v", err)
	}
	defer reader.Close()

	if _, err := reader.Next(); err != nil {
		t.Fatalf("expected first record to read, got %v", err)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("expected EOF on torn tail, got %v", err)
	}
}

func TestCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	store, err := NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	if _, err := store.Append(wal.OpPut, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("first Append failed: %v", err)
	}
	if _, err := store.Append(wal.OpPut, []byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("second Append failed: %v", err)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	segment := store.ActiveSegment()
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := os.ReadFile(segment.Path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(segment.Path, data, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	reader, err := openSegmentReader(segment.Path)
	if err != nil {
		t.Fatalf("openSegmentReader failed: %v", err)
	}
	defer reader.Close()

	if _, err := reader.Next(); err != nil {
		t.Fatalf("expected first record to read, got %v", err)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("expected EOF on corrupt record, got %v", err)
	}
}

func TestReopenWithEmptyLastSegment(t *testing.T) {
	dir := t.TempDir()

	store, err := NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}
	if _, err := store.Append(wal.OpPut, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err := os.Create(SegmentPathInDir(dir, 2)); err != nil {
		t.Fatalf("Create empty segment failed: %v", err)
	}

	reopened, err := NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	lsn, err := reopened.Append(wal.OpPut, []byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Append after reopen failed: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("expected next LSN 2, got %d", lsn)
	}
}
