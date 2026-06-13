package wal

import (
	"io"
	"nosqlEngine/src/wal/record"
	"nosqlEngine/src/wal/storage"
	"testing"
)

func readRecords(t *testing.T, store storage.AppendStorage) []record.Record {
	t.Helper()
	segments, err := store.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments failed: %v", err)
	}

	var records []record.Record
	for _, segment := range segments {
		reader, err := store.OpenSegmentReader(segment.ID)
		if err != nil {
			t.Fatalf("OpenSegmentReader failed: %v", err)
		}
		for {
			rec, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next failed: %v", err)
			}
			records = append(records, rec)
		}
	}
	return records
}

func TestWritePutFlush(t *testing.T) {
	store, err := storage.NewAppendStorageInDir(t.TempDir(), 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	w := NewWALWithStorage(store, "batch")
	if err := w.WritePut("key1", "value1"); err != nil {
		t.Fatalf("WritePut failed: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	records := readRecords(t, store)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Op != record.OpPut {
		t.Fatalf("expected put op, got %d", records[0].Op)
	}
	if string(records[0].Key) != "key1" {
		t.Fatalf("key mismatch: got %q", records[0].Key)
	}
	if string(records[0].Value) != "value1" {
		t.Fatalf("value mismatch: got %q", records[0].Value)
	}
}

func TestWriteDeleteFlush(t *testing.T) {
	store, err := storage.NewAppendStorageInDir(t.TempDir(), 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	w := NewWALWithStorage(store, "batch")
	if err := w.WriteDelete("key1"); err != nil {
		t.Fatalf("WriteDelete failed: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	records := readRecords(t, store)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Op != record.OpDelete {
		t.Fatalf("expected delete op, got %d", records[0].Op)
	}
	if string(records[0].Key) != "key1" {
		t.Fatalf("key mismatch: got %q", records[0].Key)
	}
	if len(records[0].Value) != 0 {
		t.Fatalf("expected empty value for delete, got %q", records[0].Value)
	}
}

func TestSyncModeSync(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}
	w := NewWALWithStorage(store, "sync")
	if err := w.WritePut("key1", "value1"); err != nil {
		t.Fatalf("WritePut failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := storage.NewAppendStorageInDir(dir, 1<<20, 64)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	records := readRecords(t, reopened)
	if len(records) != 1 {
		t.Fatalf("expected 1 durable record, got %d", len(records))
	}
}

func TestPurge(t *testing.T) {
	store, err := storage.NewAppendStorageInDir(t.TempDir(), 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	w := NewWALWithStorage(store, "batch")
	if err := w.WritePut("key1", "value1"); err != nil {
		t.Fatalf("WritePut failed: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := w.Purge(); err != nil {
		t.Fatalf("Purge failed: %v", err)
	}

	if records := readRecords(t, store); len(records) != 0 {
		t.Fatalf("expected no records after purge, got %d", len(records))
	}
}
