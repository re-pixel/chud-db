package wal

import (
	"nosqlEngine/src/wal/record"
	"nosqlEngine/src/wal/storage"
	"testing"
)

func TestReplayPutDelete(t *testing.T) {
	store, err := storage.NewAppendStorageInDir(t.TempDir(), 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	w := NewWALWithStorage(store, "group")
	if err := w.WritePut("key1", "value1"); err != nil {
		t.Fatalf("WritePut failed: %v", err)
	}
	if err := w.WriteDelete("key2"); err != nil {
		t.Fatalf("WriteDelete failed: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].LSN != 1 || entries[0].Op != record.OpPut {
		t.Fatalf("unexpected first entry: %+v", entries[0])
	}
	if entries[0].Key != "key1" || entries[0].Value != "value1" {
		t.Fatalf("unexpected first entry data: %+v", entries[0])
	}
	if entries[1].LSN != 2 || entries[1].Op != record.OpDelete {
		t.Fatalf("unexpected second entry: %+v", entries[1])
	}
	if entries[1].Key != "key2" || entries[1].Value != "" {
		t.Fatalf("unexpected second entry data: %+v", entries[1])
	}
}

func TestReplayMultiSegment(t *testing.T) {
	store, err := storage.NewAppendStorageInDir(t.TempDir(), 256, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	w := NewWALWithStorage(store, "group")
	for i := 0; i < 20; i++ {
		key := string([]byte{'a' + byte(i)})
		value := "value-with-enough-bytes-to-grow-segment"
		if err := w.WritePut(key, value); err != nil {
			t.Fatalf("WritePut failed: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	segments, err := store.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments failed: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("expected at least 2 segments, got %d", len(segments))
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if len(entries) != 20 {
		t.Fatalf("expected 20 entries, got %d", len(entries))
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].LSN <= entries[i-1].LSN {
			t.Fatalf("LSN not increasing at index %d", i)
		}
	}
}

func TestReplayFunc(t *testing.T) {
	store, err := storage.NewAppendStorageInDir(t.TempDir(), 1<<20, 64)
	if err != nil {
		t.Fatalf("NewAppendStorageInDir failed: %v", err)
	}

	w := NewWALWithStorage(store, "group")
	if err := w.WritePut("key1", "value1"); err != nil {
		t.Fatalf("WritePut failed: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	var count int
	err = w.ReplayFunc(func(entry Entry) error {
		count++
		if entry.Key != "key1" {
			t.Fatalf("unexpected key: %q", entry.Key)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayFunc failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected callback once, got %d", count)
	}
}
