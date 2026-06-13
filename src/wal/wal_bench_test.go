package wal

import (
	"fmt"
	"sync/atomic"
	"testing"

	"nosqlEngine/src/wal/storage"
)

func setupBenchWAL(b *testing.B) *WAL {
	b.Helper()
	store, err := storage.NewAppendStorageInDir(b.TempDir(), 1<<20, 65536)
	if err != nil {
		b.Fatalf("NewAppendStorageInDir: %v", err)
	}
	return NewWALWithStorage(store, "group")
}

func BenchmarkWALAppendPutSequential(b *testing.B) {
	w := setupBenchWAL(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("wal-key-%d", i)
		if _, err := w.AppendPut(key, "value"); err != nil {
			b.Fatalf("AppendPut: %v", err)
		}
	}
}

func BenchmarkWALAppendPutWaitDurableSequential(b *testing.B) {
	w := setupBenchWAL(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("wal-key-%d", i)
		lsn, err := w.AppendPut(key, "value")
		if err != nil {
			b.Fatalf("AppendPut: %v", err)
		}
		if err := w.WaitDurable(lsn); err != nil {
			b.Fatalf("WaitDurable: %v", err)
		}
	}
}

func BenchmarkWALAppendPutWaitDurableParallel(b *testing.B) {
	w := setupBenchWAL(b)
	var seq atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := seq.Add(1)
			key := fmt.Sprintf("wal-key-%d", i)
			lsn, err := w.AppendPut(key, "value")
			if err != nil {
				b.Fatalf("AppendPut: %v", err)
			}
			if err := w.WaitDurable(lsn); err != nil {
				b.Fatalf("WaitDurable: %v", err)
			}
		}
	})
}

func BenchmarkWALWritePutSequential(b *testing.B) {
	w := setupBenchWAL(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("wal-key-%d", i)
		if err := w.WritePut(key, "value"); err != nil {
			b.Fatalf("WritePut: %v", err)
		}
	}
}
