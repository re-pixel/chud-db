package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"nosqlEngine/src/config"
	"nosqlEngine/src/engine"
)

const (
	benchUser = "bench"
	valueSize = 64
)

var (
	cfg        = config.GetConfig()
	benchValue = strings.Repeat("v", valueSize)
)

// MEMTABLE_SIZE counts bytes (sum of key+value lengths), not entry count.
func smallKey(i int) string  { return fmt.Sprintf("k%d", i) }
func smallValue() string     { return "v" }
func smallEntryBytes() int   { return len(smallKey(0)) + len(smallValue()) }

func preloadEntryCount() int {
	n := cfg.MemtableSize / smallEntryBytes()
	if n < 2 {
		n = 2
	}
	return n - 1 // stay below flush threshold
}

func setupBenchEngine(b *testing.B) (*engine.Engine, string) {
	b.Helper()
	dir := b.TempDir()
	eng, err := engine.NewBenchEngine(dir)
	if err != nil {
		b.Fatalf("NewBenchEngine: %v", err)
	}
	eng.Start()
	b.Cleanup(func() {
		if err := eng.Shut(); err != nil {
			b.Errorf("Shut: %v", err)
		}
	})
	return eng, dir
}

func benchKey(prefix string, n int) string {
	return fmt.Sprintf("bench-%s-%d", prefix, n)
}

func waitForSSTable(b *testing.B, benchDir string) {
	b.Helper()
	sstDir := filepath.Join(benchDir, "sstable", "lvl0")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(sstDir)
		if err != nil {
			b.Fatalf("read sstable dir: %v", err)
		}
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) == ".db" {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Fatal("timed out waiting for SSTable flush")
}

func reportOpsPerSec(b *testing.B, unit string) {
	b.Helper()
	if b.N > 0 && b.Elapsed() > 0 {
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), unit)
	}
}

func BenchmarkPutSequential(b *testing.B) {
	eng, _ := setupBenchEngine(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := eng.Write(benchUser, benchKey("put", i), benchValue, false); err != nil {
			b.Fatalf("Write: %v", err)
		}
		eng.WaitForPendingFlushes()
	}
	reportOpsPerSec(b, "puts/sec")
}

// BenchmarkPutThroughput measures raw write throughput without waiting for
// flushes on every iteration. It drains the immutable queue once at the end.
func BenchmarkPutThroughput(b *testing.B) {
	eng, _ := setupBenchEngine(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := eng.Write(benchUser, benchKey("tput", i), benchValue, false); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}
	b.StopTimer()
	eng.WaitForPendingFlushes()
	reportOpsPerSec(b, "puts/sec")
}

// BenchmarkPutThroughputParallel measures concurrent raw write throughput.
func BenchmarkPutThroughputParallel(b *testing.B) {
	eng, _ := setupBenchEngine(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("tput-par-%d-%d", i, b.N)
			if err := eng.Write(benchUser, key, benchValue, false); err != nil {
				b.Errorf("Write: %v", err)
			}
			i++
		}
	})
	b.StopTimer()
	eng.WaitForPendingFlushes()
	reportOpsPerSec(b, "puts/sec")
}

// BenchmarkPutBurst fires N goroutines concurrently in a single burst to
// maximise WAL group-commit batching. Reports total throughput over the burst.
func BenchmarkPutBurst(b *testing.B) {
	const concurrency = 64
	eng, _ := setupBenchEngine(b)

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		errs := make(chan error, concurrency)
		for g := 0; g < concurrency; g++ {
			go func(g int) {
				key := fmt.Sprintf("burst-%d-%d", iter, g)
				errs <- eng.Write(benchUser, key, benchValue, false)
			}(g)
		}
		for g := 0; g < concurrency; g++ {
			if err := <-errs; err != nil {
				b.Errorf("Write: %v", err)
			}
		}
	}
	b.StopTimer()
	eng.WaitForPendingFlushes()
	// Report throughput as total writes / elapsed time.
	b.ReportMetric(float64(b.N*concurrency)/b.Elapsed().Seconds(), "puts/sec")
}

func BenchmarkPutParallel(b *testing.B) {
	eng, _ := setupBenchEngine(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("parallel-key-%d-%d", i, b.N)
			if err := eng.Write(benchUser, key, benchValue, false); err != nil {
				b.Errorf("Write: %v", err)
			}
			i++
		}
	})
	reportOpsPerSec(b, "puts/sec")
}

func BenchmarkGetMemtableHit(b *testing.B) {
	eng, _ := setupBenchEngine(b)

	preloadN := preloadEntryCount()
	keys := make([]string, preloadN)
	val := smallValue()
	for i := 0; i < preloadN; i++ {
		keys[i] = smallKey(i)
		if err := eng.Write(benchUser, keys[i], val, false); err != nil {
			b.Fatalf("preload Write: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%preloadN]
		got, found, err := eng.Read(benchUser, key)
		if err != nil {
			b.Fatalf("Read: %v", err)
		}
		if !found || got != val {
			b.Fatalf("Read miss for %q", key)
		}
	}
	reportOpsPerSec(b, "gets/sec")
}

func BenchmarkGetSSTable(b *testing.B) {
	eng, dir := setupBenchEngine(b)

	// Write enough entries to exceed MEMTABLE_SIZE bytes and trigger a flush.
	// Each entry is smallKey + smallValue bytes; write one more than the threshold.
	flushCount := cfg.MemtableSize/smallEntryBytes() + 1
	keys := make([]string, flushCount)
	val := smallValue()
	for i := 0; i < flushCount; i++ {
		keys[i] = smallKey(i)
		if err := eng.Write(benchUser, keys[i], val, false); err != nil {
			b.Fatalf("preload Write: %v", err)
		}
	}
	eng.WaitForPendingFlushes()
	waitForSSTable(b, dir)

	// Push a few more entries to ensure flushed keys are no longer in the active memtable.
	for i := flushCount; i < flushCount+5; i++ {
		if err := eng.Write(benchUser, smallKey(i), val, false); err != nil {
			b.Fatalf("evict Write: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		got, found, err := eng.Read(benchUser, key)
		if err != nil {
			b.Fatalf("Read: %v", err)
		}
		if !found || got != val {
			b.Fatalf("Read miss for %q (expected SSTable hit)", key)
		}
	}
	reportOpsPerSec(b, "gets/sec")
}

func BenchmarkDeleteSequential(b *testing.B) {
	eng, _ := setupBenchEngine(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey("del", i)
		b.StopTimer()
		if err := eng.Write(benchUser, key, benchValue, false); err != nil {
			b.Fatalf("preload Write: %v", err)
		}
		b.StartTimer()
		if err := eng.Write(benchUser, key, cfg.Tombstone, false); err != nil {
			b.Fatalf("Delete Write: %v", err)
		}
		eng.WaitForPendingFlushes()
	}
	reportOpsPerSec(b, "deletes/sec")
}

func BenchmarkMixedReadWrite(b *testing.B) {
	eng, _ := setupBenchEngine(b)

	var writeSeq atomic.Uint64
	lastKey := smallKey(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			n := writeSeq.Add(1)
			lastKey = smallKey(int(n))
			if err := eng.Write(benchUser, lastKey, smallValue(), false); err != nil {
				b.Fatalf("Write: %v", err)
			}
			eng.WaitForPendingFlushes()
		} else {
			got, found, err := eng.Read(benchUser, lastKey)
			if err != nil {
				b.Fatalf("Read: %v", err)
			}
			if !found || got != smallValue() {
				b.Fatalf("Read miss for %q", lastKey)
			}
		}
	}
}
