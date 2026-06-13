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
		eng.WaitFlushIdle()
		time.Sleep(200 * time.Millisecond)
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
		eng.WaitFlushIdle()
	}
	reportOpsPerSec(b, "puts/sec")
}

func BenchmarkPutParallel(b *testing.B) {
	// engine.Write mutates the memtable without synchronization; parallel PUT
	// is covered at the WAL layer in BenchmarkWALAppendPutWaitDurableParallel.
	b.Skip("engine.Write is not goroutine-safe; use src/wal WAL parallel benchmarks for group commit")
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

	// Two entries fill the byte-sized memtable (MEMTABLE_SIZE=20) and trigger flush.
	keys := []string{"key1", "key2"}
	vals := []string{"value1", "value2"}
	for i, key := range keys {
		if err := eng.Write(benchUser, key, vals[i], false); err != nil {
			b.Fatalf("preload Write: %v", err)
		}
	}
	eng.WaitFlushIdle()
	waitForSSTable(b, dir)

	// Clear hot keys from memtable so reads fall through to SSTables.
	if err := eng.Write(benchUser, "key3", "value3", false); err != nil {
		b.Fatalf("memtable clear Write: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		want := vals[i%len(vals)]
		got, found, err := eng.Read(benchUser, key)
		if err != nil {
			b.Fatalf("Read: %v", err)
		}
		if !found || got != want {
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
		eng.WaitFlushIdle()
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
			eng.WaitFlushIdle()
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
