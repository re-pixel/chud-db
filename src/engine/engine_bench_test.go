package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWALReplayInlineFlush(t *testing.T) {
	benchDir := t.TempDir()

	const entries = 30 // well above MEMTABLE_SIZE=20
	eng1, err := NewBenchEngine(benchDir)
	if err != nil {
		t.Fatalf("NewBenchEngine: %v", err)
	}
	eng1.Start()
	keys := make([]string, entries)
	vals := make([]string, entries)
	for i := range entries {
		keys[i] = fmt.Sprintf("rk%d", i)
		vals[i] = fmt.Sprintf("rv%d", i)
		if err := eng1.Write("", keys[i], vals[i], false); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}
	eng2, err := NewBenchEngine(benchDir)
	if err != nil {
		t.Fatalf("NewBenchEngine restart: %v", err)
	}
	eng2.Start()
	defer eng2.Shut()

	for i := range entries {
		got, ok, err := eng2.Read("", keys[i])
		if err != nil {
			t.Fatalf("Read(%q) after restart: %v", keys[i], err)
		}
		if !ok {
			t.Fatalf("key %q not found after restart", keys[i])
		}
		if got != vals[i] {
			t.Fatalf("key %q: got %q, want %q", keys[i], got, vals[i])
		}
	}
}

func TestBenchEngineUsesIsolatedDirs(t *testing.T) {
	benchDir := t.TempDir()

	eng, err := NewBenchEngine(benchDir)
	if err != nil {
		t.Fatalf("NewBenchEngine failed: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	if err := eng.Write("bench", "key1", "value1", false); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	walDir := filepath.Join(benchDir, "wal")
	walEntries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	if len(walEntries) == 0 {
		t.Fatal("expected WAL segment files under bench dir")
	}

	for level := 0; level < CONFIG.LSMLevels; level++ {
		levelDir := filepath.Join(benchDir, "sstable", fmt.Sprintf("lvl%d", level))
		if _, err := os.Stat(levelDir); err != nil {
			t.Fatalf("expected sstable level dir %s: %v", levelDir, err)
		}
	}
}

func TestNewEngineInDirUsesIsolatedDirs(t *testing.T) {
	dataRoot := t.TempDir()

	eng, err := NewEngineInDir(dataRoot)
	if err != nil {
		t.Fatalf("NewEngineInDir failed: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	if err := eng.Write("node", "key1", "value1", false); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	walDir := filepath.Join(dataRoot, "wal")
	walEntries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	if len(walEntries) == 0 {
		t.Fatal("expected WAL segment files under data root")
	}

	for level := 0; level < CONFIG.LSMLevels; level++ {
		levelDir := filepath.Join(dataRoot, "sstable", fmt.Sprintf("lvl%d", level))
		if _, err := os.Stat(levelDir); err != nil {
			t.Fatalf("expected sstable level dir %s: %v", levelDir, err)
		}
	}
}

func TestGracefulShutdownFlushesActiveMem(t *testing.T) {
	benchDir := t.TempDir()
	eng, err := NewBenchEngine(benchDir)
	if err != nil {
		t.Fatalf("NewBenchEngine: %v", err)
	}
	eng.Start()

	for _, pair := range [][2]string{{"k1", "v1"}, {"k2", "v2"}} {
		if err := eng.Write("", pair[0], pair[1], false); err != nil {
			t.Fatalf("Write(%q): %v", pair[0], err)
		}
	}

	sstDir := filepath.Join(benchDir, "sstable", "lvl0")
	if hasDBFile(t, sstDir) {
		t.Fatal("expected no SSTable before Shut, but found one")
	}

	if err := eng.Shut(); err != nil {
		t.Fatalf("Shut: %v", err)
	}

	if !hasDBFile(t, sstDir) {
		t.Fatal("Shut did not flush active memtable: no .db file found in sstable/lvl0")
	}
}

func hasDBFile(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", dir, err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".db") {
			return true
		}
	}
	return false
}

func TestBenchEngineSkipsRateLimit(t *testing.T) {
	benchDir := t.TempDir()
	eng, err := NewBenchEngine(benchDir)
	if err != nil {
		t.Fatalf("NewBenchEngine failed: %v", err)
	}
	eng.Start()
	defer eng.Shut()

	user := "bench"
	for i := 0; i < CONFIG.MaxTokens+10; i++ {
		eng.userLimiter.CheckUserTokens(user)
	}
	ok, err := eng.userLimiter.CheckUserTokens(user)
	if ok {
		t.Fatal("expected user limiter to be exhausted before Write")
	}

	if err := eng.Write(user, "after-limit", "v", false); err != nil {
		t.Fatalf("bench engine should skip rate limit: %v", err)
	}
}
