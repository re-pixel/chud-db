package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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
