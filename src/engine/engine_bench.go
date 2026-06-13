package engine

import (
	"fmt"
	"os"
	"path/filepath"

	walconfig "nosqlEngine/src/wal/config"
	"nosqlEngine/src/wal"
	"nosqlEngine/src/wal/storage"
)

// NewBenchEngine creates an Engine rooted at benchDir with isolated WAL and
// SSTable directories. Rate limiting is disabled so write throughput is not
// capped by the token bucket during benchmarks.
func NewBenchEngine(benchDir string) (*Engine, error) {
	walDir := filepath.Join(benchDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	for level := 0; level < CONFIG.LSMLevels; level++ {
		levelDir := filepath.Join(benchDir, "sstable", fmt.Sprintf("lvl%d", level))
		if err := os.MkdirAll(levelDir, 0755); err != nil {
			return nil, fmt.Errorf("create sstable lvl%d dir: %w", level, err)
		}
	}

	cfg := walconfig.Get()
	store, err := storage.NewAppendStorageInDir(walDir, cfg.WALSegmentSize, cfg.WALWriteBufferSize)
	if err != nil {
		return nil, fmt.Errorf("create bench wal storage: %w", err)
	}

	walInstance := wal.NewWALWithStorage(store, cfg.WALSyncMode)
	return newEngine(benchDir, walInstance, true), nil
}
