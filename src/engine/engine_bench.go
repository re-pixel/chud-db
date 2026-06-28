package engine

import (
	"fmt"
	"path/filepath"

	"nosqlEngine/src/wal"
)

// NewBenchEngine creates an Engine rooted at benchDir with isolated WAL and
// SSTable directories. Rate limiting is disabled so write throughput is not
// capped by the token bucket during benchmarks.
func NewBenchEngine(benchDir string) (*Engine, error) {
	if err := prepareEngineDirs(benchDir); err != nil {
		return nil, err
	}
	walInstance, err := wal.NewWALInDir(filepath.Join(benchDir, "wal"))
	if err != nil {
		return nil, fmt.Errorf("create bench wal storage: %w", err)
	}

	return newEngine(benchDir, walInstance, true, true), nil
}
