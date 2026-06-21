package engine

import (
	"fmt"
	"os"
)

type SnapshotFile struct {
	Level int
	Path  string
	Size  int64
}

type Snapshot struct {
	LSN      uint64
	SSTables []SnapshotFile
	release  func()
}

func (s *Snapshot) Release() {
	if s.release != nil {
		s.release()
		s.release = nil
	}
}

func (engine *Engine) TakeSnapshot() (*Snapshot, error) {
	engine.drainActiveMem()
	engine.WaitForPendingFlushes()

	engine.versionMu.RLock()
	lsn := engine.wal.AppendedLSN()
	var files []SnapshotFile
	for level, paths := range engine.versions {
		for _, path := range paths {
			info, err := os.Stat(path)
			if err != nil {
				engine.versionMu.RUnlock()
				return nil, fmt.Errorf("snapshot: stat %s: %w", path, err)
			}
			engine.pinFile(path)
			files = append(files, SnapshotFile{Level: level, Path: path, Size: info.Size()})
		}
	}
	engine.versionMu.RUnlock()

	return &Snapshot{
		LSN:      lsn,
		SSTables: files,
		release:  func() { engine.unpinFiles(files) },
	}, nil
}
