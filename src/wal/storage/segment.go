package storage

import (
	"fmt"
	"nosqlEngine/src/wal"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	// deferred for later: resolve this in config
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filename))))
}

func WalDir() string {
	cfg := wal.GetWalConfig()
	return filepath.Join(projectRoot(), cfg.WALDir)
}

func SegmentPath(id uint64) string {
	return filepath.Join(WalDir(), fmt.Sprintf("%09d.log", id))
}

func EnsureWalDir() error {
	return os.MkdirAll(WalDir(), 0755)
}

func parseSegmentID(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	stem := strings.TrimSuffix(name, ".log")
	if len(stem) != 9 {
		return 0, false
	}
	for _, c := range stem {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	id, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func ListSegments() ([]SegmentInfo, error) {
	dir := WalDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []SegmentInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		id, ok := parseSegmentID(entry.Name())
		if !ok {
			continue
		}
		segments = append(segments, SegmentInfo{
			ID:   id,
			Path: filepath.Join(dir, entry.Name()),
		})
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID < segments[j].ID
	})
	return segments, nil
}

func NextSegmentID() (uint64, error) {
	segments, err := ListSegments()
	if err != nil {
		return 0, err
	}
	if len(segments) == 0 {
		return 1, nil
	}
	return segments[len(segments)-1].ID + 1, nil
}
