package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// getPaths returns the paths of all the files in the sstable folder
func getProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	// Go up from src/utils/reusable.go to project root
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(filename)))
	return projectRoot
}

func DefaultDataRoot() string {
	return filepath.Join(getProjectRoot(), "data")
}

func SSTableLevelDir(dataRoot string, level int) string {
	return filepath.Join(dataRoot, "sstable", fmt.Sprintf("lvl%d", level))
}

func ListSSTablesInLevel(dataRoot string, level int) []string {
	dir := SSTableLevelDir(dataRoot, level)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var paths []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}
		paths = append(paths, filepath.Join(dir, entry.Name()))
	}
	return paths
}

func GetPaths(relativePath string, ext string) []string {
	folderPath := filepath.Join(getProjectRoot(), relativePath)
	var paths []string
	files, _ := os.ReadDir(folderPath)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ext) {
			continue
		}
		paths = append(paths, filepath.Join(folderPath, file.Name()))
	}
	return paths
}
