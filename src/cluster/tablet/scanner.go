package tablet

import (
	"errors"
	"fmt"
	"sort"
)

var ErrCannotSplit = errors.New("range cannot be split")

type RawEntry struct {
	Key   string
	Value string
}

type RangeScanner interface {
	ScanRawRange(start, end string) ([]RawEntry, error)
}

type Store interface {
	RangeScanner
}

func EstimateSize(scanner RangeScanner, start, end string) (int64, error) {
	entries, err := scanner.ScanRawRange(start, end)
	if err != nil {
		return 0, fmt.Errorf("scan range [%q, %q): %w", start, end, err)
	}
	var size int64
	for _, entry := range entries {
		size += int64(len(entry.Key) + len(entry.Value))
	}
	return size, nil
}

func PickSplitBoundary(scanner RangeScanner, start, end string) (string, error) {
	entries, err := scanner.ScanRawRange(start, end)
	if err != nil {
		return "", fmt.Errorf("scan range [%q, %q): %w", start, end, err)
	}
	if len(entries) < 2 {
		return "", ErrCannotSplit
	}

	sorted := append([]RawEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	var total int64
	for _, entry := range sorted {
		total += entrySize(entry)
	}

	var prefix int64
	var boundary string
	var bestDifference int64
	found := false
	for i := 1; i < len(sorted); i++ {
		prefix += entrySize(sorted[i-1])
		if sorted[i-1].Key == sorted[i].Key {
			continue
		}
		difference := total - 2*prefix
		if difference < 0 {
			difference = -difference
		}
		if !found || difference < bestDifference {
			boundary = sorted[i].Key
			bestDifference = difference
			found = true
		}
	}
	if !found {
		return "", ErrCannotSplit
	}
	return boundary, nil
}

func entrySize(entry RawEntry) int64 {
	return int64(len(entry.Key) + len(entry.Value))
}
