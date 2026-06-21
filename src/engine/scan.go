package engine

import (
	"nosqlEngine/src/sstable"
	"sort"
)

// Iterator is a stateful cursor over a sorted result set produced by a prefix
// or range scan.
type Iterator struct {
	data    [][]string
	index   int
	stopped bool
}

func newIterator(results map[string]string) *Iterator {
	return &Iterator{data: sortedPairs(results)}
}

func (it *Iterator) Next() (string, string, bool) {
	if it.stopped || it.index >= len(it.data) {
		return "", "", false
	}
	key := it.data[it.index][0]
	value := it.data[it.index][1]
	it.index++
	return key, value, it.index < len(it.data)
}

func (it *Iterator) Stop() { it.stopped = true }

func (it *Iterator) Reset() {
	it.index = 0
	it.stopped = false
}

func (it *Iterator) HasNext() bool {
	return !it.stopped && it.index < len(it.data)
}

// sortedPairs converts a result map to a lexicographically sorted slice of
// [key, value] pairs.
func sortedPairs(data map[string]string) [][]string {
	pairs := make([][]string, 0, len(data))
	for k, v := range data {
		pairs = append(pairs, []string{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i][0] < pairs[j][0]
	})
	return pairs
}

// scan merges results from the active memtable, immutable queue, and SSTables.
// keyPred selects matching keys; sstScan is the SSTableReader method to call
// for each table.
//
// A seen map tracks every key that has already been resolved (including
// tombstones), so a DELETE in a newer layer correctly blocks an older live
// value in a lower-level SSTable from surfacing.
func (engine *Engine) scan(
	keyPred func(key string) bool,
	sstScan func(*sstable.SSTableReader) (map[string]string, error),
) (map[string]string, error) {
	results := make(map[string]string)
	seen := make(map[string]struct{})

	engine.loadActiveMem().Scan(keyPred, func(key, value string) {
		seen[key] = struct{}{}
		if value != CONFIG.Tombstone {
			results[key] = value
		}
	})

	for _, im := range engine.immQueue.Snapshot() {
		im.Scan(keyPred, func(key, value string) {
			if _, ok := seen[key]; ok {
				return
			}
			seen[key] = struct{}{}
			if value != CONFIG.Tombstone {
				results[key] = value
			}
		})
	}

	versions, unlock := engine.lockVersions()
	defer unlock()
	for i, paths := range versions {
		ordered := paths
		if i == 0 {
			ordered = reversedPaths(paths)
		}
		for _, path := range ordered {
			reader, err := engine.tableCache.GetOrOpen(path)
			if err != nil {
				continue
			}
			ssResults, err := sstScan(reader)
			if err != nil {
				continue
			}
			for key, value := range ssResults {
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				if value != CONFIG.Tombstone {
					results[key] = value
				}
			}
		}
	}

	return results, nil
}
