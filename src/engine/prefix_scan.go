package engine

import (
	"fmt"
	"sort"
)

type PrefixIterator struct {
	data    [][]string
	index   int
	stopped bool
}

func NewPrefixIterator(results map[string]string) *PrefixIterator {
	iterator_data := SortKeysAndVals(results)
	return &PrefixIterator{data: iterator_data, index: 0, stopped: false}
}

func (pi *PrefixIterator) Next() (string, string, bool) {
	if pi.stopped || pi.index >= len(pi.data) {
		return "", "", false
	}

	key := pi.data[pi.index][0]
	value := pi.data[pi.index][1]
	pi.index++

	hasNext := pi.index < len(pi.data)
	return key, value, hasNext
}

func (pi *PrefixIterator) Stop() {
	pi.stopped = true
}

func (pi *PrefixIterator) Reset() {
	pi.index = 0
	pi.stopped = false
}

func (pi *PrefixIterator) HasNext() bool {
	return !pi.stopped && pi.index < len(pi.data)
}

func (engine *Engine) PrefixIterate(user string, prefix string) (*PrefixIterator, error) {
	if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
		return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
	}
	results, err := engine.findAllPrefixMatches(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to find prefix matches: %w", err)
	}
	return NewPrefixIterator(results), nil
}

func SortKeysAndVals(data map[string]string) [][]string {
	result_array := make([][]string, 0)
	for key, value := range data {
		tmp := []string{key, value}
		result_array = append(result_array, tmp)
	}

	sort.Slice(result_array, func(i, j int) bool {
		if len(result_array[i]) <= 1 || len(result_array[j]) <= 1 {
			return len(result_array[i]) < len(result_array[j])
		}
		return result_array[i][0] < result_array[j][0]
	})
	return result_array
}

func (engine *Engine) PrefixScan(user string, prefix string, pageNum int, pageSize int) [][]string {
	results, _ := engine.findAllPrefixMatches(prefix)

	sorted := SortKeysAndVals(results)
	return sorted[min(len(sorted), (pageNum-1)*pageSize):min(len(sorted), pageNum*pageSize)]
}

func (engine *Engine) findAllPrefixMatches(prefix string) (map[string]string, error) {
	results := make(map[string]string)

	matchPrefix := func(key string) bool {
		return len(key) >= len(prefix) && key[:len(prefix)] == prefix
	}

	for _, kv := range engine.loadActiveMem().ToRaw() {
		if matchPrefix(kv.GetKey()) && kv.GetValue() != CONFIG.Tombstone {
			results[kv.GetKey()] = kv.GetValue()
		}
	}
	for _, im := range engine.immQueue.Snapshot() {
		for _, kv := range im.ToRaw() {
			if matchPrefix(kv.GetKey()) {
				if _, seen := results[kv.GetKey()]; !seen && kv.GetValue() != CONFIG.Tombstone {
					results[kv.GetKey()] = kv.GetValue()
				}
			}
		}
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
			ssResults, err := reader.PrefixScan(prefix)
			if err != nil {
				continue
			}
			for key, value := range ssResults {
				if _, exists := results[key]; !exists && value != CONFIG.Tombstone {
					results[key] = value
				}
			}
		}
	}

	return results, nil
}
