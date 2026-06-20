package engine

import (
	"fmt"
	"nosqlEngine/src/utils"
)

type RangeIterator struct {
	data    [][]string
	index   int
	stopped bool
}

func NewRangeIterator(results map[string]string) *RangeIterator {
	iterator_data := SortKeysAndVals(results)
	return &RangeIterator{data: iterator_data, index: 0, stopped: false}
}

func (ri *RangeIterator) Next() (string, string, bool) {
	if ri.stopped || ri.index >= len(ri.data) {
		return "", "", false
	}

	key := ri.data[ri.index][0]
	value := ri.data[ri.index][1]
	ri.index++

	hasNext := ri.index < len(ri.data)
	return key, value, hasNext
}

func (ri *RangeIterator) Stop() {
	ri.stopped = true
}

func (ri *RangeIterator) Reset() {
	ri.index = 0
	ri.stopped = false
}

func (ri *RangeIterator) HasNext() bool {
	return !ri.stopped && ri.index < len(ri.data)
}

func (engine *Engine) RangeIterate(user string, start string, end string) (*RangeIterator, error) {
	if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
		return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
	}
	results, err := engine.findAllRangeMatches(start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to find range matches: %w", err)
	}
	return NewRangeIterator(results), nil
}

func (engine *Engine) RangeScan(user string, start string, end string, pageNum int, pageSize int) [][]string {
	results, _ := engine.findAllRangeMatches(start, end)

	sorted := SortKeysAndVals(results)
	return sorted[min(len(sorted), (pageNum-1)*pageSize):min(len(sorted), pageNum*pageSize)]
}

func (engine *Engine) findAllRangeMatches(start string, end string) (map[string]string, error) {
	results := make(map[string]string)

	inRange := func(key string) bool {
		return key >= start && key <= end
	}

	for _, kv := range engine.loadActiveMem().ToRaw() {
		if inRange(kv.GetKey()) {
			results[kv.GetKey()] = kv.GetValue()
		}
	}
	for _, im := range engine.immQueue.Snapshot() {
		for _, kv := range im.ToRaw() {
			if inRange(kv.GetKey()) {
				if _, seen := results[kv.GetKey()]; !seen {
					results[kv.GetKey()] = kv.GetValue()
				}
			}
		}
	}

	for level := 0; level < CONFIG.LSMLevels; level++ {
		for _, path := range utils.ListSSTablesInLevel(engine.dataRoot, level) {
			reader, err := engine.tableCache.GetOrOpen(path)
			if err != nil {
				continue
			}
			ssResults, err := reader.RangeScan(start, end)
			if err != nil {
				continue
			}
			for key, value := range ssResults {
				if _, exists := results[key]; !exists {
					results[key] = value
				}
			}
		}
	}

	return results, nil
}
