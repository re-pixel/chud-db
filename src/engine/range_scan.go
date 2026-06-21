package engine

import (
	"fmt"
	"nosqlEngine/src/sstable"
)

func (engine *Engine) RangeIterate(user string, start string, end string) (*Iterator, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}
	results, err := engine.findAllRangeMatches(start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to find range matches: %w", err)
	}
	return newIterator(results), nil
}

func (engine *Engine) RangeScan(user string, start string, end string, pageNum int, pageSize int) ([][]string, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}
	results, err := engine.findAllRangeMatches(start, end)
	if err != nil {
		return nil, err
	}
	sorted := sortedPairs(results)
	lo := min(len(sorted), (pageNum-1)*pageSize)
	hi := min(len(sorted), pageNum*pageSize)
	return sorted[lo:hi], nil
}

func (engine *Engine) findAllRangeMatches(start, end string) (map[string]string, error) {
	return engine.scan(
		func(key string) bool { return key >= start && key <= end },
		func(r *sstable.SSTableReader) (map[string]string, error) { return r.RangeScan(start, end) },
	)
}
