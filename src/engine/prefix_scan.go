package engine

import (
	"fmt"
	"nosqlEngine/src/sstable"
	"strings"
)

func (engine *Engine) PrefixIterate(user string, prefix string) (*Iterator, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}
	results, err := engine.findAllPrefixMatches(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to find prefix matches: %w", err)
	}
	return newIterator(results), nil
}

func (engine *Engine) PrefixScan(user string, prefix string, pageNum int, pageSize int) ([][]string, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}
	results, err := engine.findAllPrefixMatches(prefix)
	if err != nil {
		return nil, err
	}
	sorted := sortedPairs(results)
	lo := min(len(sorted), (pageNum-1)*pageSize)
	hi := min(len(sorted), pageNum*pageSize)
	return sorted[lo:hi], nil
}

func (engine *Engine) findAllPrefixMatches(prefix string) (map[string]string, error) {
	return engine.scan(
		func(key string) bool { return strings.HasPrefix(key, prefix) },
		func(r *sstable.SSTableReader) (map[string]string, error) { return r.PrefixScan(prefix) },
	)
}
