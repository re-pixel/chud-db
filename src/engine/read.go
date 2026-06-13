package engine

import (
	"fmt"
)

func (engine *Engine) Read(user string, key string) (string, bool, error) {
	// Read from memtables
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return "", false, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}
	for _, mem := range engine.memtables {
		if value, ok := mem.Get(key); ok {
			// Found in memtable, return value
			return value, true, nil
		}
	}
	return engine.entryRetriever.RetrieveEntry(key)
}
