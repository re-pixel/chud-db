package engine

import (
	"fmt"
	"nosqlEngine/src/service/retriever"
)

func (engine *Engine) Read(user string, key string) (string, bool, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return "", false, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}

	// Check the mutable active memtable first (most recent data).
	if value, ok := engine.loadActiveMem().Get(key); ok {
		return value, true, nil
	}

	// Then check immutable memtables newest-first (promoted but not yet on disk).
	for _, im := range engine.immQueue.Snapshot() {
		if value, ok := im.Get(key); ok {
			return value, true, nil
		}
	}

	// Allocate a fresh retriever per call so concurrent reads share no state.
	r := retriever.NewEntryRetrieverInDir(engine.block_manager, engine.dataRoot)
	return r.RetrieveEntry(key)
}
