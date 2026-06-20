package engine

import (
	"fmt"
	"nosqlEngine/src/utils"
)

func (engine *Engine) Read(user string, key string) (string, bool, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return "", false, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}

	if value, ok := engine.loadActiveMem().Get(key); ok {
		return value, true, nil
	}

	for _, im := range engine.immQueue.Snapshot() {
		if value, ok := im.Get(key); ok {
			return value, true, nil
		}
	}

	return engine.readFromSSTables(key)
}

func (engine *Engine) readFromSSTables(key string) (string, bool, error) {
	for level := 0; level < CONFIG.LSMLevels; level++ {
		for _, path := range utils.ListSSTablesInLevel(engine.dataRoot, level) {
			reader, err := engine.tableCache.GetOrOpen(path)
			if err != nil {
				continue
			}
			if v, ok, err := reader.Get(key); err == nil && ok {
				return v, true, nil
			}
		}
	}
	return "", false, nil
}
