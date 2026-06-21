package engine

import "fmt"

func (engine *Engine) Read(user string, key string) (string, bool, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return "", false, fmt.Errorf("user %s is not allowed to read: %w", user, err)
		}
	}

	if value, ok := engine.loadActiveMem().Get(key); ok {
		if value == CONFIG.Tombstone {
			return "", false, nil
		}
		return value, true, nil
	}

	for _, im := range engine.immQueue.Snapshot() {
		if value, ok := im.Get(key); ok {
			if value == CONFIG.Tombstone {
				return "", false, nil
			}
			return value, true, nil
		}
	}

	return engine.readFromSSTables(key)
}

func (engine *Engine) readFromSSTables(key string) (string, bool, error) {
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
			if v, ok, err := reader.Get(key); err == nil && ok {
				if v == CONFIG.Tombstone {
					return "", false, nil
				}
				return v, true, nil
			}
		}
	}
	return "", false, nil
}
