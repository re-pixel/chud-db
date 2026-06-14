package memtable

import appconfig "nosqlEngine/src/config"

var factoryConfig = appconfig.GetConfig()

const defaultBTreeOrder = 3

// NewMemtable returns a thread-safe memtable selected by MEMTABLE_TYPE in config.
func NewMemtable() Memtable {
	return NewSyncMemtable(newMemtableImpl())
}

func newMemtableImpl() Memtable {
	switch factoryConfig.MemtableType {
	case "skiplist":
		levels := factoryConfig.SkipListLevels
		if levels < 1 {
			levels = 4
		}
		return NewSkipList(levels)
	case "btree", "b-tree":
		return NewBTree(defaultBTreeOrder)
	default:
		return NewHashMap()
	}
}
