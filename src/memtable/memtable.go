package memtable

import "nosqlEngine/src/models/key_value"

type Memtable interface {
	Add(key string, value string) bool
	Get(key string) (string, bool)
	ToRaw() []key_value.KeyValue
	GetSize() int
	Clear() bool
}

func entryBytes(key, value string) int {
	return len(key) + len(value)
}
