package memtable

import "nosqlEngine/src/models/key_value"

type Memtable interface {
	Add(key string, value string) bool
	Get(key string) (string, bool)
	// Scan calls fn for every entry — including tombstones — whose key
	// satisfies pred. It avoids copying the full dataset.
	Scan(pred func(key string) bool, fn func(key, value string))
	ToRaw() []key_value.KeyValue
	GetSize() int
	Clear() bool
	TakeSnapshot() []key_value.KeyValue
}

func entryBytes(key, value string) int {
	return len(key) + len(value)
}
