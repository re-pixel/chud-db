package memtable

import (
	appconfig "nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"sync/atomic"
)

var hashMapConfig = appconfig.GetConfig()

type HashMap struct {
	data     map[string]string
	byteSize atomic.Int64
}

func NewHashMap() *HashMap {
	return &HashMap{data: make(map[string]string)}
}

func (h *HashMap) GetSize() int {
	return int(h.byteSize.Load())
}

func (h *HashMap) Add(key, value string) bool {
	if old, ok := h.data[key]; ok {
		h.byteSize.Add(int64(entryBytes(key, value) - entryBytes(key, old)))
	} else {
		h.byteSize.Add(int64(entryBytes(key, value)))
	}
	h.data[key] = value
	return true
}

func (h *HashMap) Get(key string) (string, bool) {
	value, ok := h.data[key]
	if !ok {
		return "", false
	}
	if value == hashMapConfig.Tombstone {
		return "", false
	}
	return value, true
}

func (h *HashMap) ToRaw() []key_value.KeyValue {
	ret := make([]key_value.KeyValue, 0, len(h.data))
	for k, v := range h.data {
		ret = append(ret, key_value.NewKeyValue(k, v))
	}
	return ret
}

func (h *HashMap) Clear() bool {
	h.data = make(map[string]string)
	h.byteSize.Store(0)
	return true
}
