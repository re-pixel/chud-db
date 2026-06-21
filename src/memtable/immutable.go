package memtable

import (
	appconfig "nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"sort"
)

var immutableConfig = appconfig.GetConfig()

// ImmutableMemtable holds a sorted, write-once snapshot of a memtable that has
// been promoted for flushing. It remains readable until MarkFlushed is called.
type ImmutableMemtable struct {
	data    []key_value.KeyValue
	maxLSN  uint64
	flushed chan struct{}
}

func NewImmutableMemtable(snapshot []key_value.KeyValue, maxLSN uint64) *ImmutableMemtable {
	key_value.SortByKeys(&snapshot)
	return &ImmutableMemtable{
		data:    snapshot,
		maxLSN:  maxLSN,
		flushed: make(chan struct{}),
	}
}

func (im *ImmutableMemtable) MaxLSN() uint64 {
	return im.maxLSN
}

func (im *ImmutableMemtable) Get(key string) (string, bool) {
	i := sort.Search(len(im.data), func(i int) bool {
		return im.data[i].GetKey() >= key
	})
	if i < len(im.data) && im.data[i].GetKey() == key {
		if im.data[i].GetValue() == immutableConfig.Tombstone {
			return "", false
		}
		return im.data[i].GetValue(), true
	}
	return "", false
}

func (im *ImmutableMemtable) Scan(pred func(key string) bool, fn func(key, value string)) {
	for _, kv := range im.data {
		if pred(kv.GetKey()) {
			fn(kv.GetKey(), kv.GetValue())
		}
	}
}

func (im *ImmutableMemtable) ToRaw() []key_value.KeyValue {
	out := make([]key_value.KeyValue, len(im.data))
	copy(out, im.data)
	return out
}

func (im *ImmutableMemtable) Len() int {
	return len(im.data)
}

func (im *ImmutableMemtable) MarkFlushed() {
	close(im.flushed)
}

func (im *ImmutableMemtable) WaitFlushed() {
	<-im.flushed
}
