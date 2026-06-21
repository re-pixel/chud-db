package memtable

import (
	"math/rand"
	appconfig "nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"sync/atomic"
)

var skipListConfig = appconfig.GetConfig()

type skipNode struct {
	key   string
	value string
	right *skipNode
	down  *skipNode
}

type SkipList struct {
	head       *skipNode
	bottomHead *skipNode
	maxLevel   int
	byteSize   atomic.Int64
	rng        *rand.Rand
}

func NewSkipList(maxLevel int) *SkipList {
	if maxLevel < 1 {
		maxLevel = 1
	}
	list := &SkipList{maxLevel: maxLevel, rng: rand.New(rand.NewSource(rand.Int63()))}
	list.reset()
	return list
}

func (s *SkipList) reset() {
	s.head = &skipNode{}
	curr := s.head
	for i := 1; i < s.maxLevel; i++ {
		curr.down = &skipNode{}
		curr = curr.down
	}
	s.bottomHead = curr // curr is now the bottom-level sentinel
	s.byteSize.Store(0)
}

func (s *SkipList) GetSize() int {
	return int(s.byteSize.Load())
}

func (s *SkipList) Get(key string) (string, bool) {
	curr := s.head
	for curr != nil {
		for curr.right != nil && curr.right.key < key {
			curr = curr.right
		}
		if curr.right != nil && curr.right.key == key {
			if curr.right.value == skipListConfig.Tombstone {
				return "", false
			}
			return curr.right.value, true
		}
		curr = curr.down
	}
	return "", false
}

func (s *SkipList) Add(key, value string) bool {
	lefts := s.findInsertPath(key)

	if existing := lefts[0].right; existing != nil && existing.key == key {
		diff := int64(entryBytes(key, value) - entryBytes(key, existing.value))
		for i := range s.maxLevel {
			if n := lefts[i].right; n != nil && n.key == key {
				n.value = value
			}
		}
		s.byteSize.Add(diff)
		return true
	}

	level := s.randomLevel()
	nodes := make([]*skipNode, level)
	for i := range level {
		nodes[i] = &skipNode{key: key, value: value, right: lefts[i].right}
		lefts[i].right = nodes[i]
	}

	for i := 1; i < level; i++ {
		nodes[i].down = nodes[i-1]
	}
	s.byteSize.Add(int64(entryBytes(key, value)))
	return true
}

func (s *SkipList) findInsertPath(key string) []*skipNode {
	lefts := make([]*skipNode, s.maxLevel)
	curr := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.right != nil && curr.right.key < key {
			curr = curr.right
		}
		lefts[i] = curr
		curr = curr.down
	}
	return lefts
}

func (s *SkipList) randomLevel() int {
	level := 1
	for level < s.maxLevel && s.rng.Intn(2) == 0 {
		level++
	}
	return level
}

func (s *SkipList) Scan(pred func(key string) bool, fn func(key, value string)) {
	for node := s.bottomHead.right; node != nil; node = node.right {
		if pred(node.key) {
			fn(node.key, node.value)
		}
	}
}

func (s *SkipList) ToRaw() []key_value.KeyValue {
	ret := make([]key_value.KeyValue, 0)
	for node := s.bottomHead.right; node != nil; node = node.right {
		ret = append(ret, key_value.NewKeyValue(node.key, node.value))
	}
	return ret
}

func (s *SkipList) Clear() bool {
	s.reset()
	return true
}

func (s *SkipList) TakeSnapshot() []key_value.KeyValue {
	raw := s.ToRaw()
	s.reset()
	return raw
}
