package memtable

import (
	"math/rand"
	appconfig "nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
)

var skipListConfig = appconfig.GetConfig()

type skipNode struct {
	key   string
	value string
	right *skipNode
	down  *skipNode
}

type SkipList struct {
	head     *skipNode
	maxLevel int
	byteSize int
	rng      *rand.Rand
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
	s.byteSize = 0
}

func (s *SkipList) bottom() *skipNode {
	curr := s.head
	for curr.down != nil {
		curr = curr.down
	}
	return curr
}

func (s *SkipList) GetSize() int {
	return s.byteSize
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
	if old, ok := s.lookupValue(key); ok {
		s.byteSize += entryBytes(key, value) - entryBytes(key, old)
		s.updateValue(key, value)
		return true
	}

	level := s.randomLevel()
	lefts := s.findInsertPath(key)

	nodes := make([]*skipNode, level)
	for i := 0; i < level; i++ {
		left := lefts[i]
		nodes[i] = &skipNode{key: key, value: value, right: left.right}
		left.right = nodes[i]
	}
	for i := 0; i < level-1; i++ {
		nodes[i+1].down = nodes[i]
	}

	s.byteSize += entryBytes(key, value)
	return true
}

func (s *SkipList) lookupValue(key string) (string, bool) {
	curr := s.head
	for curr != nil {
		for curr.right != nil && curr.right.key < key {
			curr = curr.right
		}
		if curr.right != nil && curr.right.key == key {
			return curr.right.value, true
		}
		curr = curr.down
	}
	return "", false
}

func (s *SkipList) updateValue(key, value string) {
	curr := s.head
	for curr != nil {
		walk := curr
		for walk != nil {
			if walk.key == key {
				walk.value = value
			}
			walk = walk.right
		}
		curr = curr.down
	}
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

func (s *SkipList) ToRaw() []key_value.KeyValue {
	ret := make([]key_value.KeyValue, 0)
	for node := s.bottom().right; node != nil; node = node.right {
		ret = append(ret, key_value.NewKeyValue(node.key, node.value))
	}
	return ret
}

func (s *SkipList) Clear() bool {
	s.reset()
	return true
}
