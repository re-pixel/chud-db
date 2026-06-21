package memtable

import (
	appconfig "nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"sync/atomic"
)

var bTreeConfig = appconfig.GetConfig()

type bTreeNode struct {
	keys     []string
	values   []string
	children []*bTreeNode
	isLeaf   bool
}

type BTree struct {
	root     *bTreeNode
	order    int
	byteSize atomic.Int64
}

func NewBTree(order int) *BTree {
	if order < 2 {
		order = 2
	}
	return &BTree{
		root:  &bTreeNode{isLeaf: true},
		order: order,
	}
}

func (t *BTree) GetSize() int {
	return int(t.byteSize.Load())
}

func (t *BTree) Get(key string) (string, bool) {
	return t.root.search(key)
}

func (n *bTreeNode) search(key string) (string, bool) {
	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}
	if i < len(n.keys) && key == n.keys[i] {
		if n.values[i] == bTreeConfig.Tombstone {
			return "", false
		}
		return n.values[i], true
	}
	if n.isLeaf {
		return "", false
	}
	return n.children[i].search(key)
}

func (t *BTree) Add(key, value string) bool {
	if old, ok := t.lookup(key); ok {
		t.byteSize.Add(int64(entryBytes(key, value) - entryBytes(key, old)))
		t.root.updateExisting(key, value)
		return true
	}

	t.byteSize.Add(int64(entryBytes(key, value)))

	root := t.root
	if len(root.keys) == 2*t.order-1 {
		splitRoot := &bTreeNode{isLeaf: false, children: []*bTreeNode{root}}
		t.root = splitRoot
		splitRoot.splitChild(0, t.order)
		splitRoot.insertNonFull(key, value, t.order)
		return true
	}

	root.insertNonFull(key, value, t.order)
	return true
}

func (t *BTree) lookup(key string) (string, bool) {
	return t.root.lookup(key)
}

func (n *bTreeNode) lookup(key string) (string, bool) {
	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}
	if i < len(n.keys) && key == n.keys[i] {
		return n.values[i], true
	}
	if n.isLeaf {
		return "", false
	}
	return n.children[i].lookup(key)
}

func (n *bTreeNode) updateExisting(key, value string) {
	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}
	if i < len(n.keys) && key == n.keys[i] {
		n.values[i] = value
		return
	}
	if !n.isLeaf {
		n.children[i].updateExisting(key, value)
	}
}

func (n *bTreeNode) insertNonFull(key, value string, order int) {
	i := len(n.keys) - 1
	if n.isLeaf {
		n.keys = append(n.keys, "")
		n.values = append(n.values, "")
		for i >= 0 && key < n.keys[i] {
			n.keys[i+1] = n.keys[i]
			n.values[i+1] = n.values[i]
			i--
		}
		n.keys[i+1] = key
		n.values[i+1] = value
		return
	}

	for i >= 0 && key < n.keys[i] {
		i--
	}
	i++
	if len(n.children[i].keys) == 2*order-1 {
		n.splitChild(i, order)
		if key > n.keys[i] {
			i++
		}
	}
	n.children[i].insertNonFull(key, value, order)
}

func (n *bTreeNode) splitChild(i, order int) {
	full := n.children[i]
	promotedKey := full.keys[order-1]
	promotedVal := full.values[order-1]

	sibling := &bTreeNode{isLeaf: full.isLeaf}
	sibling.keys = append(sibling.keys, full.keys[order:]...)
	sibling.values = append(sibling.values, full.values[order:]...)
	full.keys = full.keys[:order-1]
	full.values = full.values[:order-1]
	if !full.isLeaf {
		sibling.children = append(sibling.children, full.children[order:]...)
		full.children = full.children[:order]
	}

	n.children = append(n.children, nil)
	copy(n.children[i+2:], n.children[i+1:])
	n.children[i+1] = sibling

	n.keys = append(n.keys, "")
	n.values = append(n.values, "")
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.values[i+1:], n.values[i:])
	n.keys[i] = promotedKey
	n.values[i] = promotedVal
}

func (t *BTree) Scan(pred func(key string) bool, fn func(key, value string)) {
	t.root.scan(pred, fn)
}

func (n *bTreeNode) scan(pred func(key string) bool, fn func(key, value string)) {
	for i := 0; i < len(n.keys); i++ {
		if !n.isLeaf {
			n.children[i].scan(pred, fn)
		}
		if pred(n.keys[i]) {
			fn(n.keys[i], n.values[i])
		}
	}
	if !n.isLeaf {
		n.children[len(n.keys)].scan(pred, fn)
	}
}

func (t *BTree) ToRaw() []key_value.KeyValue {
	pairs := make([]key_value.KeyValue, 0)
	t.root.collect(&pairs)
	return pairs
}

func (n *bTreeNode) collect(pairs *[]key_value.KeyValue) {
	for i := 0; i < len(n.keys); i++ {
		if !n.isLeaf {
			n.children[i].collect(pairs)
		}
		*pairs = append(*pairs, key_value.NewKeyValue(n.keys[i], n.values[i]))
	}
	if !n.isLeaf {
		n.children[len(n.keys)].collect(pairs)
	}
}

func (t *BTree) Clear() bool {
	t.root = &bTreeNode{isLeaf: true}
	t.byteSize.Store(0)
	return true
}

func (t *BTree) TakeSnapshot() []key_value.KeyValue {
	raw := t.ToRaw()
	t.root = &bTreeNode{isLeaf: true}
	t.byteSize.Store(0)
	return raw
}
