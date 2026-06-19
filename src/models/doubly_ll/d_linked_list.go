package doublyll

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

type BlockKey uint64

type Block struct {
	data     []byte
	BlockKey BlockKey
	next     *Block
	prev     *Block
}

func NewBlockKey(blockNum int, filename string) BlockKey {
	data := fmt.Sprintf("%s:%d", filename, blockNum)
	return BlockKey(xxhash.Sum64String(data))
}

func NewNode(data []byte, blockKey BlockKey) *Block {
	return &Block{data: data, BlockKey: blockKey}
}

func (n *Block) Get() []byte { return n.data }
func (n *Block) Set(data []byte) { n.data = data }

type DoublyLinkedList struct {
	head   *Block
	tail   *Block
	length int
}

func NewDoublyLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{}
}

func (l *DoublyLinkedList) Front() *Block      { return l.head }
func (l *DoublyLinkedList) Back() *Block       { return l.tail }
func (l *DoublyLinkedList) ListLength() int    { return l.length }

func (l *DoublyLinkedList) InsertBeginning(n *Block) {
	n.prev = nil
	n.next = l.head
	if l.head != nil {
		l.head.prev = n
	} else {
		l.tail = n
	}
	l.head = n
	l.length++
}

func (l *DoublyLinkedList) DeleteEnd() {
	if l.tail == nil {
		return
	}
	if l.length == 1 {
		l.head = nil
		l.tail = nil
	} else {
		l.tail = l.tail.prev
		l.tail.next = nil
	}
	l.length--
}

func (l *DoublyLinkedList) MoveToFront(node *Block) {
	if node == l.head {
		return
	}
	if node == l.tail {
		l.tail = node.prev
		l.tail.next = nil
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}
	node.prev = nil
	node.next = l.head
	l.head.prev = node
	l.head = node
}
