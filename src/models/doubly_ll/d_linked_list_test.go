package doublyll

import "testing"

func makeNode(key BlockKey) *Block { return NewNode([]byte("data"), key) }

func TestInsertBeginningAndBack(t *testing.T) {
	l := NewDoublyLinkedList()
	if l.Back() != nil || l.Front() != nil {
		t.Fatal("empty list should have nil front/back")
	}

	a := makeNode(1)
	b := makeNode(2)
	l.InsertBeginning(a)
	l.InsertBeginning(b)

	if l.Front() != b {
		t.Error("front should be most recently inserted node")
	}
	if l.Back() != a {
		t.Error("back should be the first inserted node")
	}
	if l.ListLength() != 2 {
		t.Errorf("length = %d, want 2", l.ListLength())
	}
}

func TestDeleteEnd(t *testing.T) {
	l := NewDoublyLinkedList()
	l.DeleteEnd() // no-op on empty list, must not panic

	a := makeNode(1)
	b := makeNode(2)
	l.InsertBeginning(a)
	l.InsertBeginning(b)

	l.DeleteEnd()
	if l.Back() != b {
		t.Error("after deleting tail, back should be the remaining node")
	}
	if l.ListLength() != 1 {
		t.Errorf("length = %d, want 1", l.ListLength())
	}

	l.DeleteEnd()
	if l.Front() != nil || l.Back() != nil {
		t.Error("head and tail must both be nil after removing last element")
	}
}

func TestMoveToFront(t *testing.T) {
	l := NewDoublyLinkedList()
	a := makeNode(1)
	b := makeNode(2)
	c := makeNode(3)
	l.InsertBeginning(a)
	l.InsertBeginning(b)
	l.InsertBeginning(c) // order: c(head) -> b -> a(tail)

	l.MoveToFront(a) // move tail to head
	if l.Front() != a {
		t.Error("moved node should be at front")
	}
	if l.Back() != b {
		t.Error("new tail should be b after a was moved to front")
	}
	if l.ListLength() != 3 {
		t.Errorf("length changed after MoveToFront: %d", l.ListLength())
	}
}

func TestMoveToFrontMiddleNode(t *testing.T) {
	l := NewDoublyLinkedList()
	a := makeNode(1)
	b := makeNode(2)
	c := makeNode(3)
	l.InsertBeginning(a)
	l.InsertBeginning(b)
	l.InsertBeginning(c) // c -> b -> a

	l.MoveToFront(b) // move middle to head
	if l.Front() != b {
		t.Error("b should be at front")
	}
	if l.Back() != a {
		t.Error("a should still be at back")
	}
}

func TestMoveToFrontAlreadyHead(t *testing.T) {
	l := NewDoublyLinkedList()
	a := makeNode(1)
	l.InsertBeginning(a)
	l.MoveToFront(a) // no-op
	if l.Front() != a || l.Back() != a || l.ListLength() != 1 {
		t.Error("MoveToFront on sole element should be a no-op")
	}
}
