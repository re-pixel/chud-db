package engine

type BatchOp struct {
	Key    string
	Value  string
	Delete bool
}

type WriteBatch struct {
	ops []BatchOp
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

func (b *WriteBatch) Put(key, value string) *WriteBatch {
	b.ops = append(b.ops, BatchOp{Key: key, Value: value})
	return b
}

func (b *WriteBatch) Delete(key string) *WriteBatch {
	b.ops = append(b.ops, BatchOp{Key: key, Delete: true})
	return b
}

func (b *WriteBatch) Len() int {
	return len(b.ops)
}
