package engine

import (
	"fmt"
	"nosqlEngine/src/memtable"
)

// Write enqueues the operation and blocks until it is applied.
// When fromWal is true the write bypasses the queue (WAL replay only).
// sync=true (default) waits for fsync before returning; sync=false returns
// once the write is in the WAL buffer and memtable.
func (engine *Engine) Write(user, key, value string, fromWal bool) error {
	return engine.write(user, key, value, fromWal, true)
}

// WriteAsync enqueues the operation and returns once the write is in the WAL
// buffer and memtable, without waiting for fsync. On a crash, writes
// acknowledged by WriteAsync may be lost if they were not subsequently covered
// by a sync write or a graceful shutdown.
func (engine *Engine) WriteAsync(user, key, value string) error {
	return engine.write(user, key, value, false, false)
}

func (engine *Engine) write(user, key, value string, fromWal, sync bool) error {
	if fromWal {
		return engine.applyWrite(user, key, value, true)
	}
	req := writeReq{
		user:  user,
		key:   key,
		value: value,
		sync:  sync,
		done:  make(chan error, 1),
	}
	engine.writeCh <- req
	return <-req.done
}

// applyWriteToMem appends to the WAL buffer and inserts into the memtable.
// It does NOT wait for fsync — the caller is responsible for calling WaitDurable.
// Returns the assigned LSN (0 on error) and any error.
func (engine *Engine) applyWriteToMem(user, key, value string) (uint64, error) {
	if !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return 0, fmt.Errorf("user %s is not allowed to write: %w", user, err)
		}
	}

	var lsn uint64
	var err error
	if value == CONFIG.Tombstone {
		lsn, err = engine.wal.AppendDelete(key)
	} else {
		lsn, err = engine.wal.AppendPut(key, value)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to write to WAL: %w", err)
	}

	writeMem := engine.loadActiveMem()
	writeMem.Add(key, value)

	if writeMem.GetSize() >= CONFIG.MemtableSize {
		im := memtable.NewImmutableMemtable(writeMem.ToRaw(), lsn)
		engine.immQueue.Push(im)
		engine.swapActiveMem(writeMem)
	}

	return lsn, nil
}

// applyWrite is used only during WAL replay (fromWal=true). It bypasses the
// write channel and handles the replay-specific flush path.
func (engine *Engine) applyWrite(user, key, value string, fromWal bool) error {
	if !fromWal {
		panic("applyWrite called with fromWal=false; use the write channel instead")
	}

	writeMem := engine.loadActiveMem()
	writeMem.Add(key, value)

	if writeMem.GetSize() >= CONFIG.MemtableSize {
		engine.replayFlush(writeMem)
	}

	return nil
}

func (engine *Engine) ApplyBatch(user string, b *WriteBatch) error {
	return engine.applyBatch(user, b, true)
}

func (engine *Engine) ApplyBatchAsync(user string, b *WriteBatch) error {
	return engine.applyBatch(user, b, false)
}

func (engine *Engine) applyBatch(user string, b *WriteBatch, sync bool) error {
	if b == nil || len(b.ops) == 0 {
		return nil
	}
	ops := make([]BatchOp, len(b.ops))
	for i, op := range b.ops {
		ops[i] = op
		if op.Delete {
			ops[i].Value = CONFIG.Tombstone
		}
	}
	req := writeReq{
		user: user,
		ops:  ops,
		sync: sync,
		done: make(chan error, 1),
	}
	engine.writeCh <- req
	return <-req.done
}

func (engine *Engine) replayFlush(mem memtable.Memtable) {
	snapshot := mem.TakeSnapshot() // atomic copy+clear; safe because single-threaded
	engine.activeMemMu.Lock()
	engine.activeMem = memtable.NewMemtable()
	engine.activeMemMu.Unlock()
	path := engine.ss_parser.FlushMemtable(snapshot, 0)
	engine.registerSSTable(0, path)
}
