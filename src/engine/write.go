package engine

import (
	"fmt"
	"nosqlEngine/src/memtable"
)

// Write is the public API. It enqueues the operation and blocks until the
// writer goroutine has applied it and durability is confirmed.
func (engine *Engine) Write(user, key, value string, fromWal bool) error {
	if fromWal {
		return engine.applyWrite(user, key, value, true)
	}
	req := writeReq{
		user:  user,
		key:   key,
		value: value,
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

func (engine *Engine) replayFlush(mem memtable.Memtable) {
	snapshot := mem.TakeSnapshot() // atomic copy+clear; safe because single-threaded
	engine.activeMemMu.Lock()
	engine.activeMem = memtable.NewMemtable()
	engine.activeMemMu.Unlock()
	path := engine.ss_parser.FlushMemtable(snapshot)
	engine.registerSSTable(0, path)
}
