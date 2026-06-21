package engine

import (
	"fmt"
	"nosqlEngine/src/memtable"
)

// Write is the public API. It enqueues the operation and blocks until the
// writer goroutine has applied it and durability is confirmed.
func (engine *Engine) Write(user, key, value string, fromWal bool) error {
	req := writeReq{
		user:    user,
		key:     key,
		value:   value,
		fromWal: fromWal,
		done:    make(chan error, 1),
	}
	engine.writeCh <- req
	return <-req.done
}

// applyWrite is called exclusively by the writer goroutine (and directly during
// WAL replay before the writer starts). It is the sole mutator of engine state.
func (engine *Engine) applyWrite(user, key, value string, fromWal bool) error {
	if !fromWal && !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return fmt.Errorf("user %s is not allowed to write: %w", user, err)
		}
	}

	var lsn uint64
	if !fromWal {
		var err error
		if value == CONFIG.Tombstone {
			lsn, err = engine.wal.AppendDelete(key)
		} else {
			lsn, err = engine.wal.AppendPut(key, value)
		}
		if err != nil {
			return fmt.Errorf("failed to write to WAL: %w", err)
		}
	}

	writeMem := engine.loadActiveMem()
	writeMem.Add(key, value)

	if !fromWal {
		if err := engine.wal.WaitDurable(lsn); err != nil {
			return fmt.Errorf("wal durability failed: %w", err)
		}
	}

	if fromWal && writeMem.GetSize() >= CONFIG.MemtableSize {
		engine.replayFlush(writeMem)
		return nil
	}

	if !fromWal && writeMem.GetSize() >= CONFIG.MemtableSize {
		im := memtable.NewImmutableMemtable(writeMem.ToRaw(), lsn)
		engine.immQueue.Push(im)
		engine.swapActiveMem(writeMem)
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
