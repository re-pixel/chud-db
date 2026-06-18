package engine

import (
	"fmt"
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
	if !fromWal {
		engine.WaitFlushIdle()
	}

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

	writeMem := engine.memtables[engine.curr_mem_index]
	writeMem.Add(key, value)

	if !fromWal {
		if err := engine.wal.WaitDurable(lsn); err != nil {
			return fmt.Errorf("wal durability failed: %w", err)
		}
	}

	if writeMem.GetSize() >= CONFIG.MemtableSize && !fromWal {
		flushData := writeMem.TakeSnapshot()

		done := make(chan struct{})
		go func() {
			engine.flush_lock.Lock()
			defer engine.flush_lock.Unlock()

			engine.ss_parser.FlushMemtable(flushData)
			engine.wal.Purge()
			close(done)
		}()

		go func() {
			<-done
			if !engine.skipCompaction {
				engine.ss_compacter.CheckCompactionConditions(engine.block_manager, engine.dataRoot)
			}
		}()
	}

	return nil
}
