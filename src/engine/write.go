package engine

import (
	"fmt"
)

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

func (engine *Engine) applyWrite(user, key, value string, fromWal bool) error {
	if !fromWal && !engine.skipRateLimit {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return fmt.Errorf("user %s is not allowed to write: %w", user, err)
		}
	}

	if engine.checkIfMemtableFull() {
		engine.WaitFlushIdle()
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
		flushData := writeMem.ToRaw()
		engine.SetNextMemtable()

		purgeWAL := engine.curr_mem_index == 0

		done := make(chan struct{})
		go func() {
			engine.flush_lock.Lock()
			defer engine.flush_lock.Unlock()

			engine.ss_parser.FlushMemtable(flushData)
			if purgeWAL {
				engine.wal.Purge()
			}
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
