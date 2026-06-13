package engine

import (
	"fmt"
)

func (engine *Engine) Write(user string, key string, value string, fromWal bool) error {
	// check if memory full
	if engine.checkIfMemtableFull() {
		engine.memtables[engine.curr_mem_index].Clear()
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

	write_mem := engine.memtables[engine.curr_mem_index]
	write_mem.Add(key, value)

	if !fromWal {
		if err := engine.wal.WaitDurable(lsn); err != nil {
			return fmt.Errorf("wal durability failed: %w", err)
		}
	}
	if write_mem.GetSize() >= CONFIG.MemtableSize && !fromWal {
		engine.SetNextMemtable()
		done := make(chan struct{})
		go func() {
			engine.flush_lock.Lock()
			defer engine.flush_lock.Unlock()

			engine.ss_parser.FlushMemtable(write_mem.ToRaw())
			if engine.curr_mem_index == 0 {
				engine.wal.Purge()
			}
			close(done) // signal that FlushMemtable is done
		}()

		go func() {
			<-done // wait for FlushMemtable to finish
			engine.ss_compacter.CheckCompactionConditions(engine.block_manager, engine.dataRoot)
		}()
	}
	return nil
}
