package engine

import (
	"fmt"
)

func (engine *Engine) Write(user string, key string, value string, fromWal bool) error {
	// check if memory full
	if engine.checkIfMemtableFull() {
		engine.memtables[engine.curr_mem_index].Clear()
	}

	if !fromWal {
		if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
			return fmt.Errorf("user %s is not allowed to write: %w", user, err)
		}
		// write to WAL
		var ok error
		if value == CONFIG.Tombstone {
			ok = engine.wal.WriteDelete(key)
		} else {
			ok = engine.wal.WritePut(key, value)
		}
		if ok != nil {
			return fmt.Errorf("failed to write to WAL: %w", ok)
		}
	}

	write_mem := engine.memtables[engine.curr_mem_index]
	write_mem.Add(key, value)
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
			engine.ss_compacter.CheckCompactionConditions(engine.block_manager)
		}()
	}
	return nil
}
