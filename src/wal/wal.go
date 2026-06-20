package wal

import (
	"nosqlEngine/src/wal/config"
	"nosqlEngine/src/wal/record"
	"nosqlEngine/src/wal/storage"
	"sync"
)

type Entry struct {
	LSN   uint64
	Op    record.Op
	Key   string
	Value string
}

type WAL struct {
	store    storage.AppendStorage
	syncMode string

	mu         sync.Mutex
	cond       *sync.Cond
	syncing    bool
	flushedLSN uint64
}

func NewWAL() (*WAL, error) {
	store, err := storage.NewAppendStorage()
	if err != nil {
		return nil, err
	}
	cfg := config.Get()
	return NewWALWithStorage(store, cfg.WALSyncMode), nil
}

func NewWALWithStorage(store storage.AppendStorage, syncMode string) *WAL {
	w := &WAL{
		store:      store,
		syncMode:   syncMode,
		flushedLSN: store.DurableLSN(),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *WAL) AppendPut(key, value string) (uint64, error) {
	return w.store.Append(record.OpPut, []byte(key), []byte(value))
}

func (w *WAL) AppendDelete(key string) (uint64, error) {
	return w.store.Append(record.OpDelete, []byte(key), nil)
}

// WaitDurable blocks until the record identified by lsn (and everything before
// it) has been fsync'd. In "sync" mode every caller fsyncs directly. In "group"
// mode a leaderless group commit batches concurrent waiters into a single fsync.
func (w *WAL) WaitDurable(lsn uint64) error {
	if w.syncMode == "sync" {
		return w.store.Sync()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if lsn <= w.flushedLSN {
			return nil
		}
		if w.syncing {
			w.cond.Wait()
			continue
		}
		// No sync in flight and we are not durable yet: become the leader,
		// fsync on behalf of every waiter, and publish the new durable LSN.
		w.syncing = true
		w.mu.Unlock()
		err := w.store.Sync()
		w.mu.Lock()
		w.flushedLSN = w.store.DurableLSN()
		w.syncing = false
		w.cond.Broadcast()
		if err != nil && lsn > w.flushedLSN {
			return err
		}
	}
}

func (w *WAL) WritePut(key, value string) error {
	lsn, err := w.AppendPut(key, value)
	if err != nil {
		return err
	}
	return w.WaitDurable(lsn)
}

func (w *WAL) WriteDelete(key string) error {
	lsn, err := w.AppendDelete(key)
	if err != nil {
		return err
	}
	return w.WaitDurable(lsn)
}

func (w *WAL) AppendedLSN() uint64 {
	return w.store.AppendedLSN()
}

func (w *WAL) Flush() error {
	return w.store.Sync()
}

func (w *WAL) Close() error {
	return w.store.Close()
}

func (w *WAL) PurgeUpTo(checkpointLSN uint64) error {
	return w.store.PurgeUpTo(checkpointLSN)
}
