package wal

import (
	"nosqlEngine/src/wal/config"
	"nosqlEngine/src/wal/record"
	"nosqlEngine/src/wal/storage"
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
	return &WAL{
		store:    store,
		syncMode: syncMode,
	}
}

func (w *WAL) WritePut(key, value string) error {
	if _, err := w.store.Append(record.OpPut, []byte(key), []byte(value)); err != nil {
		return err
	}
	return w.syncIfNeeded()
}

func (w *WAL) WriteDelete(key string) error {
	if _, err := w.store.Append(record.OpDelete, []byte(key), nil); err != nil {
		return err
	}
	return w.syncIfNeeded()
}

func (w *WAL) Flush() error {
	return w.store.Sync()
}

func (w *WAL) Close() error {
	return w.store.Close()
}

func (w *WAL) Purge() error {
	return w.store.Purge()
}

func (w *WAL) syncIfNeeded() error {
	if w.syncMode == "sync" {
		return w.store.Sync()
	}
	return nil
}
