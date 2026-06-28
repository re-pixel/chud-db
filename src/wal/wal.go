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

	subsMu sync.Mutex
	subs   []chan struct{}
}

func NewWAL() (*WAL, error) {
	store, err := storage.NewAppendStorage()
	if err != nil {
		return nil, err
	}
	cfg := config.Get()
	return NewWALWithStorage(store, cfg.WALSyncMode), nil
}

func NewWALInDir(walDir string) (*WAL, error) {
	cfg := config.Get()
	store, err := storage.NewAppendStorageInDir(walDir, cfg.WALSegmentSize, cfg.WALWriteBufferSize)
	if err != nil {
		return nil, err
	}
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
		w.notifySubs()
		if err != nil && lsn > w.flushedLSN {
			return err
		}
	}
}

// notifySubs sends a non-blocking signal to every subscriber channel.
// Must be called while w.mu is held (to read flushedLSN consistently).
func (w *WAL) notifySubs() {
	w.subsMu.Lock()
	for _, ch := range w.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	w.subsMu.Unlock()
}

// Subscribe returns a buffered channel that receives a signal whenever the
// durable LSN advances. The caller must call Unsubscribe when done.
func (w *WAL) Subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	w.subsMu.Lock()
	w.subs = append(w.subs, ch)
	w.subsMu.Unlock()
	return ch
}

// Unsubscribe removes a channel previously returned by Subscribe.
func (w *WAL) Unsubscribe(ch chan struct{}) {
	w.subsMu.Lock()
	for i, s := range w.subs {
		if s == ch {
			w.subs = append(w.subs[:i], w.subs[i+1:]...)
			break
		}
	}
	w.subsMu.Unlock()
}

// DurableLSN returns the highest LSN that has been fsync'd to disk.
func (w *WAL) DurableLSN() uint64 {
	return w.store.DurableLSN()
}

// CursorFrom returns a WALCursor that will deliver every durable entry with
// LSN > afterLSN in order. The cursor holds a subscriber channel; call
// Close when done to release it.
func (w *WAL) CursorFrom(afterLSN uint64) (*WALCursor, error) {
	segments, err := w.store.ListSegments()
	if err != nil {
		return nil, err
	}

	// Find the index of the first segment that might contain afterLSN+1.
	// Since LSNs are monotonically increasing across segments, scan forward
	// and stop at the first segment we haven't fully passed.
	startIdx := 0
	active := w.store.ActiveSegment()
	for i, seg := range segments {
		if seg.ID == active.ID {
			break
		}
		maxLSN, err := segmentMaxLSN(w.store, seg.ID)
		if err != nil {
			return nil, err
		}
		if maxLSN <= afterLSN {
			startIdx = i + 1
		} else {
			break
		}
	}

	notify := w.Subscribe()
	return &WALCursor{
		wal:      w,
		afterLSN: afterLSN,
		segIdx:   startIdx,
		segments: segments,
		notify:   notify,
	}, nil
}

// segmentMaxLSN reads the last LSN in the given segment by scanning all records.
func segmentMaxLSN(store storage.AppendStorage, segmentID uint64) (uint64, error) {
	r, err := store.OpenSegmentReader(segmentID)
	if err != nil {
		return 0, err
	}
	defer r.Close() //nolint:errcheck
	var maxLSN uint64
	for {
		rec, err := r.Next()
		if err != nil {
			break
		}
		maxLSN = rec.LSN
	}
	return maxLSN, nil
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
