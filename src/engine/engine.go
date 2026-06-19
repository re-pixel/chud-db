package engine

import (
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/memtable"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_compacter"
	"nosqlEngine/src/service/ss_parser"
	"nosqlEngine/src/service/user_limiter"
	"nosqlEngine/src/utils"
	"nosqlEngine/src/wal"
	"nosqlEngine/src/wal/record"
	"sync"
)

var CONFIG = config.GetConfig()

type Engine struct {
	userLimiter    *user_limiter.UserLimiter
	activeMem      memtable.Memtable
	activeMemMu    sync.RWMutex
	immQueue       *immutableQueue
	wal            *wal.WAL
	ss_parser      ss_parser.SSParser
	ss_compacter   *ss_compacter.SSCompacterST
	block_manager  *block_manager.BlockManager
	writeCh        chan writeReq
	writerWG       sync.WaitGroup
	flusherWG      sync.WaitGroup
	dataRoot       string
	skipRateLimit  bool
	skipCompaction bool
}

func NewEngine() *Engine {
	walInstance, err := wal.NewWAL()
	if err != nil {
		fmt.Println("Error creating WAL:", err)
		return nil
	}
	return newEngine(utils.DefaultDataRoot(), walInstance, false, false)
}

func newEngine(dataRoot string, walInstance *wal.WAL, skipRateLimit, skipCompaction bool) *Engine {
	bm := block_manager.NewBlockManager()
	maxImm := CONFIG.MaxImmutableCount
	if maxImm < 1 {
		maxImm = 3
	}
	return &Engine{
		userLimiter:    user_limiter.NewUserLimiter(),
		activeMem:      memtable.NewMemtable(),
		immQueue:       newImmutableQueue(maxImm),
		ss_parser:      ss_parser.NewSSParser(file_writer.NewFileWriterInDir(bm, CONFIG.BlockSize, "", dataRoot)),
		ss_compacter:   ss_compacter.NewSSCompacterST(),
		wal:            walInstance,
		block_manager:  bm,
		writeCh:        make(chan writeReq, 256),
		dataRoot:       dataRoot,
		skipRateLimit:  skipRateLimit,
		skipCompaction: skipCompaction,
	}
}

func (engine *Engine) loadActiveMem() memtable.Memtable {
	engine.activeMemMu.RLock()
	defer engine.activeMemMu.RUnlock()
	return engine.activeMem
}

func (engine *Engine) WaitForPendingFlushes() {
	for _, im := range engine.immQueue.Snapshot() {
		im.WaitFlushed()
	}
}

func (engine *Engine) swapActiveMem(old memtable.Memtable) {
	newMem := memtable.NewMemtable()
	engine.activeMemMu.Lock()
	engine.activeMem = newMem
	engine.activeMemMu.Unlock()
	old.Clear()
}

func (engine *Engine) Start() {
	recoveredEntries, err := engine.wal.Replay()
	if err != nil {
		fmt.Println("Error replaying WAL:", err)
		return
	}
	for _, entry := range recoveredEntries {
		value := entry.Value
		if entry.Op == record.OpDelete {
			value = CONFIG.Tombstone
		}
		engine.applyWrite("", entry.Key, value, true)
	}
	engine.startFlusher()
	engine.startWriter()
}

func (engine *Engine) drainActiveMem() {
	mem := engine.loadActiveMem()
	if mem.GetSize() == 0 {
		return
	}
	im := memtable.NewImmutableMemtable(mem.ToRaw())
	engine.immQueue.Push(im)
	engine.swapActiveMem(mem)
}

func (engine *Engine) Shut() error {
	engine.stopWriter()
	engine.drainActiveMem()
	engine.immQueue.Close()
	engine.flusherWG.Wait()
	return engine.wal.Flush()
}
