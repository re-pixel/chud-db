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
	memtables      []memtable.Memtable
	curr_mem_index int
	wal            *wal.WAL
	ss_parser      ss_parser.SSParser
	ss_compacter   *ss_compacter.SSCompacterST
	block_manager  *block_manager.BlockManager
	flush_lock     *sync.Mutex
	writeCh        chan writeReq
	writerWG       sync.WaitGroup
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
	memtableCount := CONFIG.MemtableCount
	memtables := make([]memtable.Memtable, memtableCount)
	for i := range memtableCount {
		memtables[i] = memtable.NewMemtable()
	}
	return &Engine{
		userLimiter:    user_limiter.NewUserLimiter(),
		memtables:      memtables,
		ss_parser:      ss_parser.NewSSParser(file_writer.NewFileWriterInDir(bm, CONFIG.BlockSize, "", dataRoot)),
		ss_compacter:   ss_compacter.NewSSCompacterST(),
		wal:            walInstance,
		curr_mem_index: 0,
		block_manager:  bm,
		flush_lock:     &sync.Mutex{},
		writeCh:        make(chan writeReq, 256),
		dataRoot:       dataRoot,
		skipRateLimit:  skipRateLimit,
		skipCompaction: skipCompaction,
	}
}

func (engine *Engine) SetNextMemtable() {
	engine.curr_mem_index = (engine.curr_mem_index + 1) % CONFIG.MemtableCount
}

func (engine *Engine) checkIfMemtableFull() bool {
	return engine.memtables[engine.curr_mem_index].GetSize() >= CONFIG.MemtableSize
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
	engine.startWriter()
}

func (engine *Engine) Shut() error {
	engine.stopWriter()
	engine.WaitFlushIdle()
	return engine.wal.Flush()
}

// WaitFlushIdle blocks until any in-flight memtable flush completes.
func (engine *Engine) WaitFlushIdle() {
	engine.flush_lock.Lock()
	defer engine.flush_lock.Unlock()
}
