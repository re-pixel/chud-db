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
	"nosqlEngine/src/sstable"
	"nosqlEngine/src/utils"
	"nosqlEngine/src/wal"
	"nosqlEngine/src/wal/record"
	"os"
	"path/filepath"
	"strings"
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
	tableCache     *sstable.TableCache
	writeCh        chan writeReq
	writerWG       sync.WaitGroup
	flusherWG      sync.WaitGroup
	compactCh      chan struct{}
	compactionWG   sync.WaitGroup
	versionMu      sync.RWMutex
	versions       [][]string // versions[level] = live .db paths for that level
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
	eng := &Engine{
		userLimiter:    user_limiter.NewUserLimiter(),
		activeMem:      memtable.NewMemtable(),
		immQueue:       newImmutableQueue(maxImm),
		ss_parser:      ss_parser.NewSSParser(file_writer.NewFileWriterInDir(CONFIG.BlockSize, "", dataRoot)),
		ss_compacter:   ss_compacter.NewSSCompacterST(),
		wal:            walInstance,
		block_manager:  bm,
		tableCache:     sstable.NewTableCache(CONFIG.TableCacheSize, bm),
		writeCh:        make(chan writeReq, 256),
		compactCh:      make(chan struct{}, 1),
		dataRoot:       dataRoot,
		skipRateLimit:  skipRateLimit,
		skipCompaction: skipCompaction,
	}
	eng.initVersions()
	return eng
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

func (engine *Engine) initVersions() {
	engine.versions = make([][]string, CONFIG.LSMLevels)
	for level := 0; level < CONFIG.LSMLevels; level++ {
		engine.versions[level] = utils.ListSSTablesInLevel(engine.dataRoot, level)
		dir := utils.SSTableLevelDir(engine.dataRoot, level)
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".db.tmp") {
				os.Remove(filepath.Join(dir, e.Name())) //nolint:errcheck
			}
		}
	}
}

// registerSSTable appends a newly flushed SSTable path to the given level.
func (engine *Engine) registerSSTable(level int, path string) {
	engine.versionMu.Lock()
	engine.versions[level] = append(engine.versions[level], path)
	engine.versionMu.Unlock()
}

// installCompaction atomically swaps old paths for the new compacted path,
// evicts the old readers from the cache, then physically deletes the old files.
func (engine *Engine) installCompaction(level int, newPath string, oldPaths []string) {
	engine.versionMu.Lock()
	outLevel := level + 1
	engine.versions[outLevel] = append(engine.versions[outLevel], newPath)
	oldSet := make(map[string]struct{}, len(oldPaths))
	for _, p := range oldPaths {
		oldSet[p] = struct{}{}
	}
	filtered := engine.versions[level][:0]
	for _, p := range engine.versions[level] {
		if _, removed := oldSet[p]; !removed {
			filtered = append(filtered, p)
		}
	}
	engine.versions[level] = filtered
	for _, p := range oldPaths {
		engine.tableCache.Evict(p)
	}
	for _, p := range oldPaths {
		os.Remove(p) //nolint:errcheck
	}
	engine.versionMu.Unlock()
}

// lockVersions returns the live versions slice and an RUnlock func.
// Readers hold the returned unlock until all SSTable I/O is complete.
func (engine *Engine) lockVersions() ([][]string, func()) {
	engine.versionMu.RLock()
	return engine.versions, engine.versionMu.RUnlock
}

// reversedPaths returns a reversed copy of the slice without modifying the original.
func reversedPaths(s []string) []string {
	cp := make([]string, len(s))
	for i, v := range s {
		cp[len(s)-1-i] = v
	}
	return cp
}

// snapshotVersions returns a deep copy of versions under a brief RLock.
// Used by the compactor so merge I/O runs without holding the lock.
func (engine *Engine) snapshotVersions() [][]string {
	engine.versionMu.RLock()
	snap := make([][]string, len(engine.versions))
	for i, level := range engine.versions {
		cp := make([]string, len(level))
		copy(cp, level)
		snap[i] = cp
	}
	engine.versionMu.RUnlock()
	return snap
}

func (engine *Engine) startCompactor() {
	engine.compactionWG.Add(1)
	go engine.runCompactor()
}

func (engine *Engine) runCompactor() {
	defer engine.compactionWG.Done()
	for range engine.compactCh {
		snap := engine.snapshotVersions()
		results := engine.ss_compacter.CheckCompactionConditions(engine.block_manager, engine.dataRoot, snap)
		for _, r := range results {
			engine.installCompaction(r.Level, r.NewPath, r.OldPaths)
		}
	}
}

func (engine *Engine) Start() {
	err := engine.wal.ReplayFunc(func(entry wal.Entry) error {
		value := entry.Value
		if entry.Op == record.OpDelete {
			value = CONFIG.Tombstone
		}
		return engine.applyWrite("", entry.Key, value, true)
	})
	if err != nil {
		fmt.Println("Error replaying WAL:", err)
		return
	}
	engine.startFlusher()
	engine.startCompactor()
	engine.startWriter()
}

func (engine *Engine) drainActiveMem() {
	mem := engine.loadActiveMem()
	if mem.GetSize() == 0 {
		return
	}
	im := memtable.NewImmutableMemtable(mem.ToRaw(), engine.wal.AppendedLSN())
	engine.immQueue.Push(im)
	engine.swapActiveMem(mem)
}

func (engine *Engine) Shut() error {
	engine.stopWriter()
	engine.drainActiveMem()
	engine.immQueue.Close()
	engine.flusherWG.Wait()
	close(engine.compactCh)
	engine.compactionWG.Wait()
	return engine.wal.Flush()
}
