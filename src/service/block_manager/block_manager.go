package block_manager

import (
	"nosqlEngine/src/config"
	"os"
	"sync"
)

var CONFIG = config.GetConfig()

type BlockManager struct {
	block_size int
	lruCache   *LRUCache
	fdMu       sync.Mutex
	openFDs    map[string]*os.File
}

func NewBlockManager() *BlockManager {
	return &BlockManager{
		block_size: CONFIG.BlockSize,
		lruCache:   NewLRUCache(),
		openFDs:    make(map[string]*os.File),
	}
}


func (bm *BlockManager) ReadAt(path string, offset int64, size int) ([]byte, error) {
	isBlockRead := size == bm.block_size && offset%int64(bm.block_size) == 0
	blockID := int(offset / int64(bm.block_size))
	if isBlockRead {
		if data, ok := bm.lruCache.Get(path, blockID); ok {
			return data, nil
		}
	}
	f, err := bm.getOrOpenFD(path)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	data := buf[:n]
	if isBlockRead {
		bm.lruCache.Put(path, blockID, data)
	}
	return data, nil
}

func (bm *BlockManager) getOrOpenFD(path string) (*os.File, error) {
	bm.fdMu.Lock()
	f, ok := bm.openFDs[path]
	if ok {
		bm.fdMu.Unlock()
		return f, nil
	}
	bm.fdMu.Unlock()

	newF, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	bm.fdMu.Lock()
	if existing, ok := bm.openFDs[path]; ok {
		bm.fdMu.Unlock()
		newF.Close() //nolint:errcheck
		return existing, nil
	}
	bm.openFDs[path] = newF
	bm.fdMu.Unlock()
	return newF, nil
}

func (bm *BlockManager) CloseFile(path string) {
	bm.fdMu.Lock()
	defer bm.fdMu.Unlock()
	if f, ok := bm.openFDs[path]; ok {
		f.Close() //nolint:errcheck
		delete(bm.openFDs, path)
	}
}

func (bm *BlockManager) GetFileSize(location string) (int64, error) {
	fileInfo, err := os.Stat(location)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (bm *BlockManager) GetFileSizeBlocks(location string) (int, error) {
	size, err := bm.GetFileSize(location)
	if err != nil {
		return 0, err
	}
	if size == 0 {
		return 0, nil
	}
	return int(size) / CONFIG.BlockSize, nil
}

func (bm *BlockManager) ClearCache() {
	bm.lruCache = NewLRUCache()
}

func (bm *BlockManager) IsCached(location string, blockNumber int) bool {
	_, ok := bm.lruCache.Get(location, blockNumber)
	return ok
}
