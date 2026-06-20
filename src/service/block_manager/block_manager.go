package block_manager

import (
	"fmt"
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

func (bm *BlockManager) WriteBlock(location string, blockNumber int, data []byte) error {
	if len(data) > CONFIG.BlockSize {
		return fmt.Errorf("data size exceeds block size")
	}

	file, err := os.OpenFile(location, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(CONFIG.BlockSize * blockNumber)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	//bm.lruCache.Put(location, blockNumber, data)

	return nil
}


func (bm *BlockManager) ReadAt(path string, offset int64, size int) ([]byte, error) {
	f, err := bm.getOrOpenFD(path)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
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
