package block_manager

import (
	"fmt"
	"io"
	"nosqlEngine/src/config"
	"os"
)

var CONFIG = config.GetConfig()

type BlockManager struct {
	block_size int
	lruCache   *LRUCache
}

func NewBlockManager() *BlockManager {
	return &BlockManager{
		block_size: CONFIG.BlockSize,
		lruCache:   NewLRUCache(),
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

func (bm *BlockManager) ReadBlock(location string, blockNumber int, direction bool) ([]byte, error) {
	var forwardBlockNumber int

	if direction {
		forwardBlockNumber = blockNumber
	} else {
		fileInfo, err := os.Stat(location)
		if err != nil {
			return nil, err
		}
		totalBlocks := int(fileInfo.Size()) / CONFIG.BlockSize
		if blockNumber >= totalBlocks {
			return nil, io.EOF
		}
		forwardBlockNumber = totalBlocks - 1 - blockNumber
	}

	if data, ok := bm.lruCache.Get(location, forwardBlockNumber); ok {
		return data, nil
	}

	file, err := os.Open(location)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	var offset int64
	if direction {
		offset = int64(CONFIG.BlockSize * blockNumber)
		if offset >= fileInfo.Size() {
			return nil, io.EOF
		}
	} else {
		totalBlocks := int(fileInfo.Size()) / CONFIG.BlockSize
		if blockNumber >= totalBlocks {
			return nil, io.EOF
		}
		offset = int64(CONFIG.BlockSize * (totalBlocks - 1 - blockNumber))
		if offset < 0 {
			offset = 0
		}
	}

	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, CONFIG.BlockSize)
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, io.EOF
	}

	data := buf[:n]
	bm.lruCache.Put(location, forwardBlockNumber, data)
	return data, nil
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
