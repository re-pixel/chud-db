package file_writer

import (
	"fmt"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/utils"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

type FileWriter struct {
	block_manager   block_manager.BlockManager
	location        string
	dataRoot        string
	currentBlock    []byte
	currentBlockNum int
	blockSize       int
	offsetInBlock   int
}

func NewFileWriter(bm *block_manager.BlockManager, blockSize int, name string) *FileWriter {
	return NewFileWriterInDir(bm, blockSize, name, utils.DefaultDataRoot())
}

func NewFileWriterInDir(bm *block_manager.BlockManager, blockSize int, name string, dataRoot string) *FileWriter {
	if name == "" {
		name = generateFileName(0)
	}
	location := filepath.Join(dataRoot, name)
	if err := os.MkdirAll(filepath.Dir(location), 0755); err != nil {
		fmt.Println("Error creating sstable dir:", err)
	}
	return &FileWriter{
		block_manager:   *bm,
		location:        location,
		dataRoot:        dataRoot,
		currentBlock:    make([]byte, 0, blockSize),
		currentBlockNum: 0,
		blockSize:       blockSize,
		offsetInBlock:   0,
	}
}

// generateFileName returns a path with a .tmp extension. The file is only
// renamed to .db by Commit() once all blocks have been written, ensuring
// concurrent readers never observe a partially-written SSTable.
func generateFileName(level int) string {
	return fmt.Sprintf("sstable/lvl%d/sstable_%s.db.tmp", level, uuid.New().String())
}

// Commit atomically renames the in-progress .tmp file to the final .db path.
// After this call the SSTable becomes visible to readers.
// If the location does not end with ".tmp" this is a safe no-op.
func (fw *FileWriter) Commit() error {
	if !strings.HasSuffix(fw.location, ".tmp") {
		return nil
	}
	finalPath := fw.location[:len(fw.location)-len(".tmp")]
	if err := os.Rename(fw.location, finalPath); err != nil {
		return err
	}
	fw.location = finalPath
	return nil
}

// Block trailer constants.
// Each block ends with [usedBytes:2][blockType:1] (3 bytes total).
// usedBytes is the number of actual data bytes at the start of the block.
const (
	JumboStart  = 1
	JumboMiddle = 3
	JumboEnd    = 7
	NonJumbo    = 0
)

func (fw *FileWriter) Write(data []byte, sectionEnd bool, size []byte) int {
	if sectionEnd {
		if len(fw.currentBlock) > 0 {
			fw.FlushCurrentBlock()
		}
	}

	if len(data) == 0 {
		return fw.currentBlockNum
	}

	if fw.IsJumbo(len(data)) {
		return fw.WriteJumboData(data)
	}

	if !fw.CanWrite(len(data)) {
		fw.FlushCurrentBlock()
	}
	fw.currentBlock = append(fw.currentBlock, data...)
	fw.offsetInBlock += len(data)

	return fw.currentBlockNum
}

// IsJumbo returns true if the data cannot fit in a single block.
// A block holds at most blockSize-3 bytes of data (3 bytes reserved for the trailer).
func (fw *FileWriter) IsJumbo(dataLen int) bool {
	return dataLen > fw.blockSize-3
}

// CanWrite returns true if dataLen bytes can be appended to the current block.
func (fw *FileWriter) CanWrite(dataLen int) bool {
	return fw.offsetInBlock+dataLen+3 <= fw.blockSize
}

func (fw *FileWriter) FlushCurrentBlock() {
	if len(fw.currentBlock) == 0 {
		return
	}
	usedBytes := len(fw.currentBlock)
	padLen := fw.blockSize - usedBytes - 3
	if padLen > 0 {
		fw.currentBlock = append(fw.currentBlock, make([]byte, padLen)...)
	}
	fw.currentBlock = append(fw.currentBlock,
		byte(usedBytes>>8),
		byte(usedBytes),
		NonJumbo,
	)
	fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, fw.currentBlock) //nolint:errcheck
	fw.currentBlockNum++
	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.offsetInBlock = 0
}

func (fw *FileWriter) WriteJumboData(data []byte) int {
	if len(fw.currentBlock) > 0 {
		fw.FlushCurrentBlock()
	}

	startBlock := fw.currentBlockNum
	availablePerBlock := fw.blockSize - 3
	numBlocks := (len(data) + availablePerBlock - 1) / availablePerBlock

	dataOffset := 0
	for i := 0; i < numBlocks; i++ {
		end := min(dataOffset+availablePerBlock, len(data))
		chunk := data[dataOffset:end]
		chunkSize := len(chunk)
		dataOffset = end

		var blockType byte
		switch {
		case i == 0 && numBlocks == 1:
			blockType = JumboEnd
		case i == 0:
			blockType = JumboStart
		case i == numBlocks-1:
			blockType = JumboEnd
		default:
			blockType = JumboMiddle
		}

		blockData := make([]byte, fw.blockSize)
		copy(blockData, chunk)
		blockData[fw.blockSize-3] = byte(chunkSize >> 8)
		blockData[fw.blockSize-2] = byte(chunkSize)
		blockData[fw.blockSize-1] = blockType

		fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, blockData) //nolint:errcheck
		fw.currentBlockNum++
	}

	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.offsetInBlock = 0
	return startBlock
}

func (fw *FileWriter) WriteRaw(data []byte) (int64, error) {
	fw.FlushCurrentBlock()
	startOffset := int64(fw.currentBlockNum) * int64(fw.blockSize)
	f, err := os.OpenFile(fw.location, os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err = f.Seek(startOffset, 0); err != nil {
		return 0, err
	}
	if _, err = f.Write(data); err != nil {
		return 0, err
	}
	return startOffset, nil
}

// CurrentByteOffset returns the byte offset of the next block to be written.
func (fw *FileWriter) CurrentByteOffset() int64 {
	return int64(fw.currentBlockNum) * int64(fw.blockSize)
}

func (fw *FileWriter) GetLocation() string {
	return fw.location
}

func (fw *FileWriter) GetCurrentBlockNum() int {
	return fw.currentBlockNum
}

func (fw *FileWriter) SetLocation(location string) {
	fw.location = location
}

func (fw *FileWriter) ResetFileWriter(name string) {
	if name == "" {
		name = generateFileName(0)
	}
	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.currentBlockNum = 0
	fw.offsetInBlock = 0
	fw.location = filepath.Join(fw.dataRoot, name)
	if err := os.MkdirAll(filepath.Dir(fw.location), 0755); err != nil {
		fmt.Println("Error creating sstable dir:", err)
	}
}
