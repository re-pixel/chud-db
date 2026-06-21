package file_writer

import (
	"bufio"
	"fmt"
	"nosqlEngine/src/utils"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

type FileWriter struct {
	location        string
	dataRoot        string
	currentBlock    []byte
	currentBlockNum int
	blockSize       int
	offsetInBlock   int
	rawBytesWritten int64
	file            *os.File
	buf             *bufio.Writer
}

func NewFileWriter(blockSize int, name string) *FileWriter {
	return NewFileWriterInDir(blockSize, name, utils.DefaultDataRoot())
}

func NewFileWriterInDir(blockSize int, name string, dataRoot string) *FileWriter {
	if name == "" {
		name = generateFileName(0)
	}
	location := filepath.Join(dataRoot, name)
	if err := os.MkdirAll(filepath.Dir(location), 0755); err != nil {
		fmt.Println("Error creating sstable dir:", err)
	}
	return &FileWriter{
		location:     location,
		dataRoot:     dataRoot,
		currentBlock: make([]byte, 0, blockSize),
		blockSize:    blockSize,
	}
}

func generateFileName(level int) string {
	return fmt.Sprintf("sstable/lvl%d/sstable_%s.db.tmp", level, uuid.New().String())
}

func (fw *FileWriter) openIfNeeded() error {
	if fw.file != nil {
		return nil
	}
	f, err := os.OpenFile(fw.location, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	fw.file = f
	fw.buf = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (fw *FileWriter) Commit() error {
	if fw.file != nil {
		if err := fw.buf.Flush(); err != nil {
			return err
		}
		if err := fw.file.Close(); err != nil {
			return err
		}
		fw.file = nil
		fw.buf = nil
	}
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

func (fw *FileWriter) IsJumbo(dataLen int) bool {
	return dataLen > fw.blockSize-3
}

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
	if err := fw.openIfNeeded(); err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	fw.buf.Write(fw.currentBlock) //nolint:errcheck
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

	if err := fw.openIfNeeded(); err != nil {
		fmt.Println("Error opening file:", err)
		return startBlock
	}

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

		fw.buf.Write(blockData) //nolint:errcheck
		fw.currentBlockNum++
	}

	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.offsetInBlock = 0
	return startBlock
}

func (fw *FileWriter) WriteRaw(data []byte) (int64, error) {
	fw.FlushCurrentBlock()
	if err := fw.openIfNeeded(); err != nil {
		return 0, err
	}
	if err := fw.buf.Flush(); err != nil {
		return 0, err
	}
	startOffset := int64(fw.currentBlockNum)*int64(fw.blockSize) + fw.rawBytesWritten
	if _, err := fw.file.Write(data); err != nil {
		return 0, err
	}
	fw.rawBytesWritten += int64(len(data))
	return startOffset, nil
}

func (fw *FileWriter) CurrentByteOffset() int64 {
	return int64(fw.currentBlockNum)*int64(fw.blockSize) + fw.rawBytesWritten
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
	if fw.file != nil {
		fw.buf.Flush()    //nolint:errcheck
		fw.file.Close()   //nolint:errcheck
		fw.file = nil
		fw.buf = nil
	}
	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.currentBlockNum = 0
	fw.offsetInBlock = 0
	fw.rawBytesWritten = 0
	if name == "" {
		name = generateFileName(0)
	}
	fw.location = filepath.Join(fw.dataRoot, name)
	if err := os.MkdirAll(filepath.Dir(fw.location), 0755); err != nil {
		fmt.Println("Error creating sstable dir:", err)
	}
}
