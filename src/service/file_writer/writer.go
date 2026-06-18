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
	allDataWritten  []byte
}

func NewFileWriter(bm *block_manager.BlockManager, blockSize int, name string) *FileWriter {
	return NewFileWriterInDir(bm, blockSize, name, utils.DefaultDataRoot())
}

func NewFileWriterInDir(bm *block_manager.BlockManager, blockSize int, name string, dataRoot string) *FileWriter {
	if name == "" {
		name = generateFileName(0) // Default name if not provided
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
		allDataWritten:  make([]byte, 0),
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
// If the location does not end with ".tmp" (e.g. a test supplied a .db path
// directly) this is a safe no-op.
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

func (fw *FileWriter) Write(data []byte, sectionEnd bool, size []byte) int {
	if sectionEnd {
		if len(size) > 0 {
			fw.FlushWithSize(size)
		}
		if len(fw.currentBlock) > 0 {
			fw.FlushCurrentBlock()
		}
	}

	if fw.IsJumbo(len(data)) {
		fw.WriteJumboData(data)
		return fw.currentBlockNum
	}

	if !fw.CanWrite(len(data)) {
		// Write current block to disk and start a new block
		fw.FlushCurrentBlock()
	}
	fw.currentBlock = append(fw.currentBlock, data...)
	fw.offsetInBlock += len(data)

	return fw.currentBlockNum
}

// IsJumbo returns true if the data is larger than a single block
func (fw *FileWriter) IsJumbo(dataLen int) bool {
	// Account for notation (3 bytes) and jumbo flag (3 bytes) = 6 bytes overhead for single block
	return dataLen > fw.blockSize-6
}

// Jumbo flag constants
// Block structure: [DATA] + [<!>] + [PADDING] + [3-BYTE JUMBO FLAG]
const (
	JumboStart  = 1 // 00000001 - First block in jumbo sequence
	JumboMiddle = 3 // 00000011 - Middle block in jumbo sequence
	JumboEnd    = 7 // 00000111 - Last block in jumbo sequence
	NonJumbo    = 0 // 00000000 - Regular non-jumbo block
)

// GetJumboFlagName returns a human-readable name for the jumbo flag
func GetJumboFlagName(flag byte) string {
	switch flag {
	case JumboStart:
		return "JUMBO_START"
	case JumboMiddle:
		return "JUMBO_MIDDLE"
	case JumboEnd:
		return "JUMBO_END"
	case NonJumbo:
		return "NON_JUMBO"
	default:
		return "UNKNOWN"
	}
}

// WriteJumboData splits and writes data that is larger than a block
func (fw *FileWriter) WriteJumboData(data []byte) {

	if len(fw.currentBlock) > 0 {
		fw.FlushCurrentBlock()
	}

	// Calculate how much space is available per block
	// Every block needs space for: data + <!> + padding + jumbo_flag
	// So available space for data is: blockSize - 3 (<!>) - 3 (jumbo_flag) = blockSize - 6
	availablePerBlock := fw.blockSize - 6 // 3 bytes for <!>, 3 bytes for jumbo flag

	// Calculate number of blocks needed
	numBlocks := (len(data) + availablePerBlock - 1) / availablePerBlock

	dataOffset := 0
	for i := 0; i < numBlocks; i++ {

		remainingData := len(data) - dataOffset
		chunkSize := availablePerBlock
		if remainingData < availablePerBlock {
			chunkSize = remainingData
		}

		wrData := data[dataOffset : dataOffset+chunkSize]
		dataOffset += chunkSize

		wrDataCopy := make([]byte, len(wrData))
		copy(wrDataCopy, wrData)
		wrData = wrDataCopy

		jumboFlag := make([]byte, 3)
		if numBlocks == 1 {
			// Single jumbo block
			jumboFlag[2] = JumboStart
		} else if i == 0 {
			// First block in sequence
			jumboFlag[2] = JumboStart
		} else if i == numBlocks-1 {
			// Last block in sequence
			jumboFlag[2] = JumboEnd
		} else {
			// Middle block in sequence
			jumboFlag[2] = JumboMiddle
		}

		// Add <!> notation to every block
		notation := "<!>"
		notationBytes := []byte(notation)
		wrData = append(wrData, notationBytes...)

		// Add padding to reach block size
		if len(wrData)+3 < fw.blockSize {
			padding := make([]byte, fw.blockSize-len(wrData)-3)
			wrData = append(wrData, padding...)
		}

		// Add jumbo flag at the end
		wrData = append(wrData, jumboFlag...)

		fw.allDataWritten = append(fw.allDataWritten, wrData...)
		err := fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, wrData)

		if err != nil {
			fmt.Printf("Error writing jumbo block %d: %v\n", fw.currentBlockNum, err)
			return
		}
		fw.currentBlockNum++
		fw.currentBlock = make([]byte, 0, fw.blockSize)
		fw.offsetInBlock = 0

	}
}

// CanWrite checks if the data can fit in the current block (reserving 3 bytes for jumbo flag)
func (fw *FileWriter) CanWrite(dataLen int) bool {
	return fw.offsetInBlock+dataLen+3 <= fw.blockSize // Reserve 3 bytes for jumbo flag
}

// FlushCurrentBlock writes the current block to disk and starts a new block
func (fw *FileWriter) FlushCurrentBlock() {
	// when flushing we add a flag at the end of data to indicate that the rest is padding
	if len(fw.currentBlock) > 0 {
		// Add jumbo flag (0 = not jumbo)
		jumboFlag := make([]byte, 3)
		jumboFlag[2] = NonJumbo // Use constant for non-jumbo blocks
		notation := "<!>"       //data end notation
		notationBytes := []byte(notation)
		//add padding to ensure block size (accounting for 3-byte jumbo flag)
		if len(fw.currentBlock)+3+3 < fw.blockSize {
			padding := make([]byte, fw.blockSize-len(fw.currentBlock)-3-3)
			fw.currentBlock = append(fw.currentBlock, notationBytes...)
			fw.currentBlock = append(fw.currentBlock, padding...)
		}

		// Add jumbo flag at the end
		fw.currentBlock = append(fw.currentBlock, jumboFlag...)
		fw.allDataWritten = append(fw.allDataWritten, fw.currentBlock...)
		fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, fw.currentBlock)
		fw.currentBlockNum++
		fw.currentBlock = make([]byte, 0, fw.blockSize)
		fw.offsetInBlock = 0
	}
}

func (fw *FileWriter) FlushWithSize(size []byte) {
	//this is the same flush bit instead of padding to the top, at the last 8bytes there is size var
	fw.FlushCurrentBlock()
	// Add jumbo flag (0 = not jumbo)
	jumboFlag := make([]byte, 3)
	jumboFlag[2] = NonJumbo // Use constant for non-jumbo blocks
	notation := "<!>"       //data end notation
	// move string into bytes
	notationBytes := []byte(notation)
	if len(fw.currentBlock)+3+8+3 >= fw.blockSize {
		fw.FlushCurrentBlock()
	}
	padding := make([]byte, fw.blockSize-len(fw.currentBlock)-3-8-3) // 8 bytes for size, 3 bytes for jumbo flag

	fw.currentBlock = append(fw.currentBlock, padding...)
	fw.currentBlock = append(fw.currentBlock, size...)
	fw.currentBlock = append(fw.currentBlock, notationBytes...) // Add end of section notation
	fw.currentBlock = append(fw.currentBlock, jumboFlag...)     // Add jumbo flag at the very end
	// If the current block is already full, we need to flush it first

	fw.allDataWritten = append(fw.allDataWritten, fw.currentBlock...)
	fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, fw.currentBlock)
	fw.currentBlockNum++
	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.offsetInBlock = 0
}
func (fw *FileWriter) GetAllDataWritten() []byte {
	return fw.allDataWritten
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
		name = generateFileName(0) // Default name if not provided
	}
	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.currentBlockNum = 0
	fw.offsetInBlock = 0
	fw.allDataWritten = make([]byte, 0)
	fw.location = filepath.Join(fw.dataRoot, name)
	if err := os.MkdirAll(filepath.Dir(fw.location), 0755); err != nil {
		fmt.Println("Error creating sstable dir:", err)
	}
}
