package retriever

import (
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_reader"
	"nosqlEngine/src/utils"
)

var CONFIG = config.GetConfig()

type EntryRetriever struct {
	fileReader   file_reader.FileReader
	sstablePaths []string
	currentIndex int
	dataRoot     string
}

type EntryRetrieverPool struct {
	fileReaders    []file_reader.FileReader
	sstablePaths   []string
	currentIndex   int
	metadata       []Metadata
	readCounters   []int64  // Track read values per reader
	currentBlocks  []int64  // Current block index for each reader
	blockPositions []int    // Current position within block for each reader
	cachedBlocks   [][]byte // Cached cleaned block data for each reader
}


// type Block struct {
// 	// Placeholder struct - can be removed if not needed
// }



func NewEntryRetriever(bm *block_manager.BlockManager) *EntryRetriever {
	return NewEntryRetrieverInDir(bm, utils.DefaultDataRoot())
}

func NewEntryRetrieverInDir(bm *block_manager.BlockManager, dataRoot string) *EntryRetriever {
	sstablePaths := utils.ListSSTablesInLevel(dataRoot, 0)

	// Create a single block manager and file reader instance
	var fileReader file_reader.FileReader

	if len(sstablePaths) > 0 {
		// Initialize with the first SSTable if available
		fileReader = *file_reader.NewFileReader(sstablePaths[0], CONFIG.BlockSize, *bm)
	} else {
		// Initialize with empty path if no SSTables found
		fileReader = *file_reader.NewFileReader("", CONFIG.BlockSize, *bm)
	}

	return &EntryRetriever{
		fileReader:   fileReader,
		sstablePaths: sstablePaths,
		currentIndex: 0,
		dataRoot:     dataRoot,
	}
}

func NewEntryRetrieverPool(bm *block_manager.BlockManager, tables []string) *EntryRetrieverPool {

	fileReaders := make([]file_reader.FileReader, len(tables))
	readersPerMetadata := make([]Metadata, len(tables))
	readCounters := make([]int64, len(tables))
	currentBlocks := make([]int64, len(tables))
	blockPositions := make([]int, len(tables))
	cachedBlocks := make([][]byte, len(tables))

	for i, table := range tables {
		fileReaders[i] = *file_reader.NewFileReader(table, CONFIG.BlockSize, *bm)
		fileReaders[i].SetDirection(false) // Set default direction to forward
		md, err := deserializeMetadataOnly(fileReaders[i])
		if err != nil {
			readersPerMetadata[i] = Metadata{}
		} else {
			readersPerMetadata[i] = md
		}
		readCounters[i] = 0 // Initialize read counter

		// Initialize reading state - start from the beginning of data section
		currentBlocks[i] = 0 // Start from block 0
		blockPositions[i] = 0 // Start at beginning of block
		cachedBlocks[i] = nil // No cached block initially
	}

	return &EntryRetrieverPool{
		fileReaders:    fileReaders,
		sstablePaths:   tables,
		currentIndex:   0,
		metadata:       readersPerMetadata,
		readCounters:   readCounters,
		currentBlocks:  currentBlocks,
		blockPositions: blockPositions,
		cachedBlocks:   cachedBlocks,
	}
}

func (ep *EntryRetrieverPool) GetMetadata(index int) *Metadata {
	if index < 0 || index >= len(ep.metadata) {
		return nil
	}
	return &ep.metadata[index]
}

func (r *EntryRetrieverPool) ReadNextVal(readerIndex int) (string, string, bool, error) {

	//check the if there is a cached block
	if r.cachedBlocks[readerIndex] == nil || r.blockPositions[readerIndex] >= len(r.cachedBlocks[readerIndex])-16 {
		err := r.loadNextBlock(readerIndex)
		if err != nil {
			return "", "", false, fmt.Errorf("error loading next block: %v", err)
		}
	}
	key, value, bytesRead, err := readDataEntry(r.cachedBlocks[readerIndex][r.blockPositions[readerIndex]:])
	r.blockPositions[readerIndex] += bytesRead
	if err != nil {
		return "", "", false, fmt.Errorf("error reading data entry: %v", err)
	}
	return key, value, false, nil
}

func (r *EntryRetrieverPool) loadNextBlock(readerIndex int) error {
	reader := r.fileReaders[readerIndex]

	// Read the block at current position
	reader.SetDirection(true)
	data, readBlocks, err := reader.ReadEntry(int(r.currentBlocks[readerIndex]))
	if err != nil {
		return fmt.Errorf("error reading block %d: %v", r.currentBlocks[readerIndex], err)
	}

	// TODO: Clean the block data (remove <!> markers, handle jumbo blocks, etc.)
	// For now, just cache the raw data
	r.cachedBlocks[readerIndex] = data
	r.blockPositions[readerIndex] = 0
	r.currentBlocks[readerIndex] += int64(readBlocks)

	return nil
}

func (r *EntryRetriever) resetToNextSSTable() bool {
	r.currentIndex++
	if r.currentIndex >= len(r.sstablePaths) {
		return false
	}

	r.fileReader.ResetReader(r.sstablePaths[r.currentIndex], false)
	return true
}

func (r *EntryRetriever) RetrieveEntry(key string) (string, bool, error) {

	r.currentIndex = 0 // Reset to first SSTable
	r.sstablePaths = []string{}
	for i := 0; i <= CONFIG.LSMLevels; i++ {
		r.sstablePaths = append(r.sstablePaths, utils.ListSSTablesInLevel(r.dataRoot, i)...)
	}
	if len(r.sstablePaths) == 0 {
		return "", false, fmt.Errorf("no SSTables found")
	}
	r.fileReader.ResetReader(r.sstablePaths[r.currentIndex], false)

	for {
		r.fileReader.SetDirection(false)
		md, err := r.deserializeMetadata(key)
		if err != nil {
			if !r.resetToNextSSTable() {
				return "",false, fmt.Errorf("key %s not found in any SSTable", key)
			}
			continue
		}

		sumArray, errSum := r.deserializeSummary(md)
		if errSum != nil {
			if !r.resetToNextSSTable() {
				return "",false, fmt.Errorf("key %s not found in any SSTable", key)
			}
			continue
		}

		found := false
		for i := 0; i < len(sumArray)-1; i++ {
			if key >= sumArray[i].getKey() && key <= sumArray[i+1].getKey() {
				found = true
				// Key found, read the entry from the file
				//search the offsets
				//ending offset is sumArray[i].getOffset()
				//starting offset is sumArray[i+1].getOffset()
				totalBlocks, _ := r.fileReader.GetFileSizeBlocks()
				endOffset := sumArray[i].getOffset()
				endOffset = int64(totalBlocks) - endOffset
				startOffset := sumArray[i+1].getOffset()
				startOffset = int64(totalBlocks) - startOffset - 1

				offset, err := r.searchIndex(startOffset, endOffset, key)
				if err != nil {
					break // Break inner loop, try next SSTable
				}

				dataOffset := int64(totalBlocks) - offset - 1

				value, dataErr := r.searchData(dataOffset, key)
				if dataErr != nil {
					fmt.Printf("Error searching data in %s: %v\n", r.sstablePaths[r.currentIndex], dataErr)
					break // Break inner loop, try next SSTable
				}
				return value, true, nil // Found the key, return the value
			}
		}

		if !found {
			fmt.Printf("Key %s not found in range in %s\n", key, r.sstablePaths[r.currentIndex])
		}

		// Try next SSTable
		if !r.resetToNextSSTable() {
			return "", false, fmt.Errorf("key %s not found in any SSTable", key)
		}
	}
}

func (r *EntryRetriever) deserializeMetadata(key string) (Metadata, error) {

	i := 0
	initial, readBlocks, err := r.fileReader.ReadEntry(i)
	if err != nil {
		return Metadata{}, fmt.Errorf("error reading block %d: %v, READ %d blocks", i, err, readBlocks)
	}
	i += 1

	mdOffset := bytesToInt(initial[len(initial)-8:])

	totalBlocks, err := r.fileReader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	numOfBlocks := int64(totalBlocks) - mdOffset
	completedBlocks := make([]byte, 0, int(numOfBlocks)*CONFIG.BlockSize)
	completedBlocks = append(completedBlocks, initial...)
	for i < int(numOfBlocks) {
		block, readBlocks, err := r.fileReader.ReadEntry(i)
		if err != nil {
			return Metadata{}, fmt.Errorf("error reading block %d: %v", i, err)
		}
		completedBlocks = append(block, completedBlocks...)
		if readBlocks == int(numOfBlocks) {
			break
		}
		i += int(readBlocks)
	}
	offsetInBlock := int64(0)
	completedBlocks = append(completedBlocks, initial...)
	bf_size := bytesToInt(completedBlocks[:8])
	offsetInBlock += 8
	bf_data := completedBlocks[offsetInBlock : offsetInBlock+bf_size]
	offsetInBlock += bf_size
	b, err := bloom_filter.DeserializeFromByteArray(bf_data)

	bf_pb_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	bf_bp_bytes := completedBlocks[offsetInBlock : offsetInBlock+bf_pb_size]
	offsetInBlock += bf_pb_size
	if err != nil {
		return Metadata{}, fmt.Errorf("error deserializing bloom filter: %v", err)
	}

	ex := b.Check(key)
	if !ex {
		return Metadata{}, fmt.Errorf("key %s not found in bloom filter", key)
	}

	sum_start_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	sum_end_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	blocksInFile, err := r.fileReader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	sum_start_offset = int64(blocksInFile) - sum_start_offset
	sum_end_offset = int64(blocksInFile) - sum_end_offset

	num_of_items := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	merkle_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	merkle_data := completedBlocks[offsetInBlock : offsetInBlock+merkle_size]

	md := Metadata{
		bf_size:       bf_size,
		bf_data:       bf_data,
		summary_start: sum_start_offset,
		summary_end:   sum_end_offset,
		num_of_items:    num_of_items,
		merkle_size:   merkle_size,
		merkle_data:   merkle_data,
		bf_pb_size:    bf_pb_size,
		bf_bp_bytes:   bf_bp_bytes,
	}

	return md, nil
}

func (r *EntryRetriever) deserializeSummary(metadata Metadata) ([]KeyOffset, error) {
	sortedSummaryArray := make([]KeyOffset, 0, metadata.Getnum_of_items())
	i := metadata.summary_start

	for i < metadata.summary_end {
		offsetInBlock := 0

		data, readBlocks, err := r.fileReader.ReadEntry(int(i))
		//this is one summary block which can contain multiple entries
		if err != nil {
			return nil, err
		}
		subArray := make([]KeyOffset, 0, metadata.Getnum_of_items())
		for offsetInBlock < len(data) {
			val, offset, off, errorSum := readSummaryIndexEntry(data[offsetInBlock:])
			offsetInBlock += off
			subArray = append(subArray, KeyOffset{key: val, offset: offset})
			if errorSum != nil {
				return nil, fmt.Errorf("error reading summary entry: %v", errorSum)
			}

		}
		sortedSummaryArray = append(subArray, sortedSummaryArray...)
		i += int64(readBlocks)
	}
	return sortedSummaryArray, nil
}

func readSummaryIndexEntry(data []byte) (string, int64, int, error) {
	if len(data) < 16 {
		return "", 0, 0, fmt.Errorf("invalid summary entry data")
	}
	off := 0
	valSize := bytesToInt(data[:8])
	off += 8
	val := data[8 : 8+valSize]
	off += int(valSize)
	offset := data[8+valSize : 8+valSize+8]
	off += 8

	return string(val), bytesToInt(offset), off, nil
}

func (r *EntryRetriever) searchIndex(start int64, end int64, key string) (int64, error) {
	if start == end {
		start = start - 1
	}
	i := start
	for i < end {
		data, readBlocks, err := r.fileReader.ReadEntry(int(i))
		if err != nil {
			return 0, fmt.Errorf("error reading block %d: %v", i, err)
		}

		offsetInBlock := 0
		for offsetInBlock < len(data) {
			val, offset, off, err := readSummaryIndexEntry(data[offsetInBlock:])
			if err != nil {
				return 0, fmt.Errorf("error reading summary entry: %v", err)
			}
			offsetInBlock += off
			if val == key {
				return offset, nil
			}
		}
		i += int64(readBlocks)
	}

	return 0, fmt.Errorf("key %s not found in index", key)
}

func (r *EntryRetriever) searchData(offset int64, key string) (string, error) {
	data, _, err := r.fileReader.ReadEntry(int(offset))
	if err != nil {
		return "", fmt.Errorf("error reading data at offset %d: %v", offset, err)
	}
	offsetInBlock := 0
	for offsetInBlock < len(data) {

		keyRetrieved, value, off, err := readDataEntry(data[offsetInBlock:])
		offsetInBlock += off
		if err != nil {
			return "", fmt.Errorf("error reading summary entry: %v", err)
		}
		if keyRetrieved == key {
			return value, nil // Found the key, return the offset
		}
	}
	return "", fmt.Errorf("data not found for key %s at offset %d", key, offset)
}

func readDataEntry(data []byte) (string, string, int, error) {
	if len(data) < 16 {
		return "", "", 0, fmt.Errorf("invalid data entry")
	}
	off := 0
	keySize := bytesToInt(data[:8])
	off += 8
	key := data[8 : 8+keySize]
	off += int(keySize)
	valueSize := bytesToInt(data[8+keySize : 16+keySize])
	off += 8
	value := data[16+keySize : 16+keySize+valueSize]
	off += int(valueSize)
	return string(key), string(value), off, nil
}

