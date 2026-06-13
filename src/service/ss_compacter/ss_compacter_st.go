package ss_compacter

import (
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/merkle_tree"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/retriever"
	"nosqlEngine/src/service/ss_parser"
	"nosqlEngine/src/utils"
	"os"

	"github.com/google/uuid"
)

var CONFIG = config.GetConfig()

type SSCompacterST struct {
}

func NewSSCompacterST() *SSCompacterST {
	return &SSCompacterST{}
}

func (sc *SSCompacterST) CheckCompactionConditions(bm *block_manager.BlockManager, dataRoot string) bool {
	level := 0
	compacted := false
	for level < CONFIG.LSMLevels {
		sstFiles := utils.ListSSTablesInLevel(dataRoot, level)

		for len(sstFiles) >= CONFIG.CompactionThreshold {
			toCompact := sstFiles[:CONFIG.CompactionThreshold]
			sstFiles = sstFiles[CONFIG.CompactionThreshold:]
			lvlDir := fmt.Sprintf("lvl%d", level+1)
			fw := file_writer.NewFileWriterInDir(bm, CONFIG.BlockSize, "sstable/"+lvlDir+"/sstable_"+uuid.New().String()+".db", dataRoot)
			sc.compactTables(toCompact, fw, bm)
			for _, file := range toCompact {
				os.Remove(file)
			}
			compacted = true
		}
		level++
	}
	return compacted
}

func (sc *SSCompacterST) compactTables(tables []string, fw *file_writer.FileWriter, bm *block_manager.BlockManager) {
	counts := make([]int, len(tables)) // holds the number of items in each table
	currKeys := make([]string, len(tables))
	currValues := make([]string, len(tables))
	pool := retriever.NewEntryRetrieverPool(bm, tables)
	totalItems := 0                                    // total number of items across all tables
	for i := range tables {
		counts[i] = int(pool.GetMetadata(i).Getnum_of_items())
		totalItems += counts[i]
		currKeys[i], currValues[i], _, _ = pool.ReadNextVal(i) // Read the first key and value from each table
	}
	// For Index
	keys := []string{}
	blockOffsets := []int{}
	currBlockOffset := -1

	bloom := bloom_filter.NewBloomFilterWithParams(totalItems, 0.01) // 1% false positive rate
	merkle := merkle_tree.InitializeMerkleTree(totalItems)

	for !areAllValuesZero(counts) {
		minIndex := getMinValIndex(currKeys, currValues)
		removeDuplicateKeys(currKeys, minIndex) // Remove duplicates for the current key
		bloom.Add(currKeys[minIndex])
		merkle.AddLeaf(string(currValues[minIndex])) // Add to Merkle tree
		fullVal := append(ss_parser.SizeAndValueToBytes(currKeys[minIndex]), ss_parser.SizeAndValueToBytes(currValues[minIndex])...)
		newBlockOffset := fw.Write(fullVal, false, nil)
		if currBlockOffset != newBlockOffset {
			currBlockOffset = newBlockOffset
			keys = append(keys, currKeys[minIndex])
			blockOffsets = append(blockOffsets, currBlockOffset)
		}
		currKeys[minIndex] = "" 
		updateValsAndCounts(currKeys, currValues, counts, pool)
	}
	fw.Write(nil, true, nil) // Write end of file marker
	summaryKeys, summaryOffsets := ss_parser.SerializeIndexGetOffsets(keys, blockOffsets, fw) // Write index offsets
	initialSummaryOffset := fw.Write(nil, true, nil)
	ss_parser.SerializeSummary(summaryKeys, summaryOffsets, fw)
	prefixFilter := bloom_filter.NewPrefixBloomFilter()

	bt_pbf, _ := prefixFilter.SerializeToByteArray()
	bt_bf, _ := bloom.SerializeToByteArray()          
	ss_parser.SerializeMetaData(fw.Write(nil, true, nil), bt_bf, merkle.GetRootBytes(), totalItems, fw, initialSummaryOffset, bt_pbf) // Write metadata
}
