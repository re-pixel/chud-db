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

type SSCompacterST struct{}

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
				os.Remove(file) //nolint:errcheck
			}
			compacted = true
		}
		level++
	}
	return compacted
}

func (sc *SSCompacterST) compactTables(tables []string, fw *file_writer.FileWriter, bm *block_manager.BlockManager) {
	counts := make([]int, len(tables))
	currKeys := make([]string, len(tables))
	currValues := make([]string, len(tables))
	pool := retriever.NewEntryRetrieverPool(bm, tables)
	totalItems := 0
	for i := range tables {
		counts[i] = int(pool.GetMetadata(i).Getnum_of_items())
		totalItems += counts[i]
		currKeys[i], currValues[i], _, _ = pool.ReadNextVal(i)
	}

	bloom := bloom_filter.NewBloomFilterWithParams(totalItems, 0.01)
	prefixFilter := bloom_filter.NewPrefixBloomFilter()
	merkle := merkle_tree.InitializeMerkleTree(totalItems)

	var indexEntries []ss_parser.IndexEntry
	lastBlockNum := -1

	for !areAllValuesZero(counts) {
		minIndex := getMinValIndex(currKeys, currValues)
		removeDuplicateKeys(currKeys, minIndex)

		bloom.Add(currKeys[minIndex])
		prefixFilter.Add(currKeys[minIndex])
		merkle.AddLeaf(currValues[minIndex])

		fullVal := append(ss_parser.SizeAndValueToBytes(currKeys[minIndex]), ss_parser.SizeAndValueToBytes(currValues[minIndex])...)
		newBlockNum := fw.Write(fullVal, false, nil)
		if newBlockNum != lastBlockNum {
			lastBlockNum = newBlockNum
			indexEntries = append(indexEntries, ss_parser.IndexEntry{
				Key:    currKeys[minIndex],
				Offset: int64(newBlockNum) * int64(CONFIG.BlockSize),
			})
		}

		currKeys[minIndex] = ""
		updateValsAndCounts(currKeys, currValues, counts, pool)
	}
	fw.Write(nil, true, nil) // flush last data block

	bt_bf, _ := bloom.SerializeToByteArray()
	bt_pbf, _ := prefixFilter.SerializeToByteArray()

	indexBytes := ss_parser.SerializeIndex(indexEntries)
	indexOffset, _ := fw.WriteRaw(indexBytes) //nolint:errcheck

	filterBytes := ss_parser.SerializeFilterSection(bt_bf, bt_pbf, merkle.GetRootBytes())
	filterOffset, _ := fw.WriteRaw(filterBytes) //nolint:errcheck

	footer := ss_parser.SerializeFooter(indexOffset, int64(len(indexBytes)), filterOffset, int64(len(filterBytes)), int64(totalItems))
	fw.WriteRaw(footer) //nolint:errcheck
}
