package ss_compacter

import (
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/merkle_tree"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_parser"
	"nosqlEngine/src/sstable"
	"nosqlEngine/src/utils"
	"os"

	"github.com/google/uuid"
)

var CONFIG = config.GetConfig()

type SSCompacterST struct{}

func NewSSCompacterST() *SSCompacterST {
	return &SSCompacterST{}
}

func (sc *SSCompacterST) CheckCompactionConditions(bm *block_manager.BlockManager, dataRoot string, tc *sstable.TableCache) bool {
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
				if tc != nil {
					tc.Evict(file)
				} else {
					bm.CloseFile(file)
				}
				os.Remove(file) //nolint:errcheck
			}
			compacted = true
		}
		level++
	}
	return compacted
}

type kv struct{ key, value string }

func (sc *SSCompacterST) compactTables(tables []string, fw *file_writer.FileWriter, bm *block_manager.BlockManager) {
	streams := make([][]kv, len(tables))
	totalItems := 0
	for i, path := range tables {
		reader, err := sstable.Open(path, bm)
		if err != nil {
			continue
		}
		reader.ScanAll(func(key, value string) { //nolint:errcheck
			streams[i] = append(streams[i], kv{key, value})
		})
		totalItems += len(streams[i])
	}

	bloom := bloom_filter.NewBloomFilterWithParams(totalItems, 0.01)
	prefixFilter := bloom_filter.NewPrefixBloomFilter()
	merkle := merkle_tree.InitializeMerkleTree(totalItems)

	var indexEntries []ss_parser.IndexEntry
	lastBlockNum := -1
	indices := make([]int, len(streams))

	for {
		minKey := ""
		minIdx := -1
		for i, stream := range streams {
			if indices[i] >= len(stream) {
				continue
			}
			key := stream[indices[i]].key
			if minKey == "" || key < minKey {
				minKey = key
				minIdx = i
			}
		}
		if minIdx == -1 {
			break
		}

		value := streams[minIdx][indices[minIdx]].value

		for i, stream := range streams {
			if indices[i] < len(stream) && stream[indices[i]].key == minKey {
				indices[i]++
			}
		}

		bloom.Add(minKey)
		prefixFilter.Add(minKey)
		merkle.AddLeaf(value)

		fullVal := append(ss_parser.SizeAndValueToBytes(minKey), ss_parser.SizeAndValueToBytes(value)...)
		newBlockNum := fw.Write(fullVal, false, nil)
		if newBlockNum != lastBlockNum {
			lastBlockNum = newBlockNum
			indexEntries = append(indexEntries, ss_parser.IndexEntry{
				Key:    minKey,
				Offset: int64(newBlockNum) * int64(CONFIG.BlockSize),
			})
		}
	}
	fw.Write(nil, true, nil)

	bt_bf, _ := bloom.SerializeToByteArray()
	bt_pbf, _ := prefixFilter.SerializeToByteArray()

	indexBytes := ss_parser.SerializeIndex(indexEntries)
	indexOffset, _ := fw.WriteRaw(indexBytes)

	filterBytes := ss_parser.SerializeFilterSection(bt_bf, bt_pbf, merkle.GetRootBytes())
	filterOffset, _ := fw.WriteRaw(filterBytes)

	footer := ss_parser.SerializeFooter(indexOffset, int64(len(indexBytes)), filterOffset, int64(len(filterBytes)), int64(totalItems))
	fw.WriteRaw(footer) //nolint:errcheck
}
