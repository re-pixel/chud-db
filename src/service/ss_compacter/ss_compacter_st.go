package ss_compacter

import (
	"container/heap"
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
			fw := file_writer.NewFileWriterInDir(CONFIG.BlockSize, "sstable/"+lvlDir+"/sstable_"+uuid.New().String()+".db.tmp", dataRoot)
			isLastLevel := level+1 >= CONFIG.LSMLevels
			if err := sc.compactTables(toCompact, fw, bm, isLastLevel); err != nil {
				continue
			}
			if err := fw.Commit(); err != nil {
				os.Remove(fw.GetLocation()) //nolint:errcheck
				continue
			}
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

// heapEntry is a min-heap element holding one entry from one input stream.
type heapEntry struct {
	key, value string
	streamIdx  int
}

type mergeHeap []heapEntry

func (h mergeHeap) Len() int            { return len(h) }
func (h mergeHeap) Less(i, j int) bool  { return h[i].key < h[j].key }
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)         { *h = append(*h, x.(heapEntry)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type kv struct{ key, value string }

func (sc *SSCompacterST) compactTables(tables []string, fw *file_writer.FileWriter, bm *block_manager.BlockManager, isLastLevel bool) error {
	streams := make([][]kv, len(tables))
	totalItems := 0
	for i, path := range tables {
		reader, err := sstable.Open(path, bm)
		if err != nil {
			return fmt.Errorf("compaction: open %s: %w", path, err)
		}
		if err := reader.ScanAll(func(key, value string) {
			streams[i] = append(streams[i], kv{key, value})
		}); err != nil {
			return fmt.Errorf("compaction: scan %s: %w", path, err)
		}
		totalItems += len(streams[i])
	}

	bloom := bloom_filter.NewBloomFilterWithParams(totalItems, 0.01)
	prefixFilter := bloom_filter.NewPrefixBloomFilter()
	merkle := merkle_tree.InitializeMerkleTree(totalItems)

	// Seed the heap with the first entry from each non-empty stream.
	indices := make([]int, len(streams))
	h := &mergeHeap{}
	heap.Init(h)
	for i, stream := range streams {
		if len(stream) > 0 {
			heap.Push(h, heapEntry{key: stream[0].key, value: stream[0].value, streamIdx: i})
			indices[i] = 1
		}
	}

	var indexEntries []ss_parser.IndexEntry
	lastBlockNum := -1
	lastKey := ""

	for h.Len() > 0 {
		entry := heap.Pop(h).(heapEntry)

		// Advance this stream and push its next entry.
		si := entry.streamIdx
		if indices[si] < len(streams[si]) {
			next := streams[si][indices[si]]
			heap.Push(h, heapEntry{key: next.key, value: next.value, streamIdx: si})
			indices[si]++
		}

		// Dedup: newer levels have lower indices, so the first time we see a
		// key it is the most recent version. Skip subsequent duplicates.
		if entry.key == lastKey {
			continue
		}
		lastKey = entry.key

		// Drop tombstones at the last level — no lower level can hold the key.
		if isLastLevel && entry.value == CONFIG.Tombstone {
			continue
		}

		bloom.Add(entry.key)
		prefixFilter.Add(entry.key)
		merkle.AddLeaf(entry.value)

		fullVal := append(ss_parser.SizeAndValueToBytes(entry.key), ss_parser.SizeAndValueToBytes(entry.value)...)
		newBlockNum := fw.Write(fullVal, false, nil)
		if newBlockNum != lastBlockNum {
			lastBlockNum = newBlockNum
			indexEntries = append(indexEntries, ss_parser.IndexEntry{
				Key:    entry.key,
				Offset: int64(newBlockNum) * int64(CONFIG.BlockSize),
			})
		}
	}
	fw.Write(nil, true, nil)

	bt_bf, err := bloom.SerializeToByteArray()
	if err != nil {
		os.Remove(fw.GetLocation()) //nolint:errcheck
		return fmt.Errorf("compaction: serialize bloom filter: %w", err)
	}
	bt_pbf, err := prefixFilter.SerializeToByteArray()
	if err != nil {
		os.Remove(fw.GetLocation()) //nolint:errcheck
		return fmt.Errorf("compaction: serialize prefix filter: %w", err)
	}

	indexBytes := ss_parser.SerializeIndex(indexEntries)
	indexOffset, err := fw.WriteRaw(indexBytes)
	if err != nil {
		os.Remove(fw.GetLocation()) //nolint:errcheck
		return fmt.Errorf("compaction: write index: %w", err)
	}

	filterBytes := ss_parser.SerializeFilterSection(bt_bf, bt_pbf, merkle.GetRootBytes())
	filterOffset, err := fw.WriteRaw(filterBytes)
	if err != nil {
		os.Remove(fw.GetLocation()) //nolint:errcheck
		return fmt.Errorf("compaction: write filter: %w", err)
	}

	footer := ss_parser.SerializeFooter(indexOffset, int64(len(indexBytes)), filterOffset, int64(len(filterBytes)), int64(totalItems))
	if _, err := fw.WriteRaw(footer); err != nil {
		os.Remove(fw.GetLocation()) //nolint:errcheck
		return fmt.Errorf("compaction: write footer: %w", err)
	}

	return nil
}
