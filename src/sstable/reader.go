package sstable

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/ss_parser"
	"os"
	"sort"
)

var CONFIG = config.GetConfig()

const (
	NonJumbo    byte = 0
	JumboStart  byte = 1
	JumboMiddle byte = 3
	JumboEnd    byte = 7
)

type SSTableReader struct {
	path         string
	filter       *bloom_filter.BloomFilter
	prefixFilter *bloom_filter.PrefixBloomFilter
	index        []ss_parser.IndexEntry
	itemCount    int64
	maxLSN       uint64
	bm           *block_manager.BlockManager
}

func Open(path string, bm *block_manager.BlockManager) (*SSTableReader, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("sstable open: %w", err)
	}
	fileSize := info.Size()
	if fileSize < ss_parser.FooterSize {
		return nil, fmt.Errorf("sstable open: %s too small (%d bytes)", path, fileSize)
	}

	footerBytes, err := bm.ReadAt(path, fileSize-ss_parser.FooterSize, ss_parser.FooterSize)
	if err != nil {
		return nil, fmt.Errorf("sstable open: read footer: %w", err)
	}

	magic := int64(binary.BigEndian.Uint64(footerBytes[48:]))
	if magic != ss_parser.FooterMagic {
		return nil, fmt.Errorf("sstable open: bad magic in %s (got %x)", path, uint64(magic))
	}

	indexOffset  := int64(binary.BigEndian.Uint64(footerBytes[0:]))
	indexSize    := int64(binary.BigEndian.Uint64(footerBytes[8:]))
	filterOffset := int64(binary.BigEndian.Uint64(footerBytes[16:]))
	filterSize   := int64(binary.BigEndian.Uint64(footerBytes[24:]))
	itemCount    := int64(binary.BigEndian.Uint64(footerBytes[32:]))
	maxLSN       := binary.BigEndian.Uint64(footerBytes[40:])

	indexBytes, err := bm.ReadAt(path, indexOffset, int(indexSize))
	if err != nil {
		return nil, fmt.Errorf("sstable open: read index: %w", err)
	}
	index, err := parseIndex(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("sstable open: parse index: %w", err)
	}

	filterBytes, err := bm.ReadAt(path, filterOffset, int(filterSize))
	if err != nil {
		return nil, fmt.Errorf("sstable open: read filter: %w", err)
	}
	bf, pbf, err := parseFilterSection(filterBytes)
	if err != nil {
		return nil, fmt.Errorf("sstable open: parse filter: %w", err)
	}

	return &SSTableReader{
		path:         path,
		filter:       bf,
		prefixFilter: pbf,
		index:        index,
		itemCount:    itemCount,
		maxLSN:       maxLSN,
		bm:           bm,
	}, nil
}

func (r *SSTableReader) MaxLSN() uint64 { return r.maxLSN }

func (r *SSTableReader) Get(key string) (string, bool, error) {
	if !r.filter.Check(key) {
		return "", false, nil
	}

	i := sort.Search(len(r.index), func(j int) bool {
		return r.index[j].Key > key
	})
	if i == 0 {
		return "", false, nil
	}

	blockData, err := r.readDataBlock(r.index[i-1].Offset)
	if err != nil {
		return "", false, fmt.Errorf("get %q: %w", key, err)
	}

	off := 0
	for off < len(blockData) {
		k, v, n, err := readDataEntry(blockData[off:])
		if err != nil {
			break
		}
		off += n
		if k == key {
			return v, true, nil
		}
	}
	return "", false, nil
}

func (r *SSTableReader) PrefixScan(prefix string) (map[string]string, error) {
	if !r.prefixFilter.Contains(prefix) {
		return nil, nil
	}

	results := make(map[string]string)

	startIdx := sort.Search(len(r.index), func(j int) bool {
		return r.index[j].Key >= prefix
	})
	if startIdx > 0 {
		startIdx--
	}

	matchesPrefix := func(key string) bool {
		return len(key) >= len(prefix) && key[:len(prefix)] == prefix
	}
	isPastPrefix := func(key string) bool {
		n := len(prefix)
		if len(key) < n {
			n = len(key)
		}
		return key[:n] > prefix[:n]
	}

	for i := startIdx; i < len(r.index); i++ {
		blockData, err := r.readDataBlock(r.index[i].Offset)
		if err != nil {
			return results, err
		}

		done := false
		off := 0
		for off < len(blockData) {
			key, value, n, err := readDataEntry(blockData[off:])
			if err != nil {
				break
			}
			off += n
			if matchesPrefix(key) {
				results[key] = value
			} else if isPastPrefix(key) {
				done = true
				break
			}
		}
		if done {
			break
		}
	}
	return results, nil
}

func (r *SSTableReader) RangeScan(start, end string) (map[string]string, error) {
	results := make(map[string]string)

	startIdx := sort.Search(len(r.index), func(j int) bool {
		return r.index[j].Key >= start
	})
	if startIdx > 0 {
		startIdx--
	}

	for i := startIdx; i < len(r.index); i++ {
		if r.index[i].Key > end {
			break
		}

		blockData, err := r.readDataBlock(r.index[i].Offset)
		if err != nil {
			return results, err
		}

		off := 0
		for off < len(blockData) {
			key, value, n, err := readDataEntry(blockData[off:])
			if err != nil {
				break
			}
			off += n
			if key >= start && key <= end {
				results[key] = value
			} else if key > end {
				return results, nil
			}
		}
	}
	return results, nil
}

func (r *SSTableReader) ItemCount() int64 {
	return r.itemCount
}

func (r *SSTableReader) ScanAll(fn func(key, value string)) error {
	for _, entry := range r.index {
		blockData, err := r.readDataBlock(entry.Offset)
		if err != nil {
			return err
		}
		off := 0
		for off < len(blockData) {
			key, value, n, err := readDataEntry(blockData[off:])
			if err != nil {
				break
			}
			off += n
			fn(key, value)
		}
	}
	return nil
}

func (r *SSTableReader) readDataBlock(blockOffset int64) ([]byte, error) {
	block, err := r.bm.ReadAt(r.path, blockOffset, CONFIG.BlockSize)
	if err != nil {
		return nil, err
	}
	if len(block) < 3 {
		return nil, fmt.Errorf("block at offset %d too short (%d bytes)", blockOffset, len(block))
	}
	blockType := block[len(block)-1]
	cleanData := extractClean(block)

	switch blockType {
	case NonJumbo, JumboEnd:
		return cleanData, nil
	case JumboStart:
		return r.readJumboForward(blockOffset, cleanData)
	default:
		return nil, fmt.Errorf("unexpected block type %d at offset %d", blockType, blockOffset)
	}
}

func (r *SSTableReader) readJumboForward(startOffset int64, firstData []byte) ([]byte, error) {
	result := append([]byte(nil), firstData...)
	currentOffset := startOffset + int64(CONFIG.BlockSize)
	for {
		block, err := r.bm.ReadAt(r.path, currentOffset, CONFIG.BlockSize)
		if err != nil {
			return nil, err
		}
		blockType := block[len(block)-1]
		result = append(result, extractClean(block)...)
		if blockType == JumboEnd {
			break
		}
		currentOffset += int64(CONFIG.BlockSize)
	}
	return result, nil
}

func extractClean(block []byte) []byte {
	if len(block) < 3 {
		return block
	}
	usedBytes := int(block[len(block)-3])<<8 | int(block[len(block)-2])
	if usedBytes > len(block)-3 {
		return block[:len(block)-3]
	}
	return block[:usedBytes]
}

func readDataEntry(data []byte) (key, value string, bytesRead int, err error) {
	if len(data) < 16 {
		return "", "", 0, fmt.Errorf("data entry too short (%d bytes)", len(data))
	}
	off := 0
	keyLen := int(binary.BigEndian.Uint64(data[off:]))
	off += 8
	if off+keyLen > len(data) {
		return "", "", 0, fmt.Errorf("key truncated (need %d have %d)", keyLen, len(data)-off)
	}
	key = string(data[off : off+keyLen])
	off += keyLen
	if off+8 > len(data) {
		return "", "", 0, fmt.Errorf("value length field truncated")
	}
	valLen := int(binary.BigEndian.Uint64(data[off:]))
	off += 8
	if off+valLen > len(data) {
		return "", "", 0, fmt.Errorf("value truncated (need %d have %d)", valLen, len(data)-off)
	}
	value = string(data[off : off+valLen])
	off += valLen
	return key, value, off, nil
}

func parseIndex(data []byte) ([]ss_parser.IndexEntry, error) {
	var entries []ss_parser.IndexEntry
	off := 0
	for off < len(data) {
		if len(data)-off < 8 {
			break
		}
		keyLen := int(binary.BigEndian.Uint64(data[off:]))
		off += 8
		if off+keyLen > len(data) {
			return nil, fmt.Errorf("index: key truncated at offset %d", off)
		}
		key := string(data[off : off+keyLen])
		off += keyLen
		if off+8 > len(data) {
			return nil, fmt.Errorf("index: offset field truncated")
		}
		blockOffset := int64(binary.BigEndian.Uint64(data[off:]))
		off += 8
		entries = append(entries, ss_parser.IndexEntry{Key: key, Offset: blockOffset})
	}
	return entries, nil
}

func parseFilterSection(data []byte) (*bloom_filter.BloomFilter, *bloom_filter.PrefixBloomFilter, error) {
	off := 0
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("filter section too short")
	}
	bfLen := int(binary.BigEndian.Uint64(data[off:]))
	off += 8
	if off+bfLen > len(data) {
		return nil, nil, fmt.Errorf("bloom filter bytes truncated")
	}
	bf, err := bloom_filter.DeserializeFromByteArray(data[off : off+bfLen])
	if err != nil {
		return nil, nil, fmt.Errorf("bloom filter: %w", err)
	}
	off += bfLen

	if off+8 > len(data) {
		return nil, nil, fmt.Errorf("prefix filter length field truncated")
	}
	pbfLen := int(binary.BigEndian.Uint64(data[off:]))
	off += 8
	if off+pbfLen > len(data) {
		return nil, nil, fmt.Errorf("prefix filter bytes truncated")
	}
	pbf, err := bloom_filter.DeserializePrefixBloomFilter(data[off : off+pbfLen])
	if err != nil {
		return nil, nil, fmt.Errorf("prefix filter: %w", err)
	}

	return bf, pbf, nil
}
