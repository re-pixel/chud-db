package ss_parser

import (
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/models/merkle_tree"
	"nosqlEngine/src/service/file_writer"
)

type SSParser interface {
	FlushMemtable(data []key_value.KeyValue)
}

type SSParserImpl struct {
	fileWriter file_writer.FileWriterInterface
}

func NewSSParser(fileWriter file_writer.FileWriterInterface) *SSParserImpl {
	return &SSParserImpl{fileWriter: fileWriter}
}

func (ssParser *SSParserImpl) FlushMemtable(data []key_value.KeyValue) {
	key_value.SortByKeys(&data)

	filter := bloom_filter.NewBloomFilterWithParams(len(data), 0.01)
	filter.AddMultiple(key_value.GetKeys(data))

	prefixFilter := bloom_filter.NewPrefixBloomFilter()
	prefixFilter.AddMultiple(key_value.GetKeys(data))

	merkleTree := merkle_tree.InitializeMerkleTree(len(data))
	for _, kv := range data {
		merkleTree.AddLeaf(kv.GetValue())
	}

	// Write data blocks and collect one index entry per block boundary.
	indexEntries := SerializeDataBuildIndex(ssParser.fileWriter, data)
	ssParser.fileWriter.Write(nil, true, nil) // flush last partial data block

	// Write index as raw bytes immediately after the last data block.
	indexBytes := SerializeIndex(indexEntries)
	indexOffset, _ := ssParser.fileWriter.WriteRaw(indexBytes) //nolint:errcheck

	// Write filter section (bloom + prefix bloom + merkle root) as raw bytes.
	bt_bf, _ := filter.SerializeToByteArray()
	bt_pbf, _ := prefixFilter.SerializeToByteArray()
	filterBytes := SerializeFilterSection(bt_bf, bt_pbf, merkleTree.GetRootBytes())
	filterOffset, _ := ssParser.fileWriter.WriteRaw(filterBytes) //nolint:errcheck

	// Write the 48-byte footer as the final raw bytes of the file.
	footer := SerializeFooter(indexOffset, int64(len(indexBytes)), filterOffset, int64(len(filterBytes)), int64(len(data)))
	ssParser.fileWriter.WriteRaw(footer) //nolint:errcheck

	ssParser.fileWriter.Commit() //nolint:errcheck
	ssParser.fileWriter.ResetFileWriter("")
}
