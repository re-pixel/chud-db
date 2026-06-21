package ss_parser

import (
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/models/merkle_tree"
	"nosqlEngine/src/service/file_writer"
)

type SSParser interface {
	FlushMemtable(data []key_value.KeyValue, maxLSN uint64) string
}

type SSParserImpl struct {
	fileWriter file_writer.FileWriterInterface
}

func NewSSParser(fileWriter file_writer.FileWriterInterface) *SSParserImpl {
	return &SSParserImpl{fileWriter: fileWriter}
}

func (ssParser *SSParserImpl) FlushMemtable(data []key_value.KeyValue, maxLSN uint64) string {
	filter := bloom_filter.NewBloomFilterWithParams(len(data), 0.01)
	filter.AddMultiple(key_value.GetKeys(data))

	prefixFilter := bloom_filter.NewPrefixBloomFilter()
	prefixFilter.AddMultiple(key_value.GetKeys(data))

	merkleTree := merkle_tree.InitializeMerkleTree(len(data))
	for _, kv := range data {
		merkleTree.AddLeaf(kv.GetValue())
	}

	indexEntries := SerializeDataBuildIndex(ssParser.fileWriter, data)
	ssParser.fileWriter.Write(nil, true, nil)

	indexBytes := SerializeIndex(indexEntries)
	indexOffset, _ := ssParser.fileWriter.WriteRaw(indexBytes) //nolint:errcheck

	bt_bf, _ := filter.SerializeToByteArray()
	bt_pbf, _ := prefixFilter.SerializeToByteArray()
	filterBytes := SerializeFilterSection(bt_bf, bt_pbf, merkleTree.GetRootBytes())
	filterOffset, _ := ssParser.fileWriter.WriteRaw(filterBytes) //nolint:errcheck

	footer := SerializeFooter(indexOffset, int64(len(indexBytes)), filterOffset, int64(len(filterBytes)), int64(len(data)), maxLSN)
	ssParser.fileWriter.WriteRaw(footer) //nolint:errcheck

	ssParser.fileWriter.Commit() //nolint:errcheck
	path := ssParser.fileWriter.GetLocation()
	ssParser.fileWriter.ResetFileWriter("")
	return path
}
