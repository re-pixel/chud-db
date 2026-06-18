package ss_parser

import (
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/models/merkle_tree"
	"nosqlEngine/src/service/file_writer"
)

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
	merkleTree := merkle_tree.InitializeMerkleTree(len(data))
	for _, kv := range data {
		merkleTree.AddLeaf(kv.GetValue())
	}
	keys, offsets := SerializeDataGetOffsets(ssParser.fileWriter, data)
	ssParser.fileWriter.Write(nil, true, nil) // Write end of section marker

	sumKeys, sumOffsets := SerializeIndexGetOffsets(keys, offsets, ssParser.fileWriter)
	initialSummaryOffset := ssParser.fileWriter.Write(nil, true, nil)

	SerializeSummary(sumKeys, sumOffsets, ssParser.fileWriter)
	bt_bf, _ := filter.SerializeToByteArray()
	prefixFilter := bloom_filter.NewPrefixBloomFilter()
	prefixFilter.AddMultiple(key_value.GetKeys(data))
	bt_pbf, _ := prefixFilter.SerializeToByteArray()
	SerializeMetaData(ssParser.fileWriter.Write(nil, true, nil), bt_bf, merkleTree.GetRootBytes(), len(data), ssParser.fileWriter, initialSummaryOffset, bt_pbf)

	// Atomically publish the completed SSTable and prepare for the next flush.
	ssParser.fileWriter.Commit()         //nolint:errcheck
	ssParser.fileWriter.ResetFileWriter("")
}
