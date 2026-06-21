package ss_parser

import (
	"encoding/binary"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

var CONFIG = config.GetConfig()

const FooterSize = 56
const FooterMagic = int64(0x0D1AACCE55DB0002)

type IndexEntry struct {
	Key    string
	Offset int64 // byte offset of the data block from the start of the file
}

func SerializeDataBuildIndex(fw file_writer.FileWriterInterface, keyValues []key_value.KeyValue) []IndexEntry {
	var index []IndexEntry
	lastBlock := -1
	for _, kv := range keyValues {
		value := append(SizeAndValueToBytes(kv.GetKey()), SizeAndValueToBytes(kv.GetValue())...)
		blockNum := fw.Write(value, false, nil)
		if blockNum != lastBlock {
			index = append(index, IndexEntry{
				Key:    kv.GetKey(),
				Offset: int64(blockNum) * int64(CONFIG.BlockSize),
			})
			lastBlock = blockNum
		}
	}
	return index
}

func SerializeIndex(entries []IndexEntry) []byte {
	var buf []byte
	for _, e := range entries {
		buf = append(buf, IntToBytes(int64(len(e.Key)))...)
		buf = append(buf, []byte(e.Key)...)
		buf = append(buf, IntToBytes(e.Offset)...)
	}
	return buf
}

func SerializeFilterSection(bf []byte, pbf []byte, merkle []byte) []byte {
	var buf []byte
	buf = append(buf, IntToBytes(int64(len(bf)))...)
	buf = append(buf, bf...)
	buf = append(buf, IntToBytes(int64(len(pbf)))...)
	buf = append(buf, pbf...)
	buf = append(buf, IntToBytes(int64(len(merkle)))...)
	buf = append(buf, merkle...)
	return buf
}

func SerializeFooter(indexOffset, indexSize, filterOffset, filterSize, itemCount int64, maxLSN uint64) []byte {
	buf := make([]byte, FooterSize)
	binary.BigEndian.PutUint64(buf[0:], uint64(indexOffset))
	binary.BigEndian.PutUint64(buf[8:], uint64(indexSize))
	binary.BigEndian.PutUint64(buf[16:], uint64(filterOffset))
	binary.BigEndian.PutUint64(buf[24:], uint64(filterSize))
	binary.BigEndian.PutUint64(buf[32:], uint64(itemCount))
	binary.BigEndian.PutUint64(buf[40:], maxLSN)
	binary.BigEndian.PutUint64(buf[48:], uint64(FooterMagic))
	return buf
}

func IntToBytes(n int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

func SizeAndValueToBytes(value string) []byte {
	valueBytes := []byte(value)
	valueSizeBytes := IntToBytes(int64(len(valueBytes)))
	return append(valueSizeBytes, valueBytes...)
}
