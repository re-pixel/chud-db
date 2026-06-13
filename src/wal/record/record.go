package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const recordHeaderSize = 25

type Op uint8

const (
	OpPut    Op = 0x01
	OpDelete Op = 0x02
)

type Record struct {
	LSN   uint64
	Op    Op
	Key   []byte
	Value []byte
}

func EncodeRecord(op Op, key, value []byte, lsn uint64) ([]byte, error) {
	if op != OpPut && op != OpDelete {
		return nil, fmt.Errorf("invalid wal op: %d", op)
	}
	if op == OpDelete && len(value) != 0 {
		return nil, fmt.Errorf("delete record must have empty value")
	}

	recordLen := recordHeaderSize + len(key) + len(value)
	buf := make([]byte, recordLen)

	binary.LittleEndian.PutUint32(buf[4:], uint32(recordLen))
	binary.LittleEndian.PutUint64(buf[8:], lsn)
	buf[16] = byte(op)
	binary.LittleEndian.PutUint32(buf[17:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[21:], uint32(len(value)))
	copy(buf[recordHeaderSize:], key)
	copy(buf[recordHeaderSize+len(key):], value)

	crc := crc32.ChecksumIEEE(buf[4:recordLen])
	binary.LittleEndian.PutUint32(buf[0:], crc)

	return buf, nil
}

func DecodeRecord(buf []byte) (Record, error) {
	if len(buf) < recordHeaderSize {
		return Record{}, fmt.Errorf("record too short: %d bytes", len(buf))
	}

	recordLen := int(binary.LittleEndian.Uint32(buf[4:8]))
	if recordLen < recordHeaderSize {
		return Record{}, fmt.Errorf("invalid record length: %d", recordLen)
	}
	if len(buf) < recordLen {
		return Record{}, fmt.Errorf("buffer shorter than record length: have %d want %d", len(buf), recordLen)
	}

	recordBuf := buf[:recordLen]
	expectedCRC := binary.LittleEndian.Uint32(recordBuf[0:4])
	if crc32.ChecksumIEEE(recordBuf[4:recordLen]) != expectedCRC {
		return Record{}, fmt.Errorf("record crc mismatch")
	}

	op := Op(recordBuf[16])
	if op != OpPut && op != OpDelete {
		return Record{}, fmt.Errorf("invalid wal op: %d", op)
	}

	keyLen := int(binary.LittleEndian.Uint32(recordBuf[17:21]))
	valLen := int(binary.LittleEndian.Uint32(recordBuf[21:25]))
	if recordHeaderSize+keyLen+valLen != recordLen {
		return Record{}, fmt.Errorf("record length mismatch")
	}

	keyStart := recordHeaderSize
	keyEnd := keyStart + keyLen
	valEnd := keyEnd + valLen

	return Record{
		LSN:   binary.LittleEndian.Uint64(recordBuf[8:16]),
		Op:    op,
		Key:   recordBuf[keyStart:keyEnd],
		Value: recordBuf[keyEnd:valEnd],
	}, nil
}
