package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"nosqlEngine/src/config"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_reader"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/utils"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

var CONFIG = config.GetConfig()

// WALEntry represents a single log entry in the WAL
// Operation: "PUT" or "DELETE"
type WALEntry struct {
	Operation string
	Key       string
	Value     string // empty for DELETE
	Timestamp int64  // seconds since epoch
}

// WAL handles writing to the write-ahead log file with a buffer pool and supports rotation/archiving
// Usage: wal, _ := NewWAL("data/wal/wal.log", 100)
//
//	wal.Rotate("data/wal/wal-20250625.log")
//	wal.Archive("data/wal/wal-20250625.log", "data/wal/archive/wal-20250625.log")
//	wal.Delete("data/wal/wal-20250625.log")
type WAL struct {
	//file        *os.File
	buffer      []WALEntry              // changed from []string to []WALEntry
	bufferSize  int                     // buffer pool size
	segmentSize int                     // size of each segment in bytes
	writer      *file_writer.FileWriter // add FileWriter for block writing
	bm          *block_manager.BlockManager
}

// NewWAL creates or opens a WAL file for appending, with a buffer pool of given size
func NewWAL(block_manager *block_manager.BlockManager) (*WAL, error) {
	// f, err := os.OpenFile("data/wal/current-wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	return nil, err
	// }
	bufferSize := CONFIG.WALBufferSize                                                             // default buffer size
	segmentSize := CONFIG.WALSegmentSize                                                           // default segment size in bytes
	writer := file_writer.NewFileWriter(block_manager, CONFIG.BlockSize, generateWALSegmentName()) // Create a new FileWriter with the segment size
	return &WAL{buffer: make([]WALEntry, 0, bufferSize), bufferSize: bufferSize, segmentSize: segmentSize, writer: writer, bm: block_manager}, nil
}

// encodeWALEntry encodes a WALEntry into the binary WAL format
func encodeWALEntry(entry WALEntry) ([]byte, error) {
	keyBytes := []byte(entry.Key)
	valueBytes := []byte(entry.Value)
	keySize := uint64(len(keyBytes))
	valueSize := uint64(len(valueBytes))
	var tombstone byte = 0
	if entry.Operation == "DELETE" {
		tombstone = 1
	}
	buf := new(bytes.Buffer)
	// Reserve space for CRC (4 bytes)
	buf.Write(make([]byte, 4))
	// Timestamp (8 bytes, use int64 seconds)
	ts := make([]byte, 8)
	binary.LittleEndian.PutUint64(ts, uint64(entry.Timestamp))
	buf.Write(ts)
	// Tombstone (1 byte)
	buf.WriteByte(tombstone)
	// Key Size (8 bytes)
	ks := make([]byte, 8)
	binary.LittleEndian.PutUint64(ks, keySize)
	buf.Write(ks)
	// Value Size (8 bytes)
	vs := make([]byte, 8)
	binary.LittleEndian.PutUint64(vs, valueSize)
	buf.Write(vs)
	// Key
	buf.Write(keyBytes)
	// Value
	buf.Write(valueBytes)
	// Compute CRC over everything except the first 4 bytes
	crc := crc32.ChecksumIEEE(buf.Bytes()[4:])
	binary.LittleEndian.PutUint32(buf.Bytes()[0:4], crc)
	return buf.Bytes(), nil
}

// WritePut logs a PUT operation to the WAL buffer
func (w *WAL) WritePut(key, value string) error {
	entry := WALEntry{
		Operation: "PUT",
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
	}
	w.buffer = append(w.buffer, entry)

	if len(w.buffer) >= w.bufferSize {
		return w.Flush()
	}
	return nil
}

// WriteDelete logs a DELETE operation to the WAL buffer
func (w *WAL) WriteDelete(key string) error {
	entry := WALEntry{
		Operation: "DELETE",
		Key:       key,
		Value:     "",
		Timestamp: time.Now().Unix(),
	}
	// data, err := encodeWALEntry(entry)
	// if err != nil {
	// 	return err
	// }
	w.buffer = append(w.buffer, entry)
	if len(w.buffer) >= w.bufferSize {
		return w.Flush()
	}
	return nil
}

func (w *WAL) Flush() error {
	if len(w.buffer) == 0 {
		return nil
	}
	for _, entry := range w.buffer {
		data, err := encodeWALEntry(entry)
		if err != nil {
			return err
		}
		if w.writer != nil {
			w.writer.Write(data, false, nil)
			// If the segment size is reached, archive the current WAL segment
		} else {
			return nil
		}
	}
	size, err := getFileSize(w.writer.GetLocation())
	if err != nil {
		return err
	}
	if size >= int64(w.segmentSize) {
		w.Rotate()
	}
	w.buffer = w.buffer[:0]
	return nil
}

func getFileSize(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

// // Close flushes the buffer and closes the WAL file
// func (w *WAL) Close() error {
// 	if err := w.Flush(); err != nil {
// 		return err
// 	}
// 	return w.writer.Close()
// }

func (w *WAL) Rotate() error {
	// w.writer.SetLocation(generateWALSegmentName())
	w.writer = file_writer.NewFileWriter(w.bm, CONFIG.BlockSize, generateWALSegmentName())
	w.buffer = make([]WALEntry, 0, w.bufferSize)
	return nil
}

// Helper to generate a rotated WAL filename with timestampc

func generateWALSegmentName() string {
	return fmt.Sprintf("wal/wal-%s.log", time.Now().Format("20060102-150405.000000000"))
}

// Helper to read and parse a single WAL entry from the file
func readWALEntry(reader *file_reader.FileReader, blockNum int) (*WALEntry, uint32, []byte, int, error) {
	content, blocksUsed, _ := reader.ReadEntry(blockNum)
	if len(content) == 0 {
		return nil, 0, nil, 0, io.EOF // No more entries
	}

	crc := binary.LittleEndian.Uint32(content[0:4])
	ts := int64(binary.LittleEndian.Uint64(content[4:12]))
	tombstone := content[12]
	keySize := binary.LittleEndian.Uint64(content[13:21])
	valueSize := binary.LittleEndian.Uint64(content[21:29])
	if len(content) < 29+int(keySize)+int(valueSize) {
		return nil, 0, nil, 0, fmt.Errorf("invalid WAL entry size: %d bytes", len(content))
	}
	key := content[29 : 29+keySize]
	value := content[29+keySize : 29+keySize+valueSize]
	payload := content[4:]
	op := "PUT"
	if tombstone == 1 {
		op = "DELETE"
	}
	entry := &WALEntry{
		Operation: op,
		Key:       string(key),
		Value:     string(value),
		Timestamp: ts,
	}
	return entry, crc, payload, blocksUsed, nil
}

func GetWALSegmentPaths() ([]string, error) {
	segmentPaths := utils.GetPaths("data/wal", ".log")
	return segmentPaths, nil
}
func getProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	// Go up from src/service/file_writer/writer.go to project root
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filename))))
	return projectRoot
}

// ReplayWAL reads all the WAL segment files and returns all entries (for recovery)
func ReplayWAL(block_manager *block_manager.BlockManager) ([]WALEntry, error) {
	var allEntries []WALEntry
	reader := file_reader.NewFileReader("", CONFIG.BlockSize, *block_manager)
	// Get the list of WAL segment files
	segmentPaths, err := GetWALSegmentPaths()
	if err != nil {
		return nil, err
	}
	// Replay each segment
	for _, segment := range segmentPaths {
		reader.SetLocation(segment)
		entries, err := replayWALSegment(reader)
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, entries...)
	}
	return allEntries, nil
}

func replayWALSegment(reader *file_reader.FileReader) ([]WALEntry, error) {
	var entries []WALEntry
	var blockIdx int
	for {
		entry, crc, payload, blocksUsed, err := readWALEntry(reader, blockIdx)
		if err == io.EOF {
			break // End of segment
		}
		blockIdx += blocksUsed
		// Validate CRC
		if crc32.ChecksumIEEE(payload) != crc {
			return nil, fmt.Errorf("WAL entry CRC mismatch")
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}

// WAL deletes the WAL folder, to be used when all memtables are flushed
func (wal *WAL) DeleteWALSegments() error {
	segmentPaths, err := GetWALSegmentPaths()
	if err != nil {
		return nil
	}
	for _, segment := range segmentPaths {
		if err := os.Remove(segment); err != nil {
			return fmt.Errorf("failed to delete WAL segment %s: %w", segment, err)
		}
	}
	wal.Rotate() // Reset the WAL to a new segment
	return nil
}

func (w *WAL) SetBufferSize(size int) {
	w.bufferSize = size
}

func (w *WAL) SetSegmentSize(size int) {
	w.segmentSize = size
}

func (w *WAL) SetWriterLocation(location string) {
	w.writer.SetLocation(location)
}
