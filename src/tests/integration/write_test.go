package integration

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/config"
	b "nosqlEngine/src/service/block_manager"
	fw "nosqlEngine/src/service/file_writer"
	r "nosqlEngine/src/service/retriever"
	"nosqlEngine/src/service/ss_compacter"
	"nosqlEngine/src/service/ss_parser"
	m "nosqlEngine/src/storage/memtable"
	"testing"

	"github.com/google/uuid"
)

var CONFIG = config.GetConfig()

func bytesToInt(buf []byte) int64 {

	return int64(binary.BigEndian.Uint64(buf))
}
func TestWritePathIntegration(t *testing.T) {
	// Setup block manager and file writer
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	fileWriter := fw.NewFileWriter(bm, blockSize, "sstable/sstable_"+uuid.New().String()+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)
	mt := m.NewMemtable()

	// Create a set of key-value pairs
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		mt.Add(key, value)
	}

	// Write the memtable to disk via the parser and file writer
	ssParser.FlushMemtable(mt.ToRaw())

	// Read the file to verify the data
	data, err := bm.ReadBlock(fileWriter.GetLocation(), 0, true)
	if err != nil {
		t.Fatalf("Failed to read block: %v", err)
	}

	//check if the block data matches the expected serialized data
	expectedKey := "key1"
	expectedValue := "value1"

	keySize := bytesToInt(data[:8])
	valueSize := bytesToInt(data[8+keySize : 16+keySize])

	if string(data[8:8+keySize]) != expectedKey {
		t.Errorf("Key mismatch: got %s, want %s", data[8:8+keySize], expectedKey)
	}
	if string(data[16+keySize:16+keySize+valueSize]) != expectedValue {
		t.Errorf("Value mismatch: got %s, want %s", data[16+keySize:16+keySize+valueSize], expectedValue)
	}
}

func TestWriteRead(t *testing.T) {
	mt := m.NewMemtable()
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	uuidStr := uuid.New().String()

	fileWriter := fw.NewFileWriter(bm, blockSize, "sstable/lvl0/sstable_"+uuidStr+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)

	// Create a set of key-value pairs
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("keyyy%d", i+1)

		value := fmt.Sprintf("valueee%d", i+1)
		mt.Add(key, value)

	}

	// Write the memtable to disk via the parser and file writer
	ssParser.FlushMemtable(mt.ToRaw())
	fmt.Print(
		"File written successfully, now reading the data back...\n")

	retriever := r.NewEntryRetriever(bm)

	_, res, err := retriever.RetrieveEntry("keyyy1")

	if err != nil {
		t.Fatalf("Failed to retrieve entry: %v for metadata: %v", err, res)
	}

}

func TestPrefixScan(t *testing.T) {
	mt := m.NewMemtable()
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	uuidStr := uuid.New().String()

	fileWriter := fw.NewFileWriter(bm, blockSize,uuidStr+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)

	// Create a set of key-value pairs
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i+1)

		value := fmt.Sprintf("valueee%d", i+1)
		mt.Add(key, value)

	}

	// Write the memtable to disk via the parser and file writer
	ssParser.FlushMemtable(mt.ToRaw())
	fmt.Print(
		"File written successfully, now reading the data back...\n")

	multiRetriever := r.NewMultiRetriever(bm)
	results, err := multiRetriever.GetPrefixEntries("key1")
	if err != nil {
		t.Fatalf("Failed to retrieve prefix entries: %v", err)
	}
	fmt.Printf("Retrieved %d entries with prefix 'key1':\n", len(results))
	fmt.Println("Entries:", results)
}

func TestCompacter(t *testing.T) {
	bm := b.NewBlockManager()
	sc := ss_compacter.NewSSCompacterST()

	if !sc.CheckCompactionConditions(bm) {
		t.Fatalf("Compaction conditions not met")
	}

	fmt.Println("Compaction completed successfully")
}
func TestGas(t *testing.T) {
	bm := b.NewBlockManager()
	retriever := r.NewEntryRetriever(bm)

	// Test retrieving a non-existent entry
	_, _, err := retriever.RetrieveEntry("keyyy7")
	if err != nil {
		t.Fatalf("Expected error for non-existent key, got nil")
	}
}
