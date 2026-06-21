package integration

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/config"
	m "nosqlEngine/src/memtable"
	b "nosqlEngine/src/service/block_manager"
	fw "nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_compacter"
	"nosqlEngine/src/service/ss_parser"
	"nosqlEngine/src/sstable"
	"nosqlEngine/src/utils"
	kv "nosqlEngine/src/models/key_value"
	"testing"

	"github.com/google/uuid"
)

var CONFIG = config.GetConfig()

func bytesToInt(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func TestWritePathIntegration(t *testing.T) {
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	fileWriter := fw.NewFileWriter(blockSize, "sstable/sstable_"+uuid.New().String()+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)
	mt := m.NewMemtable()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		mt.Add(key, value)
	}

	// Capture location before FlushMemtable resets the writer.
	writtenLocation := fileWriter.GetLocation()
	raw := mt.ToRaw()
	kv.SortByKeys(&raw)
	ssParser.FlushMemtable(raw)

	data, err := bm.ReadAt(writtenLocation, 0, CONFIG.BlockSize)
	if err != nil {
		t.Fatalf("Failed to read block: %v", err)
	}

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

	fileWriter := fw.NewFileWriter(blockSize, "sstable/lvl0/sstable_"+uuidStr+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("keyyy%d", i+1)
		value := fmt.Sprintf("valueee%d", i+1)
		mt.Add(key, value)
	}

	writtenPath := fileWriter.GetLocation()
	raw := mt.ToRaw()
	kv.SortByKeys(&raw)
	ssParser.FlushMemtable(raw)
	fmt.Print("File written successfully, now reading the data back...\n")

	reader, err := sstable.Open(writtenPath, bm)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}

	value, ok, err := reader.Get("keyyy1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !ok {
		t.Fatalf("key keyyy1 not found in SSTable")
	}
	if value != "valueee1" {
		t.Errorf("Value mismatch: got %q, want %q", value, "valueee1")
	}
}

func TestPrefixScan(t *testing.T) {
	mt := m.NewMemtable()
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	uuidStr := uuid.New().String()

	fileWriter := fw.NewFileWriter(blockSize, "sstable/lvl0/sstable_prefix_"+uuidStr+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("valueee%d", i+1)
		mt.Add(key, value)
	}

	writtenPath := fileWriter.GetLocation()
	raw := mt.ToRaw()
	kv.SortByKeys(&raw)
	ssParser.FlushMemtable(raw)
	fmt.Print("File written successfully, now reading the data back...\n")

	reader, err := sstable.Open(writtenPath, bm)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}

	results, err := reader.PrefixScan("key1")
	if err != nil {
		t.Fatalf("PrefixScan returned error: %v", err)
	}
	fmt.Printf("Retrieved %d entries with prefix 'key1':\n", len(results))
	if len(results) == 0 {
		t.Error("Expected at least one result for prefix 'key1'")
	}
}

func TestCompacter(t *testing.T) {
	bm := b.NewBlockManager()
	sc := ss_compacter.NewSSCompacterST()
	dataRoot := utils.DefaultDataRoot()

	// Write exactly CompactionThreshold SSTables into lvl0 so the test is
	// self-contained and not dependent on leftover files from other runs.
	threshold := config.GetConfig().CompactionThreshold
	ssParser := ss_parser.NewSSParser(fw.NewFileWriter(CONFIG.BlockSize, ""))
	for i := 0; i < threshold; i++ {
		mt := m.NewMemtable()
		for j := 0; j < 5; j++ {
			mt.Add(fmt.Sprintf("compacter-key-%d-%d", i, j), fmt.Sprintf("val-%d-%d", i, j))
		}
		raw := mt.ToRaw()
		kv.SortByKeys(&raw)
		ssParser.FlushMemtable(raw)
	}

	versions := make([][]string, config.GetConfig().LSMLevels)
	for level := 0; level < config.GetConfig().LSMLevels; level++ {
		versions[level] = utils.ListSSTablesInLevel(dataRoot, level)
	}

	results := sc.CheckCompactionConditions(bm, dataRoot, versions)
	if len(results) == 0 {
		t.Fatalf("Compaction conditions not met")
	}

	fmt.Println("Compaction completed successfully")
}

func TestGas(t *testing.T) {
	mt := m.NewMemtable()
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	uuidStr := uuid.New().String()

	fileWriter := fw.NewFileWriter(blockSize, "sstable/lvl0/sstable_gas_"+uuidStr+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("gaskey%d", i+1)
		value := fmt.Sprintf("gasvalue%d", i+1)
		mt.Add(key, value)
	}

	writtenPath := fileWriter.GetLocation()
	raw := mt.ToRaw()
	kv.SortByKeys(&raw)
	ssParser.FlushMemtable(raw)

	reader, err := sstable.Open(writtenPath, bm)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}

	_, ok, err := reader.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get for missing key returned error: %v", err)
	}
	if ok {
		t.Fatal("Expected key not found, but Get returned ok=true")
	}
}
