package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed config.json
var configData []byte

type Config struct {
	BlockSize                    int     `json:"BLOCK_SIZE"`
	Tombstone                    string  `json:"TOMBSTONE"`
	TokenRefillRate              float64 `json:"TOKEN_REFILL_RATE"`
	MaxTokens                    int     `json:"MAX_TOKEN"`
	MemtableType                 string  `json:"MEMTABLE_TYPE"`
	MemtableCount                int     `json:"MEMTABLE_COUNT"`
	MemtableSize                 int     `json:"MEMTABLE_SIZE"`
	WALBufferSize                int     `json:"WAL_BUFFER_SIZE"`
	WALSegmentSize               int     `json:"WAL_SEGMENT_SIZE"`
	BloomFilterFalsePositiveRate float64 `json:"BLOOM_FILTER_FALSE_POSITIVE_RATE"`
	BloomFilterExpectedElements  int     `json:"BLOOM_FILTER_EXPECTED_ELEMENTS"`
	LSMLevels                    int     `json:"LSM_LEVELS"`
	LSMBaseDir                   string  `json:"LSM_BASE_DIR"`
	MinPrefixLength              int     `json:"MIN_PREFIX_LENGTH"`
	MaxPrefixLength              int     `json:"MAX_PREFIX_LENGTH"`
	SkipListLevels               int     `json:"SKIP_LIST_LEVELS"`
	CompactionThreshold          int     `json:"COMPACTION_THRESHOLD"`
	CacheCapacity                int     `json:"CACHE_CAPACITY"`
	MaxImmutableCount            int     `json:"MAX_IMMUTABLE_COUNT"`
}

func GetConfig() Config {
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		panic(fmt.Sprintf("failed to parse config file: %v", err))
	}

	return config
}
