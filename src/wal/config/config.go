package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed config.json
var walConfigData []byte

type Config struct {
	WALDir             string `json:"WAL_DIR"`
	WALSegmentSize     int64  `json:"WAL_SEGMENT_SIZE"`
	WALWriteBufferSize int    `json:"WAL_WRITE_BUFFER_SIZE"`
	WALSyncMode        string `json:"WAL_SYNC_MODE"`
}

func Get() Config {
	var cfg Config
	if err := json.Unmarshal(walConfigData, &cfg); err != nil {
		panic(fmt.Sprintf("failed to parse wal config file: %v", err))
	}
	return cfg
}
