package wal

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed config.json
var walConfigData []byte

type WalConfig struct {
	WALDir              string `json:"WAL_DIR"`
	WALSegmentSize      int64  `json:"WAL_SEGMENT_SIZE"`
	WALWriteBufferSize  int    `json:"WAL_WRITE_BUFFER_SIZE"`
}

func GetWalConfig() WalConfig {
	var cfg WalConfig
	if err := json.Unmarshal(walConfigData, &cfg); err != nil {
		panic(fmt.Sprintf("failed to parse wal config file: %v", err))
	}
	return cfg
}
