package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const envPrefix = "NOSQL_CLUSTER_"

type Config struct {
	NodeID              string        `json:"node_id"`
	ClusterID           string        `json:"cluster_id"`
	DataDir             string        `json:"data_dir"`
	ListenAddr          string        `json:"listen_addr"`
	AdvertiseAddr       string        `json:"advertise_addr"`
	Seeds               []string      `json:"seeds"`
	ReplicationFactor   int           `json:"replication_factor"`
	ReadQuorum          int           `json:"read_quorum"`
	WriteQuorum         int           `json:"write_quorum"`
	TabletSplitBytes    int64         `json:"tablet_split_bytes"`
	TabletMergeBytes    int64         `json:"tablet_merge_bytes"`
	AntiEntropyInterval time.Duration `json:"-"`
	GossipInterval      time.Duration `json:"-"`
	PingTimeout         time.Duration `json:"-"`
	SuspectTimeout      time.Duration `json:"-"`
	DeadTimeout         time.Duration `json:"-"`
	IndirectPingFanout  int           `json:"indirect_ping_fanout"`
}

type fileConfig struct {
	NodeID              *string  `json:"node_id"`
	ClusterID           *string  `json:"cluster_id"`
	DataDir             *string  `json:"data_dir"`
	ListenAddr          *string  `json:"listen_addr"`
	AdvertiseAddr       *string  `json:"advertise_addr"`
	Seeds               []string `json:"seeds"`
	ReplicationFactor   *int     `json:"replication_factor"`
	ReadQuorum          *int     `json:"read_quorum"`
	WriteQuorum         *int     `json:"write_quorum"`
	TabletSplitBytes    *int64   `json:"tablet_split_bytes"`
	TabletMergeBytes    *int64   `json:"tablet_merge_bytes"`
	AntiEntropyInterval *string  `json:"anti_entropy_interval"`
	GossipInterval      *string  `json:"gossip_interval"`
	PingTimeout         *string  `json:"ping_timeout"`
	SuspectTimeout      *string  `json:"suspect_timeout"`
	DeadTimeout         *string  `json:"dead_timeout"`
	IndirectPingFanout  *int     `json:"indirect_ping_fanout"`
}

func DefaultConfig() Config {
	return Config{
		NodeID:              "node-1",
		ClusterID:           "nosql-cluster",
		DataDir:             "data/cluster/node-1",
		ListenAddr:          "0.0.0.0:7000",
		AdvertiseAddr:       "127.0.0.1:7000",
		Seeds:               nil,
		ReplicationFactor:   3,
		ReadQuorum:          2,
		WriteQuorum:         2,
		TabletSplitBytes:    256 << 20,
		TabletMergeBytes:    64 << 20,
		AntiEntropyInterval: time.Minute,
		GossipInterval:      time.Second,
		PingTimeout:         500 * time.Millisecond,
		SuspectTimeout:      5 * time.Second,
		DeadTimeout:         30 * time.Second,
		IndirectPingFanout:  3,
	}
}

func Load(path string) (Config, error) {
	cfg := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("load cluster config %s: %w", path, err)
		}
		if err := applyFileConfig(&cfg, data); err != nil {
			return Config{}, err
		}
	}

	if err := applyEnv(&cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (cfg Config) Validate() error {
	if strings.TrimSpace(cfg.NodeID) == "" {
		return fmt.Errorf("node_id must not be empty")
	}
	if strings.TrimSpace(cfg.ClusterID) == "" {
		return fmt.Errorf("cluster_id must not be empty")
	}
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		return fmt.Errorf("listen_addr must not be empty")
	}
	if strings.TrimSpace(cfg.AdvertiseAddr) == "" {
		return fmt.Errorf("advertise_addr must not be empty")
	}
	if cfg.ReplicationFactor < 1 {
		return fmt.Errorf("replication_factor must be >= 1")
	}
	if cfg.ReadQuorum < 1 || cfg.ReadQuorum > cfg.ReplicationFactor {
		return fmt.Errorf("read_quorum must be between 1 and replication_factor")
	}
	if cfg.WriteQuorum < 1 || cfg.WriteQuorum > cfg.ReplicationFactor {
		return fmt.Errorf("write_quorum must be between 1 and replication_factor")
	}
	if cfg.ReadQuorum+cfg.WriteQuorum <= cfg.ReplicationFactor {
		return fmt.Errorf("read_quorum + write_quorum must exceed replication_factor")
	}
	if cfg.TabletMergeBytes <= 0 {
		return fmt.Errorf("tablet_merge_bytes must be > 0")
	}
	if cfg.TabletSplitBytes <= cfg.TabletMergeBytes {
		return fmt.Errorf("tablet_split_bytes must be greater than tablet_merge_bytes")
	}
	if cfg.AntiEntropyInterval <= 0 {
		return fmt.Errorf("anti_entropy_interval must be > 0")
	}
	if cfg.GossipInterval <= 0 {
		return fmt.Errorf("gossip_interval must be > 0")
	}
	if cfg.PingTimeout <= 0 {
		return fmt.Errorf("ping_timeout must be > 0")
	}
	if cfg.SuspectTimeout <= cfg.PingTimeout {
		return fmt.Errorf("suspect_timeout must be greater than ping_timeout")
	}
	if cfg.DeadTimeout <= cfg.SuspectTimeout {
		return fmt.Errorf("dead_timeout must be greater than suspect_timeout")
	}
	if cfg.IndirectPingFanout < 0 {
		return fmt.Errorf("indirect_ping_fanout must be >= 0")
	}
	return nil
}

func applyFileConfig(cfg *Config, data []byte) error {
	var fc fileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parse cluster config: %w", err)
	}

	if fc.NodeID != nil {
		cfg.NodeID = *fc.NodeID
	}
	if fc.ClusterID != nil {
		cfg.ClusterID = *fc.ClusterID
	}
	if fc.DataDir != nil {
		cfg.DataDir = *fc.DataDir
	}
	if fc.ListenAddr != nil {
		cfg.ListenAddr = *fc.ListenAddr
	}
	if fc.AdvertiseAddr != nil {
		cfg.AdvertiseAddr = *fc.AdvertiseAddr
	}
	if fc.Seeds != nil {
		cfg.Seeds = append([]string(nil), fc.Seeds...)
	}
	if fc.ReplicationFactor != nil {
		cfg.ReplicationFactor = *fc.ReplicationFactor
	}
	if fc.ReadQuorum != nil {
		cfg.ReadQuorum = *fc.ReadQuorum
	}
	if fc.WriteQuorum != nil {
		cfg.WriteQuorum = *fc.WriteQuorum
	}
	if fc.TabletSplitBytes != nil {
		cfg.TabletSplitBytes = *fc.TabletSplitBytes
	}
	if fc.TabletMergeBytes != nil {
		cfg.TabletMergeBytes = *fc.TabletMergeBytes
	}
	if fc.AntiEntropyInterval != nil {
		d, err := time.ParseDuration(*fc.AntiEntropyInterval)
		if err != nil {
			return fmt.Errorf("parse anti_entropy_interval: %w", err)
		}
		cfg.AntiEntropyInterval = d
	}
	if fc.GossipInterval != nil {
		d, err := time.ParseDuration(*fc.GossipInterval)
		if err != nil {
			return fmt.Errorf("parse gossip_interval: %w", err)
		}
		cfg.GossipInterval = d
	}
	if fc.PingTimeout != nil {
		d, err := time.ParseDuration(*fc.PingTimeout)
		if err != nil {
			return fmt.Errorf("parse ping_timeout: %w", err)
		}
		cfg.PingTimeout = d
	}
	if fc.SuspectTimeout != nil {
		d, err := time.ParseDuration(*fc.SuspectTimeout)
		if err != nil {
			return fmt.Errorf("parse suspect_timeout: %w", err)
		}
		cfg.SuspectTimeout = d
	}
	if fc.DeadTimeout != nil {
		d, err := time.ParseDuration(*fc.DeadTimeout)
		if err != nil {
			return fmt.Errorf("parse dead_timeout: %w", err)
		}
		cfg.DeadTimeout = d
	}
	if fc.IndirectPingFanout != nil {
		cfg.IndirectPingFanout = *fc.IndirectPingFanout
	}
	return nil
}

func applyEnv(cfg *Config) error {
	if v, ok := getenv("NODE_ID"); ok {
		cfg.NodeID = v
	}
	if v, ok := getenv("CLUSTER_ID"); ok {
		cfg.ClusterID = v
	}
	if v, ok := getenv("DATA_DIR"); ok {
		cfg.DataDir = v
	}
	if v, ok := getenv("LISTEN_ADDR"); ok {
		cfg.ListenAddr = v
	}
	if v, ok := getenv("ADVERTISE_ADDR"); ok {
		cfg.AdvertiseAddr = v
	}
	if v, ok := getenv("SEEDS"); ok {
		cfg.Seeds = splitCSV(v)
	}
	if v, ok := getenv("REPLICATION_FACTOR"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse %sREPLICATION_FACTOR: %w", envPrefix, err)
		}
		cfg.ReplicationFactor = n
	}
	if v, ok := getenv("READ_QUORUM"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse %sREAD_QUORUM: %w", envPrefix, err)
		}
		cfg.ReadQuorum = n
	}
	if v, ok := getenv("WRITE_QUORUM"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse %sWRITE_QUORUM: %w", envPrefix, err)
		}
		cfg.WriteQuorum = n
	}
	if v, ok := getenv("TABLET_SPLIT_BYTES"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("parse %sTABLET_SPLIT_BYTES: %w", envPrefix, err)
		}
		cfg.TabletSplitBytes = n
	}
	if v, ok := getenv("TABLET_MERGE_BYTES"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("parse %sTABLET_MERGE_BYTES: %w", envPrefix, err)
		}
		cfg.TabletMergeBytes = n
	}
	if v, ok := getenv("ANTI_ENTROPY_INTERVAL"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse %sANTI_ENTROPY_INTERVAL: %w", envPrefix, err)
		}
		cfg.AntiEntropyInterval = d
	}
	if v, ok := getenv("GOSSIP_INTERVAL"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse %sGOSSIP_INTERVAL: %w", envPrefix, err)
		}
		cfg.GossipInterval = d
	}
	if v, ok := getenv("PING_TIMEOUT"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse %sPING_TIMEOUT: %w", envPrefix, err)
		}
		cfg.PingTimeout = d
	}
	if v, ok := getenv("SUSPECT_TIMEOUT"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse %sSUSPECT_TIMEOUT: %w", envPrefix, err)
		}
		cfg.SuspectTimeout = d
	}
	if v, ok := getenv("DEAD_TIMEOUT"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse %sDEAD_TIMEOUT: %w", envPrefix, err)
		}
		cfg.DeadTimeout = d
	}
	if v, ok := getenv("INDIRECT_PING_FANOUT"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse %sINDIRECT_PING_FANOUT: %w", envPrefix, err)
		}
		cfg.IndirectPingFanout = n
	}
	return nil
}

func getenv(key string) (string, bool) {
	return os.LookupEnv(envPrefix + key)
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	seeds := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			seeds = append(seeds, part)
		}
	}
	return seeds
}
