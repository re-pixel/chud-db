package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigValidates(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}
	if cfg.ReplicationFactor != 3 || cfg.ReadQuorum != 2 || cfg.WriteQuorum != 2 {
		t.Fatalf("unexpected default quorum config: %+v", cfg)
	}
}

func TestLoadAppliesJSONFile(t *testing.T) {
	path := writeConfig(t, `{
		"node_id": "node-2",
		"cluster_id": "test-cluster",
		"data_dir": "data/cluster/node-2",
		"listen_addr": "0.0.0.0:7100",
		"advertise_addr": "10.0.0.2:7100",
		"seeds": ["10.0.0.1:7100", "10.0.0.3:7100"],
		"replication_factor": 3,
		"read_quorum": 2,
		"write_quorum": 2,
		"tablet_split_bytes": 1048576,
		"tablet_merge_bytes": 262144,
		"anti_entropy_interval": "30s"
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.NodeID != "node-2" {
		t.Fatalf("node id = %q", cfg.NodeID)
	}
	if cfg.AdvertiseAddr != "10.0.0.2:7100" {
		t.Fatalf("advertise addr = %q", cfg.AdvertiseAddr)
	}
	if len(cfg.Seeds) != 2 || cfg.Seeds[0] != "10.0.0.1:7100" || cfg.Seeds[1] != "10.0.0.3:7100" {
		t.Fatalf("seeds = %#v", cfg.Seeds)
	}
	if cfg.AntiEntropyInterval != 30*time.Second {
		t.Fatalf("anti entropy interval = %v", cfg.AntiEntropyInterval)
	}
}

func TestLoadEnvOverridesFile(t *testing.T) {
	path := writeConfig(t, `{
		"node_id": "file-node",
		"listen_addr": "0.0.0.0:7100",
		"advertise_addr": "10.0.0.2:7100",
		"replication_factor": 3,
		"read_quorum": 2,
		"write_quorum": 2,
		"tablet_split_bytes": 1048576,
		"tablet_merge_bytes": 262144
	}`)
	t.Setenv("NOSQL_CLUSTER_NODE_ID", "env-node")
	t.Setenv("NOSQL_CLUSTER_SEEDS", "10.0.0.1:7100, 10.0.0.2:7100,,")
	t.Setenv("NOSQL_CLUSTER_ANTI_ENTROPY_INTERVAL", "45s")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.NodeID != "env-node" {
		t.Fatalf("node id = %q", cfg.NodeID)
	}
	if len(cfg.Seeds) != 2 || cfg.Seeds[0] != "10.0.0.1:7100" || cfg.Seeds[1] != "10.0.0.2:7100" {
		t.Fatalf("seeds = %#v", cfg.Seeds)
	}
	if cfg.AntiEntropyInterval != 45*time.Second {
		t.Fatalf("anti entropy interval = %v", cfg.AntiEntropyInterval)
	}
}

func TestValidateRejectsBadQuorumOverlap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationFactor = 3
	cfg.ReadQuorum = 1
	cfg.WriteQuorum = 2

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "read_quorum + write_quorum") {
		t.Fatalf("expected quorum overlap error, got %v", err)
	}
}

func TestValidateRejectsBadTabletThresholds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TabletSplitBytes = cfg.TabletMergeBytes

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "tablet_split_bytes") {
		t.Fatalf("expected tablet threshold error, got %v", err)
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	path := writeConfig(t, `{"anti_entropy_interval": "not-a-duration"}`)

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "anti_entropy_interval") {
		t.Fatalf("expected duration parse error, got %v", err)
	}
}

func TestLoadRejectsInvalidEnvInteger(t *testing.T) {
	t.Setenv("NOSQL_CLUSTER_REPLICATION_FACTOR", "many")

	_, err := Load("")
	if err == nil || !strings.Contains(err.Error(), "REPLICATION_FACTOR") {
		t.Fatalf("expected env integer parse error, got %v", err)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
