package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	clusterconfig "nosqlEngine/src/cluster/config"
	clustermembership "nosqlEngine/src/cluster/membership"
	clusternode "nosqlEngine/src/cluster/node"
	clusterreplication "nosqlEngine/src/cluster/replication"
	clusterring "nosqlEngine/src/cluster/ring"
	"nosqlEngine/src/cluster/transport/pb"
	"nosqlEngine/src/engine"

	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "node: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to cluster node config JSON")
	flag.Parse()

	cfg, err := clusterconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if len(cfg.RangeMapReplicas) != cfg.ReplicationFactor {
		fmt.Fprintf(os.Stderr, "node %s: warning: range_map_replicas has %d entries but replication_factor is %d - these should agree\n",
			cfg.NodeID, len(cfg.RangeMapReplicas), cfg.ReplicationFactor)
	}

	eng, err := engine.NewEngineInDir(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}
	eng.Start()
	defer eng.Shut() //nolint:errcheck

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}

	ringTable, err := clusterring.NewTable(cfg.NodeID, clusterring.RangeMap{
		Generation: cfg.RangeMapGeneration,
		Ranges:     []clusterring.Range{{Start: "", End: "", Replicas: cfg.RangeMapReplicas}},
	})
	if err != nil {
		return fmt.Errorf("build initial range map: %w", err)
	}
	ringServer := clusterring.NewServer(ringTable)

	store := clusternode.NewNodeStore(eng, clusternode.DefaultStoreUser)
	nodeServer := clusternode.NewServer(cfg, store, ringTable)

	membershipClient := clustermembership.NewClient()
	defer membershipClient.Close() //nolint:errcheck

	table := clustermembership.NewTable(cfg.ClusterID, clustermembership.Member{
		NodeID:        cfg.NodeID,
		AdvertiseAddr: cfg.AdvertiseAddr,
		RangeMapEpoch: cfg.RangeMapGeneration,
	})
	gossipServer := clustermembership.NewServer(table, membershipClient, cfg.PingTimeout)
	gossiper := clustermembership.NewGossiper(table, membershipClient, clustermembership.GossiperConfig{
		GossipInterval:     cfg.GossipInterval,
		PingTimeout:        cfg.PingTimeout,
		SuspectTimeout:     cfg.SuspectTimeout,
		DeadTimeout:        cfg.DeadTimeout,
		IndirectPingFanout: cfg.IndirectPingFanout,
		Seeds:              cfg.Seeds,
	})

	ringClient := clusterring.NewClient()
	defer ringClient.Close() //nolint:errcheck

	syncer := clusterring.NewSyncer(
		ringTable,
		ringClient,
		membershipPeerEpochs{table},
		clusterring.LocalEpochPublisherFunc(func(epoch uint64) { table.SetLocalRangeMapEpoch(epoch) }),
		clusterring.SyncerConfig{Interval: cfg.GossipInterval, PullTimeout: cfg.PingTimeout},
	)

	replicationClient := clusterreplication.NewClient()
	defer replicationClient.Close() //nolint:errcheck

	coordinator := clusterreplication.NewCoordinator(
		cfg.NodeID,
		ringTable,
		membershipAddressResolver{table},
		replicationClient,
		store,
		clusterreplication.Config{
			ReplicationTimeout: cfg.ReplicationTimeout,
			ReadQuorum:         cfg.ReadQuorum,
			WriteQuorum:        cfg.WriteQuorum,
		},
	)
	replicationServer := clusterreplication.NewServer(coordinator)

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, nodeServer)
	pb.RegisterGossipServiceServer(grpcServer, gossipServer)
	pb.RegisterRangeMapServiceServer(grpcServer, ringServer)
	pb.RegisterReplicationServiceServer(grpcServer, replicationServer)

	serveErr := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			serveErr <- err
		}
	}()

	gossipCtx, stopGossip := context.WithCancel(context.Background())
	gossiper.Start(gossipCtx)
	syncer.Start(gossipCtx)

	fmt.Printf("node %s listening on %s (advertise %s)\n", cfg.NodeID, cfg.ListenAddr, cfg.AdvertiseAddr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		fmt.Printf("node %s shutting down after %s\n", cfg.NodeID, sig)
		stopGossip()
		gossiper.Stop()
		syncer.Stop()
		shutdownGRPC(grpcServer)
		return nil
	case err := <-serveErr:
		stopGossip()
		gossiper.Stop()
		syncer.Stop()
		return fmt.Errorf("serve grpc: %w", err)
	}
}

// membershipPeerEpochs adapts a membership.Table into the
// clusterring.PeerEpochSource the range map Syncer needs, without
// making the ring package depend on membership.
type membershipPeerEpochs struct {
	table *clustermembership.Table
}

func (m membershipPeerEpochs) PeerEpochs() []clusterring.PeerEpoch {
	snapshot := m.table.Snapshot()
	localID := m.table.LocalID()

	peers := make([]clusterring.PeerEpoch, 0, len(snapshot))
	for _, member := range snapshot {
		if member.NodeID == localID || member.Status == clustermembership.StatusDead {
			continue
		}
		peers = append(peers, clusterring.PeerEpoch{
			NodeID:        member.NodeID,
			AdvertiseAddr: member.AdvertiseAddr,
			RangeMapEpoch: member.RangeMapEpoch,
		})
	}
	return peers
}

// membershipAddressResolver adapts a membership.Table into the
// clusterreplication.AddressResolver the Coordinator needs, without
// making the replication package depend on membership.
type membershipAddressResolver struct {
	table *clustermembership.Table
}

func (m membershipAddressResolver) Address(nodeID string) (string, bool) {
	member, ok := m.table.Get(nodeID)
	if !ok {
		return "", false
	}
	return member.AdvertiseAddr, true
}

func shutdownGRPC(server *grpc.Server) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case <-done:
	case <-ctx.Done():
		server.Stop()
	}
}
