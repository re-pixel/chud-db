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

	clusterantientropy "nosqlEngine/src/cluster/antientropy"
	clusterconfig "nosqlEngine/src/cluster/config"
	clustercoordination "nosqlEngine/src/cluster/coordination"
	clustermembership "nosqlEngine/src/cluster/membership"
	clusternode "nosqlEngine/src/cluster/node"
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

	antiEntropyStore := nodeAntiEntropyStore{store}
	antiEntropyServer := clusterantientropy.NewServer(antiEntropyStore, ringTable)

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

	coordinationClient := clustercoordination.NewClient()
	defer coordinationClient.Close() //nolint:errcheck

	coordinator := clustercoordination.NewCoordinator(
		cfg.NodeID,
		ringTable,
		membershipAddressResolver{table},
		coordinationClient,
		store,
		clustercoordination.Config{
			ReplicationTimeout: cfg.ReplicationTimeout,
			ReadQuorum:         cfg.ReadQuorum,
			WriteQuorum:        cfg.WriteQuorum,
		},
	)
	coordinationServer := clustercoordination.NewServer(coordinator)

	antiEntropyClient := clusterantientropy.NewClient()
	defer antiEntropyClient.Close() //nolint:errcheck

	antiEntropyScheduler := clusterantientropy.NewScheduler(
		clusterantientropy.SchedulerConfig{
			NodeID:            cfg.NodeID,
			Interval:          cfg.AntiEntropyInterval,
			Timeout:           cfg.AntiEntropyTimeout,
			Fanout:            cfg.AntiEntropyFanout,
			LeafItemThreshold: cfg.AntiEntropyLeafItemThreshold,
			MaxDepth:          cfg.AntiEntropyMaxDepth,
		},
		antiEntropyStore,
		membershipAddressResolver{table},
		antiEntropyClient,
		ringOwnedRanges{table: ringTable, localID: cfg.NodeID},
	)

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, nodeServer)
	pb.RegisterGossipServiceServer(grpcServer, gossipServer)
	pb.RegisterRangeMapServiceServer(grpcServer, ringServer)
	pb.RegisterCoordinationServiceServer(grpcServer, coordinationServer)
	pb.RegisterAntiEntropyServiceServer(grpcServer, antiEntropyServer)

	serveErr := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			serveErr <- err
		}
	}()

	gossipCtx, stopGossip := context.WithCancel(context.Background())
	gossiper.Start(gossipCtx)
	syncer.Start(gossipCtx)
	antiEntropyScheduler.Start(gossipCtx)

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
		antiEntropyScheduler.Stop()
		shutdownGRPC(grpcServer)
		return nil
	case err := <-serveErr:
		stopGossip()
		gossiper.Stop()
		syncer.Stop()
		antiEntropyScheduler.Stop()
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
// clustercoordination.AddressResolver the Coordinator needs, without
// making the coordination package depend on membership.
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

// nodeAntiEntropyStore adapts a *clusternode.NodeStore into the
// clusterantientropy.Store the Server and Scheduler need. Put/Delete
// are promoted directly from the embedded NodeStore (identical
// signatures already); only ScanRawRange needs a real adapter method,
// converting node's own RawKV into antientropy's RawEntry (both plain
// (key, raw value) pairs) so neither package has to import the other.
type nodeAntiEntropyStore struct {
	*clusternode.NodeStore
}

func (n nodeAntiEntropyStore) ScanRawRange(start, end string) ([]clusterantientropy.RawEntry, error) {
	rows, err := n.NodeStore.ScanRawRange(start, end)
	if err != nil {
		return nil, err
	}
	entries := make([]clusterantientropy.RawEntry, len(rows))
	for i, row := range rows {
		entries[i] = clusterantientropy.RawEntry{Key: row.Key, Value: row.Value}
	}
	return entries, nil
}

// ringOwnedRanges adapts a ring.Table into the
// clusterantientropy.OwnedRangesSource the Scheduler needs: every range
// whose replica set includes the local node, tagged with its full
// replica set so Scheduler can pick another replica to repair against.
type ringOwnedRanges struct {
	table   *clusterring.Table
	localID string
}

func (r ringOwnedRanges) OwnedRanges() []clusterantientropy.OwnedRange {
	snapshot := r.table.Snapshot()

	owned := make([]clusterantientropy.OwnedRange, 0, len(snapshot.Ranges))
	for _, rng := range snapshot.Ranges {
		for _, id := range rng.Replicas {
			if id != r.localID {
				continue
			}
			owned = append(owned, clusterantientropy.OwnedRange{
				Start:    rng.Start,
				End:      rng.End,
				Replicas: append([]string(nil), rng.Replicas...),
			})
			break
		}
	}
	return owned
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
