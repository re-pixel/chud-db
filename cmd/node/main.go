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

	store := clusternode.NewNodeStore(eng, clusternode.DefaultStoreUser)
	nodeServer := clusternode.NewServer(cfg, store)

	membershipClient := clustermembership.NewClient()
	defer membershipClient.Close() //nolint:errcheck

	table := clustermembership.NewTable(cfg.ClusterID, clustermembership.Member{
		NodeID:        cfg.NodeID,
		AdvertiseAddr: cfg.AdvertiseAddr,
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

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, nodeServer)
	pb.RegisterGossipServiceServer(grpcServer, gossipServer)

	serveErr := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			serveErr <- err
		}
	}()

	gossipCtx, stopGossip := context.WithCancel(context.Background())
	gossiper.Start(gossipCtx)

	fmt.Printf("node %s listening on %s (advertise %s)\n", cfg.NodeID, cfg.ListenAddr, cfg.AdvertiseAddr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		fmt.Printf("node %s shutting down after %s\n", cfg.NodeID, sig)
		stopGossip()
		gossiper.Stop()
		shutdownGRPC(grpcServer)
		return nil
	case err := <-serveErr:
		stopGossip()
		gossiper.Stop()
		return fmt.Errorf("serve grpc: %w", err)
	}
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
