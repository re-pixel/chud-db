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
	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, nodeServer)

	serveErr := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			serveErr <- err
		}
	}()

	fmt.Printf("node %s listening on %s (advertise %s)\n", cfg.NodeID, cfg.ListenAddr, cfg.AdvertiseAddr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case sig := <-signals:
		fmt.Printf("node %s shutting down after %s\n", cfg.NodeID, sig)
		shutdownGRPC(grpcServer)
		return nil
	case err := <-serveErr:
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
