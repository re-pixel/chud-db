// Package replication implements the leaderless quorum coordinator: any
// node can accept a client's Put/Delete/Get, fan it out to the key's
// actual owners (per the local ring.Table), and answer once enough
// replicas have acknowledged. It is a leaf package - it only depends on
// versioning, mirroring the ring.Syncer/PeerEpochSource lesson from
// Phase 3 so this package stays independently testable with fakes.
package replication
