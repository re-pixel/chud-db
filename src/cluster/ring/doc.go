// Package ring implements the range map: the versioned routing table
// that maps keys to the replica set that owns them. It is a pure
// domain package (no transport dependencies); cluster/ring's gRPC
// server/client live alongside it in this same package for cohesion,
// mirroring cluster/membership's layout.
package ring
