// Package antientropy implements background Merkle-tree repair between
// replicas of the same owned range: periodically compare a range's
// contents against another replica, drill down to the specific
// diverging keys, and propagate only causally-settled staleness (a
// replica that missed a write, or holds a causally superseded value).
// Genuine concurrent conflicts are left untouched on both sides -
// resolving those is the client's responsibility via the coordinator's
// sibling exposure (see src/cluster/coordination). A brand-new or
// emptied replica for a range is treated the same way as any other
// out-of-sync replica; there is no separate bootstrap mechanism.
package antientropy
