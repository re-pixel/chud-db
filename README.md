# NoSQL Engine (in progress)

A from-scratch **NoSQL key-value storage engine** written in Go, implementing an **LSM-tree** architecture inspired by RocksDB and Cassandra. Built as a learning project toward a distributed database system.

- **Language:** Go 1.23.2
- **Module:** `nosqlEngine`
- **Interface:** interactive CLI (`go run ./cmd`) + programmatic API

---

## Architecture

```
Write ──► WAL (group commit) ──► Memtable ──► Immutable queue ──► Flusher ──► SSTable (lvl0)
                                                                              │
                                                                    Compactor (size-tiered)
                                                                              │
                                                                        SSTable (lvl1..N)

Read ──► Active memtable ──► Immutable queue ──► SSTable levels (bloom → index → data)
```

### Write path

1. Writes are serialized through a **write queue** (single writer goroutine) — eliminates all write-path locking.
2. The WAL writer buffers multiple writes per call, then issues **one fsync** covering the entire batch (group commit). Async writes (`WriteAsync`) skip the fsync wait entirely.
3. Each write goes into the **active memtable** (hash map, skip-list, or B-tree, selectable via config).
4. When the memtable reaches `MEMTABLE_SIZE` bytes it is sealed into an **immutable memtable** carrying its max LSN and pushed to the immutable queue.
5. The **flusher goroutine** drains the immutable queue, writing sorted SSTables to `lvl0`. Flush and compaction run concurrently.
6. When a level accumulates `COMPACTION_THRESHOLD` SSTables, the **compactor goroutine** k-way-merges them into the next level (min-heap, O(N log K)).

### Read path

1. Active memtable (`O(1)` for hash map, `O(log n)` for skip-list/B-tree).
2. Immutable queue, newest-first.
3. SSTables: level 0 newest-first, levels 1+ in order. Per table: Bloom filter → in-memory dense index → block-aligned data read via LRU block cache.

### SSTable format

Single-file layout, all fields big-endian:

```
[Data blocks][Index block][Filter block][Footer: 56 bytes]

Footer: [indexOffset:8][indexSize:8][filterOffset:8][filterSize:8]
        [itemCount:8][maxLSN:8][magic:8]
```

The **Bloom filter** and **dense index** are loaded into memory when an SSTable is first opened (`SSTableReader`). `TableCache` (LRU, configurable capacity) keeps the most recently accessed readers in memory; evicted readers release their file descriptors back to the FD pool in `BlockManager`.

### Concurrency model

| Component | Mechanism |
|---|---|
| Write serialization | Single writer goroutine + buffered channel |
| Memtable reads | `sync.RWMutex` on active mem pointer; inner impl lock-free where possible |
| Immutable queue | `sync.Mutex` + `sync.Cond` for backpressure |
| SSTable version registry | `sync.RWMutex`; readers hold RLock for all SSTable I/O |
| Compaction | Snapshots version list under brief RLock, then works without any lock |
| Rate limiting | Per-user token buckets |

---

## Distribution layer readiness

Three APIs are implemented to support the first replica joining:

### WAL streaming (`WALCursor`)

```go
cursor, err := engine.wal.CursorFrom(afterLSN)
for {
    entry, err := cursor.Next()
    if err == io.EOF {
        <-cursor.Notify() // blocks until durableLSN advances
        continue
    }
    // ship entry to follower
}
cursor.Close()
```

`Next()` only returns entries that have been fsync'd. `Notify()` is a buffered channel signalled on every WAL flush — no polling needed.

### Snapshot export

```go
snap, err := engine.TakeSnapshot()
// snap.LSN    — WAL position at snapshot time
// snap.SSTables — []SnapshotFile{Level, Path, Size}
// transfer files, then:
snap.Release() // unpins files; deferred compaction deletions run
```

`TakeSnapshot` drains the active memtable, waits for all pending flushes, then captures a consistent `{LSN, file list}` pair. Files are reference-counted — compaction cannot delete a pinned file; deletion is deferred until `Release()`.

### Safe compaction LSN gate

```go
engine.SetSafeCompactionLSN(minReplicaLSN)
```

Each SSTable footer stores the `maxLSN` of its entries. The compactor gates tombstone dropping on:

```
dropTombstones = isLastLevel &&
    (safeCompactionLSN == 0 || (batchMaxLSN > 0 && batchMaxLSN <= safeCompactionLSN))
```

- `safeCompactionLSN == 0` (default) preserves standalone behaviour — tombstones drop freely.
- In distributed mode, the distribution layer bumps this as replicas confirm progress, preventing data resurrection on lagging replicas.

---

## Cluster node (distribution layer)

`cmd/node` boots the storage engine, cluster gRPC services, SWIM membership, quorum coordination, anti-entropy repair, and dynamic tablet management.

```bash
go build -o bin/nosql-node ./cmd/node
./bin/nosql-node -config examples/cluster/node-1.json
```

Config is loaded from the `-config` JSON file (see `examples/cluster/node-*.json`), then overridden by `NOSQL_CLUSTER_*` environment variables (e.g. `NOSQL_CLUSTER_NODE_ID`, `NOSQL_CLUSTER_SEEDS`).

### Local 3-node smoke test

`examples/cluster/node-{1,2,3}.json` configure three nodes on `127.0.0.1:7001-7003`, each seeded with the other two's addresses. Run each in its own terminal from the repo root:

```bash
go build -o bin/nosql-node ./cmd/node
./bin/nosql-node -config examples/cluster/node-1.json
./bin/nosql-node -config examples/cluster/node-2.json
./bin/nosql-node -config examples/cluster/node-3.json
```

Within a couple of `gossip_interval` ticks each node's membership table converges to all three nodes marked `alive`. Membership is in-memory only — restart a node and it re-bootstraps from the seed list.

The three example configs start from a matching range map
(`range_map_generation: 1`, `range_map_replicas: ["node-1", "node-2"]`,
`replication_factor: 2`) so the same smoke test cluster can be used to
check range ownership: querying `RangeMapService.GetRangeMap` on any of
the three nodes returns the identical single global range
`["", "") -> [node-1, node-2]`, and `node-3` — deliberately left out of
the replica list — rejects `NodeService.Put`/`Get`/`RangeScan` with
`FailedPrecondition`, while `node-1`/`node-2` serve them normally. The
configured tablet thresholds are intentionally high enough that this
initial map remains unchanged during an ordinary smoke test.

### Coordinated writes/reads (quorum replication)

Every node also serves `CoordinationService` — a client-facing
coordinator that any node can run for any key, regardless of whether it
owns that key itself. This is the direct, visible contrast with the
hard-reject behavior above: `NodeService.Put` on `node-3` rejects a
non-owned key outright, but `CoordinationService.Put` on `node-3` for
the very same key succeeds, because the coordinator resolves the real
owners (`node-1`, `node-2`) from its local range map and fans the write
out to them over gRPC (`CoordinatedPutRequest`/`CoordinatedGetRequest`
on `CoordinationService`, e.g. via `grpcurl` or a small Go client using
`pb.NewCoordinationServiceClient`).

With the 3-node cluster above running:
- `CoordinationService.Put` issued *to node-3* for a new key returns
  `acks=2 required=2` and lands durably on `node-1` and `node-2` —
  `CoordinationService.Get` from *any* of the three nodes (including
  node-3, which owns nothing) then returns the same value, proving
  replication actually happened rather than each node just having a
  separate, disjoint engine.
- Kill one replica (e.g. `node-2`) and retry: a `Put` with
  `write_quorum=1` still succeeds (`acks=1 required=1`, the one
  remaining owner acking), while the same `Put` with `write_quorum=2`
  fails with `codes.Unavailable` ("got 1/2 acks") — demonstrating quorum
  degradation without any rollback of the write that *did* land on the
  surviving replica (normal leaderless/AP behavior; anti-entropy
  reconciliation is what eventually catches the straggler back up once
  it returns).

### Conflict resolution

`CoordinatedGetResponse` returns a `versions` list rather than a single
value: empty means not found, one entry means every replica the read
heard from agreed (check that entry's own `deleted` flag — tombstones
are returned as a version, not hidden as "not found"), and two or more
entries means the replicas hold genuinely concurrent writes that
`versioning.Reconcile` could not causally order. The coordinator never
picks a winner on the caller's behalf; resolving siblings — e.g.
merging values, or just picking one — is left to the client. The
response's top-level `vector_clock` is the causal join of every
returned version, safe to hand back as the `context` of a follow-up
`Put` so that write dominates everything the client just saw.

To reproduce a genuine conflict on the 3-node cluster above (the
underlying engine stores one value per key per replica, so siblings
only arise when *different* replicas end up holding *different*
concurrent versions):

1. Kill `node-2`. `Put` a key via `node-1` with `write_quorum=1` and no
   context — it lands only on `node-1` with clock `{node-1: 1}`.
2. Restart `node-2`, then kill `node-1`. `Put` the *same key* via
   `node-2` with `write_quorum=1` and no context — it lands only on
   `node-2` with clock `{node-2: 1}`.
3. Restart `node-1`. `Get` the key from any node — neither clock
   dominates the other, so the response's `versions` list has exactly
   2 entries, one per replica, and `vector_clock` merges both
   (`{node-1: 1, node-2: 1}`).

### Anti-entropy repair

Quorum writes/reads (above) tolerate a down replica but never fix it up
— a replica that misses writes while it's down (or never catches a
write at all, e.g. `write_quorum=1` succeeding with only one of two
owners acking) just stays behind forever unless something reconciles
it. Every node also runs a background anti-entropy `Scheduler`: for
each range it owns, on every `anti_entropy_interval` tick it picks one
other replica of that range and walks a **tiered Merkle tree** over
the two replicas' key ranges — recursively comparing root hashes
(`AntiEntropyService.GetMerkleRoot`), splitting into `anti_entropy_
fanout` sub-ranges wherever they mismatch, until a bucket is small
enough (`anti_entropy_leaf_item_threshold`) or deep enough
(`anti_entropy_max_depth`) to diff key-by-key instead
(`AntiEntropyService.StreamRange`). Only causally-dominant versions
are propagated (`AntiEntropyService.RepairKeys`); genuine concurrent
siblings are left alone for the client to resolve, exactly as in
`Coordinated Get` above. An empty new replica facing a full peer is
just the extreme case of this same walk — the whole tree gets visited
and streamed, which is also how a brand-new node bootstraps its
initial state.

To see it repair a straggler on the 3-node cluster above:

1. Kill `node-2`. `Put` a key via `node-1`'s `CoordinationService` with
   `write_quorum=1` — it lands only on `node-1` (`acks=1 required=1`).
2. Restart `node-2`. `NodeService.Get` *directly against node-2* (not
   through the coordinator) returns not-found — it never received the
   write and has nothing in its own log to replay.
3. Wait one `anti_entropy_interval` tick. `NodeService.Get` against
   `node-2` again now returns the value, with the exact same vector
   clock `node-1` stamped (`{node-1: 1}`) — proof it arrived via
   anti-entropy pulling `node-1`'s causally-dominant version, not via
   the original (quorum-1) write it never saw.

### Dynamic tablets

Each node periodically measures the ranges it owns. A range at or above
`tablet_split_bytes` is split near its byte-weighted midpoint; adjacent
ranges at or below `tablet_merge_bytes` are merged when the deciding
node owns both. `tablet_check_interval` controls the check frequency,
and `tablet_settling_ticks` prevents a newly assigned replica from
proposing a merge while it is still empty.

Replica placement uses rendezvous hashing over alive members. Every
change retains enough existing replicas to keep the data reachable,
introducing at most one empty replica at a time. Anti-entropy then
copies the affected range to that replica. Per-range generations allow
unrelated changes to converge independently; deterministic proposal
IDs resolve simultaneous changes to the same range.

To exercise splitting on the local cluster, start all three nodes with:

```bash
export NOSQL_CLUSTER_TABLET_SPLIT_BYTES=1024
export NOSQL_CLUSTER_TABLET_MERGE_BYTES=256
export NOSQL_CLUSTER_TABLET_CHECK_INTERVAL=1s
export NOSQL_CLUSTER_TABLET_SETTLING_TICKS=1
export NOSQL_CLUSTER_ANTI_ENTROPY_INTERVAL=1s
```

Write unique keys with values large enough to cross 1 KiB, wait a check
tick, then inspect every node:

```bash
grpcurl -plaintext -import-path proto -proto cluster/v1/cluster.proto \
  127.0.0.1:7001 nosql.cluster.v1.RangeMapService/GetRangeMap
```

The response changes from one global range to contiguous ranges with
higher per-range `generation` values. Repeat against ports `7002` and
`7003` to confirm propagation. If a child lists `node-3`, a direct
`NodeService.Get` for a key in that child succeeds after anti-entropy
runs, demonstrating reassignment and catch-up.

Range scans are still single-tablet operations: a `NodeService.RangeScan`
whose interval crosses a tablet boundary returns `FailedPrecondition`.
Point reads and writes continue routing through the current range map.
Range metadata is currently in memory: a rolling restart recovers it
from live peers, while restarting the whole cluster resets to the
configured bootstrap range.

---

## Benchmarks

Machine: AMD Ryzen 7 7730U, WSL2 (Linux), `go test -bench=. -benchtime=5s`.  
All writes are sync-durability (fsync) unless noted. Value size: 64 bytes.

| Benchmark | ops/sec | Notes |
|---|---|---|
| `PutSequential` | **697** | Single goroutine, sync, waits flush per op |
| `PutThroughput` | **764** | Single goroutine, sync, no flush wait per op |
| `PutThroughputParallel` | **5,937** | 16 goroutines, sync, group commit batching |
| `PutParallel` | **5,915** | Same as above, different key space |
| `PutBurst` | **3,327** | 64-goroutine bursts, group commit |
| `PutAsyncThroughput` | **2,765** | Single goroutine, no fsync wait |
| `PutAsyncParallel` | **3,466** | 16 goroutines, no fsync wait |
| `GetMemtableHit` | **410,429** | Active memtable (hash map) |
| `GetSSTable` | **441,070** | SSTable read via block cache (warm) |
| `DeleteSequential` | **875** | Write + tombstone, sync |

Key observations:
- **Sequential sync puts are WAL-bound** — every op pays one fsync (~1.4 ms). The single-writer design means no write amplification from lock contention.
- **Parallel sync puts show ~8.5x speedup** over sequential. Group commit amortizes the fsync cost across concurrent writers: 16 goroutines sharing one fsync per batch.
- **SSTable reads match memtable reads** because the block cache keeps hot blocks in memory; the Bloom filter + in-memory dense index avoids most disk seeks entirely.
- **Async writes** offer a middle ground — WAL-buffered but not fsync'd; useful for bulk ingest where a subsequent sync write or graceful shutdown provides the durability boundary.

---

## Build & run

```bash
go mod tidy
go build -o bin/nosql-engine ./cmd
./bin/nosql-engine

# or without building:
go run ./cmd
```

### Tests

```bash
go test ./...                                    # all packages
go test ./src/tests/integration/ -v              # integration tests
go test ./src/tests/benchmark/ -bench=. -benchtime=5s  # benchmarks
```

**Note:** tests write real files under `data/`. Run from the repo root.  
**Note:** `config.json` is embedded at compile time — config changes require a rebuild.

---

## Configuration (`src/config/config.json`)

| Key | Default | Description |
|---|---|---|
| `BLOCK_SIZE` | 4096 | Disk I/O block size in bytes |
| `MEMTABLE_SIZE` | 1000 | Flush threshold in bytes (sum of key+value lengths) |
| `MEMTABLE_TYPE` | `hashmap` | `hashmap`, `skiplist`, or `btree` |
| `MAX_IMMUTABLE_COUNT` | 4 | Immutable queue depth before writes block |
| `WAL_SEGMENT_SIZE` | 65536 | Max bytes per WAL segment file |
| `WAL_BUFFER_SIZE` | 8 | WAL write buffer slots for group commit |
| `LSM_LEVELS` | 4 | Number of LSM levels |
| `COMPACTION_THRESHOLD` | 4 | SSTables per level before compaction |
| `CACHE_CAPACITY` | 512 | Block cache LRU capacity (number of blocks) |
| `TABLE_CACHE_SIZE` | 64 | SSTableReader LRU capacity |
| `BLOOM_FILTER_FALSE_POSITIVE_RATE` | 0.01 | Target Bloom filter FPR |

---

## License

MIT
