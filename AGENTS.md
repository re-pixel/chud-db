# AGENTS.md

Guidance for AI agents working in this repository.

## Project Overview

NoSQL-Engine is a from-scratch **NoSQL key-value database engine** built in **Go**, implementing an **LSM-tree (Log-Structured Merge tree)** storage architecture inspired by RocksDB and Cassandra. It supports `PUT` / `GET` / `DELETE`, batch writes, prefix and range scans, exposed through an interactive CLI. The engine is being evolved toward a distributed system — the single-node layer is feature-complete.

- **Language:** Go (module `nosqlEngine`, Go `1.23.2`).
- **Dependencies:** `github.com/google/uuid` (SSTable filenames), `github.com/cespare/xxhash/v2` (bloom-filter hashing).

## Build, Run & Test

Run all commands from the repo root (`/home/relja/projects/NoSQL-Engine`).

| Task | Command |
|------|---------|
| Sync deps | `go mod tidy` |
| Build | `go build -o bin/nosql-engine ./cmd` |
| Run (no build) | `go run ./cmd` |
| Run binary | `./bin/nosql-engine` |
| Run all tests | `go test ./...` |
| Run integration tests | `go test ./src/tests/integration/ -v` |
| Run benchmarks | `go test ./src/tests/benchmark/ -bench=. -benchtime=5s` |
| Format | `go fmt ./...` |
| Vet | `go vet ./...` |

## Architecture

The central type is `Engine` (`src/engine/engine.go`). Lifecycle: `NewEngine()` constructs; `Start()` replays the WAL and starts background goroutines; `Shut()` drains, flushes, and waits for all goroutines.

### Write path (`src/engine/write.go`, `src/engine/write_queue.go`)

1. All writes enter the engine via a **write channel** and are processed by a single `runWriter` goroutine — no write-path locking.
2. `runWriter` drains all pending requests into a batch, calls `applyWriteToMem` per entry (WAL buffer append + memtable insert), then calls `wal.WaitDurable(maxLSN)` **once** for the entire batch (**group commit**).
3. Async writes (`WriteAsync`, `ApplyBatchAsync`) are signalled immediately after memtable insert without waiting for fsync.
4. When the active memtable reaches `MEMTABLE_SIZE` bytes, it is sealed as an `ImmutableMemtable` (carries `maxLSN`) and pushed to the **immutable queue** (`src/engine/immutable_queue.go`).
5. The **flusher goroutine** (`src/engine/flusher.go`) drains the immutable queue, calls `ss_parser.FlushMemtable(data, maxLSN)`, and registers the resulting SSTable in the version registry.
6. After each flush, the flusher signals the **compactor goroutine** via a non-blocking channel. Compaction and flushing are fully decoupled.
7. **Batch write API** (`src/engine/batch.go`): `NewWriteBatch().Put(...).Delete(...)` → `engine.ApplyBatch(user, batch)` — all ops in one fsync.

### Read path (`src/engine/read.go`, `src/engine/scan.go`)

1. Rate-limit check.
2. Active memtable lookup.
3. Immutable queue scan (newest first).
4. SSTable levels via `engine.lockVersions()` — level 0 newest-first, levels 1+ in registration order. Per table: Bloom filter → in-memory dense index → block-aligned data read via LRU block cache.
5. Tombstone values (`CONFIG.Tombstone`) are filtered at the engine boundary — never returned to callers.

Prefix and range scans share a unified `engine.scan` helper (`src/engine/scan.go`) that handles tombstone shadowing across all three layers via a `seen` map.

### SSTable format (`src/service/ss_parser`, `src/sstable`)

Single-file layout, all fields big-endian:

```
[Data blocks][Index block][Filter block][Footer: 56 bytes]

Footer: [indexOffset:8][indexSize:8][filterOffset:8][filterSize:8]
        [itemCount:8][maxLSN:8][magic:8=0x0D1AACCE55DB0002]
```

- The **index** is a dense sorted list of (key, blockOffset) pairs loaded fully into memory when an SSTable is opened.
- The **filter block** contains a Bloom filter and a prefix Bloom filter, also loaded into memory.
- `SSTableReader` (`src/sstable/reader.go`) holds the in-memory index + filters and an FD from the `BlockManager` pool.
- `TableCache` (`src/sstable/table_cache.go`) is an LRU cache of `SSTableReader` instances (capacity: `TABLE_CACHE_SIZE`). Evicted readers close their FDs.
- **`maxLSN`** in the footer enables the safe compaction LSN gate for distributed tombstone safety.

**Existing `.db` files from older versions are incompatible** — delete `data/sstable/` before running after a footer format change.

### Compaction (`src/service/ss_compacter/ss_compacter_st.go`)

- Size-tiered: when a level has ≥ `COMPACTION_THRESHOLD` SSTables, merge them into the next level.
- K-way merge with a min-heap (`O(N log K)`).
- Atomic writes: output to `.db.tmp`, then `os.Rename` on success.
- Tombstone dropping: only at the last LSM level, gated on `safeCompactionLSN` (see below).

### Version registry (`src/engine/engine.go`)

`engine.versions [][]string` holds live SSTable paths per level under `versionMu sync.RWMutex`. Readers hold the RLock for the duration of all SSTable I/O. The compactor takes a snapshot of versions under a brief RLock and works without holding the lock.

### Distribution layer APIs

Three APIs are ready for the replication layer:

| API | Location | Purpose |
|-----|----------|---------|
| `wal.CursorFrom(afterLSN)` | `src/wal/cursor.go` | Stream durable WAL entries from a given LSN; `Notify()` channel avoids polling |
| `engine.TakeSnapshot()` | `src/engine/snapshot.go` | Consistent `{LSN, []SSTables}` snapshot; pins files until `Snapshot.Release()` |
| `engine.SetSafeCompactionLSN(lsn)` | `src/engine/engine.go` | Prevent tombstone GC until all replicas have applied that LSN |

## Directory Structure

```
cmd/main.go                    # CLI entry point (REPL)
bin/nosql-engine               # Prebuilt binary
src/
  config/                      # Config struct + embedded config.json
  engine/                      # Orchestrator
    engine.go                  # Engine struct, version registry, lifecycle
    write.go                   # Write / WriteAsync / ApplyBatch / ApplyBatchAsync
    write_queue.go             # writeReq, runWriter (group commit)
    batch.go                   # WriteBatch, BatchOp
    read.go                    # Read
    scan.go                    # Unified scan helper, Iterator, sortedPairs
    prefix_scan.go             # PrefixScan
    range_scan.go              # RangeScan
    flusher.go                 # Flusher goroutine
    immutable_queue.go         # ImmutableQueue with backpressure
    snapshot.go                # TakeSnapshot, Snapshot, SnapshotFile
  memtable/                    # Memtable interface + implementations
    memtable.go                # Memtable interface (Add/Get/Scan/ToRaw/...)
    factory.go                 # NewMemtable() — reads MEMTABLE_TYPE from config
    hashmap.go / skiplist.go / btree.go
    sync.go                    # syncMemtable (RWMutex wrapper)
    immutable.go               # ImmutableMemtable (carries maxLSN)
  models/                      # Pure data structures
    bloom_filter/              # BloomFilter + PrefixBloomFilter
    merkle_tree/
    doubly_ll/                 # Used by LRU cache
    key_value/, hash_map/, skip_list/, b_tree/
    countmin_sketch/, hyperloglog/, simhash/  # present but unused
  service/
    block_manager/             # FD pool + LRU block cache (ReadAt)
    file_writer/               # Buffered single-FD block writer (used by SSParser + compactor)
    ss_parser/                 # SSTable serialization (FlushMemtable, footer utils)
    ss_compacter/              # Size-tiered compaction + prealloc stubs
    token_bucket/              # Token bucket rate-limiting
    user_limiter/              # Per-user token buckets
  sstable/                     # SSTable read side
    reader.go                  # SSTableReader (Open, Get, PrefixScan, ScanAll, MaxLSN)
    table_cache.go             # TableCache (LRU of SSTableReaders)
  wal/                         # Write-ahead log
    wal.go                     # WAL (Append*, WaitDurable, Subscribe, CursorFrom, PurgeUpTo)
    cursor.go                  # WALCursor (Next, Notify, Close)
    replay.go                  # ReplayFunc
    config/                    # WAL-specific config (WAL_SEGMENT_SIZE, WAL_SYNC_MODE, etc.)
    record/                    # Record encoding/decoding + CRC
    storage/                   # AppendFileStorage, segment rotation, pre-allocation
  utils/reusable.go            # ListSSTablesInLevel, SSTableLevelDir, DefaultDataRoot
  tests/
    integration/write_test.go  # Integration tests (write real files under data/)
    benchmark/engine_bench_test.go  # Benchmarks (PutSequential, BatchPut10/100, GetSSTable, ...)
data/sstable/lvl0..N           # SSTable .db files (created at runtime)
data/wal/                      # WAL .log segments (created at runtime)
assets/                        # PNG architecture diagrams
README.md, CLI_USAGE.md
```

## Code Conventions

- **Packages & files:** `snake_case` directory names matching the package name; one package per directory; `snake_case.go` files.
- **Constructors:** `New<Type>()` pattern.
- **Config access:** package-level `var CONFIG = config.GetConfig()`.
- **Errors:** return `error`, wrap with `fmt.Errorf("... %w", err)`.
- **Comments:** only explain non-obvious intent or constraints. Do not narrate what code does.
- **No emojis** in code or comments.

## Configuration (`src/config/config.json`, embedded at compile time — rebuild required after edits)

| Key | Value | Description |
|-----|-------|-------------|
| `BLOCK_SIZE` | 4096 | Disk I/O block size (bytes) |
| `MEMTABLE_SIZE` | 1000 | Flush threshold (sum of key+value bytes) |
| `MEMTABLE_TYPE` | `hashmap` | `hashmap`, `skiplist`, or `btree` |
| `MAX_IMMUTABLE_COUNT` | 4 | Immutable queue depth before writes block |
| `WAL_SEGMENT_SIZE` | 65536 | Max bytes per WAL segment |
| `LSM_LEVELS` | 4 | Number of LSM levels |
| `COMPACTION_THRESHOLD` | 4 | SSTables per level before compaction |
| `CACHE_CAPACITY` | 512 | Block cache LRU capacity (blocks) |
| `TABLE_CACHE_SIZE` | 64 | SSTableReader LRU capacity |
| `TOMBSTONE` | `<DELETED!>` | Internal tombstone marker |

WAL also has its own config (`src/wal/config/config.json`) with `WAL_SYNC_MODE` (`sync` or `group`), `WAL_SEGMENT_SIZE`, and `WAL_WRITE_BUFFER_SIZE`.

## Testing

- Standard Go `testing` only (no testify).
- Integration tests in `src/tests/integration/write_test.go` write real files under `data/` — run from repo root.
- Engine-level concurrency tests in `src/engine/concurrency_test.go`.
- Memtable unit tests in `src/memtable/`.
- WAL unit + group-commit tests in `src/wal/`.
- Benchmarks in `src/tests/benchmark/engine_bench_test.go`.

## Gotchas

- **Run from the repo root.** Data paths are resolved via `runtime.Caller` / `getProjectRoot()`.
- **`config.json` is embedded** — config edits require a rebuild.
- **Old SSTable files are incompatible** after any footer format change (magic `0x0D1AACCE55DB0002`). Delete `data/sstable/` before running with new code.
- **WAL pre-allocation** uses `fallocate(FALLOC_FL_KEEP_SIZE)` on Linux; the `prealloc_other.go` stub is a no-op on other platforms.
- **`countmin_sketch`, `hyperloglog`, `simhash`** are implemented but not wired into any engine path.
- **`safeCompactionLSN`** defaults to `0` — tombstones drop freely (standalone mode). Set it via `engine.SetSafeCompactionLSN` when running with replicas.
