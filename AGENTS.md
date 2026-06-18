# AGENTS.md

Guidance for AI agents working in this repository.

## Project Overview

NoSQL-Engine is a from-scratch **NoSQL key-value database engine** built in **Go**, implementing an **LSM-tree (Log-Structured Merge tree)** storage architecture inspired by Cassandra / RocksDB / DynamoDB. It supports `PUT` / `GET` / `DELETE` plus prefix and range scans/iterators, exposed through an interactive CLI.

- **Language:** Go (module `nosqlEngine`, Go `1.23.2`).
- **Dependencies (minimal):** `github.com/google/uuid` (direct, SSTable filenames), `github.com/cespare/xxhash/v2` (indirect, bloom-filter hashing).
- **Nature:** Educational/academic project. Despite "production-ready" framing in the README, treat it as a learning codebase (debug prints, stubbed components, and some bugs exist — see Gotchas).

## Build, Run & Test

Run all commands from the repo root (`/home/relja/projects/NoSQL-Engine`). There is **no Makefile, Dockerfile, or CI config** — it's a plain `go` toolchain project.

| Task | Command |
|------|---------|
| Sync deps | `go mod tidy` |
| Build | `go build -o bin/nosql-engine ./cmd` |
| Run (no build) | `go run ./cmd` |
| Run binary | `./bin/nosql-engine` |
| Run all tests | `go test ./...` |
| Run integration tests | `go test ./src/tests/integration/ -v` |
| Run a single test | `go test ./src/tests/integration/ -run TestWriteRead -v` |
| Format | `go fmt ./...` |
| Vet | `go vet ./...` |

## Architecture

Layered design: `cmd` → `engine` (orchestrator) → `storage` + `service` → `models`.

The central type is `Engine` (`src/engine/engine.go`), which wires all subsystems. Lifecycle: `NewEngine()` constructs; `Start()` replays the WAL for crash recovery; `Shut()` flushes the WAL.

**Write path** (`src/engine/write.go`):
1. Rate-limit check via token bucket (`user_limiter`).
2. Append to **WAL** for durability (DELETE writes a tombstone).
3. Insert into the current **memtable** (hash map).
4. On reaching `MEMTABLE_SIZE`, rotate memtable and **asynchronously flush** to an SSTable (goroutine guarded by `flush_lock`), then check compaction conditions.

**Read path** (`src/engine/read.go`):
1. Rate-limit check.
2. Scan in-memory memtables (newest first).
3. Fall back to SSTables across all LSM levels via `retriever`.

**SSTable lookup:** read Metadata → check Bloom filter → use Summary (sparse in-memory index) to bound search → scan Index blocks for offset → read Data block. Blocks read in reverse (metadata appended at end of file).

**SSTable layout** (`src/service/ss_parser`): Data → Index → Summary → Metadata (bloom filter + prefix bloom filter + Merkle root + item count + offsets). All I/O goes through the block-based `FileWriter`.

**Compaction** (`src/service/ss_compacter`): **size-tiered** — when a level has ≥ `COMPACTION_THRESHOLD` SSTables, k-way merge them into a new SSTable on the next level, dedup keys, rebuild bloom + merkle, delete originals. Can cascade across levels.

## Directory Structure

```
cmd/main.go              # CLI entry point (REPL, command parsing, colored output)
bin/nosql-engine         # Prebuilt binary
src/
  config/                # config.go (embeds config.json) + config.json
  engine/                # Orchestrator: engine.go, read.go, write.go, range_scan.go, prefix_scan.go
  storage/
    memtable/            # memtable factory + interface
    wal/                 # write-ahead log (encode/flush/rotate/replay)
  service/               # I/O + stateless services
    block_manager/       # fixed-size block disk I/O + LRU cache
    file_writer/         # block-buffered writing
    file_reader/         # block reading (forward/reverse)
    ss_parser/           # SSTable serialization
    ss_compacter/        # size-tiered compaction
    retriever/           # SSTable lookups & prefix scans
    token_bucket/        # rate-limiting algorithm
    user_limiter/        # per-user token buckets
  models/                # Pure data structures
    key_value/, hash_map/, skip_list/, b_tree/
    bloom_filter/, merkle_tree/, doubly_ll/
    countmin_sketch/, hyperloglog/, simhash/   # present but unused
  utils/reusable.go      # GetPaths() helper
  tests/integration/     # write_test.go (the only test file)
data/sstable/lvl0..N     # SSTables (.db); data/wal/ holds .log segments (created at runtime)
assets/                  # PNG architecture diagrams
README.md, CLI_USAGE.md  # Docs
```

## Code Conventions

- **Packages & files:** `snake_case` directory names matching the package name; one package per directory; `snake_case.go` files.
- **Interfaces:** split into separate `*_interface.go` files (e.g. `memtable_interface.go`).
- **Constructors:** `New<Type>()` pattern (e.g. `NewEngine`, `NewWAL`).
- **Config access:** each package declares a package-level `var CONFIG = config.GetConfig()`.
- **Visibility:** standard Go capitalization rules; getters like `GetKey()`, `GetValue()`.
- **Errors:** return `error`, wrapping with `fmt.Errorf("... %w", err)`.
- **Note:** struct field naming is inconsistent (mix of `snake_case` and `camelCase`); `snake_case` fields are non-idiomatic Go but used in places. Match the surrounding file's style when editing.

## Configuration

`src/config/config.json` is **embedded into the binary at compile time** via `//go:embed` (`src/config/config.go`). **Editing it requires a rebuild** to take effect. Current values are small (tuned for testing): `BLOCK_SIZE: 40`, `MEMTABLE_SIZE: 20`, `LSM_LEVELS: 2`, `COMPACTION_THRESHOLD: 2`, `TOMBSTONE: "<DELETED!>"`.

## Testing

- Standard Go `testing` package only (no testify).
- A single file: `src/tests/integration/write_test.go` (package `integration`), with `TestWritePathIntegration`, `TestWriteRead`, `TestPrefixScan`, `TestWALWriteRead`, `TestCompacter`, `TestGas`.
- Tests exercise the service layer directly and **write real files under `data/`** (filesystem side effects). Some assume pre-existing SSTables (e.g. `TestCompacter`).

## Gotchas

- **Run from the repo root.** Data paths (`data/sstable/lvl0`, `data/wal`) are resolved relative to source files via `runtime.Caller` / `getProjectRoot()` (duplicated in several files). The engine expects to run from within the repo tree.
- **`config.json` is embedded** — config edits need a rebuild, not just a file change.
- **Config typo bug:** the JSON key is `"CACHE_CAPCAITY"` but the struct expects `CACHE_CAPACITY`, so `CacheCapacity` silently stays `0`.
- **Stubbed components:** the memtable factory always returns a hash map (skip-list / b-tree wiring is incomplete); the LRU cache `Put`/`Get` calls in `block_manager` are commented out; `hyperloglog`, `countmin_sketch`, and `simhash` models are present but unused.
- **Debug output:** `fmt.Print`/`Println` statements and commented-out dead code remain in production paths.
- **README config example is aspirational** (e.g. `BLOCK_SIZE: 4096`) and differs from the actual `config.json`.
