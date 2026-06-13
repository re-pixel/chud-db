# Benchmarks

Track engine performance before and during the rewrite. Compare each run against the committed baseline.

## Prerequisites

```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

Run from the **repository root** (embedded config is compiled into the binary; rebuild after config changes).

## Run benchmarks

```bash
./scripts/bench.sh
```

Results are written to `benchmarks/runs/<git-sha>-<timestamp>.txt` (gitignored). If `benchmarks/baseline.txt` exists, `benchstat` prints a diff at the end.

## Capture or update the baseline

After reviewing a run:

```bash
cp benchmarks/runs/<latest>.txt benchmarks/baseline.txt
git add benchmarks/baseline.txt
```

Update the baseline only when a performance change is intentional and permanent (e.g. a completed WAL rewrite). Do not update the baseline for regressions you plan to fix.

## What is measured

| Package | Benchmarks |
|---------|--------------|
| `src/tests/benchmark` | `PutSequential`, `GetMemtableHit`, `GetSSTable`, `DeleteSequential`, `MixedReadWrite` |
| `src/wal` | WAL append, `WaitDurable` (sequential + parallel group commit), `WritePut` |

Engine `PutParallel` is skipped: `engine.Write` is not goroutine-safe. Use `BenchmarkWALAppendPutWaitDurableParallel` for group-commit throughput.

## Notes

- **`MEMTABLE_SIZE` is bytes** (key+value length sum), not entry count. Current embedded config uses small values tuned for tests.
- PUT latency includes **WAL fsync** (group mode: one fsync per sequential write; parallel batching in WAL benchmarks).
- Expect high variance on fsync-heavy benchmarks; use `-count=5` or higher and compare trends, not single runs.
- Benchmarks use isolated temp dirs via `engine.NewBenchEngine` (no pollution of repo `data/`).
