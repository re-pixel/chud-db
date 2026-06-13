#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p benchmarks/runs

SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="benchmarks/runs/${SHA}-${STAMP}.txt"

echo "Running benchmarks (results -> $OUT)"
go test ./src/tests/benchmark/ ./src/wal/ \
  -run='^$' \
  -bench=. \
  -benchmem \
  -count=5 \
  -timeout=30m \
  2>&1 | tee "$OUT"

echo "Saved: $OUT"

if [[ -f benchmarks/baseline.txt ]]; then
  if command -v benchstat >/dev/null 2>&1; then
    echo ""
    echo "Comparison vs benchmarks/baseline.txt:"
    benchstat benchmarks/baseline.txt "$OUT"
  else
    echo ""
    echo "Install benchstat to compare: go install golang.org/x/perf/cmd/benchstat@latest"
  fi
fi
