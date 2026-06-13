# NoSQL-Engine — common development targets (run from repo root)

GO       ?= go
BIN      := bin/nosql-engine
CMD      := ./cmd
BENCH    := ./scripts/bench.sh

.PHONY: help build run run-dev test test-integration test-wal test-bench bench bench-quick fmt vet tidy check clean clean-data

help: ## List available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build CLI binary to bin/nosql-engine
	$(GO) build -o $(BIN) $(CMD)

run: build ## Build and run the interactive CLI
	./$(BIN)

run-dev: ## Run CLI without building a binary (go run)
	$(GO) run $(CMD)

test: ## Run all unit tests
	$(GO) test ./...

test-integration: ## Run integration tests (writes under data/)
	$(GO) test ./src/tests/integration/ -v

test-wal: ## Run WAL package tests
	$(GO) test ./src/wal/... -v

test-bench: ## Run benchmark tests once (smoke, no stats)
	$(GO) test ./src/tests/benchmark/ ./src/wal/ -run='^$$' -bench=. -benchtime=100ms -count=1

bench: ## Run full benchmark suite and save to benchmarks/runs/
	$(BENCH)

bench-quick: ## Short benchmark run for local dev (-count=1)
	$(GO) test ./src/tests/benchmark/ ./src/wal/ \
		-run='^$$' -bench=. -benchmem -benchtime=100ms -count=1

fmt: ## Format Go sources
	$(GO) fmt ./...

vet: ## Run go vet
	$(GO) vet ./...

tidy: ## Sync go.mod / go.sum
	$(GO) mod tidy

check: fmt vet test ## Format, vet, and run unit tests

clean: ## Remove built binary
	rm -f $(BIN)

clean-data: ## Remove runtime WAL and SSTable data (keeps .gitkeep)
	rm -rf data/wal/*
	rm -f data/sstable/lvl0/*.db data/sstable/lvl1/*.db data/sstable/lvl2/*.db data/sstable/lvl3/*.db
