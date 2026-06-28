#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v protoc-gen-go >/dev/null 2>&1; then
  echo "protoc-gen-go is required. Run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
  exit 1
fi

if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  echo "protoc-gen-go-grpc is required. Run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" >&2
  exit 1
fi

mkdir -p "${ROOT_DIR}/src/cluster/transport/pb"

if command -v buf >/dev/null 2>&1; then
  (cd "${ROOT_DIR}" && buf generate proto)
  exit 0
fi

if ! command -v protoc >/dev/null 2>&1; then
  echo "buf or protoc is required. Install buf or protobuf-compiler." >&2
  exit 1
fi

protoc \
  -I "${ROOT_DIR}/proto" \
  --go_out="${ROOT_DIR}" \
  --go_opt=paths=import,module=nosqlEngine \
  --go-grpc_out="${ROOT_DIR}" \
  --go-grpc_opt=paths=import,module=nosqlEngine \
  "${ROOT_DIR}/proto/cluster/v1/cluster.proto"
