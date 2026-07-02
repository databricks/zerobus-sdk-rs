#!/bin/bash
set -e

# Regenerate the Go bindings (go/tests/pb) used by the cgo Go SDK integration
# tests. These are generated from the canonical schema owned by the Rust core
# (rust/sdk/zerobus_service.proto) — the single source of truth shared with the
# Rust core and Java SDK. Do not add a local copy of the .proto here.
#
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH. Pin the plugins to
# the versions that produced the committed bindings so regeneration is
# deterministic (a newer plugin can emit a different file and create a spurious
# diff):
#
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROTO_DIR="${REPO_ROOT}/rust/sdk"

echo "Generating Go test bindings from ${PROTO_DIR}/zerobus_service.proto..."

mkdir -p "${SCRIPT_DIR}/pb"

# The canonical schema sets no go_package (it is shared by two Go modules that
# target different import paths), so map it to this module's package here.
GO_PKG="github.com/databricks/zerobus-sdk/go/tests/pb"

# The schema imports google/protobuf/duration.proto (a well-known type). Standard
# protoc installs resolve it from their bundled include automatically; if yours
# keeps the well-known types elsewhere, point PROTOC_WKT_INCLUDE at that dir.
INC_ARGS=(-I "${PROTO_DIR}")
[ -n "${PROTOC_WKT_INCLUDE:-}" ] && INC_ARGS+=(-I "${PROTOC_WKT_INCLUDE}")

# The committed bindings import the legacy duration package
# (github.com/golang/protobuf/ptypes/duration), not the modern durationpb. Pin
# the duration.proto import explicitly so regeneration reproduces the committed
# file byte-for-byte instead of resolving durationpb from the bundled WKT include.
DURATION_PKG="github.com/golang/protobuf/ptypes/duration"

protoc "${INC_ARGS[@]}" \
    --go_out="${SCRIPT_DIR}/pb" --go_opt=paths=source_relative \
    --go_opt=Mzerobus_service.proto="${GO_PKG}" \
    --go_opt=Mgoogle/protobuf/duration.proto="${DURATION_PKG}" \
    --go-grpc_out="${SCRIPT_DIR}/pb" --go-grpc_opt=paths=source_relative \
    --go-grpc_opt=Mzerobus_service.proto="${GO_PKG}" \
    zerobus_service.proto

echo "✓ Regenerated go/tests/pb from the canonical schema"
