#!/bin/bash
set -e
# Regenerate the Go bindings for orders.proto into pb/.
# Requires protoc and protoc-gen-go on PATH:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
echo "Generating Go code from orders.proto..."
mkdir -p pb
protoc --go_out=pb --go_opt=paths=source_relative orders.proto
echo "✓ Generated pb/orders.pb.go"
