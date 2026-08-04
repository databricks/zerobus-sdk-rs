// Package zerobuspb holds the protobuf and gRPC bindings for the Zerobus
// ingestion service, generated from the canonical schema at
// rust/sdk/zerobus_service.proto.
//
// It is internal to the pure-Go SDK: the wire types and the generated
// ZerobusClient (a bidirectional EphemeralStream RPC) are implementation
// details, not part of the SDK's public API.
//
// To regenerate after the canonical schema changes, run `go generate ./...`
// from this package's directory. This requires protoc on PATH plus the
// protoc-gen-go and protoc-gen-go-grpc plugins. Pin these to the versions that
// produced the committed bindings so regeneration is deterministic (a newer
// plugin can emit a different file and create a spurious diff):
//
//	# expects protoc v6.33.0
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
//	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
//
// The canonical schema sets no go_package, so its import is remapped to this
// package via the Mzerobus_service.proto option. google/protobuf/duration.proto
// is vendored under third_party/ so generation does not depend on a system-wide
// protobuf include directory; its import is remapped to the canonical durationpb
// package via its own M option.
package zerobuspb

//go:generate protoc -I ../../../rust/sdk -I third_party --go_out=. --go_opt=paths=source_relative --go_opt=Mgoogle/protobuf/duration.proto=google.golang.org/protobuf/types/known/durationpb --go_opt=Mzerobus_service.proto=github.com/databricks/zerobus-sdk/purego/internal/zerobuspb --go-grpc_out=. --go-grpc_opt=paths=source_relative --go-grpc_opt=Mgoogle/protobuf/duration.proto=google.golang.org/protobuf/types/known/durationpb --go-grpc_opt=Mzerobus_service.proto=github.com/databricks/zerobus-sdk/purego/internal/zerobuspb zerobus_service.proto
