# Go SDK

Go wrapper around the Rust core via cgo and the C FFI library.

## Structure

```
go/
├── zerobus.go       # Public API (ZerobusSdk, ZerobusStream)
├── ffi.go           # cgo bindings — calls C functions from zerobus.h
├── arrow_ffi.go     # Arrow Flight cgo bindings
├── arrow_stream.go  # Arrow stream public API
├── ack.go           # Acknowledgment types
├── errors.go        # ZerobusError
├── types.go         # Configuration types
├── build.go         # Build tags and cgo link directives
├── build_rust.sh    # Script to build the Rust FFI library
├── lib/             # Pre-built static libraries per platform
│   ├── linux_x86_64/libzerobus_ffi.a
│   ├── linux_arm64/libzerobus_ffi.a
│   ├── darwin_x86_64/libzerobus_ffi.a
│   ├── darwin_arm64/libzerobus_ffi.a
│   └── windows_x86_64/libzerobus_ffi.a
├── tests/           # Integration tests
└── examples/        # Usage examples
```

## Build commands

Run from `go/`:

- `make build` — Build Rust FFI lib + Go SDK
- `make build-rust` — Build only Rust FFI layer
- `make build-go` — Build only Go SDK (requires pre-built FFI lib)
- `make test` — Run tests
- `make lint` — go vet + cargo clippy
- `make fmt` — gofmt + cargo fmt

## FFI boundary: cgo + static linking

This is the most memory-management-sensitive wrapper. Key considerations:

### Memory ownership

- **Opaque pointers**: Go holds `unsafe.Pointer` to Rust-allocated `CZerobusSdk` / `CZerobusStream`. Rust owns the memory.
- **Explicit free required**: `zerobus_sdk_free()`, `zerobus_stream_free()`, `zerobus_arrow_stream_free()`.
- **Finalizers**: Both `ZerobusSdk` and `ZerobusStream` register `runtime.SetFinalizer` for GC-triggered cleanup, but explicit `Free()`/`Close()` is preferred for deterministic resource release.
- **Error strings**: C-allocated error messages (`CResult.error_message`) must be freed via `zerobus_free_error_message()` after converting to Go string with `C.GoString()`.

### Memory pinning (critical)

Go's GC can relocate heap objects. When passing Go memory to C:
- `runtime.Pinner` is used to pin Go slices/pointers before passing to Rust.
- Every FFI function that passes Go data uses: create pinner → pin → call C → defer unpin.
- **Never remove `runtime.Pinner` calls** — doing so causes "cgo argument has Go pointer to unpinned Go pointer" panics.
- Requires Go 1.21+ (the `runtime.Pinner` API).

### Handle registry for callbacks

When using custom `HeadersProvider` (instead of default OAuth):
- A `cgo.Handle` wraps the Go interface value and prevents GC collection.
- Handles are stored in `streamHandleRegistry` (mutex-protected map, keyed by stream pointer).
- **Cleanup sequence**: lock registry → delete handle → remove from map → unlock → free C stream.
- Leaking a handle leaks the Go `HeadersProvider` object and any resources it holds.

### Arrow batch cleanup

- `zerobus_arrow_free_batch_array()` must be called after reading unacknowledged batches.
- Same handle registry pattern applies for Arrow streams with custom headers.

## Breaking change rules

Public API is everything exported (capitalized) in the `go/` package:

- Removing or renaming exported types, functions, methods, or struct fields is breaking.
- Changing function signatures is breaking.
- Deprecate with `// Deprecated: Use X instead.` godoc comment.
- Go module versioning follows git tags (`go/v*`). Major version changes require a `/v2` module path suffix.

## Performance notes

- cgo calls have ~100-200ns overhead per invocation. Batch APIs (`IngestProtoRecords`, `IngestJsonRecords`) amortize this.
- Record data (`[]byte` for proto, `string` for JSON) is pinned and passed by pointer — no extra copy beyond what cgo requires.
- The static FFI library is linked at build time. No runtime loading overhead.

## Thread safety

Go SDK is safe for concurrent use from multiple goroutines. Internal synchronization handles concurrent `Ingest` calls. The handle registry uses a mutex.

## Changelog and documentation

- Every PR must update `go/NEXT_CHANGELOG.md` under the appropriate section if it changes user-facing behavior.
- Update `go/README.md` if the change affects usage, setup, or API surface.
- Add or update examples in `go/examples/` for new or modified APIs.
- Add godoc comments for all new exported types, functions, and methods.

## Release

- Version source: git tag only (no version file in Go).
- Tag: `go/v<semver>` → triggers `release-go.yml` → creates git tag. Go modules are resolved via git, no artifact upload needed.
- The Go SDK links pre-built static FFI libraries from `go/lib/`. An FFI release (`ffi/v*`) must happen first if Rust FFI code changed, and the updated `.a` files must be committed to `go/lib/` before the Go release.
- On version bump PR: move `NEXT_CHANGELOG.md` contents to `CHANGELOG.md`, reset `NEXT_CHANGELOG.md`.

## Config

- Go >= 1.21 required
- CGO_ENABLED=1 required
- Rust toolchain required for building FFI from source
