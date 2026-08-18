# Go SDK

Go wrapper around the Rust core via cgo and the C FFI library.

## Client code patterns (performance)

When writing or reviewing client/example code, follow the idiomatic async flow.
`IngestRecordOffset()` and `IngestRecordsOffset()` (on `ZerobusStream`), as well as
`IngestBatch()` (on `ZerobusArrowStream`), return as soon as the record is queued;
the SDK sends it and tracks its acknowledgment in the background.

- Ingest in a loop, then call `Flush()` to confirm durability — once for a
  bounded batch, or periodically for a long-running stream.
- Acks are ordered, so if you only need to confirm a group of records, call
  `WaitForOffset()` on the LAST offset — it confirms all prior offsets too.
- Use `WaitForOffset()` when a specific record must be confirmed before
  continuing; prefer `Flush()` for bulk durability. Avoid calling
  `WaitForOffset()` after every record in a tight loop, since that limits
  throughput to one record per round-trip.
- There is no ack-callback API in the Go SDK; use `Flush()` / `WaitForOffset()`
  (or the fire-and-forget `IngestRecordNowait`/`IngestRecordsNowait`).

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
│   ├── linux_amd64/libzerobus_ffi.a
│   ├── linux_arm64/libzerobus_ffi.a
│   ├── darwin_amd64/libzerobus_ffi.a
│   ├── darwin_arm64/libzerobus_ffi.a
│   └── windows_amd64/libzerobus_ffi.a
├── tests/           # Integration tests
└── examples/        # Usage examples
```

## Build commands

Run from `go/`:

- `make build` — Build Rust FFI lib + Go SDK
- `make build-rust` — Build only Rust FFI layer
- `make build-go` — Build Rust FFI (via `build-rust`) then the Go SDK
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

### Handle ownership for callbacks

When using custom `HeadersProvider` (instead of default OAuth):
- A `cgo.Handle` wraps the Go interface value and prevents GC collection.
- The handle is passed to the FFI as `user_data`, and its **ownership is handed to
  the FFI** along with a `free_user_data` destroy callback (`goFreeHeadersProvider`).
  The FFI invokes it exactly once on every path — on success after any in-flight
  `get_headers` returns, and on a failed create before returning — and that is where
  `handle.Delete()` runs. The Go side must therefore **never** call `handle.Delete()`
  itself, not even when create fails; doing so would double-delete the `cgo.Handle`
  (panic).
- This replaces the older per-stream handle registry: freeing the handle on `close()`
  could race a recovery `get_headers` still running on a worker thread (use-after-free).
- Leaking a handle leaks the Go `HeadersProvider` object and any resources it holds.

### Arrow batch cleanup

- `zerobus_arrow_free_batch_array()` must be called after reading unacknowledged batches.
- Arrow streams with custom headers use the same FFI-owned handle destroy path.

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

Concurrent `Ingest` calls are internally synchronized. Serialize `Close` with
every other operation on the same stream.

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
