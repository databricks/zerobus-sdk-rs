# C++ SDK

C++ wrapper around the Rust core via the C FFI library (`rust/ffi/zerobus.h`).
Like Go, it consumes the C FFI directly; unlike Go there is no GC, so resource
management is RAII and errors are reported by throwing `zerobus::ZerobusException`.

## Structure

```
cpp/
├── include/zerobus/      # Public headers (the API surface)
│   ├── zerobus.hpp       # Umbrella header — include this
│   ├── sdk.hpp           # Sdk + SdkBuilder + TableProperties
│   ├── stream.hpp        # Stream (proto/JSON ingestion)
│   ├── arrow_stream.hpp  # ArrowStream (Arrow Flight, Beta)
│   ├── proto_schema.hpp  # ProtoSchema (UC JSON → descriptor + encoder)
│   ├── headers_provider.hpp  # HeadersProvider interface
│   ├── ack_callback.hpp  # AckCallback interface + AckCallback::from adapter
│   ├── config.hpp        # StreamOptions / ArrowStreamOptions / CallbackWaitPolicy / enums
│   ├── record.hpp        # UnackedRecord
│   ├── error.hpp         # ZerobusException
│   └── version.hpp       # ZEROBUS_CPP_VERSION
├── src/                  # Implementation (the only place that includes zerobus.h)
│   ├── sdk.cpp / stream.cpp / arrow_stream.cpp / proto_schema.cpp
│   ├── ack_callback.cpp  # AckCallback::from lambda adapter
│   ├── headers_callback.cpp  # extern "C" trampoline for HeadersProvider
│   └── detail/           # Internal: ffi_util, config_convert, headers_callback, ack_callback
├── tests/                # Dependency-free unit tests (plain executables)
├── examples/             # Runnable usage examples (JSON, proto, Arrow)
├── cmake/
│   ├── BuildRustFfi.cmake       # Builds libzerobus_ffi from local Rust source
│   └── zerobus-config.cmake.in  # find_package(zerobus) package-config template
├── CMakeLists.txt
└── Makefile              # Convenience wrappers (build/test/lint/fmt)
```

The public headers never include `zerobus.h`; they hold opaque, forward-declared
FFI handles. Only `src/` includes the generated C header, keeping the C FFI an
implementation detail of the SDK.

## Build commands

Run from `cpp/`:

- `make build` — Configure (CMake) and build the SDK, tests, and the examples
  under `examples/` (gated on `ZEROBUS_BUILD_EXAMPLES`, on by default)
- `make build-ffi` — Build only the Rust C FFI static library
- `make test` — Build and run the test suite (`ctest`)
- `make lint` — Formatting check + compiler warnings (`-Wall -Wextra`)
- `make fmt` — Format all sources with clang-format (Google style)
- `make clean` — Remove the build directory
- `make test SANITIZE=address` — Build + run the suite under a sanitizer
  (`address`, `thread`, or `undefined`; CMake option `-DZEROBUS_SANITIZE=`). Off
  by default. Targets the memory/lifetime bugs this FFI wrapper is prone to
  (use-after-free, double-free). `address`/`thread` also instrument the Rust FFI
  (nightly `-Zsanitizer` + `-Zbuild-std`) — a sanitizer only observes code
  compiled with it, so leaving the core uninstrumented would miss the races/UAF
  that live inside the FFI. This needs a nightly toolchain with
  `rust-src` and build the C++ side with clang to match the LLVM sanitizer
  runtime (`CLANG_CC`/`CLANG_CXX`, default `clang`/`clang++`); GCC can't resolve
  the instrumented archive's runtime symbols. `undefined` uses the default
  compiler and a plain FFI build. Sanitizer builds use their own build dir
  (`build-<sanitizer>`), so they don't clash with a normal `build/` (CMake
  caches the compiler per build dir).

CMake builds the FFI library from local Rust source by default
(`cargo build --release` in `rust/ffi`). To link a prebuilt/vendored library
instead, configure with `-DZEROBUS_FFI_LIBRARY=<path-to-.a>` and
`-DZEROBUS_FFI_HEADER_DIR=<dir>`.

## FFI boundary: RAII + static linking

### Memory ownership

- **Opaque handles**: each wrapper class holds a raw pointer to a Rust-allocated
  handle (`CZerobusSdk`, `CZerobusStream`, `CArrowStream`, `CZerobusProtoSchema`).
  Rust owns the memory.
- **RAII cleanup**: destructors call the matching free/close function
  (`zerobus_sdk_free`, `zerobus_stream_close` + `zerobus_stream_free`,
  `zerobus_arrow_stream_close` + `zerobus_arrow_stream_free`, etc.). Wrapper
  objects are **move-only** (no copies) — moving nulls the source handle so it
  is freed exactly once.
- **Error strings**: FFI calls route their `CResult` through a helper in
  `detail/ffi_util.hpp`, which converts failure into a `ZerobusException` and
  **always** frees `CResult.error_message` via `zerobus_free_error_message`.
- **Returned arrays**: `Stream::get_unacked_records` /
  `ArrowStream::get_unacked_batches` copy the payloads out, then free the FFI
  array (`zerobus_free_record_array` / `zerobus_arrow_free_batch_array`).
  `ProtoSchema::encode_json` frees its buffer with `zerobus_free_proto_bytes`;
  `descriptor_bytes()` copies the borrowed bytes.

### Headers provider callback

When using a custom `HeadersProvider`:
- `detail::zerobus_cpp_headers_trampoline` is an `extern "C"` function passed to
  `zerobus_sdk_create_stream_with_headers_provider`. `user_data` is a
  heap-allocated `std::shared_ptr<HeadersProvider>*` whose **ownership is handed
  to the FFI**, alongside `detail::zerobus_cpp_headers_free` as the FFI's
  provider `free_user_data` destroy callback.
- The FFI invokes `zerobus_cpp_headers_free` exactly once, on every path (on
  success after the core's last reference drops — i.e. after any in-flight
  `get_headers()` has returned; on a failed create before returning). This is
  what closes the recovery-vs-teardown use-after-free: a slow `get_headers()`
  racing stream teardown can no longer run on a freed provider. Because the FFI
  owns the provider, the `Stream` / `ArrowStream` no longer keeps a
  `shared_ptr<HeadersProvider>` member, and the wrapper must **not** free
  `user_data` itself (doing so would double-free).
- The trampoline marshals the returned map into `CHeaders` using the allocators
  the Rust core's `zerobus_free_headers` expects to free. Do not change the
  allocator without checking `zerobus_free_headers` in the FFI crate.
- Exceptions thrown from `get_headers()` are caught and surfaced as a
  headers-provider error; they never cross the FFI boundary as C++ exceptions.

### Ack callback

When a `StreamOptions::ack_callback` is set:
- `detail::zerobus_cpp_ack_on_ack_trampoline` /
  `zerobus_cpp_ack_on_error_trampoline` are the `extern "C"` functions wired into
  the C config (`ack_on_ack` / `ack_on_error`); `user_data` is the raw
  `AckCallback*`. `to_c()` installs them only when the callback is non-null and
  zeroes all three fields otherwise (see `detail/config_convert.hpp`).
- The `Stream` keeps a `std::shared_ptr<AckCallback>` alive for its whole
  lifetime, but that is **not always sufficient**: a callback still running when
  `close()` hits its wait budget (`StreamOptions::callback_wait_policy`) can be
  invoked on a freed object once the `Stream` is destroyed. `CallbackWaitPolicy::
  forever()` is the only policy that guarantees no callback runs after `close()`
  returns. This is the canonical FFI-side view of the contract in
  `include/zerobus/ack_callback.hpp` — keep the two in sync.
- `AckCallback::on_ack` / `on_error` are `noexcept`. The trampolines must not let
  anything unwind across the boundary; an escaping exception calls
  `std::terminate` at the throw site.

## Configuration mapping

`detail::to_c` (overloaded for `StreamOptions` and `ArrowStreamOptions`) seeds
the C config struct from the live FFI defaults (`zerobus_get_default_config` /
`zerobus_arrow_get_default_config`) and then overrides each known field. The
`Stream` variant expresses optional fields (e.g. `stream_paused_max_wait_time_ms`)
with the C struct's `has_*` presence flags; the Arrow variant uses a `-1`
sentinel for "wait the full server-specified pause duration". `config_defaults_test`
and `arrow_config_defaults_test` fail the build if the hand-kept scalar defaults
in `StreamOptions` / `ArrowStreamOptions` drift from the FFI defaults.

`StreamOptions::callback_wait_policy` (a `CallbackWaitPolicy`) also maps to a
`has_*`/value pair: `forever()` clears `has_callback_max_wait_time_ms` so Rust
reads `None` (drain indefinitely), `duration(ms)` sets an explicit finite budget,
and `use_default()` leaves the FFI seed in place. The ack-callback trampolines
and `ack_user_data` are set from `StreamOptions::ack_callback` (see [Ack
callback](#ack-callback)).

## Thread safety

- A `Stream` / `ArrowStream` is **not** safe for concurrent use — serialize
  access externally (same contract as the Rust core).
- A `HeadersProvider::get_headers()` may be invoked from an internal worker
  thread; the core guards against overlapping calls but implementations should
  be thread-safe with respect to their own state.
- A single `ProtoSchema` may be used by concurrent readers
  (`descriptor_bytes`, `encode_json`); destruction must not race those calls.
  `tests/concurrency_test.cpp` exercises this under ThreadSanitizer (which
  instruments the Rust core too), so a race in the read path is caught in CI.
  The destruction-vs-read contract, though, is not tested — it is the caller's
  responsibility, not something the TSan job enforces.

## Breaking change rules

Public API is everything under `include/zerobus/`:

- Removing/renaming public types, methods, or struct fields, or changing a method
  signature, is breaking.
- Adding new methods, new overloads, or new struct fields with defaults is safe.
- Deprecate with `[[deprecated("Use X instead")]]` and keep for at least one
  minor release before removal in the next major (semver, tags `cpp/v*`).

## Performance notes

- Every FFI crossing serializes data; prefer the batch APIs
  (`ingest_proto_records`, `ingest_json_records`) over per-record calls in hot
  paths.
- Neither batch API copies payloads: `detail/proto_batch.hpp` builds the FFI's
  pointer/length arrays from pointers into the caller's buffers, and
  `tests/proto_batch_test.cpp` asserts that by pointer identity (a copying
  regression would still behave correctly). `ingest_proto_records` also takes
  `const ProtoRecordView*` + count, for records held outside a
  `vector<vector<uint8_t>>`. JSON has no such overload: its FFI needs
  NUL-terminated `const char*`, so `std::string` is already the zero-copy shape.
- Ingestion is asynchronous: `ingest_*` queues and returns. Never wait per
  record (`wait_for_offset`/`flush` in the loop); flush once at the end or flush
  periodically. Examples and doc comments must follow this pattern.
- The static FFI library is linked at build time — no runtime loading overhead.

## Changelog and documentation

- Every PR must update `cpp/NEXT_CHANGELOG.md` under the appropriate section if it
  changes user-facing behavior.
- Update `cpp/README.md` if the change affects usage, setup, or API surface.
- Add or update examples in `cpp/examples/` for new or modified APIs.
- Add doc comments (`///`) for all new public items.

## Release

The distribution model is CMake + GitHub Releases only — no package manager (no
Conan, vcpkg, etc.), mirroring the C FFI. The C++ wrapper is always built from
the consumer's own compiler (its C++ ABI is not stable across compiler/stdlib),
so we do **not** ship a prebuilt `libzerobus_cpp`. What we ship prebuilt is the
Rust C FFI archive (`libzerobus_ffi.a`) — a stable C ABI, and the one piece a
consumer can't build without a Rust toolchain. A consumer then either builds
everything from source (needs Rust) via `add_subdirectory` / `FetchContent` /
`find_package`, or points `-DZEROBUS_FFI_LIBRARY=` at the matching prebuilt FFI
archive from a release and builds only the C++ (no Rust needed).

- Create the `cpp/v<semver>` tag.
- Manually dispatch the `release-zerobus-sdk-cpp.yml` orchestrator (in
  `secure-public-registry-releases-eng`) with the tag as `ref`. It invokes
  `release-cpp.yml` in this repo to build the per-platform bundles (the Rust C
  FFI archive cross-built with `cargo-zigbuild`, plus the platform-neutral C++
  source tree), scans the artifacts, and cuts the GitHub Release with a
  per-platform `tar.gz` attached and notes from `cpp/CHANGELOG.md`.
- Version source: `cpp/include/zerobus/version.hpp` (`ZEROBUS_CPP_VERSION`) and
  the `project(... VERSION ...)` line in `cpp/CMakeLists.txt` — keep them in
  sync (CMake fails configuration if they disagree). The release validates the
  tag version against `ZEROBUS_CPP_VERSION`.
- The C++ SDK links the FFI static library. If Rust FFI code changed, an FFI
  release (`ffi/v*`) must happen first; for source builds the workspace must be
  present.
- On version bump PR: move `NEXT_CHANGELOG.md` contents to `CHANGELOG.md` under a
  `## Release v<semver>` heading (the release notes are extracted by matching
  that heading), reset `NEXT_CHANGELOG.md`.

## Config / requirements

- C++17 compiler (GCC, Clang, or MSVC)
- CMake ≥ 3.16
- Rust toolchain required for building the FFI from source
- Tests are **dependency-free**: each is a plain executable that returns
  non-zero on failure and is registered with CTest (see `tests/CMakeLists.txt`),
  so the build is hermetic — no vendored framework, no package manager, no
  network. Tests reach into the private `src/detail` headers, so each links the
  library target and adds `src/` to its include path. If real third-party C++
  test dependencies are ever needed, that's the point to revisit the tooling —
  not before.
