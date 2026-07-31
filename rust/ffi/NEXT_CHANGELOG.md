# NEXT CHANGELOG

## Release v1.6.0

### Major Changes

### New Features and Improvements

- Add callback-based async overloads for all previously blocking stream operations: stream creation (`zerobus_sdk_create_stream_async`, `zerobus_sdk_create_stream_with_headers_provider_async`), stream recreation (`zerobus_sdk_recreate_stream_async`), offset-returning ingest calls (`zerobus_stream_ingest_proto_record_async`, `zerobus_stream_ingest_json_record_async`, `zerobus_stream_ingest_proto_records_async`, `zerobus_stream_ingest_json_records_async`), completion methods (`zerobus_stream_wait_for_offset_async`, `zerobus_stream_flush_async`, `zerobus_stream_close_async`), and unacked-record retrieval (`zerobus_stream_get_unacked_records_async`). These APIs return immediately after validation/scheduling and complete via callbacks; caller-owned string/descriptor/config inputs are copied before return, and SDK/stream handles must remain valid until callback completion.

### Bug Fixes

- Fixed a use-after-free in which a custom headers provider could be freed by the
  wrapper while a Rust worker thread was still inside a synchronous
  `get_headers()` callback into it during connection recovery. The FFI now takes
  ownership of the provider `user_data`: `CallbackHeadersProvider` invokes a
  caller-supplied `free_user_data` destroy callback from its `Drop`, which runs
  only after every task that could call `get_headers()` is gone (the supervisor
  task holds its own `Arc` across the in-flight call). The provider is
  constructed before any fallible work in `create_stream_with_headers_provider`,
  `create_arrow_stream_with_headers_provider`, and
  `create_stream_with_headers_provider_async`, so `free_user_data` is invoked
  exactly once on every path — on success after the last reference drops, on a
  failed create before returning (synchronously for the async variant's
  scheduling failures, in the spawned task for its asynchronous failures).

### Documentation

### Internal Changes

- Add headers-provider ownership tests: `free_user_data` fires once on `Drop` and once on failed create, a null destroy callback is a no-op, and a teardown test that reproduces the recovery-vs-teardown race (a blocking in-flight `get_headers` on one `Arc` clone while another is dropped) asserts the free is deferred until the callback returns. Test-only; no ABI or behavior change.
- Fix the darwin static-library build in `release-ffi.yml`: invoke `cargo-zigbuild rustc` (the binary directly) instead of `cargo zigbuild rustc`, which routed `rustc` as a positional into the zigbuild subcommand and failed with `unexpected argument 'rustc'`. Release tooling only; no ABI or behavior change.
- Add ack-callback live-teardown / use-after-free tests. They drive the real `CallbackAckCallback` bridge over a heap-allocated `user_data` through the real callback-handler task, then tear it down via the production teardown code, asserting no callback fires after teardown returns and that `user_data` is safe to release at that point. All teardown paths are covered: drain-within-`callback_max_wait_time_ms`, wait-indefinitely, and a callback still synchronously in-flight when a bounded budget expires — which the drain aborts but cannot preempt, so the callback outlives `teardown()` and `user_data` must stay alive until it finishes. Test-only; no ABI or behavior change.
- Add tests asserting the ack-callback `Arc` is released, not leaked to a background task, when `zerobus_sdk_create_stream` / `zerobus_sdk_create_stream_with_headers_provider` fails. Uses a `#[cfg(test)]` drop hook keyed to a test-only sentinel `user_data`. Test-only; no ABI or behavior change.
- Move the header-callback helpers `zerobus_alloc_header_array`, `zerobus_alloc_cstring`, and `zerobus_free_headers` from `arrow.rs` into the shared `common.rs` module, next to the `CHeader`/`CHeaders` types they serve. No behavior, signature, or ABI change; their declarations move ahead of the Arrow functions in the generated `zerobus.h`.

### Behavior Changes

- Server-initiated Arrow Flight rotation now waits only for records submitted on the active connection, half-closes its request, and drains late responses before reconnecting. `stream_paused_max_wait_time_ms = 0` skips the ACK wait but still permits bounded transport cleanup.

### Breaking Changes

- `zerobus_sdk_create_stream_with_headers_provider`,
  `zerobus_sdk_create_arrow_stream_with_headers_provider`, and
  `zerobus_sdk_create_stream_with_headers_provider_async` take a new
  `free_user_data` parameter (a nullable `void (*)(void *user_data)`) after
  `user_data`. Callers must hand ownership of `user_data` across and supply a
  destroy callback (or pass null to opt out and manage the lifetime themselves).
  This changes the generated `zerobus.h` signatures, so Go, .NET, and any other
  C FFI consumer must update their call sites.

### Deprecations

### API Changes

- Add `CreateStreamAsyncCallback`, `OffsetAsyncCallback`, `BoolAsyncCallback`, and `RecordArrayAsyncCallback` plus the full async stream API set (`*_async` overloads for create/recreate/ingest/wait/flush/get_unacked_records/close) to `zerobus.h`. Callback `const CResult *` values are valid only for the duration of each callback; any error text must be copied during the call.
