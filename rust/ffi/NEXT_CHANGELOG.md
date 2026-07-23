# NEXT CHANGELOG

## Release v1.6.0

### Major Changes

### New Features and Improvements

- Add callback-based async overloads for all previously blocking stream operations: stream creation (`zerobus_sdk_create_stream_async`, `zerobus_sdk_create_stream_with_headers_provider_async`), stream recreation (`zerobus_sdk_recreate_stream_async`), offset-returning ingest calls (`zerobus_stream_ingest_proto_record_async`, `zerobus_stream_ingest_json_record_async`, `zerobus_stream_ingest_proto_records_async`, `zerobus_stream_ingest_json_records_async`), completion methods (`zerobus_stream_wait_for_offset_async`, `zerobus_stream_flush_async`, `zerobus_stream_close_async`), and unacked-record retrieval (`zerobus_stream_get_unacked_records_async`). These APIs return immediately after validation/scheduling and complete via callbacks; caller-owned string/descriptor/config inputs are copied before return, and SDK/stream handles must remain valid until callback completion.

### Bug Fixes

### Documentation

### Internal Changes

- Fix the darwin static-library build in `release-ffi.yml`: invoke `cargo-zigbuild rustc` (the binary directly) instead of `cargo zigbuild rustc`, which routed `rustc` as a positional into the zigbuild subcommand and failed with `unexpected argument 'rustc'`. Release tooling only; no ABI or behavior change.
- Add ack-callback live-teardown / use-after-free tests. They drive the real `CallbackAckCallback` bridge over a heap-allocated `user_data` through the real callback-handler task, then tear it down via the production teardown code, asserting no callback fires after teardown returns and that `user_data` is safe to release at that point. All teardown paths are covered: drain-within-`callback_max_wait_time_ms`, wait-indefinitely, and a callback still synchronously in-flight when a bounded budget expires — which the drain aborts but cannot preempt, so the callback outlives `teardown()` and `user_data` must stay alive until it finishes. Test-only; no ABI or behavior change.
- Add tests asserting the ack-callback `Arc` is released, not leaked to a background task, when `zerobus_sdk_create_stream` / `zerobus_sdk_create_stream_with_headers_provider` fails. Uses a `#[cfg(test)]` drop hook keyed to a test-only sentinel `user_data`. Test-only; no ABI or behavior change.
- Move the header-callback helpers `zerobus_alloc_header_array`, `zerobus_alloc_cstring`, and `zerobus_free_headers` from `arrow.rs` into the shared `common.rs` module, next to the `CHeader`/`CHeaders` types they serve. No behavior, signature, or ABI change; their declarations move ahead of the Arrow functions in the generated `zerobus.h`.

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

- Add `CreateStreamAsyncCallback`, `OffsetAsyncCallback`, `BoolAsyncCallback`, and `RecordArrayAsyncCallback` plus the full async stream API set (`*_async` overloads for create/recreate/ingest/wait/flush/get_unacked_records/close) to `zerobus.h`. Callback `const CResult *` values are valid only for the duration of each callback; any error text must be copied during the call.
