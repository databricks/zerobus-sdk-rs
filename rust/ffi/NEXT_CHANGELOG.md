# NEXT CHANGELOG

## Release v1.6.0

### Major Changes

### New Features and Improvements

- Add callback-based async overloads for all previously blocking stream operations: stream creation (`zerobus_sdk_create_stream_async`, `zerobus_sdk_create_stream_with_headers_provider_async`), stream recreation (`zerobus_sdk_recreate_stream_async`), offset-returning ingest calls (`zerobus_stream_ingest_proto_record_async`, `zerobus_stream_ingest_json_record_async`, `zerobus_stream_ingest_proto_records_async`, `zerobus_stream_ingest_json_records_async`), completion methods (`zerobus_stream_wait_for_offset_async`, `zerobus_stream_flush_async`, `zerobus_stream_close_async`), and unacked-record retrieval (`zerobus_stream_get_unacked_records_async`). These APIs return immediately after validation/scheduling and complete via callbacks; caller-owned string/descriptor/config inputs are copied before return, and SDK/stream handles must remain valid until callback completion.

### Bug Fixes

### Documentation

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes

- Add `CreateStreamAsyncCallback`, `OffsetAsyncCallback`, `BoolAsyncCallback`, and `RecordArrayAsyncCallback` plus the full async stream API set (`*_async` overloads for create/recreate/ingest/wait/flush/get_unacked_records/close) to `zerobus.h`. Callback `const CResult *` values are valid only for the duration of each callback; any error text must be copied during the call.
