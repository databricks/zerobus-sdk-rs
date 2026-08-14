# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Updated dependency snippets to version 1.4.0 and corrected README and example
  code for stream cleanup, recreation, unique local variables, and a single
  durability barrier after queued ingestion. Clarified that acknowledgment
  callbacks fire once per logical ingest submission, including one callback per
  batch ingest call.
- Documented that published JARs support Java 8 while source builds need JDK 11,
  that macOS JNI artifacts are not in the current release set, that
  `recoveryRetries` defaults to 4, and that `flush()` waits for durability rather
  than callback completion.
- Maven Central snippets no longer tell users to redeclare compile-scope
  transitives (`protobuf-java`, `slf4j-api`). Stream-builder examples use
  try-with-resources. GenerateProto docs no longer claim STRUCT support, JAR
  examples use `zerobus-ingest-sdk` 1.4.0, and proto examples generate
  `AirQualityProto.java` with `protoc` instead of treating it as checked in.
  Example README snippets and the JSON/proto `SingleRecordExample` programs
  queue records or batches and call `flush()` once instead of
  `waitForOffset()` after a single ingest.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
