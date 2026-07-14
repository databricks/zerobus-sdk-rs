# NEXT CHANGELOG

## Release v1.3.0

### Major Changes

### New Features and Improvements

- Added native library support for Linux musl (Alpine) on x86_64 and aarch64. The libc flavor is detected automatically at runtime; override with `-Dzerobus.libc=musl|glibc`.

### Bug Fixes

- Preserve unacknowledged Proto, JSON, and Arrow payloads after a close/flush failure so callers can recreate the stream without losing records.
- Propagate stream close failures after native resources and recovery data have been safely finalized instead of silently reporting success.
- Release native stream handles when asynchronous stream creation or recreation futures are cancelled before completion.
- Release native streams created during failed stream recreation attempts.
- Validate stream creation, ingestion, batch, parser/serializer, offset, and configuration arguments before crossing JNI.
- Clear Java exceptions thrown by acknowledgment callbacks and bound JNI local-reference usage for large batches.

### Documentation

- Clarified that Arrow Flight ingestion requires Arrow dependencies even when using the fat JAR.

### Internal Changes

- Removed the unused Arrow JNI table-name lookup and close streams in place until recovery data is copied to Java.

### Breaking Changes

### Deprecations

### API Changes
