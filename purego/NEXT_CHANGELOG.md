# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

### Bug Fixes

### Documentation

- Flush recovery no longer treats every flush error as terminal. The JSON single
  example retrieves unacknowledged records on flush failure before teardown and
  replays them on a fresh stream. The JSON batch example demonstrates that a
  batch produces a single ack callback event and waits for that callback before
  exit.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
