# NEXT CHANGELOG

## Release v2.4.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

- Add a `testing`-feature-gated `CallbackHandlerHarness` that drives the real callback-handler task and reproduces `close()`'s teardown, and split the callback drain-then-abort / wait-indefinitely logic out of `shutdown_all_tasks_gracefully` into `ZerobusStream::shutdown_callback_task` so it can be exercised in isolation. Test-only; no change to shipped behavior or the default (non-`testing`) build.

### Breaking Changes

### Deprecations

### API Changes
